/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sandboxupdateops

import (
	"context"
	"reflect"
	"sort"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	agentsv1alpha1 "github.com/openkruise/agents/api/v1alpha1"
	"github.com/openkruise/agents/pkg/utils"
	"github.com/openkruise/agents/pkg/utils/expectations"
)

// opsRevision encodes the (creationTimestamp, name) identity of an ops as
// "<RFC3339 UTC>/<name>". RFC3339 UTC timestamps are fixed-width, so a plain
// string comparison of two revisions yields the (creationTimestamp, name)
// total order the stamp protocol is built on.
func opsRevision(ops *agentsv1alpha1.SandboxUpdateOps) string {
	return ops.CreationTimestamp.UTC().Format(time.RFC3339) + "/" + ops.Name
}

// blockedByModeBarrier is the controller-side defensive re-check of the
// admission matrix: an ops may proceed only when every other active ops in
// the namespace is template-mode alongside a template-mode self. Any
// combination involving patch mode is a conflict, resolved deterministically
// by the same (creationTimestamp, name) total order the stamps use: this ops
// backs off when a conflicting *older* active ops exists, so both sides of a
// race compute the same winner and exactly one proceeds.
func (r *Reconciler) blockedByModeBarrier(ctx context.Context, ops *agentsv1alpha1.SandboxUpdateOps) (bool, error) {
	opsList := &agentsv1alpha1.SandboxUpdateOpsList{}
	if err := r.List(ctx, opsList, client.InNamespace(ops.Namespace), client.UnsafeDisableDeepCopy); err != nil {
		return false, err
	}
	myRev := opsRevision(ops)
	for i := range opsList.Items {
		other := &opsList.Items[i]
		if other.Name == ops.Name {
			continue
		}
		// Phase-based activity check, same as the webhook: a deleting ops
		// still holding its finalizer stays blocking until it is gone.
		if other.Status.Phase == agentsv1alpha1.SandboxUpdateOpsCompleted ||
			other.Status.Phase == agentsv1alpha1.SandboxUpdateOpsFailed {
			continue
		}
		if ops.Spec.IsTemplateMode() && other.Spec.IsTemplateMode() {
			continue
		}
		if opsRevision(other) >= myRev {
			// The conflicting ops is newer; it is the one that backs off.
			continue
		}
		klog.InfoS("SandboxUpdateOps blocked by mixed-mode barrier",
			"ops", klog.KObj(ops), "blockingOps", klog.KObj(other))
		r.Recorder.Eventf(ops, v1.EventTypeWarning, eventReasonBlocked,
			"Blocked by active SandboxUpdateOps %s: patch-mode ops cannot run concurrently with other ops", other.Name)
		// Stay Pending (not terminal) so the webhook keeps rejecting further
		// conflicting creations while this ops waits its turn.
		if ops.Status.Phase == "" {
			newStatus := ops.Status.DeepCopy()
			newStatus.Phase = agentsv1alpha1.SandboxUpdateOpsPending
			if err := r.updateStatus(ctx, ops, newStatus); err != nil {
				return true, err
			}
		}
		return true, nil
	}
	return false, nil
}

// reconcileTemplateMode drives a template-mode ops with the stamp-based
// last-writer-wins protocol. Per sandbox, the update rules read two markers:
// the pending record (a round is in flight — wait for its terminal state) and
// the stamp (the newest landed revision — the sole comparison input):
//
//	round in flight                     → wait for its terminal state
//	stamp equal to this ops             → settled; count as updated
//	stamp newer than this ops           → skip; count as superseded
//	no stamp / older stamp:
//	  template already equals target    → stamp directly (no-op fast path)
//	  otherwise                         → start a round
//
// Round terminal handling (promotion, failure release) is owned by the
// sandbox controller, so this loop only observes outcomes.
func (r *Reconciler) reconcileTemplateMode(ctx context.Context, ops *agentsv1alpha1.SandboxUpdateOps, sandboxList *agentsv1alpha1.SandboxList) (ctrl.Result, error) {
	myRev := opsRevision(ops)

	var updated, failed, updating, waiting, superseded int32
	var candidates, fastPathTargets, resumePhase2 []*agentsv1alpha1.Sandbox

	for i := range sandboxList.Items {
		sbx := &sandboxList.Items[i]
		if !sbx.DeletionTimestamp.IsZero() {
			continue
		}
		// Terminal phases have completed their lifecycle and cannot be upgraded.
		if sbx.Status.Phase == agentsv1alpha1.SandboxSucceeded ||
			sbx.Status.Phase == agentsv1alpha1.SandboxFailed {
			continue
		}
		// The state filter only gates new candidates; a sandbox already
		// following this ops stays tracked regardless of its phase.
		if sbx.Labels[agentsv1alpha1.LabelSandboxUpdateOps] != ops.Name &&
			!isStateIncluded(ops, sbx.Status.Phase) {
			continue
		}
		if utils.IsControlledBySandboxSet(sbx) {
			continue
		}

		// Skip stale cache reads: decisions must be made on a sandbox state at
		// least as new as our own last write.
		ResourceVersionExpectations.Observe(sbx)
		if isSatisfied, unsatisfiedDuration := ResourceVersionExpectations.IsSatisfied(sbx); !isSatisfied {
			if unsatisfiedDuration < expectations.ExpectationTimeout {
				klog.InfoS("Not satisfied resourceVersion for Sandbox in SandboxUpdateOps, wait for cache event",
					"sandbox", klog.KObj(sbx), "ops", klog.KObj(ops))
				return ctrl.Result{RequeueAfter: expectations.ExpectationTimeout - unsatisfiedDuration}, nil
			}
			klog.InfoS("ResourceVersionExpectations unsatisfied overtime for Sandbox in SandboxUpdateOps, wait for cache event timeout",
				"timeout", unsatisfiedDuration, "sandbox", klog.KObj(sbx), "ops", klog.KObj(ops))
			ResourceVersionExpectations.Delete(sbx)
		}

		category := classifyTemplateSandbox(sbx, ops, myRev)
		klog.InfoS("Classified sandbox (template mode)", "sandbox", klog.KObj(sbx), "category", category, "ops", klog.KObj(ops))

		switch category {
		case sandboxUpdated:
			r.syncSandboxUpgradeState(ctx, sbx, ops, sandboxUpdated)
			updated++
		case sandboxFailed:
			r.syncSandboxUpgradeState(ctx, sbx, ops, sandboxFailed)
			failed++
		case sandboxUpdating:
			r.syncSandboxUpgradeState(ctx, sbx, ops, sandboxUpdating)
			updating++
		case sandboxWaiting:
			waiting++
		case sandboxSuperseded:
			superseded++
		case sandboxNoNeedUpdate:
			continue
		case sandboxFastPath:
			fastPathTargets = append(fastPathTargets, sbx)
		case sandboxCandidate:
			candidates = append(candidates, sbx)
		case sandboxResumeSucceed:
			r.syncSandboxUpgradeState(ctx, sbx, ops, sandboxResumeSucceed)
			updating++
			resumePhase2 = append(resumePhase2, sbx)
		}
	}

	// No-op fast path: the sandbox already carries the target template and no
	// round is in flight, so starting a round would never bump the template
	// revision and the pending record would become a permanent occupancy
	// signal. Advance the stamp directly instead; optimistic locking
	// re-verifies the preconditions against the read revision.
	var opErr error
	var fastPathFailed int32
	for _, sbx := range fastPathTargets {
		if err := r.applyFastPathStamp(ctx, sbx, myRev); err != nil {
			klog.ErrorS(err, "Failed to fast-path stamp sandbox",
				"sandbox", klog.KObj(sbx), "ops", klog.KObj(ops))
			// Count towards total but towards no settled category, so the
			// terminal equality cannot hold: the ops stays non-terminal and
			// the error-driven retry re-attempts the stamp.
			fastPathFailed++
			if opErr == nil {
				opErr = err
			}
			continue
		}
		updated++
	}

	total := updated + failed + updating + waiting + superseded + fastPathFailed + int32(len(candidates)) // #nosec G115 -- K8s object count
	newStatus := ops.Status.DeepCopy()
	newStatus.ObservedGeneration = ops.Generation
	newStatus.Replicas = total
	newStatus.UpdatedReplicas = updated
	newStatus.FailedReplicas = failed
	newStatus.UpdatingReplicas = updating
	newStatus.WaitingReplicas = waiting
	newStatus.SupersededReplicas = superseded

	switch {
	case ops.Status.Phase == "" || ops.Status.Phase == agentsv1alpha1.SandboxUpdateOpsPending:
		newStatus.Phase = agentsv1alpha1.SandboxUpdateOpsUpdating

	// Terminal accounting is stamp-based: every tracked sandbox carries this
	// ops' stamp (updated), a newer stamp (superseded), or terminally failed
	// this ops' round. total includes updating and waiting, so the equality
	// implies both are zero.
	case updated+failed+superseded == total && len(candidates) == 0:
		if failed > 0 {
			newStatus.Phase = agentsv1alpha1.SandboxUpdateOpsFailed
		} else {
			newStatus.Phase = agentsv1alpha1.SandboxUpdateOpsCompleted
		}
	}

	// Phase 2 of the paused two-phase flow: the sandbox resumed with the old
	// template; write the target template and drop the resume trigger. No
	// concurrency limit — these sandboxes already hold the window slot their
	// phase-1 round claimed. spec.paused only brakes starting new rounds; a
	// two-phase flow already in flight must always be finished, otherwise
	// the sandbox would stay resumed with the old template indefinitely.
	if newStatus.Phase == agentsv1alpha1.SandboxUpdateOpsUpdating {
		for _, sbx := range resumePhase2 {
			klog.InfoS("Applying template (phase 2)", "sandbox", klog.KObj(sbx), "ops", klog.KObj(ops))
			if err := r.applyTemplatePhase2(ctx, sbx, ops); err != nil {
				klog.ErrorS(err, "Failed to apply template (phase 2)",
					"sandbox", klog.KObj(sbx), "ops", klog.KObj(ops))
				r.Recorder.Eventf(ops, v1.EventTypeWarning, "PatchFailed",
					"Failed to patch sandbox %s: %v", sbx.Name, err)
				if opErr == nil {
					opErr = err
				}
			} else {
				r.Recorder.Eventf(ops, v1.EventTypeNormal, "SandboxUpgrading",
					"Patching sandbox %s after resuming", sbx.Name)
			}
		}
	}

	// Start new rounds within the maxUnavailable window. This ops' own
	// failures consume the window (circuit breaker).
	if newStatus.Phase == agentsv1alpha1.SandboxUpdateOpsUpdating && !ops.Spec.Paused && len(candidates) > 0 {
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].Name < candidates[j].Name
		})
		maxConcurrent := calculateMaxUnavailable(ops.Spec.UpdateStrategy.MaxUnavailable, total)
		toUpgrade := int(maxConcurrent) - int(updating) - int(failed)
		if toUpgrade > len(candidates) {
			toUpgrade = len(candidates)
		}
		for i := 0; i < toUpgrade; i++ {
			klog.InfoS("Starting update round on sandbox", "sandbox", klog.KObj(candidates[i]), "ops", klog.KObj(ops))
			if err := r.startTemplateRound(ctx, candidates[i], ops, myRev); err != nil {
				klog.ErrorS(err, "Failed to start update round on sandbox",
					"sandbox", klog.KObj(candidates[i]), "ops", klog.KObj(ops))
				r.Recorder.Eventf(ops, v1.EventTypeWarning, "PatchFailed",
					"Failed to patch sandbox %s: %v", candidates[i].Name, err)
				if opErr == nil {
					opErr = err
				}
			} else {
				r.Recorder.Eventf(ops, v1.EventTypeNormal, "SandboxUpgrading",
					"Upgrading sandbox %s", candidates[i].Name)
			}
		}
	}

	if err := r.updateStatus(ctx, ops, newStatus); err != nil {
		return ctrl.Result{}, err
	}
	if opErr != nil {
		return ctrl.Result{}, opErr
	}
	// Waiting sandboxes are occupied by another ops' round; that sandbox's
	// events route to the ops named by its label, never to this one, so poll
	// to re-read stamps and occupancy.
	if waiting > 0 {
		return ctrl.Result{RequeueAfter: templateModePollInterval}, nil
	}
	return ctrl.Result{}, nil
}

// classifyTemplateSandbox applies the stamp-protocol update rules to one
// sandbox. The pending record only signals occupancy; the stamp is the sole
// comparison input.
func classifyTemplateSandbox(sbx *agentsv1alpha1.Sandbox, ops *agentsv1alpha1.SandboxUpdateOps, myRev string) sandboxUpdateState {
	pending := sbx.Annotations[agentsv1alpha1.AnnotationUpdateOpsPendingRevision]
	if pending != "" && pending != myRev {
		// A round is in flight for another ops. Never re-target it — wait for
		// its terminal state and re-read the stamp.
		return sandboxWaiting
	}
	if pending == myRev {
		cond := findCondition(sbx.Status.Conditions, string(agentsv1alpha1.SandboxConditionUpgrading))
		if cond != nil && cond.Reason == agentsv1alpha1.SandboxUpgradingReasonResumeSucceed {
			return sandboxResumeSucceed
		}
		// Phase 1 set only the resume trigger and the sandbox was un-paused
		// before ResumeSucceed; the template write is still pending.
		if cond == nil && sbx.Status.Phase == agentsv1alpha1.SandboxRunning && !templateMatchesOps(sbx, ops) {
			return sandboxResumeSucceed
		}
		// This ops' round is in flight; the sandbox-side terminal handling
		// settles it (promotion on success, release on failure).
		return sandboxUpdating
	}

	// No round in flight: the stamp rules decide.
	stamp := sbx.Annotations[agentsv1alpha1.AnnotationUpdateOpsRevision]
	switch {
	case stamp == myRev:
		// Settled. The stamp records delivery; later template drift is not
		// chased.
		return sandboxUpdated
	case stamp > myRev:
		// Delivery evidence of a newer intent; settle immediately, no
		// dependency on the newer ops' lifecycle.
		return sandboxSuperseded
	}

	// A cleared pending record with no stamp and this ops' label means this
	// ops' round terminally failed.
	if sbx.Labels[agentsv1alpha1.LabelSandboxUpdateOps] == ops.Name && isUpgradeTerminallyFailed(sbx) {
		return sandboxFailed
	}

	// New-candidate gating, identical to patch mode.
	if !isStateIncluded(ops, sbx.Status.Phase) {
		return sandboxNoNeedUpdate
	}
	if sbx.Status.Phase == agentsv1alpha1.SandboxPaused && !isPausedStable(sbx) {
		return sandboxNoNeedUpdate
	}

	// The fast path must own every already-matching sandbox: starting a round
	// on one would never change the template revision and would orphan the
	// pending record. A mid-upgrade or not-yet-observed sandbox is not steady
	// enough to stamp — count it as waiting so it stays in total and is
	// polled; once steady it takes the fast path. NoNeedUpdate would drop it
	// from total and let this ops reach a terminal phase without settling it.
	if templateMatchesOps(sbx, ops) {
		if sbx.Status.Phase != agentsv1alpha1.SandboxUpgrading && sbx.Generation == sbx.Status.ObservedGeneration {
			return sandboxFastPath
		}
		return sandboxWaiting
	}
	return sandboxCandidate
}

// templateMatchesOps reports whether the sandbox already carries exactly the
// ops' target template snapshot, on the same carrier field.
func templateMatchesOps(sbx *agentsv1alpha1.Sandbox, ops *agentsv1alpha1.SandboxUpdateOps) bool {
	if ops.Spec.Template != nil {
		return sbx.Spec.TemplateRef == nil && reflect.DeepEqual(sbx.Spec.Template, ops.Spec.Template)
	}
	return sbx.Spec.Template == nil && reflect.DeepEqual(sbx.Spec.TemplateRef, ops.Spec.TemplateRef)
}

// isPausedStable reports whether a Paused sandbox has fully settled into the
// paused state (Paused condition True and spec.Paused still true). Only a
// fully paused sandbox can enter the two-phase upgrade; transient states are
// picked up by a later reconcile.
func isPausedStable(sbx *agentsv1alpha1.Sandbox) bool {
	pausedCond := findCondition(sbx.Status.Conditions, string(agentsv1alpha1.SandboxConditionPaused))
	return pausedCond != nil && pausedCond.Status == metav1.ConditionTrue && sbx.Spec.Paused
}

// isUpgradeTerminallyFailed reports whether the sandbox's Upgrading condition
// carries a terminal failure reason.
func isUpgradeTerminallyFailed(sbx *agentsv1alpha1.Sandbox) bool {
	cond := findCondition(sbx.Status.Conditions, string(agentsv1alpha1.SandboxConditionUpgrading))
	return cond != nil && cond.Status == metav1.ConditionFalse &&
		(cond.Reason == agentsv1alpha1.SandboxUpgradingReasonPreUpgradeFailed ||
			cond.Reason == agentsv1alpha1.SandboxUpgradingReasonPostUpgradeFailed ||
			cond.Reason == agentsv1alpha1.SandboxUpgradingReasonUpgradePodFailed ||
			cond.Reason == agentsv1alpha1.SandboxUpgradingReasonCheckpointFailed)
}

// setTemplateFromOps overwrites the sandbox's template snapshot with the ops'
// target, on the ops' carrier field. Template mode is a full overwrite:
// omitted fields are removals, not "keep as is". The sandbox's own
// volumeClaimTemplates are never touched.
func setTemplateFromOps(modified *agentsv1alpha1.Sandbox, ops *agentsv1alpha1.SandboxUpdateOps) {
	if ops.Spec.Template != nil {
		modified.Spec.Template = ops.Spec.Template.DeepCopy()
		modified.Spec.TemplateRef = nil
		return
	}
	modified.Spec.TemplateRef = ops.Spec.TemplateRef.DeepCopy()
	modified.Spec.Template = nil
}

// startTemplateRound starts a round on a sandbox: one atomic patch carrying
// the target template, the pending record, the ops label, and the upgrade
// policy/lifecycle fields, so no observer sees a record/template mismatch.
// For a paused sandbox the pending record lands with the phase-1 patch and
// the template follows in phase 2, so the occupancy signal covers the whole
// two-phase span.
func (r *Reconciler) startTemplateRound(ctx context.Context, sbx *agentsv1alpha1.Sandbox, ops *agentsv1alpha1.SandboxUpdateOps, myRev string) error {
	modified := sbx.DeepCopy()

	policyType := agentsv1alpha1.SandboxUpgradePolicyRecreate
	if ops.Spec.UpdateStrategy.Type == agentsv1alpha1.SandboxUpdateOpsStrategyCheckpointRestore {
		policyType = agentsv1alpha1.SandboxUpgradePolicyCheckpointRestore
	}
	modified.Spec.UpgradePolicy = &agentsv1alpha1.SandboxUpgradePolicy{Type: policyType}

	if ops.Spec.Lifecycle != nil {
		modified.Spec.Lifecycle = ops.Spec.Lifecycle.DeepCopy()
	} else {
		modified.Spec.Lifecycle = nil
	}

	if modified.Labels == nil {
		modified.Labels = map[string]string{}
	}
	modified.Labels[agentsv1alpha1.LabelSandboxUpdateOps] = ops.Name
	delete(modified.Labels, agentsv1alpha1.LabelSandboxUpgradeFailed)

	if modified.Annotations == nil {
		modified.Annotations = map[string]string{}
	}
	modified.Annotations[agentsv1alpha1.AnnotationUpdateOpsPendingRevision] = myRev

	if sbx.Status.Phase == agentsv1alpha1.SandboxPaused {
		modified.Annotations[agentsv1alpha1.AnnotationUpgradeResumeTrigger] = agentsv1alpha1.True
		return r.patchAndExpect(ctx, sbx, modified, " (resume trigger)")
	}

	setTemplateFromOps(modified, ops)
	return r.patchAndExpect(ctx, sbx, modified, "")
}

// applyTemplatePhase2 is phase 2 of the two-phase flow for paused sandboxes:
// write the target template and remove the resume trigger. Policy, lifecycle,
// label, and pending record were already written by phase 1.
func (r *Reconciler) applyTemplatePhase2(ctx context.Context, sbx *agentsv1alpha1.Sandbox, ops *agentsv1alpha1.SandboxUpdateOps) error {
	modified := sbx.DeepCopy()
	setTemplateFromOps(modified, ops)
	delete(modified.Annotations, agentsv1alpha1.AnnotationUpgradeResumeTrigger)
	return r.patchAndExpect(ctx, sbx, modified, " (phase 2)")
}

// applyFastPathStamp advances the stamp directly on a sandbox that already
// carries the target template: a single metadata-only patch, no pending
// record, no policy or lifecycle writes. Optimistic locking re-verifies the
// classification preconditions — a conflict means the sandbox changed under
// us and the next reconcile re-evaluates from fresh state.
func (r *Reconciler) applyFastPathStamp(ctx context.Context, sbx *agentsv1alpha1.Sandbox, myRev string) error {
	modified := sbx.DeepCopy()
	if modified.Annotations == nil {
		modified.Annotations = map[string]string{}
	}
	modified.Annotations[agentsv1alpha1.AnnotationUpdateOpsRevision] = myRev
	return r.patchAndExpect(ctx, sbx, modified, " (fast-path stamp)")
}
