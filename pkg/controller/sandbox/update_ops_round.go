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

package sandbox

import (
	"context"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	agentsv1alpha1 "github.com/openkruise/agents/api/v1alpha1"
	"github.com/openkruise/agents/pkg/controller/sandbox/core"
	"github.com/openkruise/agents/pkg/utils"
)

// ensureUpdateOpsRoundTerminal implements the sandbox-side terminal handling
// of the SandboxUpdateOps stamp protocol. A round starts when a
// SandboxUpdateOps writes the pending record annotation; its terminal
// lifecycle is owned here, independent of the SandboxUpdateOps object, so a
// round left in flight by a deleted ops is still settled:
//
//   - Upgrade succeeded: the pending record is promoted to the stamp and
//     spec.upgradePolicy is cleared, in one atomic patch. The stamp is the
//     durable record of the newest landed revision; clearing the policy keeps
//     later template edits from silently re-entering the pod-replacement
//     upgrade lifecycle.
//   - Upgrade terminally failed: the pending record is cleared and no stamp
//     is written, releasing the occupancy signal so a newer ops can rescue
//     the sandbox.
//   - Orphaned round: the round never entered the upgrade flow and can no
//     longer progress (no Upgrading condition, no resume trigger, template
//     revision unchanged) — e.g. the ops was deleted in the phase-1 → phase-2
//     window of the paused two-phase flow. The pending record is cleared so
//     the sandbox does not stay occupied forever.
//
// Decisions are made on the persisted status only, so the handling is
// idempotent and reconcile-driven. All patches use optimistic locking: a
// conflict means the sandbox changed under us (e.g. another ops started a
// round), and the next reconcile re-evaluates from fresh state.
//
// A terminal condition only settles the round it belongs to. A new round's
// round-start patch bumps generation and changes the template hash, while the
// persisted status still carries the previous round's condition — settling on
// that stale condition would promote or release a round that never ran. Every
// branch below is therefore gated on the status having observed the current
// spec (generation observed, template hash current); the controller's own
// revision detection resets the stale condition before the next reconcile.
//
// It returns true when a patch was issued; the caller should end the current
// reconcile and let the watch event drive the next one.
func (r *SandboxReconciler) ensureUpdateOpsRoundTerminal(ctx context.Context, box *agentsv1alpha1.Sandbox) (bool, error) {
	pending := box.Annotations[agentsv1alpha1.AnnotationUpdateOpsPendingRevision]
	if pending == "" {
		return false, nil
	}
	if !isCurrentRoundStatus(box) {
		return false, nil
	}

	cond := utils.GetSandboxCondition(&box.Status, string(agentsv1alpha1.SandboxConditionUpgrading))
	switch {
	case isUpgradeCondSucceeded(cond):
		// Promotion: stamp, pending-record removal, and policy clearing are
		// one atomic patch so no observer sees a half-settled round.
		modified := box.DeepCopy()
		delete(modified.Annotations, agentsv1alpha1.AnnotationUpdateOpsPendingRevision)
		modified.Spec.UpgradePolicy = nil
		if !strings.HasPrefix(pending, agentsv1alpha1.UpdateOpsPatchPendingPrefix) {
			// Patch-mode rounds never join the stamp protocol: their pending
			// record is occupancy only, not promotion material.
			modified.Annotations[agentsv1alpha1.AnnotationUpdateOpsRevision] = pending
		}
		if err := r.patchRoundTerminal(ctx, box, modified); err != nil {
			return false, err
		}
		klog.FromContext(ctx).Info("Settled update-ops round on success",
			"sandbox", klog.KObj(box), "revision", pending)
		return true, nil

	case isUpgradeCondTerminalFailure(cond):
		modified := box.DeepCopy()
		delete(modified.Annotations, agentsv1alpha1.AnnotationUpdateOpsPendingRevision)
		if err := r.patchRoundTerminal(ctx, box, modified); err != nil {
			return false, err
		}
		klog.FromContext(ctx).Info("Cleared update-ops pending record after terminal upgrade failure",
			"sandbox", klog.KObj(box), "revision", pending, "reason", cond.Reason)
		return true, nil

	case cond == nil && isUpdateOpsRoundOrphaned(box):
		modified := box.DeepCopy()
		delete(modified.Annotations, agentsv1alpha1.AnnotationUpdateOpsPendingRevision)
		if err := r.patchRoundTerminal(ctx, box, modified); err != nil {
			return false, err
		}
		klog.FromContext(ctx).Info("Cleared orphaned update-ops pending record",
			"sandbox", klog.KObj(box), "revision", pending)
		return true, nil
	}
	return false, nil
}

// patchRoundTerminal applies a round-terminal patch with optimistic locking.
// Conflicts are swallowed: the conflicting write already triggers a new
// reconcile that re-evaluates the round from fresh state.
func (r *SandboxReconciler) patchRoundTerminal(ctx context.Context, box, modified *agentsv1alpha1.Sandbox) error {
	err := r.Patch(ctx, modified, client.MergeFromWithOptions(box, client.MergeFromWithOptimisticLock{}))
	if apierrors.IsConflict(err) {
		klog.FromContext(ctx).Info("Conflict on update-ops round-terminal patch, re-evaluating on next reconcile",
			"sandbox", klog.KObj(box))
		return nil
	}
	return err
}

// isCurrentRoundStatus reports whether the persisted status has observed the
// current spec, so a persisted Upgrading condition provably belongs to the
// round that the current spec (and pending record) describe. A round-start
// patch bumps generation and — whenever the template changes — the sandbox
// hash, so a stale status means the condition belongs to a previous round.
func isCurrentRoundStatus(box *agentsv1alpha1.Sandbox) bool {
	if box.Generation != box.Status.ObservedGeneration {
		return false
	}
	hash, _ := core.HashSandbox(box)
	return hash == box.Status.UpdateRevision
}

// isUpgradeCondSucceeded reports whether the Upgrading condition is in the
// terminal-success state (Reason=Succeeded, Status=True).
func isUpgradeCondSucceeded(cond *metav1.Condition) bool {
	return cond != nil &&
		cond.Reason == agentsv1alpha1.SandboxUpgradingReasonSucceeded &&
		cond.Status == metav1.ConditionTrue
}

// isUpgradeCondTerminalFailure reports whether the Upgrading condition carries
// a terminal failure reason. These are the reasons SandboxUpdateOps classifies
// as failed; the round is settled with no stamp written.
func isUpgradeCondTerminalFailure(cond *metav1.Condition) bool {
	if cond == nil || cond.Status != metav1.ConditionFalse {
		return false
	}
	switch cond.Reason {
	case agentsv1alpha1.SandboxUpgradingReasonPreUpgradeFailed,
		agentsv1alpha1.SandboxUpgradingReasonCheckpointFailed,
		agentsv1alpha1.SandboxUpgradingReasonUpgradePodFailed,
		agentsv1alpha1.SandboxUpgradingReasonPostUpgradeFailed:
		return true
	}
	return false
}

// isUpdateOpsRoundOrphaned reports whether a pending record belongs to a round
// that can no longer progress: the upgrade flow was never entered (the caller
// checked there is no Upgrading condition), no resume trigger is pending, and
// the template revision is unchanged, so nothing will ever drive the round to
// a terminal state. Only steady phases qualify — a transitional phase may
// still enter the upgrade flow on a later reconcile.
func isUpdateOpsRoundOrphaned(box *agentsv1alpha1.Sandbox) bool {
	if box.Annotations[agentsv1alpha1.AnnotationUpgradeResumeTrigger] == agentsv1alpha1.True {
		return false
	}
	if box.Status.Phase != agentsv1alpha1.SandboxRunning && box.Status.Phase != agentsv1alpha1.SandboxPaused {
		return false
	}
	hash, _ := core.HashSandbox(box)
	return hash == box.Status.UpdateRevision
}
