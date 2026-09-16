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

package core

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	agentsv1alpha1 "github.com/openkruise/agents/api/v1alpha1"
	"github.com/openkruise/agents/pkg/utils"
	"github.com/openkruise/agents/pkg/utils/expectations"
	"github.com/openkruise/agents/pkg/utils/inplaceupdate"
)

// Event reasons for upgrade lifecycle transitions.
const (
	EventUpgradeResuming          = "UpgradeResuming"
	EventUpgradeResumed           = "UpgradeResumed"
	EventUpgradePreUpgradeFailed  = "PreUpgradeFailed"
	EventUpgradePodReplaced       = "UpgradePodReplaced"
	EventUpgradePodInplaceUpdate  = "UpgradePodInplaceUpdate"
	EventUpgradePodFailed         = "UpgradePodFailed"
	EventUpgradePostUpgradeFailed = "PostUpgradeFailed"
	EventUpgradeSucceeded         = "UpgradeSucceeded"
)

// UpgradeControl manages the sandbox upgrade lifecycle state machine.
// It orchestrates the full upgrade flow: PreUpgrade → (Checkpointing) →
// UpgradePod → PostUpgrade → Succeeded, delegating pod operations to
// PodControl and checkpoint operations to CheckpointControl.
type UpgradeControl struct {
	client.Client
	checkpointControl *CheckpointControl
	podControl        *PodControl
	recorder          record.EventRecorder
	lifecycleHookFunc LifecycleHookFunc
	initializer       SandboxInitializer
	syncStatusFromPod func(pod *corev1.Pod, newStatus *agentsv1alpha1.SandboxStatus, syncReadyCondition bool)
	resumeFunc        ResumeFunc
	// 原地更新结果通过 Upgrading 汇报；Ready 按阶段和真实 Pod 状态映射，不写 InplaceUpdate。
	inplaceUpdateControl *inplaceupdate.InPlaceUpdateControl
}

// NewUpgradeControl creates a new UpgradeControl.
//
// podControl is passed in as a parameter so that callers can inject a
// customised PodControl (e.g. with a different PodGenerateFunc) when needed.
func NewUpgradeControl(
	cli client.Client,
	checkpointControl *CheckpointControl,
	podControl *PodControl,
	recorder record.EventRecorder,
	lifecycleHookFunc LifecycleHookFunc,
	initializer SandboxInitializer,
	syncStatusFromPod func(pod *corev1.Pod, newStatus *agentsv1alpha1.SandboxStatus, syncReadyCondition bool),
	resumeFunc ResumeFunc,
	inplaceUpdateControl *inplaceupdate.InPlaceUpdateControl,
) *UpgradeControl {
	return &UpgradeControl{
		Client:               cli,
		checkpointControl:    checkpointControl,
		podControl:           podControl,
		recorder:             recorder,
		lifecycleHookFunc:    lifecycleHookFunc,
		initializer:          initializer,
		syncStatusFromPod:    syncStatusFromPod,
		resumeFunc:           resumeFunc,
		inplaceUpdateControl: inplaceUpdateControl,
	}
}

// recordUpgradeEvent emits an event on the sandbox object. It is safe to call
// when the recorder is nil (e.g. in unit tests that do not assert events).
func (r *UpgradeControl) recordUpgradeEvent(box *agentsv1alpha1.Sandbox, eventType, reason, messageFmt string, args ...any) {
	if r.recorder == nil {
		return
	}
	r.recorder.Eventf(box, eventType, reason, messageFmt, args...)
}

// RequiresPodReplacementUpgrade returns true when the sandbox's upgrade policy
// requires pod replacement (Recreate or CheckpointRestore). These policies
// delete the old pod and create a new one during the UpgradePod step.
//
// InplaceUpdate is deliberately excluded: it also runs the upgrade lifecycle,
// but patches the existing pod instead of replacing it. Use RequiresUpgradeSandbox
// to decide whether the sandbox enters the Upgrading phase at all.
func RequiresPodReplacementUpgrade(box *agentsv1alpha1.Sandbox) bool {
	return box.Spec.UpgradePolicy != nil &&
		(box.Spec.UpgradePolicy.Type == agentsv1alpha1.SandboxUpgradePolicyRecreate ||
			box.Spec.UpgradePolicy.Type == agentsv1alpha1.SandboxUpgradePolicyCheckpointRestore)
}

// RequiresUpgradeSandbox returns true when the sandbox's upgrade policy drives the
// sandbox through the Upgrading phase and its lifecycle state machine
// (PreUpgrade → Checkpointing → UpgradePod → PostUpgrade).
//
// 所有显式升级策略（包括 InplaceUpdate）都进入 Upgrading 并执行生命周期。
// Ready 独立反映 Pod 健康与当前步骤；无升级策略的 Claim 更新仍留在 Running。
func RequiresUpgradeSandbox(box *agentsv1alpha1.Sandbox) bool {
	if box.Spec.UpgradePolicy == nil {
		return false
	}
	switch box.Spec.UpgradePolicy.Type {
	case agentsv1alpha1.SandboxUpgradePolicyRecreate,
		agentsv1alpha1.SandboxUpgradePolicyCheckpointRestore,
		agentsv1alpha1.SandboxUpgradePolicyInplaceUpdate:
		return true
	}
	return false
}

// RequiresInplaceUpgrade returns true when the sandbox's upgrade policy performs
// the UpgradePod step by patching the existing pod in place.
func RequiresInplaceUpgrade(box *agentsv1alpha1.Sandbox) bool {
	return box.Spec.UpgradePolicy != nil &&
		box.Spec.UpgradePolicy.Type == agentsv1alpha1.SandboxUpgradePolicyInplaceUpdate
}

// EnsureSandboxUpgraded drives the sandbox upgrade state machine.
//
// The state transitions are:
//
//	Resuming → ResumeSucceed → PreUpgrade → Checkpointing → UpgradePod → PostUpgrade → Succeeded
//
// 失败状态停止本轮自动推进。新 SUO 重置生命周期；同轮成功 hook 不重复执行。
func (r *UpgradeControl) EnsureSandboxUpgraded(ctx context.Context, args EnsureFuncArgs) (retErr error) {
	pod, box, newStatus := args.Pod, args.Box, args.NewStatus
	isCheckpointRestore := box.Spec.UpgradePolicy != nil &&
		box.Spec.UpgradePolicy.Type == agentsv1alpha1.SandboxUpgradePolicyCheckpointRestore

	if err := ctx.Err(); err != nil {
		return err
	}
	if pod != nil {
		r.syncStatusFromPod(pod, newStatus, false)
	}
	setUpdateReady(newStatus, podIsReady(pod), agentsv1alpha1.SandboxReadyReasonUpgrading, "sandbox is upgrading")
	upgradeCond := utils.GetSandboxCondition(newStatus, string(agentsv1alpha1.SandboxConditionUpgrading))
	// First entry — if the sandbox was paused, start with Resuming to ensure it is
	// woken up before proceeding with the upgrade lifecycle. Otherwise start at
	// PreUpgrade directly.
	if upgradeCond == nil {
		initialReason := agentsv1alpha1.SandboxUpgradingReasonPreUpgrade
		if pausedCond := utils.GetSandboxCondition(newStatus, string(agentsv1alpha1.SandboxConditionPaused)); pausedCond != nil {
			initialReason = agentsv1alpha1.SandboxUpgradingReasonResuming
		}
		upgradeCond = &metav1.Condition{
			Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
			Status:             metav1.ConditionFalse,
			Reason:             initialReason,
			LastTransitionTime: metav1.Now(),
			ObservedGeneration: box.Generation,
		}
		utils.SetSandboxCondition(newStatus, *upgradeCond)
	}
	upgradeCond.ObservedGeneration = box.Generation
	utils.SetSandboxCondition(newStatus, *upgradeCond)
	if IsUpgradeFailed(upgradeCond) {
		setUpdateReady(newStatus, false, agentsv1alpha1.SandboxReadyReasonUpgrading, upgradeCond.Message)
		return nil
	}
	if upgradeCond.Status == metav1.ConditionTrue {
		return nil
	}
	if newStatus.UpgradeProgress == nil {
		// 先持久化起点和预算，下一轮才允许执行有副作用的步骤。
		timeout := 300 * time.Second
		if box.Spec.UpgradePolicy != nil && box.Spec.UpgradePolicy.TimeoutSeconds != nil {
			timeout = time.Duration(*box.Spec.UpgradePolicy.TimeoutSeconds) * time.Second
		}
		now := metav1.Now()
		newStatus.UpgradeProgress = &agentsv1alpha1.SandboxUpgradeProgress{
			OperationID: box.Annotations[agentsv1alpha1.AnnotationUpgradeOperation], Revision: newStatus.UpdateRevision,
			StartedAt: now, Deadline: metav1.NewTime(now.Add(timeout)),
		}
		if pod != nil {
			newStatus.UpgradeProgress.SourcePodUID = string(pod.UID)
		}
		return nil
	}
	progress := newStatus.UpgradeProgress
	if progress.Revision != newStatus.UpdateRevision {
		r.failUpgrade(args, upgradeCond, agentsv1alpha1.SandboxUpgradingReasonUpgradePodFailed,
			"target changed within the active operation; submit a new SandboxUpdateOps to start a new lifecycle")
		return nil
	}
	// 已完成 hook 且本次观察到 Ready 时优先收敛成功；否则到期后不再执行新副作用。
	if !time.Now().Before(progress.Deadline.Time) &&
		!(upgradeCond.Reason == agentsv1alpha1.SandboxUpgradingReasonPostUpgrade && progress.PostUpgrade == agentsv1alpha1.SandboxUpgradeHookSucceeded && podIsReady(pod)) {
		failReason := agentsv1alpha1.SandboxUpgradingReasonUpgradePodFailed
		switch upgradeCond.Reason {
		case agentsv1alpha1.SandboxUpgradingReasonPreUpgrade:
			failReason = agentsv1alpha1.SandboxUpgradingReasonPreUpgradeFailed
		case agentsv1alpha1.SandboxUpgradingReasonPostUpgrade:
			failReason = agentsv1alpha1.SandboxUpgradingReasonPostUpgradeFailed
		case agentsv1alpha1.SandboxUpgradingReasonCheckpointing:
			failReason = agentsv1alpha1.SandboxUpgradingReasonCheckpointFailed
		}
		r.failUpgrade(args, upgradeCond, failReason, "upgrade deadline exceeded; automatic execution stopped: "+inplaceWaitMessage(pod))
		return nil
	}
	ctx, cancel := context.WithDeadline(ctx, progress.Deadline.Time)
	defer cancel()

	switch upgradeCond.Reason {
	// When upgrading a paused Sandbox, it must first be resumed successfully,
	// then upgraded, and finally paused again.
	// Since spec.paused=true at this point, two scenarios are possible during
	// the upgrade:
	// 1. spec.paused remains true throughout the upgrade. After the upgrade
	//    succeeds, the Sandbox enters Running state, and since spec.paused=true,
	//    the pause logic is triggered.
	// 2. If the user triggers a resume (spec.paused=false) during the upgrade,
	//    the Sandbox enters Running state with spec.paused=false after the
	//    upgrade, so no pause logic is triggered and it stays Running.
	//
	// Could this conflict with pauseTime? If PauseTime is set, will pause be
	// triggered again during the upgrade?
	// No. pauseTime takes effect by the sandbox controller setting spec.paused=true.
	// However, the current logic never changes spec.paused during the upgrade flow.
	// The Sandbox is in the Upgrading phase — the resume logic is implemented
	// inline here, without transitioning to Resuming/Running. Therefore, even if
	// pauseTime fires and sets spec.paused=true, it does not matter: the pause
	// will only be re-triggered after the upgrade succeeds and the Sandbox
	// enters Running state.
	case agentsv1alpha1.SandboxUpgradingReasonResuming:
		return r.handleResuming(ctx, args, upgradeCond, newStatus)
	// ResumeSucceed is a transient waiting state. When the template is
	// patched, calculateStatus detects the hash change and calls
	// determineUpgradeResumeReason to transition to PreUpgrade.
	//
	// Abandonment: if the resume trigger annotation has been removed (e.g.,
	// ops was deleted during the phase-1→phase-2 window) and the template
	// was not patched (UpdateRevision unchanged), abandon the upgrade and
	// return to Running. The sandbox already resumed successfully — it just
	// never received the template patch. With no ops to drive phase 2,
	// staying in ResumeSucceed would block forever.
	case agentsv1alpha1.SandboxUpgradingReasonResumeSucceed:
		if box.Annotations[agentsv1alpha1.AnnotationUpgradeResumeTrigger] != agentsv1alpha1.True &&
			newStatus.UpdateRevision == box.Status.UpdateRevision {
			klog.InfoS("Resume trigger annotation removed before template patch, abandoning upgrade", "sandbox", klog.KObj(box))
			utils.RemoveSandboxCondition(newStatus, string(agentsv1alpha1.SandboxConditionUpgrading))
			newStatus.Phase = agentsv1alpha1.SandboxRunning
			return nil
		}
		klog.InfoS("Waiting for template patch after resume", "sandbox", klog.KObj(box))
		return nil
	case agentsv1alpha1.SandboxUpgradingReasonPreUpgrade:
		// 在前置 hook 产生副作用前完成能静态确定的原地更新校验。
		if RequiresInplaceUpgrade(box) && pod != nil {
			if _, err := validateInplaceUpdate(pod, box); err != nil {
				r.failUpgrade(args, upgradeCond, agentsv1alpha1.SandboxUpgradingReasonUpgradePodFailed, err.Error())
				if classifyInplaceError(err) == inplaceClassQoSRejected && progress.PreUpgrade == "" && podIsReady(pod) {
					newStatus.Phase = agentsv1alpha1.SandboxRunning
					setUpdateReady(newStatus, true, "", "")
				}
				return nil
			}
		}
		result, err := r.runUpgradeHook(ctx, args, true)
		if err != nil {
			if isTerminalInplaceError(err) {
				r.failUpgrade(args, upgradeCond, agentsv1alpha1.SandboxUpgradingReasonPreUpgradeFailed, err.Error())
				return nil
			}
			return err
		}
		if !result.Succeeded {
			r.failUpgrade(args, upgradeCond, agentsv1alpha1.SandboxUpgradingReasonPreUpgradeFailed, result.Message)
			return nil
		}

		// Always transition to Checkpointing; EnsureCheckpointForUpgrade will
		// short-circuit and return done=true if CheckpointRestore is not enabled.
		klog.InfoS("preUpgrade completed, transitioning to Checkpointing", "sandbox", klog.KObj(box))
		upgradeCond.Reason = agentsv1alpha1.SandboxUpgradingReasonCheckpointing
		upgradeCond.Message = ""
		utils.SetSandboxCondition(newStatus, *upgradeCond)
		fallthrough
	case agentsv1alpha1.SandboxUpgradingReasonCheckpointing:
		checkpointDone, cpName, err := r.checkpointControl.EnsureCheckpointForUpgrade(ctx, box)
		if err != nil {
			klog.ErrorS(err, "Checkpoint failed during upgrade", "sandbox", klog.KObj(box), "checkpoint", cpName)
			if cpName != "" || isTerminalInplaceError(err) {
				r.failUpgrade(args, upgradeCond, agentsv1alpha1.SandboxUpgradingReasonCheckpointFailed, err.Error())
				return nil
			}
			upgradeCond.Message = utils.TruncateConditionMessage(err.Error())
			utils.SetSandboxCondition(newStatus, *upgradeCond)
			return err
		}
		if !checkpointDone {
			klog.InfoS("Waiting for checkpoint to complete", "sandbox", klog.KObj(box), "checkpoint", cpName)
			upgradeCond.Reason = agentsv1alpha1.SandboxUpgradingReasonCheckpointing
			upgradeCond.Message = fmt.Sprintf("Waiting for checkpoint %s to complete before pod deletion", cpName)
			utils.SetSandboxCondition(newStatus, *upgradeCond)
			return nil
		}
		klog.InfoS("Checkpoint succeeded, transitioning to UpgradePod", "sandbox", klog.KObj(box))
		upgradeCond.Reason = agentsv1alpha1.SandboxUpgradingReasonUpgradePod
		upgradeCond.Message = ""
		utils.SetSandboxCondition(newStatus, *upgradeCond)
		fallthrough
	case agentsv1alpha1.SandboxUpgradingReasonUpgradePod:
		upgradedPod, done, err := r.executeUpgradePodStep(ctx, args, upgradeCond)
		if err != nil {
			return err
		}
		if !done {
			return nil // upgrade in progress, or a terminal failure was recorded
		}
		pod = upgradedPod

		// UpgradePod step completed
		upgradeCond.Reason = agentsv1alpha1.SandboxUpgradingReasonPostUpgrade
		upgradeCond.Message = ""
		utils.SetSandboxCondition(newStatus, *upgradeCond)
		fallthrough
	case agentsv1alpha1.SandboxUpgradingReasonPostUpgrade:
		args.Pod = pod
		result, err := r.runUpgradeHook(ctx, args, false)
		if err != nil {
			if isTerminalInplaceError(err) {
				r.failUpgrade(args, upgradeCond, agentsv1alpha1.SandboxUpgradingReasonPostUpgradeFailed, err.Error())
				return nil
			}
			return err
		}
		if !result.Succeeded {
			r.failUpgrade(args, upgradeCond, agentsv1alpha1.SandboxUpgradingReasonPostUpgradeFailed, result.Message)
			return nil
		}
		if !podIsReady(pod) {
			upgradeCond.Message = utils.TruncateConditionMessage("PostUpgrade completed; waiting for Pod Ready: " + inplaceWaitMessage(pod))
			utils.SetSandboxCondition(newStatus, *upgradeCond)
			setUpdateReady(newStatus, false, agentsv1alpha1.SandboxReadyReasonUpgrading, upgradeCond.Message)
			return nil
		}

		// For CheckpointRestore, clean up checkpoint CRs after the entire upgrade succeeds.
		if isCheckpointRestore {
			r.checkpointControl.CleanupCheckpoints(ctx, box)
		}

		klog.InfoS("postUpgrade completed, transitioning to Succeeded", "sandbox", klog.KObj(box))
		r.recordUpgradeEvent(box, corev1.EventTypeNormal, EventUpgradeSucceeded, "Upgrade completed successfully")
		upgradeCond.Reason = agentsv1alpha1.SandboxUpgradingReasonSucceeded
		upgradeCond.Status = metav1.ConditionTrue
		upgradeCond.Message = ""
		upgradeCond.LastTransitionTime = metav1.Now()
		utils.SetSandboxCondition(newStatus, *upgradeCond)
		newStatus.Phase = agentsv1alpha1.SandboxRunning
		utils.SetSandboxCondition(newStatus, metav1.Condition{
			Type:               string(agentsv1alpha1.SandboxConditionReady),
			Status:             metav1.ConditionTrue,
			Reason:             agentsv1alpha1.SandboxReadyReasonPodReady,
			Message:            "",
			LastTransitionTime: metav1.Now(),
		})
	}

	return nil
}

// IsUpgradeFailed 识别真正终止状态；普通等待和可重试写入错误不使用 Failed Reason。
func IsUpgradeFailed(cond *metav1.Condition) bool {
	if cond == nil || cond.Status != metav1.ConditionFalse {
		return false
	}
	switch cond.Reason {
	case agentsv1alpha1.SandboxUpgradingReasonPreUpgradeFailed, agentsv1alpha1.SandboxUpgradingReasonUpgradePodFailed,
		agentsv1alpha1.SandboxUpgradingReasonPostUpgradeFailed, agentsv1alpha1.SandboxUpgradingReasonCheckpointFailed:
		return true
	}
	return false
}

func (r *UpgradeControl) failUpgrade(args EnsureFuncArgs, cond *metav1.Condition, reason, msg string) {
	previous := utils.GetSandboxCondition(&args.Box.Status, string(agentsv1alpha1.SandboxConditionUpgrading))
	report := previous == nil || previous.Status != metav1.ConditionFalse || previous.Reason != reason ||
		previous.Message != utils.TruncateConditionMessage(msg) || previous.ObservedGeneration != args.Box.Generation
	cond.Status, cond.Reason, cond.Message = metav1.ConditionFalse, reason, utils.TruncateConditionMessage(msg)
	cond.ObservedGeneration = args.Box.Generation
	utils.SetSandboxCondition(args.NewStatus, *cond)
	setUpdateReady(args.NewStatus, false, agentsv1alpha1.SandboxReadyReasonUpgrading, cond.Message)
	if report {
		r.recordUpgradeEvent(args.Box, corev1.EventTypeWarning, reason, "%s", cond.Message)
	}
}

// hook 执行前落盘 Running。成功结果若未持久化，下一次按结果未知停止，而非重复副作用。
func (r *UpgradeControl) runUpgradeHook(ctx context.Context, args EnsureFuncArgs, pre bool) (upgradeActionResult, error) {
	progress := args.NewStatus.UpgradeProgress
	mark := &progress.PostUpgrade
	if pre {
		mark = &progress.PreUpgrade
	}
	if *mark == agentsv1alpha1.SandboxUpgradeHookSucceeded {
		return upgradeActionResult{Succeeded: true}, nil
	}
	if *mark == agentsv1alpha1.SandboxUpgradeHookRunning {
		return upgradeActionResult{Message: "previous hook execution result is unknown; automatic replay stopped"}, nil
	}
	if !hasUpgradeAction(args.Box, pre) {
		*mark = agentsv1alpha1.SandboxUpgradeHookSucceeded
		return upgradeActionResult{Succeeded: true}, nil
	}
	if err := ctx.Err(); err != nil {
		return upgradeActionResult{}, err
	}
	*mark = agentsv1alpha1.SandboxUpgradeHookRunning
	if err := r.persistUpgradeProgress(ctx, args); err != nil {
		*mark = ""
		return upgradeActionResult{}, fmt.Errorf("cannot persist hook execution intent: %w", err)
	}
	action := args.Box.Spec.Lifecycle.PostUpgrade
	if pre {
		action = args.Box.Spec.Lifecycle.PreUpgrade
	}
	boxForHook := args.Box.DeepCopy()
	boxForHook.Status = *args.NewStatus.DeepCopy()
	result := r.executeUpgradeAction(ctx, args.Pod, boxForHook, action)
	if result.Succeeded {
		*mark = agentsv1alpha1.SandboxUpgradeHookSucceeded
		// 成功事实先单独落盘，后续 checkpoint 或初始化失败不能导致重跑已成功 hook。
		if err := r.persistUpgradeProgress(ctx, args); err != nil {
			*mark = agentsv1alpha1.SandboxUpgradeHookRunning
			return upgradeActionResult{}, fmt.Errorf("cannot persist hook success: %w", err)
		}
	}
	return result, nil
}

func (r *UpgradeControl) persistUpgradeProgress(ctx context.Context, args EnsureFuncArgs) error {
	modified := args.Box.DeepCopy()
	modified.Status = *args.NewStatus.DeepCopy()
	if err := r.Status().Patch(ctx, modified, client.MergeFromWithOptions(args.Box.DeepCopy(), client.MergeFromWithOptimisticLock{})); err != nil {
		return err
	}
	ResourceVersionExpectations.Expect(modified)
	// 同步本次独立副本的版本，约束后续状态写入。
	args.Box.ResourceVersion = modified.ResourceVersion
	args.Box.Status = *modified.Status.DeepCopy()
	return nil
}

// handleResuming processes the Resuming state of the upgrade lifecycle.
// It resumes the paused sandbox, waits for pod readiness, and transitions
// to ResumeSucceed when the resume is complete.
// Returns nil while waiting and an error if resume or initialize fails.
func (r *UpgradeControl) handleResuming(ctx context.Context, args EnsureFuncArgs, upgradeCond *metav1.Condition, newStatus *agentsv1alpha1.SandboxStatus) error {
	pod, box := args.Pod, args.Box
	// The sandbox was paused — resume it before proceeding with the upgrade.
	pausedCond := utils.GetSandboxCondition(newStatus, string(agentsv1alpha1.SandboxConditionPaused))
	if pausedCond == nil || pausedCond.Status == metav1.ConditionFalse {
		// Sandbox is still pausing (or pause condition missing), wait for it to complete.
		klog.InfoS("Sandbox is still pausing, waiting before upgrade", "sandbox", klog.KObj(box))
		return nil
	}
	// Record the resume event only on the first entry into Resuming;
	// subsequent reconciles see a non-empty Message and skip the
	// duplicate event while waiting for pod readiness.
	if upgradeCond.Message == "" {
		r.recordUpgradeEvent(box, corev1.EventTypeNormal, EventUpgradeResuming, "Resuming paused sandbox for upgrade")
		upgradeCond.Message = "Resume triggered, waiting for pod readiness"
		utils.SetSandboxCondition(newStatus, *upgradeCond)
	}
	if err := r.resumeFunc(ctx, args); err != nil {
		return err
	}
	// Check if resume succeeded by looking at the Resumed condition.
	resumedCond := utils.GetSandboxCondition(newStatus, string(agentsv1alpha1.SandboxConditionResumed))
	if resumedCond == nil || resumedCond.Status != metav1.ConditionTrue {
		klog.InfoS("Sandbox resume in progress, waiting before upgrade", "sandbox", klog.KObj(box))
		return nil
	}
	// pod may be nil here if resumeFunc just created it but the local
	// args.Pod copy is stale. Re-fetch is not needed — the controller will
	// re-reconcile with the updated pod. Just wait for the next cycle.
	if pod == nil {
		klog.InfoS("Pod not yet available after resume, waiting for next reconcile", "sandbox", klog.KObj(box))
		return nil
	}
	pCond := utils.GetPodCondition(&pod.Status, corev1.PodReady)
	if pCond == nil || pCond.Status != corev1.ConditionTrue {
		klog.InfoS("Waiting for pod ready before initialization", "sandbox", klog.KObj(box))
		return nil
	}
	// Sync status from the resumed pod before Initialize so that runtime
	// re-init targets the current Pod IP/UID. The resumed pod may differ
	// from the paused one, and using stale status would leave the upgrade
	// stuck in Resuming.
	r.syncStatusFromPod(pod, newStatus, false)
	// Only initialize the old pod when a PreUpgrade hook is configured.
	// The Initialize call (runtime re-init, security token, CSI re-mount)
	// prepares the old pod for PreUpgrade execution. If no PreUpgrade hook
	// is configured, the old pod is about to be deleted in the UpgradePod
	// step, so initializing it would be wasted work. The new pod created
	// in performRecreateUpgrade is always initialized regardless.
	if hasUpgradeAction(box, true) {
		if err := r.initializer.Initialize(ctx, box, newStatus); err != nil {
			return err
		}
	}
	// Resume succeeded. Transition to ResumeSucceed and wait for
	// SandboxUpdateOps to patch the template before proceeding.
	klog.InfoS("Sandbox resumed successfully, waiting for template patch", "sandbox", klog.KObj(box))
	r.recordUpgradeEvent(box, corev1.EventTypeNormal, EventUpgradeResumed, "Sandbox resumed, waiting for template patch")
	upgradeCond.Reason = agentsv1alpha1.SandboxUpgradingReasonResumeSucceed
	upgradeCond.Message = ""
	if newStatus.UpgradeProgress != nil {
		newStatus.UpgradeProgress.SourcePodUID = string(pod.UID)
	}
	utils.SetSandboxCondition(newStatus, *upgradeCond)
	// Resume succeeded: drop the Paused condition, kept through Resuming.
	// The direct resume path cleans it up in the controller's
	// finalizeResumePhase; the upgrade path calls resumeFunc directly, so it
	// cleans up here.
	utils.RemoveSandboxCondition(newStatus, string(agentsv1alpha1.SandboxConditionPaused))
	return nil
}

// executeUpgradePodStep runs the UpgradePod step with the strategy selected by
// the sandbox's upgrade policy: patching the pod in place, or replacing it.
//
// It returns the pod to use for the following PostUpgrade step, and done=false
// when the caller must stop this reconcile — either because the upgrade is still
// in progress, or because a terminal failure was already recorded on the
// Upgrading condition.
func (r *UpgradeControl) executeUpgradePodStep(ctx context.Context, args EnsureFuncArgs, upgradeCond *metav1.Condition) (*corev1.Pod, bool, error) {
	box, newStatus := args.Box, args.NewStatus

	if RequiresInplaceUpgrade(box) {
		step, err := r.performInplaceUpgrade(ctx, args)
		if err != nil {
			upgradeCond.Message = utils.TruncateConditionMessage(err.Error())
			utils.SetSandboxCondition(newStatus, *upgradeCond)
			setUpdateReady(newStatus, false, agentsv1alpha1.SandboxReadyReasonUpgrading, upgradeCond.Message)
			if isTerminalInplaceError(err) {
				r.failUpgrade(args, upgradeCond, agentsv1alpha1.SandboxUpgradingReasonUpgradePodFailed, err.Error())
				return nil, false, nil
			}
			return nil, false, err
		}
		// 配置生效才进入 PostUpgrade；正常等待中的 Ready 跟随 Pod。
		setUpdateReady(newStatus, podIsReady(args.Pod), agentsv1alpha1.SandboxReadyReasonUpgrading, inplaceWaitMessage(args.Pod))
		if step != inplaceUpdateStepSucceeded {
			upgradeCond.Message = utils.TruncateConditionMessage(inplaceWaitMessage(args.Pod))
			utils.SetSandboxCondition(newStatus, *upgradeCond)
			return nil, false, nil
		}
		klog.InfoS("In-place UpgradePod step completed, transitioning to PostUpgrade", "sandbox", klog.KObj(box))
		r.recordUpgradeEvent(box, corev1.EventTypeNormal, EventUpgradePodInplaceUpdate, "Pod updated in place successfully, proceeding to PostUpgrade")
		// The pod was patched rather than replaced, so the pod reference stays valid
		// and no re-fetch or re-initialization is required.
		return args.Pod, true, nil
	}

	done, err := r.performRecreateUpgrade(ctx, args)
	if err != nil || !done {
		// 旧 Pod 已删除或替换尚未完成时，不能延续删除前观察到的 Ready=True。
		msg := inplaceWaitMessage(args.Pod)
		if err != nil {
			msg = err.Error()
		}
		setUpdateReady(newStatus, false, agentsv1alpha1.SandboxReadyReasonUpgrading, msg)
		if err != nil && isTerminalInplaceError(err) {
			r.failUpgrade(args, upgradeCond, agentsv1alpha1.SandboxUpgradingReasonUpgradePodFailed, msg)
			return nil, false, nil
		}
		return nil, false, err
	}

	klog.InfoS("UpgradePod step completed, transitioning to PostUpgrade", "sandbox", klog.KObj(box))
	r.recordUpgradeEvent(box, corev1.EventTypeNormal, EventUpgradePodReplaced, "Pod replaced successfully, proceeding to PostUpgrade")

	// Re-fetch the Pod after recreate upgrade, since the old pod object is stale (deleted and replaced).
	var freshPod corev1.Pod
	if err := r.Get(ctx, types.NamespacedName{Namespace: box.Namespace, Name: box.Name}, &freshPod); err != nil {
		klog.ErrorS(err, "Failed to re-fetch pod after recreate upgrade", "sandbox", klog.KObj(box))
		return nil, false, err
	}
	return &freshPod, true, nil
}

// performInplaceUpgrade 仅返回共享引擎的阶段和错误，Condition 由外层 adapter 映射。
// 新 SUO 可以修正未完成的镜像目标，不以旧轮次失败或 Pod 未 Ready 阻挡下发。
func (r *UpgradeControl) performInplaceUpgrade(ctx context.Context, args EnsureFuncArgs) (inplaceUpdateStepResult, error) {
	pod, box := args.Pod, args.Box
	if pod == nil {
		// 保留暂停恢复或 Pod 丢失时的创建能力；现存 Pod 绝不因镜像失败被删除。
		_, err := r.performRecreateUpgrade(ctx, args)
		return inplaceUpdateStepPatchDelivered, err
	}
	if r.inplaceUpdateControl == nil {
		return inplaceUpdateStepInProgress, fmt.Errorf("in-place upgrade is not configured for sandbox %s/%s", box.Namespace, box.Name)
	}
	return runInplaceUpdateStep(ctx, r.inplaceUpdateControl, pod, box, args.NewStatus.UpdateRevision)
}

// performRecreateUpgrade handles the Recreate upgrade step (delete old pod + create new pod).
// For CheckpointRestore policy, it passes the checkpoint ID so the new pod
// restores its writable layer from the checkpoint.
func (r *UpgradeControl) performRecreateUpgrade(ctx context.Context, args EnsureFuncArgs) (bool, error) {
	pod, box, newStatus := args.Pod, args.Box, args.NewStatus

	isCheckpointRestore := box.Spec.UpgradePolicy != nil &&
		box.Spec.UpgradePolicy.Type == agentsv1alpha1.SandboxUpgradePolicyCheckpointRestore

	// 新轮次的重建策略必须替换原 Pod，即使旧原地更新已经写入相同目标 hash。
	sourcePod := ""
	if newStatus.UpgradeProgress != nil {
		sourcePod = newStatus.UpgradeProgress.SourcePodUID
	}
	if pod != nil && (pod.Labels[agentsv1alpha1.PodLabelTemplateHash] != newStatus.UpdateRevision ||
		(sourcePod != "" && string(pod.UID) == sourcePod)) {
		if !pod.DeletionTimestamp.IsZero() {
			klog.InfoS("Waiting for pod deletion to complete", "sandbox", klog.KObj(box))
			return false, nil
		}
		// Delete pod
		ScaleExpectation.ExpectScale(GetControllerKey(box), expectations.Delete, box.Name)
		if err := r.Delete(ctx, pod); err != nil {
			ScaleExpectation.ObserveScale(GetControllerKey(box), expectations.Delete, box.Name)
			if !errors.IsNotFound(err) {
				klog.ErrorS(err, "Failed to delete pod for upgrade", "sandbox", klog.KObj(box))
				return false, err
			}
		}
		klog.InfoS("Deleted old pod for upgrade", "sandbox", klog.KObj(box))
		return false, nil
	}

	// Step 2: Create new Pod (old pod deleted)
	if pod == nil {
		// TODO: The virtual kubelet may have status reporting delays after
		// pod deletion. Previously a blocking time.Sleep(1s) was used here
		// to mitigate this. It is removed to avoid blocking the reconcile
		// loop. If VK status issues resurface, consider adding a requeue
		// delay or an annotation-based gate before pod creation.
		klog.InfoS("Creating new pod for upgrade", "sandbox", klog.KObj(box))
		// Both Recreate and CheckpointRestore render the new pod from the current
		// template, so the runtime HTTPS capability of the new pod matches the
		// current injection configuration and the stamp is accurate.
		createArgs := CreatePodArgs{Box: box, NewStatus: newStatus, AdvertiseRuntimeTLS: true}
		// For CheckpointRestore, set the checkpoint ID annotation so the
		// checkpoint controller can restore the pod's writable layer.
		if isCheckpointRestore {
			_, checkpointID := r.checkpointControl.GetCheckpointResumeData(ctx, box)
			if checkpointID == "" {
				// Should never happen: the checkpoint succeeded before the
				// upgrade reached this step, so it must carry an ID. Block
				// the upgrade and surface the inconsistency instead of
				// creating a pod that cannot restore its writable layer.
				err := fmt.Errorf("checkpoint ID not found for CheckpointRestore upgrade")
				klog.FromContext(ctx).Error(err, "CheckpointRestore upgrade blocked: checkpoint has no ID", "sandbox", klog.KObj(box))
				return false, err
			}
			createArgs.CheckpointID = checkpointID
		}
		newPod, err := r.podControl.CreatePod(ctx, createArgs)
		if err != nil {
			klog.ErrorS(err, "Failed to create new pod for upgrade", "sandbox", klog.KObj(box))
			return false, err
		}
		if newPod != nil {
			klog.InfoS("New pod created for upgrade", "sandbox", klog.KObj(box), "pod", klog.KObj(newPod))
		}
		return false, nil
	}

	// 等待容器启动后初始化并执行 PostUpgrade，最终 Ready 留给生命周期收尾判断。
	cond := utils.GetSandboxCondition(newStatus, string(agentsv1alpha1.SandboxConditionUpgrading))
	if !podContainersRunning(pod) {
		klog.InfoS("Waiting for new pod to be ready", "sandbox", klog.KObj(box))
		for _, cStatus := range pod.Status.ContainerStatuses {
			if cStatus.State.Waiting != nil {
				reason := cStatus.State.Waiting.Reason
				// 拉取退避、创建与重启都由本轮预算约束，不把临时等待提前终止。
				if reason != "InvalidImageName" && reason != "ErrImageNeverPull" {
					continue
				}
				// Other waiting reasons indicate container startup failure
				klog.InfoS("container waiting with abnormal reason", "sandbox", klog.KObj(box),
					"container", cStatus.Name, "reason", reason, "message", cStatus.State.Waiting.Message)
				r.failUpgrade(args, cond, agentsv1alpha1.SandboxUpgradingReasonUpgradePodFailed,
					fmt.Sprintf("container %s: %s - %s", cStatus.Name, reason, cStatus.State.Waiting.Message))
				return false, nil
			} else if cStatus.State.Terminated != nil {
				klog.InfoS("container terminated unexpectedly", "sandbox", klog.KObj(box),
					"container", cStatus.Name, "reason", cStatus.State.Terminated.Reason,
					"exitCode", cStatus.State.Terminated.ExitCode, "message", cStatus.State.Terminated.Message)
				cond.Message = utils.TruncateConditionMessage(fmt.Sprintf("container %s: terminated with exit code %d - %s",
					cStatus.Name, cStatus.State.Terminated.ExitCode, cStatus.State.Terminated.Reason))
				utils.SetSandboxCondition(newStatus, *cond)
			}
		}
		return false, nil
	}

	r.syncStatusFromPod(pod, newStatus, false)

	// Step 4: Perform post-recreate-upgrade initialization (re-init runtime, re-mount CSI).
	if err := r.initializer.Initialize(ctx, box, newStatus); err != nil {
		return false, err
	}

	return true, nil
}

// 容器运行仅代表执行通道可能可用，不代替 Pod Ready。
func podContainersRunning(pod *corev1.Pod) bool {
	if pod == nil || !pod.DeletionTimestamp.IsZero() || len(pod.Spec.Containers) == 0 {
		return false
	}
	statuses := make(map[string]corev1.ContainerStatus, len(pod.Status.ContainerStatuses))
	for _, status := range pod.Status.ContainerStatuses {
		statuses[status.Name] = status
	}
	for _, container := range pod.Spec.Containers {
		if statuses[container.Name].State.Running == nil {
			return false
		}
	}
	return true
}

// upgradeActionResult represents the result of executing an upgrade hook.
type upgradeActionResult struct {
	Succeeded bool
	Message   string
}

// executeUpgradeAction executes an upgrade action and returns the result.
// If action is nil (not configured), it returns success directly.
// If pod is nil and action is configured, it returns failure.
func (r *UpgradeControl) executeUpgradeAction(ctx context.Context, pod *corev1.Pod, box *agentsv1alpha1.Sandbox, action *agentsv1alpha1.UpgradeAction) upgradeActionResult {
	if action == nil {
		return upgradeActionResult{Succeeded: true, Message: "no hook configured, skipped"}
	}
	if pod == nil {
		return upgradeActionResult{Succeeded: false, Message: "pod not found, cannot execute hook"}
	}

	exitCode, stdout, stderr, err := r.lifecycleHookFunc(ctx, box, action)
	if err != nil {
		msg := fmt.Sprintf("hook execution error: %v, stderr: %s, stdout: %s", err, stderr, stdout)
		return upgradeActionResult{Succeeded: false, Message: utils.TruncateConditionMessage(msg)}
	}
	if exitCode != 0 {
		msg := fmt.Sprintf("hook failed with exit code %d, stderr: %s, stdout: %s", exitCode, stderr, stdout)
		return upgradeActionResult{Succeeded: false, Message: utils.TruncateConditionMessage(msg)}
	}
	return upgradeActionResult{Succeeded: true, Message: fmt.Sprintf("hook succeeded, stdout: %s", stdout)}
}

// hasUpgradeAction checks if the sandbox has a non-empty upgrade action configured.
// If pre is true, checks PreUpgrade; otherwise checks PostUpgrade.
func hasUpgradeAction(box *agentsv1alpha1.Sandbox, pre bool) bool {
	if box.Spec.Lifecycle == nil {
		return false
	}
	var action *agentsv1alpha1.UpgradeAction
	if pre {
		action = box.Spec.Lifecycle.PreUpgrade
	} else {
		action = box.Spec.Lifecycle.PostUpgrade
	}
	if action == nil || action.Exec == nil || len(action.Exec.Command) == 0 {
		return false
	}
	return true
}
