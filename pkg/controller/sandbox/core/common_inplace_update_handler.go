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
	"errors"
	"fmt"
	"strings"

	agentsv1alpha1 "github.com/openkruise/agents/api/v1alpha1"
	"github.com/openkruise/agents/pkg/tracing"
	"github.com/openkruise/agents/pkg/utils/inplaceupdate"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// The shared engine only owns the Pod update facts; Conditions, events and the
// final readiness gate are handled by the caller. The error class describes the
// cause, does not directly decide whether to retry, and does not prove the old
// Pod is healthy.
type inplaceErrorClass int

const (
	inplaceClassUpdateFailed inplaceErrorClass = iota
	inplaceClassUntrackedPod
	inplaceClassUnsupportedChange
	inplaceClassQoSRejected
	inplaceClassStateCorrupted
)

// inplaceUpdateError is the single error type the engine returns for
// classified step failures: a class for dispatch plus a message for humans.
// The underlying cause, when there is one, stays reachable through Unwrap
// and is what requeuing branches hand back to the reconciler.
type inplaceUpdateError struct {
	Class inplaceErrorClass
	Cause error
	msg   string
}

func (e *inplaceUpdateError) Error() string {
	switch {
	case e.msg != "" && e.Cause != nil:
		return e.msg + ": " + e.Cause.Error()
	case e.msg != "":
		return e.msg
	case e.Cause != nil:
		return e.Cause.Error()
	}
	return "in-place update failed"
}

func (e *inplaceUpdateError) Unwrap() error { return e.Cause }

// newInplaceError builds a classified failure with a fixed message.
func newInplaceError(class inplaceErrorClass, msg string) *inplaceUpdateError {
	return &inplaceUpdateError{Class: class, msg: msg}
}

// wrapInplaceError builds a classified failure around an underlying cause;
// prefix may be empty when the cause message stands on its own.
func wrapInplaceError(class inplaceErrorClass, prefix string, cause error) *inplaceUpdateError {
	return &inplaceUpdateError{Class: class, msg: prefix, Cause: cause}
}

// Unclassified errors also fall under UpdateFailed, keeping the original cause
// for the caller to decide how to handle it.
func classifyInplaceError(err error) inplaceErrorClass {
	var ie *inplaceUpdateError
	if errors.As(err, &ie) {
		return ie.Class
	}
	return inplaceClassUpdateFailed
}

// SUO 原地 adapter 使用此分类；Claim 和重建保留各自原有的错误处理。
func isTerminalInplaceError(err error) bool {
	if classifyInplaceError(err) != inplaceClassUpdateFailed {
		return true
	}
	var resizeErr *inplaceupdate.ResizeNotSupportedError
	var applyErr *inplaceupdate.ResizeInfeasibleError
	var imageErr *inplaceupdate.ImagePullFailedError
	return errors.As(err, &resizeErr) || errors.As(err, &applyErr) ||
		errors.As(err, &imageErr) || apierrors.IsForbidden(err) ||
		apierrors.IsUnauthorized(err) || apierrors.IsInvalid(err) ||
		apierrors.IsBadRequest(err) || apierrors.IsMethodNotSupported(err)
}

// The step is still valid when error is non-nil; Succeeded only means the
// configuration took effect, not that the Pod is Ready.
type inplaceUpdateStepResult int

const (
	// Pre-check failed; this write has not been issued yet.
	inplaceUpdateStepInProgress inplaceUpdateStepResult = iota
	// A write has been attempted or is waiting to take effect, including write
	// failures and partially successful writes.
	inplaceUpdateStepPatchDelivered
	inplaceUpdateStepSucceeded
)

// SUO 在 UpgradePod 的实际写入前校验；不改变共享 PreUpgrade 的执行时机。
// 未完成的上一轮更新不阻止修正目标。
func validateInplaceUpdate(pod *corev1.Pod, box *agentsv1alpha1.Sandbox) (*inplaceupdate.InPlaceUpdateState, error) {
	if pod.Labels[agentsv1alpha1.PodLabelTemplateHash] == "" {
		return nil, newInplaceError(inplaceClassUntrackedPod, "pod has no template-hash label and does not support in-place update")
	}
	_, immutable := HashSandbox(box)
	if recorded := box.Annotations[agentsv1alpha1.SandboxHashImmutablePart]; recorded != "" && recorded != immutable {
		return nil, newInplaceError(inplaceClassUnsupportedChange, "in-place update only supports changing container images, resources and template metadata")
	}
	// init container 不由原地执行器写入；仅比较模板声明的镜像和资源，忽略注入的容器。
	for _, desired := range box.Spec.Template.Spec.InitContainers {
		found := false
		for _, actual := range pod.Spec.InitContainers {
			if actual.Name == desired.Name {
				found = actual.Image == desired.Image && inplaceupdate.ResourcesExactlyEqual(desired.Resources, actual.Resources)
				break
			}
		}
		if !found {
			return nil, newInplaceError(inplaceClassUnsupportedChange, "InplaceUpdate does not support init container changes")
		}
	}
	state, err := inplaceupdate.GetPodInPlaceUpdateState(pod)
	if err != nil {
		return nil, wrapInplaceError(inplaceClassStateCorrupted, "cannot determine in-place update progress", err)
	}
	if orig, target, changed := inplaceupdate.TargetConvergenceMode.CheckResizeQoSChange(box, pod); changed {
		return nil, newInplaceError(inplaceClassQoSRejected, fmt.Sprintf("resource resize would change QoS class from %s to %s, resize rejected", orig, target))
	}
	return state, nil
}

// Configuration taking effect and final Ready are judged separately; the wait
// diagnostics are produced by the adapter based on Pod status.
func handleInPlaceUpdateCommon(ctx context.Context, control *inplaceupdate.InPlaceUpdateControl,
	pod *corev1.Pod, box *agentsv1alpha1.Sandbox, targetRevision string, mode inplaceupdate.UpdateMode,
) (inplaceUpdateStepResult, error) {
	// 兼容路径的校验及报告顺序由 Claim adapter 保留；这里仅观察已有更新或执行写入。
	if mode != inplaceupdate.TargetConvergenceMode {
		if pod.Labels[agentsv1alpha1.PodLabelTemplateHash] == targetRevision {
			completed, err := inplaceupdate.IsInplaceUpdateCompleted(ctx, pod)
			return inplaceCompletionStep(completed), err
		}
		state, err := inplaceupdate.GetPodInPlaceUpdateState(pod)
		if err != nil {
			return inplaceUpdateStepInProgress, err
		}
		if state != nil {
			completed, err := mode.IsInplaceUpdateCompleted(ctx, pod, state)
			return inplaceCompletionStep(completed), err
		}
		changed, err := deliverInplacePatch(ctx, control, pod, box, targetRevision, mode)
		return inplaceCompletionStep(!changed && err == nil), err
	}
	if err := ctx.Err(); err != nil {
		return inplaceUpdateStepInProgress, wrapInplaceError(inplaceClassUpdateFailed, "update cancelled", err)
	}
	state, err := validateInplaceUpdate(pod, box)
	if err != nil {
		return inplaceUpdateStepInProgress, err
	}
	metadataOnly := isMetadataOnlyChangeWithMode(pod, box, inplaceupdate.TargetConvergenceMode)
	if pod.Labels[agentsv1alpha1.PodLabelTemplateHash] == targetRevision && metadataOnly {
		return observeInplaceUpdate(ctx, pod, state)
	}

	// Deliver the new target directly; the lower layer continues any pending
	// resource tracking and rebuilds the image target record.
	if _, err := deliverInplacePatch(ctx, control, pod, box, targetRevision, inplaceupdate.TargetConvergenceMode); err != nil {
		return inplaceUpdateStepPatchDelivered, wrapInplaceError(inplaceClassUpdateFailed, "cannot deliver in-place update", err)
	}
	if metadataOnly {
		return observeInplaceUpdate(ctx, pod, state)
	}
	return inplaceUpdateStepPatchDelivered, nil
}

func inplaceCompletionStep(completed bool) inplaceUpdateStepResult {
	if completed {
		return inplaceUpdateStepSucceeded
	}
	return inplaceUpdateStepPatchDelivered
}

func observeInplaceUpdate(ctx context.Context, pod *corev1.Pod, state *inplaceupdate.InPlaceUpdateState) (inplaceUpdateStepResult, error) {
	completed, err := inplaceupdate.TargetConvergenceMode.IsInplaceUpdateCompleted(ctx, pod, state)
	if err != nil {
		return inplaceUpdateStepPatchDelivered, wrapInplaceError(inplaceClassUpdateFailed, "in-place pod update failed", err)
	}
	return inplaceCompletionStep(completed), nil
}

// describeInplaceWaitReason reports, from pod status facts only, why an
// in-flight in-place round may not be progressing: containers stuck in a
// waiting state (e.g. ImagePullBackOff, ErrImagePull) with the kubelet's
// reason and message. It returns "" when nothing abnormal is visible, so
// callers can distinguish "normally progressing" from "visibly stuck".
func describeInplaceWaitReason(pod *corev1.Pod) string {
	var parts []string
	for i := range pod.Status.ContainerStatuses {
		cs := &pod.Status.ContainerStatuses[i]
		if cs.State.Waiting == nil {
			continue
		}
		reason := cs.State.Waiting.Reason
		if cs.State.Waiting.Message != "" {
			reason += ": " + cs.State.Waiting.Message
		}
		parts = append(parts, fmt.Sprintf("container %s waiting: %s", cs.Name, reason))
	}
	return strings.Join(parts, "; ")
}

// deliverInplacePatch invokes control.Update under a PatchPod tracing span.
// Write retention is handled by the write-tracking client underneath.
func deliverInplacePatch(
	ctx context.Context,
	control *inplaceupdate.InPlaceUpdateControl,
	pod *corev1.Pod,
	box *agentsv1alpha1.Sandbox,
	targetRevision string,
	mode inplaceupdate.UpdateMode,
) (bool, error) {
	opts := inplaceupdate.InPlaceUpdateOptions{
		Mode:     mode,
		Pod:      pod,
		Box:      box,
		Revision: targetRevision,
	}
	patchCtx, span := tracing.StartControllerSpan(ctx, tracing.SpanControllerPatchPod)
	changed, err := control.Update(patchCtx, opts)
	tracing.EndSpan(patchCtx, span, err)
	return changed, err
}

// isMetadataOnlyChange returns true if the only difference between the pod and
// the sandbox template is metadata (labels/annotations), with no image or
// resource changes. When this is the case, the controller can directly patch
// the pod metadata without going through the full in-place update flow.
func isMetadataOnlyChange(pod *corev1.Pod, box *agentsv1alpha1.Sandbox) bool {
	return isMetadataOnlyChangeWithMode(pod, box, inplaceupdate.CompatibilityMode)
}

// 默认资源比较保留至少满足目标的合同；SUO 显式要求声明的资源键精确一致。
func isMetadataOnlyChangeWithMode(pod *corev1.Pod, box *agentsv1alpha1.Sandbox, mode inplaceupdate.UpdateMode) bool {
	if box.Spec.Template == nil {
		return false
	}
	originContainers := make(map[string]corev1.Container, len(box.Spec.Template.Spec.Containers))
	for i := range box.Spec.Template.Spec.Containers {
		obj := box.Spec.Template.Spec.Containers[i]
		originContainers[obj.Name] = obj
	}
	for i := range pod.Spec.Containers {
		container := pod.Spec.Containers[i]
		origin, ok := originContainers[container.Name]
		if !ok {
			continue
		}
		if origin.Image != container.Image {
			return false
		}
		resourcesSatisfied := inplaceupdate.IsResourceSatisfied(origin.Resources, container.Resources)
		if mode == inplaceupdate.TargetConvergenceMode {
			resourcesSatisfied = inplaceupdate.ResourcesExactlyEqual(origin.Resources, container.Resources)
		}
		if !resourcesSatisfied {
			return false
		}
	}
	return true
}
