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

// 共享引擎只负责 Pod 更新事实；Condition、事件和最终就绪门槛由调用方处理。
// 错误分类描述原因，不直接决定是否重试，也不证明旧 Pod 健康。
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

// 未分类错误也归 UpdateFailed，保留原始原因供调用方决定处置。
func classifyInplaceError(err error) inplaceErrorClass {
	var ie *inplaceUpdateError
	if errors.As(err, &ie) {
		return ie.Class
	}
	return inplaceClassUpdateFailed
}

// 调用方共用错误处置规则；未知写入结果先重试观察，不冒充终止失败。
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

// isUnsupportedResizeError 判断终态错误是否为 resize 不被支持，用于把
// 通用 Failed 映射到更细的 UnsupportedResize 终态原因。
func isUnsupportedResizeError(err error) bool {
	var resizeErr *inplaceupdate.ResizeNotSupportedError
	return errors.As(err, &resizeErr)
}

// 阶段在 error 非空时仍然有效；Succeeded 只表示配置生效，不表示 Pod Ready。
type inplaceUpdateStepResult int

const (
	// 前置检查失败，尚未发起本次写入。
	inplaceUpdateStepInProgress inplaceUpdateStepResult = iota
	// 已尝试写入或正在等待生效，包括写入失败和部分写入成功。
	inplaceUpdateStepPatchDelivered
	inplaceUpdateStepSucceeded
)

// 前置检查不产生写入，Upgrade 可在 PreUpgrade 之前复用；不能用旧轮次未完成拒绝新目标。
func validateInplaceUpdate(pod *corev1.Pod, box *agentsv1alpha1.Sandbox) (*inplaceupdate.InPlaceUpdateState, error) {
	if pod.Labels[agentsv1alpha1.PodLabelTemplateHash] == "" {
		return nil, newInplaceError(inplaceClassUntrackedPod, "pod has no template-hash label and does not support in-place update")
	}
	_, immutable := HashSandbox(box)
	if recorded := box.Annotations[agentsv1alpha1.SandboxHashImmutablePart]; recorded != "" && recorded != immutable {
		return nil, newInplaceError(inplaceClassUnsupportedChange, "in-place update only supports changing container images, resources and template metadata")
	}
	state, err := inplaceupdate.GetPodInPlaceUpdateState(pod)
	if err != nil {
		return nil, wrapInplaceError(inplaceClassStateCorrupted, "cannot determine in-place update progress", err)
	}
	if orig, target, changed := inplaceupdate.CheckResizeQoSChange(box, pod); changed {
		return nil, newInplaceError(inplaceClassQoSRejected, fmt.Sprintf("resource resize would change QoS class from %s to %s, resize rejected", orig, target))
	}
	return state, nil
}

// 配置生效与最终 Ready 分开判断，等待诊断由 adapter 根据 Pod 状态生成。
func runInplaceUpdateStep(ctx context.Context, control *inplaceupdate.InPlaceUpdateControl,
	pod *corev1.Pod, box *agentsv1alpha1.Sandbox, targetRevision string,
) (inplaceUpdateStepResult, error) {
	if err := ctx.Err(); err != nil {
		return inplaceUpdateStepInProgress, wrapInplaceError(inplaceClassUpdateFailed, "update cancelled", err)
	}
	state, err := validateInplaceUpdate(pod, box)
	if err != nil {
		return inplaceUpdateStepInProgress, err
	}
	metadataOnly := isMetadataOnlyChange(pod, box)
	if pod.Labels[agentsv1alpha1.PodLabelTemplateHash] == targetRevision && metadataOnly {
		return observeInplaceUpdate(ctx, pod, state)
	}

	// 新目标直接下发；底层负责延续未落实的资源跟踪并重建镜像目标记录。
	if _, err := deliverInplacePatch(ctx, control, pod, box, targetRevision); err != nil {
		return inplaceUpdateStepPatchDelivered, wrapInplaceError(inplaceClassUpdateFailed, "cannot deliver in-place update", err)
	}
	if metadataOnly {
		return observeInplaceUpdate(ctx, pod, state)
	}
	return inplaceUpdateStepPatchDelivered, nil
}

func observeInplaceUpdate(ctx context.Context, pod *corev1.Pod, state *inplaceupdate.InPlaceUpdateState) (inplaceUpdateStepResult, error) {
	completed, err := inplaceupdate.IsInplaceUpdateCompleted(ctx, pod, state)
	if err != nil {
		return inplaceUpdateStepPatchDelivered, wrapInplaceError(inplaceClassUpdateFailed, "in-place pod update failed", err)
	}
	if completed {
		return inplaceUpdateStepSucceeded, nil
	}
	return inplaceUpdateStepPatchDelivered, nil
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
) (bool, error) {
	opts := inplaceupdate.InPlaceUpdateOptions{
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
//
// Resource comparison is exact for every resource declared in the sandbox
// template: the pod must have the same value for each. Extra resources
// injected into the pod (e.g., by LimitRanger or other admission webhooks) are
// ignored so that metadata-only changes are not mistakenly treated as
// in-place updates. Using exact comparison (not >=) ensures that lowering a
// resource in the template is detected as a real change requiring an in-place
// resize, not a no-op metadata patch that would bypass the QoS guard.
func isMetadataOnlyChange(pod *corev1.Pod, box *agentsv1alpha1.Sandbox) bool {
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
		if !inplaceupdate.ResourcesExactlyEqual(origin.Resources, container.Resources) {
			return false
		}
	}
	return true
}
