/*
Copyright 2025.

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

package validating

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	intstrutil "k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/kubernetes/pkg/apis/core"
	corev1conv "k8s.io/kubernetes/pkg/apis/core/v1"
	corevalidation "k8s.io/kubernetes/pkg/apis/core/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	agentsv1alpha1 "github.com/openkruise/agents/api/v1alpha1"
	webhookutils "github.com/openkruise/agents/pkg/webhook/utils"
)

// SandboxUpdateOpsValidatingHandler handles validation for SandboxUpdateOps resources.
type SandboxUpdateOpsValidatingHandler struct {
	Client  client.Client
	Decoder admission.Decoder
}

// +kubebuilder:webhook:path=/validate-sandboxupdateops,mutating=false,failurePolicy=fail,sideEffects=None,admissionReviewVersions=v1;v1beta1,groups=agents.kruise.io,resources=sandboxupdateops,verbs=create;update,versions=v1alpha1,name=v-suo.kb.io

func (h *SandboxUpdateOpsValidatingHandler) Path() string {
	return "/validate-sandboxupdateops"
}

func (h *SandboxUpdateOpsValidatingHandler) Enabled() bool {
	return true
}

func (h *SandboxUpdateOpsValidatingHandler) Handle(ctx context.Context, req admission.Request) admission.Response {
	obj := &agentsv1alpha1.SandboxUpdateOps{}
	if err := h.Decoder.Decode(req, obj); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	switch req.Operation {
	case admissionv1.Create:
		return h.handleCreate(ctx, obj)
	case admissionv1.Update:
		return h.handleUpdate(req, obj)
	default:
		return admission.Allowed("")
	}
}

func (h *SandboxUpdateOpsValidatingHandler) handleCreate(ctx context.Context, obj *agentsv1alpha1.SandboxUpdateOps) admission.Response {
	var errList field.ErrorList
	specPath := field.NewPath("spec")

	// 1. Validate Selector is non-empty and valid
	if obj.Spec.Selector == nil {
		errList = append(errList, field.Required(specPath.Child("selector"), "selector is required"))
	} else {
		if _, err := metav1.LabelSelectorAsSelector(obj.Spec.Selector); err != nil {
			errList = append(errList, field.Invalid(specPath.Child("selector"), obj.Spec.Selector, err.Error()))
		}
	}

	// 2. Validate exactly one update mode is set: patch (legacy, namespace-
	// exclusive) or template/templateRef (stamp protocol). A patch is an
	// increment relative to the sandbox's current template, not a comparable
	// final state, so the two modes are mutually exclusive.
	patchMode := len(obj.Spec.Patch.Raw) > 0
	templateMode := obj.Spec.IsTemplateMode()
	switch {
	case patchMode && templateMode:
		errList = append(errList, field.Forbidden(specPath, "patch and template/templateRef are mutually exclusive; exactly one update mode must be set"))
	case !patchMode && !templateMode:
		errList = append(errList, field.Required(specPath, "exactly one update mode must be set: patch or template/templateRef"))
	}
	if obj.Spec.Template != nil && obj.Spec.TemplateRef != nil {
		errList = append(errList, field.Invalid(specPath.Child("templateRef"), obj.Spec.TemplateRef, "templateRef and template is mutual exclusive"))
	}
	if len(obj.Spec.VolumeClaimTemplates) != 0 {
		errList = append(errList, field.Forbidden(specPath.Child("volumeClaimTemplates"), "volumeClaimTemplates is not supported by batch update"))
	}
	// The template must be self-contained: omitted fields are removals, not
	// "keep as is", so it is validated with the same rules as
	// SandboxSet.spec.template.
	if obj.Spec.Template != nil && obj.Spec.TemplateRef == nil {
		errList = append(errList, validatePodTemplateSpec(obj.Spec.Template, specPath.Child("template"))...)
	}

	// 3. Validate MaxUnavailable if specified
	if obj.Spec.UpdateStrategy.MaxUnavailable != nil {
		if _, err := intstrutil.GetScaledValueFromIntOrPercent(
			intstrutil.ValueOrDefault(obj.Spec.UpdateStrategy.MaxUnavailable, intstrutil.FromInt(0)), 100, true); err != nil {
			errList = append(errList, field.Invalid(specPath.Child("updateStrategy", "maxUnavailable"), obj.Spec.UpdateStrategy.MaxUnavailable, "maxUnavailable is invalid"))
		}
	}

	// 4. Validate Lifecycle configuration
	if obj.Spec.Lifecycle != nil {
		lifecyclePath := specPath.Child("lifecycle")
		if obj.Spec.Lifecycle.PreUpgrade != nil && obj.Spec.Lifecycle.PreUpgrade.Exec == nil {
			errList = append(errList, field.Required(lifecyclePath.Child("preUpgrade", "exec"), "exec is required when preUpgrade is specified"))
		}
		if obj.Spec.Lifecycle.PostUpgrade != nil && obj.Spec.Lifecycle.PostUpgrade.Exec == nil {
			errList = append(errList, field.Required(lifecyclePath.Child("postUpgrade", "exec"), "exec is required when postUpgrade is specified"))
		}
	}

	// 5. When using CheckpointRestore strategy, the patch must not modify container images.
	// CheckpointRestore preserves the writable layer of containers whose image is unchanged;
	// changing an image would invalidate the checkpoint.
	// Template mode carries a full template whose actual change set is a
	// per-sandbox diff, so it cannot be checked at admission.
	if obj.Spec.UpdateStrategy.Type == agentsv1alpha1.SandboxUpdateOpsStrategyCheckpointRestore && len(obj.Spec.Patch.Raw) > 0 {
		patchTmpl := &corev1.PodTemplateSpec{}
		if err := json.Unmarshal(obj.Spec.Patch.Raw, patchTmpl); err != nil {
			errList = append(errList, field.Invalid(specPath.Child("patch"), obj.Spec.Patch, "failed to parse patch as PodTemplateSpec: "+err.Error()))
		} else if msg := validateNoImageChange(patchTmpl); msg != "" {
			errList = append(errList, field.Forbidden(specPath.Child("patch"), msg))
		}
	}

	// 6. Mixed-mode admission barrier: admit iff no active (non-terminal)
	// SandboxUpdateOps exists in the namespace, or the new ops and every
	// active ops are template-mode (which coordinate per sandbox via the
	// stamp protocol). Any combination involving patch mode degenerates to
	// namespace-exclusive serialization. The phase-based check also blocks
	// while a terminal ops is held by a finalizer during deletion.
	opsList := &agentsv1alpha1.SandboxUpdateOpsList{}
	if err := h.Client.List(ctx, opsList, client.InNamespace(obj.Namespace)); err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	for i := range opsList.Items {
		existing := &opsList.Items[i]
		if existing.Name == obj.Name {
			continue
		}
		if existing.Status.Phase == agentsv1alpha1.SandboxUpdateOpsCompleted ||
			existing.Status.Phase == agentsv1alpha1.SandboxUpdateOpsFailed {
			continue
		}
		if templateMode && existing.Spec.IsTemplateMode() {
			continue
		}
		errList = append(errList, field.Forbidden(specPath, "there is an active SandboxUpdateOps in the same namespace: "+existing.Name))
		break
	}

	if len(errList) > 0 {
		return admission.Errored(http.StatusUnprocessableEntity, errList.ToAggregate())
	}
	return admission.Allowed("")
}

// validateNoImageChange checks whether the given patch template modifies any container
// or init container image. Returns a non-empty string describing the violation if found.
func validateNoImageChange(tmpl *corev1.PodTemplateSpec) string {
	for _, c := range tmpl.Spec.Containers {
		if c.Image != "" {
			return fmt.Sprintf("CheckpointRestore strategy does not support modifying container images (container %q)", c.Name)
		}
	}
	for _, c := range tmpl.Spec.InitContainers {
		if c.Image != "" {
			return fmt.Sprintf("CheckpointRestore strategy does not support modifying init container images (container %q)", c.Name)
		}
	}
	return ""
}

// validatePodTemplateSpec validates that a template-mode snapshot is a
// self-contained, valid pod template, mirroring the SandboxSet template
// validation.
func validatePodTemplateSpec(tmpl *corev1.PodTemplateSpec, fldPath *field.Path) field.ErrorList {
	errList := field.ErrorList{}
	coreTemplate := &core.PodTemplateSpec{}
	if err := corev1conv.Convert_v1_PodTemplateSpec_To_core_PodTemplateSpec(tmpl.DeepCopy(), coreTemplate, nil); err != nil {
		errList = append(errList, field.Invalid(fldPath, tmpl, fmt.Sprintf("Convert_v1_PodTemplateSpec_To_core_PodTemplateSpec failed: %v", err)))
		return errList
	}
	errList = append(errList, corevalidation.ValidatePodTemplateSpec(coreTemplate, fldPath, webhookutils.DefaultPodValidationOptions)...)
	return errList
}

func (h *SandboxUpdateOpsValidatingHandler) handleUpdate(req admission.Request, newObj *agentsv1alpha1.SandboxUpdateOps) admission.Response {
	oldObj := &agentsv1alpha1.SandboxUpdateOps{}
	if err := h.Decoder.DecodeRaw(req.OldObject, oldObj); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	var errList field.ErrorList
	specPath := field.NewPath("spec")

	// Only updateStrategy.maxUnavailable and paused are mutable. Everything
	// else is immutable after creation: intent is bound to creationTimestamp
	// (which orders the stamps in template mode), so new intent requires a
	// new SandboxUpdateOps.
	if !reflect.DeepEqual(oldObj.Spec.Selector, newObj.Spec.Selector) {
		errList = append(errList, field.Forbidden(specPath.Child("selector"), "selector is immutable"))
	}
	if !reflect.DeepEqual(oldObj.Spec.Patch, newObj.Spec.Patch) {
		errList = append(errList, field.Forbidden(specPath.Child("patch"), "patch is immutable"))
	}
	if !reflect.DeepEqual(oldObj.Spec.EmbeddedSandboxTemplate, newObj.Spec.EmbeddedSandboxTemplate) {
		errList = append(errList, field.Forbidden(specPath.Child("template"), "template and templateRef are immutable"))
	}
	if !reflect.DeepEqual(oldObj.Spec.Lifecycle, newObj.Spec.Lifecycle) {
		errList = append(errList, field.Forbidden(specPath.Child("lifecycle"), "lifecycle is immutable"))
	}
	if oldObj.Spec.UpdateStrategy.Type != newObj.Spec.UpdateStrategy.Type {
		errList = append(errList, field.Forbidden(specPath.Child("updateStrategy", "type"), "updateStrategy.type is immutable"))
	}
	if !reflect.DeepEqual(oldObj.Spec.StateFilter, newObj.Spec.StateFilter) {
		errList = append(errList, field.Forbidden(specPath.Child("stateFilter"), "stateFilter is immutable"))
	}

	if len(errList) > 0 {
		return admission.Errored(http.StatusUnprocessableEntity, errList.ToAggregate())
	}
	return admission.Allowed("")
}
