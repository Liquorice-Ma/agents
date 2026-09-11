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

package mutating

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"

	admissionv1 "k8s.io/api/admission/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	agentsv1alpha1 "github.com/openkruise/agents/api/v1alpha1"
	"github.com/openkruise/agents/pkg/utils/defaults"
)

// Defaulter mutates SandboxUpdateOps at creation time:
//
//   - templateRef is resolved against the SandboxTemplate in the same
//     namespace and frozen into spec.template as a snapshot; the reference is
//     cleared. The stamp protocol's per-sandbox comparison needs a stable
//     target: a live reference would let a later template edit silently
//     change the intent of an already-created ops. A missing reference
//     rejects the creation.
//   - the resulting inline template (frozen or directly specified) gets the
//     same pod defaults the SandboxTemplate defaulter applies, so the
//     validating webhook's self-containment check accepts a template that
//     omits defaulted fields.
type Defaulter struct {
	Client  client.Client
	Decoder admission.Decoder
}

// +kubebuilder:webhook:path=/default-sandboxupdateops,mutating=true,failurePolicy=fail,sideEffects=None,admissionReviewVersions=v1;v1beta1,groups=agents.kruise.io,resources=sandboxupdateops,verbs=create,versions=v1alpha1,name=md-suo.kb.io

func (h *Defaulter) Path() string {
	return "/default-sandboxupdateops"
}

func (h *Defaulter) Enabled() bool {
	return true
}

func (h *Defaulter) Handle(ctx context.Context, req admission.Request) admission.Response {
	if req.Operation != admissionv1.Create {
		return admission.Allowed("")
	}
	obj := &agentsv1alpha1.SandboxUpdateOps{}
	if err := h.Decoder.Decode(req, obj); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}
	clone := obj.DeepCopy()

	// Freeze templateRef into an inline template snapshot. From here on the
	// ops no longer tracks the referenced SandboxTemplate; new intent
	// requires a new SandboxUpdateOps.
	if obj.Spec.TemplateRef != nil {
		refTemplate := &agentsv1alpha1.SandboxTemplate{}
		if err := h.Client.Get(ctx, client.ObjectKey{Namespace: obj.Namespace, Name: obj.Spec.TemplateRef.Name}, refTemplate); err != nil {
			return admission.Errored(http.StatusUnprocessableEntity,
				fmt.Errorf("failed to resolve templateRef %q: %w", obj.Spec.TemplateRef.Name, err))
		}
		if refTemplate.Spec.Template != nil {
			obj.Spec.Template = refTemplate.Spec.Template.DeepCopy()
		}
		obj.Spec.TemplateRef = nil
	}

	setDefaultPodTemplate(obj.Spec.Template)

	if !reflect.DeepEqual(obj, clone) {
		marshaled, err := json.Marshal(obj)
		if err != nil {
			return admission.Errored(http.StatusInternalServerError, err)
		}
		return admission.PatchResponseFromRaw(req.Object.Raw, marshaled)
	}
	return admission.Allowed("")
}

// setDefaultPodTemplate applies the same defaults as the SandboxTemplate
// defaulter: the sandbox is updated from this snapshot as-is, so defaulted
// fields must be materialized here for the snapshot to be self-contained.
func setDefaultPodTemplate(template *v1.PodTemplateSpec) {
	if template == nil {
		return
	}
	if ptr.Deref(template.Spec.AutomountServiceAccountToken, true) {
		template.Spec.AutomountServiceAccountToken = ptr.To(false)
	}
	defaults.SetDefaultPodSpec(&template.Spec)
}
