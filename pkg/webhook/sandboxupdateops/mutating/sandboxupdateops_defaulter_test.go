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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	agentsv1alpha1 "github.com/openkruise/agents/api/v1alpha1"
)

func init() {
	_ = agentsv1alpha1.AddToScheme(scheme.Scheme)
}

func newTestDefaulter(objs ...runtime.Object) *Defaulter {
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithRuntimeObjects(objs...).
		Build()
	return &Defaulter{
		Client:  fakeClient,
		Decoder: admission.NewDecoder(scheme.Scheme),
	}
}

func makeRequest(t *testing.T, op admissionv1.Operation, obj *agentsv1alpha1.SandboxUpdateOps) admission.Request {
	raw, err := json.Marshal(obj)
	require.NoError(t, err)
	return admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: op,
			Object:    runtime.RawExtension{Raw: raw},
		},
	}
}

func templateOpsFixture() *agentsv1alpha1.SandboxUpdateOps {
	return &agentsv1alpha1.SandboxUpdateOps{
		ObjectMeta: metav1.ObjectMeta{Name: "test-ops", Namespace: "default"},
		Spec: agentsv1alpha1.SandboxUpdateOpsSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
		},
	}
}

func busyboxTemplate() *corev1.PodTemplateSpec {
	return &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "main", Image: "busybox:1.0"}},
		},
	}
}

func TestHandle_TemplateRefFrozenWithDefaults(t *testing.T) {
	sbt := &agentsv1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "tpl-a", Namespace: "default"},
		Spec: agentsv1alpha1.SandboxTemplateSpec{
			Template: busyboxTemplate(),
		},
	}
	ops := templateOpsFixture()
	ops.Spec.TemplateRef = &agentsv1alpha1.SandboxTemplateRef{Name: "tpl-a"}

	h := newTestDefaulter(sbt)
	req := makeRequest(t, admissionv1.Create, ops)
	resp := h.Handle(context.Background(), req)
	require.True(t, resp.Allowed)

	// The frozen snapshot and the cleared reference must both be patched.
	var paths []string
	for _, p := range resp.Patches {
		paths = append(paths, p.Path)
	}
	assert.Contains(t, paths, "/spec/template")
	assert.Contains(t, paths, "/spec/templateRef")

	for _, p := range resp.Patches {
		if p.Path != "/spec/template" {
			continue
		}
		raw, err := json.Marshal(p.Value)
		require.NoError(t, err)
		tmpl := &corev1.PodTemplateSpec{}
		require.NoError(t, json.Unmarshal(raw, tmpl))
		// The reference content is frozen…
		require.Len(t, tmpl.Spec.Containers, 1)
		assert.Equal(t, "busybox:1.0", tmpl.Spec.Containers[0].Image)
		// …with the same pod defaults the SandboxTemplate defaulter applies,
		// so the validating self-containment check accepts it.
		assert.Equal(t, ptr.To(false), tmpl.Spec.AutomountServiceAccountToken)
		assert.Equal(t, corev1.DNSClusterFirst, tmpl.Spec.DNSPolicy)
		assert.Equal(t, corev1.RestartPolicyAlways, tmpl.Spec.RestartPolicy)
		assert.Equal(t, corev1.PullIfNotPresent, tmpl.Spec.Containers[0].ImagePullPolicy)
	}
}

func TestHandle_TemplateRefMissingRejected(t *testing.T) {
	ops := templateOpsFixture()
	ops.Spec.TemplateRef = &agentsv1alpha1.SandboxTemplateRef{Name: "no-such-template"}

	h := newTestDefaulter()
	resp := h.Handle(context.Background(), makeRequest(t, admissionv1.Create, ops))
	assert.False(t, resp.Allowed)
	assert.Contains(t, resp.Result.Message, "failed to resolve templateRef")
}

func TestHandle_InlineTemplateGetsDefaults(t *testing.T) {
	ops := templateOpsFixture()
	ops.Spec.Template = busyboxTemplate()

	h := newTestDefaulter()
	resp := h.Handle(context.Background(), makeRequest(t, admissionv1.Create, ops))
	require.True(t, resp.Allowed)

	// Defaulting an existing inline template produces fine-grained JSONPatch
	// paths under /spec/template/spec.
	var paths []string
	for _, p := range resp.Patches {
		paths = append(paths, p.Path)
	}
	assert.Contains(t, paths, "/spec/template/spec/automountServiceAccountToken")
	assert.Contains(t, paths, "/spec/template/spec/restartPolicy")

	for _, p := range resp.Patches {
		if p.Path == "/spec/template/spec/automountServiceAccountToken" {
			assert.Equal(t, false, p.Value)
		}
		if p.Path == "/spec/template/spec/restartPolicy" {
			assert.Equal(t, string(corev1.RestartPolicyAlways), p.Value)
		}
	}
}

func TestHandle_PatchModeUntouched(t *testing.T) {
	ops := templateOpsFixture()
	ops.Spec.Patch = runtime.RawExtension{Raw: []byte(`{"spec":{"containers":[{"name":"main","image":"busybox:2.0"}]}}`)}

	h := newTestDefaulter()
	resp := h.Handle(context.Background(), makeRequest(t, admissionv1.Create, ops))
	assert.True(t, resp.Allowed)
	assert.Empty(t, resp.Patches)
}

func TestHandle_NonCreateOperationAllowed(t *testing.T) {
	ops := templateOpsFixture()
	ops.Spec.TemplateRef = &agentsv1alpha1.SandboxTemplateRef{Name: "tpl-a"}

	h := newTestDefaulter()
	resp := h.Handle(context.Background(), makeRequest(t, admissionv1.Update, ops))
	assert.True(t, resp.Allowed)
	assert.Empty(t, resp.Patches)
}
