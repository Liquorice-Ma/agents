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

package sandboxupdateops

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	agentsv1alpha1 "github.com/openkruise/agents/api/v1alpha1"
)

func TestApplySandboxPatch_SuccessfulTemplatePatch(t *testing.T) {
	ops := &agentsv1alpha1.SandboxUpdateOps{
		ObjectMeta: metav1.ObjectMeta{Name: "ops-1", Namespace: "default"},
		Spec: agentsv1alpha1.SandboxUpdateOpsSpec{
			Patch: mustMarshalPatch(corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "main", Image: "nginx:2.0"},
					},
				},
			}),
		},
	}
	sbx := &agentsv1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sbx-1",
			Namespace: "default",
			Labels:    map[string]string{"app": "test"},
		},
		Spec: agentsv1alpha1.SandboxSpec{
			EmbeddedSandboxTemplate: agentsv1alpha1.EmbeddedSandboxTemplate{
				Template: &corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "main", Image: "nginx:1.0"},
						},
					},
				},
			},
		},
	}

	r := newTestReconciler(sbx)
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(sbx), sbx))
	err := r.applySandboxPatch(context.Background(), sbx, ops)
	assert.NoError(t, err)

	// Verify the sandbox was patched
	updated := &agentsv1alpha1.Sandbox{}
	err = r.Get(context.Background(), types.NamespacedName{Name: "sbx-1", Namespace: "default"}, updated)
	assert.NoError(t, err)
	assert.Equal(t, "nginx:2.0", updated.Spec.Template.Spec.Containers[0].Image)
}

func TestApplySandboxPatch_SetsUpgradePolicyRecreate(t *testing.T) {
	ops := &agentsv1alpha1.SandboxUpdateOps{
		ObjectMeta: metav1.ObjectMeta{Name: "ops-1", Namespace: "default"},
		Spec:       agentsv1alpha1.SandboxUpdateOpsSpec{},
	}
	sbx := &agentsv1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sbx-1",
			Namespace: "default",
			Labels:    map[string]string{"app": "test"},
		},
		Spec: agentsv1alpha1.SandboxSpec{
			EmbeddedSandboxTemplate: agentsv1alpha1.EmbeddedSandboxTemplate{
				Template: &corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "main", Image: "busybox"},
						},
					},
				},
			},
		},
	}

	r := newTestReconciler(sbx)
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(sbx), sbx))
	err := r.applySandboxPatch(context.Background(), sbx, ops)
	assert.NoError(t, err)

	updated := &agentsv1alpha1.Sandbox{}
	err = r.Get(context.Background(), types.NamespacedName{Name: "sbx-1", Namespace: "default"}, updated)
	assert.NoError(t, err)
	assert.NotNil(t, updated.Spec.UpgradePolicy)
	assert.Equal(t, agentsv1alpha1.SandboxUpgradePolicyRecreate, updated.Spec.UpgradePolicy.Type)
}

func TestApplySandboxPatch_InplaceUpdateSetsInplacePolicy(t *testing.T) {
	ops := &agentsv1alpha1.SandboxUpdateOps{
		ObjectMeta: metav1.ObjectMeta{Name: "ops-1", Namespace: "default"},
		Spec: agentsv1alpha1.SandboxUpdateOpsSpec{
			UpdateStrategy: agentsv1alpha1.SandboxUpdateOpsStrategy{
				Type: agentsv1alpha1.SandboxUpdateOpsStrategyInplaceUpdate,
			},
		},
	}
	sbx := &agentsv1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sbx-1",
			Namespace: "default",
			Labels:    map[string]string{"app": "test"},
		},
		Spec: agentsv1alpha1.SandboxSpec{
			EmbeddedSandboxTemplate: agentsv1alpha1.EmbeddedSandboxTemplate{
				Template: &corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "main", Image: "busybox"},
						},
					},
				},
			},
			// Simulate a policy left over from a previous Recreate ops; the
			// InplaceUpdate strategy must replace it so the sandbox controller
			// runs the upgrade lifecycle with the in-place UpgradePod step.
			UpgradePolicy: &agentsv1alpha1.SandboxUpgradePolicy{
				Type: agentsv1alpha1.SandboxUpgradePolicyRecreate,
			},
		},
	}

	r := newTestReconciler(sbx)
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(sbx), sbx))
	err := r.applySandboxPatch(context.Background(), sbx, ops)
	assert.NoError(t, err)

	updated := &agentsv1alpha1.Sandbox{}
	err = r.Get(context.Background(), types.NamespacedName{Name: "sbx-1", Namespace: "default"}, updated)
	assert.NoError(t, err)
	require.NotNil(t, updated.Spec.UpgradePolicy)
	assert.Equal(t, agentsv1alpha1.SandboxUpgradePolicyInplaceUpdate, updated.Spec.UpgradePolicy.Type)
}

func TestApplySandboxPatch_CheckpointRestoreSetsCheckpointRestorePolicy(t *testing.T) {
	ops := &agentsv1alpha1.SandboxUpdateOps{
		ObjectMeta: metav1.ObjectMeta{Name: "ops-1", Namespace: "default"},
		Spec: agentsv1alpha1.SandboxUpdateOpsSpec{
			UpdateStrategy: agentsv1alpha1.SandboxUpdateOpsStrategy{
				Type: agentsv1alpha1.SandboxUpdateOpsStrategyCheckpointRestore,
			},
		},
	}
	sbx := &agentsv1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sbx-1",
			Namespace: "default",
			Labels:    map[string]string{"app": "test"},
		},
		Spec: agentsv1alpha1.SandboxSpec{
			EmbeddedSandboxTemplate: agentsv1alpha1.EmbeddedSandboxTemplate{
				Template: &corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "main", Image: "busybox"},
						},
					},
				},
			},
		},
	}

	r := newTestReconciler(sbx)
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(sbx), sbx))
	err := r.applySandboxPatch(context.Background(), sbx, ops)
	assert.NoError(t, err)

	updated := &agentsv1alpha1.Sandbox{}
	err = r.Get(context.Background(), types.NamespacedName{Name: "sbx-1", Namespace: "default"}, updated)
	assert.NoError(t, err)
	require.NotNil(t, updated.Spec.UpgradePolicy)
	assert.Equal(t, agentsv1alpha1.SandboxUpgradePolicyCheckpointRestore, updated.Spec.UpgradePolicy.Type)
}

func TestApplySandboxPatch_NewOperation(t *testing.T) {
	for _, strategy := range []agentsv1alpha1.SandboxUpdateOpsStrategyType{
		agentsv1alpha1.SandboxUpdateOpsStrategyInplaceUpdate,
		agentsv1alpha1.SandboxUpdateOpsStrategyRecreate,
	} {
		for _, paused := range []bool{false, true} {
			timeout := int32(47)
			for _, config := range []struct {
				withLifecycle bool
				timeout       *int32
			}{{false, nil}, {true, nil}, {false, &timeout}, {true, &timeout}} {
				t.Run(fmt.Sprintf("%s/paused=%t/lifecycle=%t/defaultTimeout=%t", strategy, paused, config.withLifecycle, config.timeout == nil), func(t *testing.T) {
					ops := newSandboxUpdateOps("new-ops", "default", agentsv1alpha1.SandboxUpdateOpsUpdating, false, nil)
					ops.UID = "new-operation"
					ops.Spec.UpdateStrategy.Type = strategy
					ops.Spec.UpdateStrategy.TimeoutSeconds = config.timeout
					ops.Spec.Patch = mustMarshalPatch(corev1.PodTemplateSpec{Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "main", Image: "busybox:2.0"}},
					}})
					if config.withLifecycle {
						ops.Spec.Lifecycle = &agentsv1alpha1.SandboxLifecycle{PostUpgrade: &agentsv1alpha1.UpgradeAction{
							Exec: &corev1.ExecAction{Command: []string{"new-hook"}},
						}}
					}
					sbx := newSandbox("operation-patch", "default", "old-ops", agentsv1alpha1.SandboxUpgrading, nil)
					if paused {
						sbx.Spec.Paused = true
						sbx.Status.Phase = agentsv1alpha1.SandboxPaused
						sbx.Status.Conditions = []metav1.Condition{{Type: string(agentsv1alpha1.SandboxConditionPaused), Status: metav1.ConditionTrue}}
					}
					oldTimeout := int32(100)
					sbx.Spec.UpgradePolicy = &agentsv1alpha1.SandboxUpgradePolicy{Type: agentsv1alpha1.SandboxUpgradePolicyRecreate, TimeoutSeconds: &oldTimeout}
					sbx.Annotations = map[string]string{
						agentsv1alpha1.AnnotationUpgradeOperation:     "old-operation",
						agentsv1alpha1.AnnotationUpgradeResumeTrigger: agentsv1alpha1.True,
					}
					sbx.Labels[agentsv1alpha1.LabelSandboxUpgradeFailed] = agentsv1alpha1.True
					sbx.Spec.Lifecycle = &agentsv1alpha1.SandboxLifecycle{PreUpgrade: &agentsv1alpha1.UpgradeAction{
						Exec: &corev1.ExecAction{Command: []string{"old-hook"}},
					}}
					sbx.Status.UpgradeProgress = &agentsv1alpha1.SandboxUpgradeProgress{
						OperationID: "old-operation", PreUpgrade: "Succeeded", PostUpgrade: "Succeeded",
					}
					r := newTestReconciler(sbx)
					require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(sbx), sbx))
					original := sbx.DeepCopy()
					patches := 0
					r.Client = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
						Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
							patches++
							return c.Patch(ctx, obj, patch, opts...)
						},
					})
					require.NoError(t, r.applySandboxPatch(t.Context(), sbx, ops))
					require.Equal(t, 1, patches)
					require.Equal(t, original, sbx)
					updated := &agentsv1alpha1.Sandbox{}
					require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(sbx), updated))
					t.Cleanup(func() { ResourceVersionExpectations.Delete(updated) })
					require.Equal(t, string(ops.UID), updated.Annotations[agentsv1alpha1.AnnotationUpgradeOperation])
					require.Equal(t, ops.Name, updated.Labels[agentsv1alpha1.LabelSandboxUpdateOps])
					require.NotContains(t, updated.Labels, agentsv1alpha1.LabelSandboxUpgradeFailed)
					require.NotNil(t, updated.Spec.UpgradePolicy)
					require.Equal(t, string(strategy), string(updated.Spec.UpgradePolicy.Type))
					require.Equal(t, config.timeout, updated.Spec.UpgradePolicy.TimeoutSeconds)
					require.Equal(t, ops.Spec.Lifecycle, updated.Spec.Lifecycle)
					// SUO 只下发新身份；旧进度由 Sandbox Controller 接纳新操作时清理。
					require.Equal(t, original.Status, updated.Status)
					if paused {
						require.Equal(t, "busybox:1.0", updated.Spec.Template.Spec.Containers[0].Image)
						require.Equal(t, agentsv1alpha1.True, updated.Annotations[agentsv1alpha1.AnnotationUpgradeResumeTrigger])
						// 模拟本轮恢复已完成；真实调谐门槛由第二阶段 Reconcile 用例覆盖。
						updated.Status.Phase = agentsv1alpha1.SandboxUpgrading
						updated.Status.UpgradeProgress.OperationID = string(ops.UID)
						updated.Status.Conditions = []metav1.Condition{{Type: string(agentsv1alpha1.SandboxConditionUpgrading), Status: metav1.ConditionFalse, Reason: agentsv1alpha1.SandboxUpgradingReasonResumeSucceed}}
						require.NoError(t, r.Status().Update(t.Context(), updated))
						phaseOne := updated.DeepCopy()
						require.NoError(t, r.applyTemplatePatch(t.Context(), updated, ops))
						require.Equal(t, phaseOne, updated)
						require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(sbx), updated))
						require.Equal(t, 2, patches)
						require.Equal(t, phaseOne.Spec.UpgradePolicy, updated.Spec.UpgradePolicy)
						require.Equal(t, phaseOne.Spec.Lifecycle, updated.Spec.Lifecycle)
						require.Equal(t, phaseOne.Status, updated.Status)
						require.Equal(t, string(ops.UID), updated.Annotations[agentsv1alpha1.AnnotationUpgradeOperation])
					}
					require.Equal(t, "busybox:2.0", updated.Spec.Template.Spec.Containers[0].Image)
					require.NotContains(t, updated.Annotations, agentsv1alpha1.AnnotationUpgradeResumeTrigger)
				})
			}
		}
	}
}

func TestApplySandboxPatch_StaleVersion(t *testing.T) {
	for _, phaseTwo := range []bool{false, true} {
		t.Run(fmt.Sprintf("phaseTwo=%t", phaseTwo), func(t *testing.T) {
			ops := newSandboxUpdateOps("stale-ops", "default", agentsv1alpha1.SandboxUpdateOpsUpdating, false, nil)
			ops.UID = "old-operation"
			ops.Spec.Patch = mustMarshalPatch(corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "main", Image: "busybox:2.0"}},
			}})
			sbx := newSandbox("stale-patch", "default", ops.Name, agentsv1alpha1.SandboxRunning, nil)
			r := newTestReconciler(sbx)
			require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(sbx), sbx))
			current := sbx.DeepCopy()
			current.Annotations = map[string]string{agentsv1alpha1.AnnotationUpgradeOperation: "new-operation"}
			current.Spec.Template.Spec.Containers[0].Image = "busybox:3.0"
			current.Spec.UpgradePolicy = &agentsv1alpha1.SandboxUpgradePolicy{Type: agentsv1alpha1.SandboxUpgradePolicyInplaceUpdate}
			require.NoError(t, r.Update(t.Context(), current))
			var err error
			if phaseTwo {
				err = r.applyTemplatePatch(t.Context(), sbx, ops)
			} else {
				err = r.applySandboxPatch(t.Context(), sbx, ops)
			}
			require.True(t, apierrors.IsConflict(err), "expected conflict, got %v", err)
			stored := &agentsv1alpha1.Sandbox{}
			require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(sbx), stored))
			require.Equal(t, current, stored)
		})
	}
}

func TestValidateInplaceUpdateFeasible_EmptyInputReturnsEmpty(t *testing.T) {
	ops := &agentsv1alpha1.SandboxUpdateOps{
		Spec: agentsv1alpha1.SandboxUpdateOpsSpec{
			Patch: runtime.RawExtension{Raw: []byte(`{"spec":{"containers":[{"name":"main","image":"v2"}]}}`)},
		},
	}
	// A sandbox without an inline template short-circuits: nothing can change
	// the immutable part.
	sbxNoTemplate := &agentsv1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: "sbx-1", Namespace: "default"},
		Spec:       agentsv1alpha1.SandboxSpec{},
	}
	assert.Empty(t, validateInplaceUpdateFeasible(sbxNoTemplate, ops))

	// An empty patch also short-circuits.
	sbx := &agentsv1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: "sbx-1", Namespace: "default"},
		Spec: agentsv1alpha1.SandboxSpec{
			EmbeddedSandboxTemplate: agentsv1alpha1.EmbeddedSandboxTemplate{
				Template: &corev1.PodTemplateSpec{},
			},
		},
	}
	assert.Empty(t, validateInplaceUpdateFeasible(sbx, &agentsv1alpha1.SandboxUpdateOps{}))
}

func TestApplySandboxPatch_CopiesLifecycle(t *testing.T) {
	lifecycle := &agentsv1alpha1.SandboxLifecycle{
		PreUpgrade: &agentsv1alpha1.UpgradeAction{
			Exec: &corev1.ExecAction{
				Command: []string{"/bin/bash", "-c", "backup.sh"},
			},
			TimeoutSeconds: 30,
		},
	}
	ops := &agentsv1alpha1.SandboxUpdateOps{
		ObjectMeta: metav1.ObjectMeta{Name: "ops-1", Namespace: "default"},
		Spec: agentsv1alpha1.SandboxUpdateOpsSpec{
			Lifecycle: lifecycle,
		},
	}
	sbx := &agentsv1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sbx-1",
			Namespace: "default",
			Labels:    map[string]string{"app": "test"},
		},
		Spec: agentsv1alpha1.SandboxSpec{
			EmbeddedSandboxTemplate: agentsv1alpha1.EmbeddedSandboxTemplate{
				Template: &corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "main", Image: "busybox"},
						},
					},
				},
			},
		},
	}

	r := newTestReconciler(sbx)
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(sbx), sbx))
	err := r.applySandboxPatch(context.Background(), sbx, ops)
	assert.NoError(t, err)

	updated := &agentsv1alpha1.Sandbox{}
	err = r.Get(context.Background(), types.NamespacedName{Name: "sbx-1", Namespace: "default"}, updated)
	assert.NoError(t, err)
	assert.NotNil(t, updated.Spec.Lifecycle)
	assert.NotNil(t, updated.Spec.Lifecycle.PreUpgrade)
	assert.Equal(t, []string{"/bin/bash", "-c", "backup.sh"}, updated.Spec.Lifecycle.PreUpgrade.Exec.Command)
	assert.Equal(t, int32(30), updated.Spec.Lifecycle.PreUpgrade.TimeoutSeconds)
}

func TestApplySandboxPatch_AddsTrackingLabel(t *testing.T) {
	ops := &agentsv1alpha1.SandboxUpdateOps{
		ObjectMeta: metav1.ObjectMeta{Name: "my-ops", Namespace: "default"},
		Spec:       agentsv1alpha1.SandboxUpdateOpsSpec{},
	}
	sbx := &agentsv1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sbx-1",
			Namespace: "default",
			Labels:    map[string]string{"app": "test"},
		},
		Spec: agentsv1alpha1.SandboxSpec{
			EmbeddedSandboxTemplate: agentsv1alpha1.EmbeddedSandboxTemplate{
				Template: &corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "main", Image: "busybox"},
						},
					},
				},
			},
		},
	}

	r := newTestReconciler(sbx)
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(sbx), sbx))
	err := r.applySandboxPatch(context.Background(), sbx, ops)
	assert.NoError(t, err)

	updated := &agentsv1alpha1.Sandbox{}
	err = r.Get(context.Background(), types.NamespacedName{Name: "sbx-1", Namespace: "default"}, updated)
	assert.NoError(t, err)
	assert.Equal(t, "my-ops", updated.Labels[agentsv1alpha1.LabelSandboxUpdateOps])
}

func TestApplySandboxPatch_InvalidPatchJSON(t *testing.T) {
	ops := &agentsv1alpha1.SandboxUpdateOps{
		ObjectMeta: metav1.ObjectMeta{Name: "ops-1", Namespace: "default"},
		Spec: agentsv1alpha1.SandboxUpdateOpsSpec{
			Patch: runtime.RawExtension{Raw: []byte("{invalid json}")},
		},
	}
	sbx := &agentsv1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sbx-1",
			Namespace: "default",
			Labels:    map[string]string{"app": "test"},
		},
		Spec: agentsv1alpha1.SandboxSpec{
			EmbeddedSandboxTemplate: agentsv1alpha1.EmbeddedSandboxTemplate{
				Template: &corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "main", Image: "nginx:1.0"},
						},
					},
				},
			},
		},
	}

	r := newTestReconciler(sbx)
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(sbx), sbx))
	err := r.applySandboxPatch(context.Background(), sbx, ops)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to apply strategic merge patch")
}

func TestApplySandboxPatch_PatchAPIError(t *testing.T) {
	ops := &agentsv1alpha1.SandboxUpdateOps{
		ObjectMeta: metav1.ObjectMeta{Name: "ops-1", Namespace: "default"},
		Spec:       agentsv1alpha1.SandboxUpdateOpsSpec{},
	}
	sbx := &agentsv1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sbx-1",
			Namespace: "default",
			Labels:    map[string]string{"app": "test"},
		},
		Spec: agentsv1alpha1.SandboxSpec{
			EmbeddedSandboxTemplate: agentsv1alpha1.EmbeddedSandboxTemplate{
				Template: &corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "main", Image: "busybox"},
						},
					},
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(testScheme).
		WithStatusSubresource(&agentsv1alpha1.SandboxUpdateOps{}, &agentsv1alpha1.Sandbox{}).
		WithRuntimeObjects(sbx).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				return fmt.Errorf("simulated patch error")
			},
		}).
		Build()
	r := &Reconciler{
		Client:   fakeClient,
		Scheme:   testScheme,
		Recorder: record.NewFakeRecorder(100),
	}

	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(sbx), sbx))
	err := r.applySandboxPatch(context.Background(), sbx, ops)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "simulated patch error")
}

func TestApplySandboxPatch_NilLabelsCreatesMap(t *testing.T) {
	ops := &agentsv1alpha1.SandboxUpdateOps{
		ObjectMeta: metav1.ObjectMeta{Name: "ops-1", Namespace: "default"},
		Spec:       agentsv1alpha1.SandboxUpdateOpsSpec{},
	}
	sbx := &agentsv1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sbx-1",
			Namespace: "default",
		},
		Spec: agentsv1alpha1.SandboxSpec{
			EmbeddedSandboxTemplate: agentsv1alpha1.EmbeddedSandboxTemplate{
				Template: &corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "main", Image: "busybox"},
						},
					},
				},
			},
		},
	}

	r := newTestReconciler(sbx)
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(sbx), sbx))
	err := r.applySandboxPatch(context.Background(), sbx, ops)
	assert.NoError(t, err)

	updated := &agentsv1alpha1.Sandbox{}
	err = r.Get(context.Background(), types.NamespacedName{Name: "sbx-1", Namespace: "default"}, updated)
	assert.NoError(t, err)
	assert.Equal(t, "ops-1", updated.Labels[agentsv1alpha1.LabelSandboxUpdateOps])
}

func TestApplySandboxPatch_PausedSetsResumeTriggerAnnotation(t *testing.T) {
	ops := &agentsv1alpha1.SandboxUpdateOps{
		ObjectMeta: metav1.ObjectMeta{Name: "ops-1", Namespace: "default"},
		Spec: agentsv1alpha1.SandboxUpdateOpsSpec{
			Patch: mustMarshalPatch(corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "main", Image: "nginx:2.0"},
					},
				},
			}),
		},
	}
	sbx := &agentsv1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sbx-1",
			Namespace: "default",
			Labels:    map[string]string{"app": "test"},
		},
		Spec: agentsv1alpha1.SandboxSpec{
			EmbeddedSandboxTemplate: agentsv1alpha1.EmbeddedSandboxTemplate{
				Template: &corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "main", Image: "nginx:1.0"},
						},
					},
				},
			},
		},
		Status: agentsv1alpha1.SandboxStatus{Phase: agentsv1alpha1.SandboxPaused},
	}

	r := newTestReconciler(sbx)
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(sbx), sbx))
	err := r.applySandboxPatch(context.Background(), sbx, ops)
	assert.NoError(t, err)

	updated := &agentsv1alpha1.Sandbox{}
	err = r.Get(context.Background(), types.NamespacedName{Name: "sbx-1", Namespace: "default"}, updated)
	assert.NoError(t, err)
	// Phase 1: annotation set, template NOT patched
	assert.Equal(t, agentsv1alpha1.True, updated.Annotations[agentsv1alpha1.AnnotationUpgradeResumeTrigger])
	assert.Equal(t, "nginx:1.0", updated.Spec.Template.Spec.Containers[0].Image)
	assert.NotNil(t, updated.Spec.UpgradePolicy)
	assert.Equal(t, agentsv1alpha1.SandboxUpgradePolicyRecreate, updated.Spec.UpgradePolicy.Type)
}

func TestApplyTemplatePatch_Success(t *testing.T) {
	ops := &agentsv1alpha1.SandboxUpdateOps{
		ObjectMeta: metav1.ObjectMeta{Name: "ops-1", Namespace: "default"},
		Spec: agentsv1alpha1.SandboxUpdateOpsSpec{
			Patch: mustMarshalPatch(corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "main", Image: "nginx:2.0"},
					},
				},
			}),
		},
	}
	sbx := &agentsv1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sbx-1",
			Namespace: "default",
			Labels:    map[string]string{"app": "test"},
			Annotations: map[string]string{
				agentsv1alpha1.AnnotationUpgradeResumeTrigger: agentsv1alpha1.True,
			},
		},
		Spec: agentsv1alpha1.SandboxSpec{
			EmbeddedSandboxTemplate: agentsv1alpha1.EmbeddedSandboxTemplate{
				Template: &corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "main", Image: "nginx:1.0"},
						},
					},
				},
			},
		},
	}

	r := newTestReconciler(sbx)
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(sbx), sbx))
	err := r.applyTemplatePatch(context.Background(), sbx, ops)
	assert.NoError(t, err)

	updated := &agentsv1alpha1.Sandbox{}
	err = r.Get(context.Background(), types.NamespacedName{Name: "sbx-1", Namespace: "default"}, updated)
	assert.NoError(t, err)
	// Phase 2: template patched, annotation removed
	assert.Equal(t, "nginx:2.0", updated.Spec.Template.Spec.Containers[0].Image)
	_, exists := updated.Annotations[agentsv1alpha1.AnnotationUpgradeResumeTrigger]
	assert.False(t, exists)
}

func TestApplyTemplatePatch_PatchError(t *testing.T) {
	ops := &agentsv1alpha1.SandboxUpdateOps{
		ObjectMeta: metav1.ObjectMeta{Name: "ops-1", Namespace: "default"},
		Spec: agentsv1alpha1.SandboxUpdateOpsSpec{
			Patch: mustMarshalPatch(corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "main", Image: "nginx:2.0"},
					},
				},
			}),
		},
	}
	sbx := &agentsv1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sbx-1",
			Namespace: "default",
			Labels:    map[string]string{"app": "test"},
			Annotations: map[string]string{
				agentsv1alpha1.AnnotationUpgradeResumeTrigger: agentsv1alpha1.True,
			},
		},
		Spec: agentsv1alpha1.SandboxSpec{
			EmbeddedSandboxTemplate: agentsv1alpha1.EmbeddedSandboxTemplate{
				Template: &corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "main", Image: "nginx:1.0"},
						},
					},
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(testScheme).
		WithStatusSubresource(&agentsv1alpha1.SandboxUpdateOps{}, &agentsv1alpha1.Sandbox{}).
		WithRuntimeObjects(sbx).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, cli client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				return fmt.Errorf("simulated patch error")
			},
		}).
		Build()
	r := &Reconciler{
		Client:   fakeClient,
		Scheme:   testScheme,
		Recorder: record.NewFakeRecorder(100),
	}
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(sbx), sbx))
	err := r.applyTemplatePatch(context.Background(), sbx, ops)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "simulated patch error")
}

func TestSanitizeTemplatePatch(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "annotation-only patch drops null containers",
			raw:  `{"metadata":{"annotations":{"agents.kruise.io/upgrade-marker":"v2"}},"spec":{"containers":null}}`,
			want: `{"metadata":{"annotations":{"agents.kruise.io/upgrade-marker":"v2"}}}`,
		},
		{
			name: "patch with real containers is unchanged",
			raw:  `{"spec":{"containers":[{"name":"main","image":"nginx:2.0"}]}}`,
			want: `{"spec":{"containers":[{"name":"main","image":"nginx:2.0"}]}}`,
		},
		{
			name: "null optional lists are kept as delete directives",
			raw:  `{"spec":{"containers":[{"name":"main"}],"initContainers":null,"volumes":null}}`,
			want: `{"spec":{"containers":[{"name":"main"}],"initContainers":null,"volumes":null}}`,
		},
		{
			name: "invalid json is returned unchanged",
			raw:  `{invalid json}`,
			want: `{invalid json}`,
		},
		{
			name: "patch without spec is unchanged",
			raw:  `{"metadata":{"labels":{"app":"test"}}}`,
			want: `{"metadata":{"labels":{"app":"test"}}}`,
		},
		{
			// The sanitize round trip must not corrupt int64 fields beyond
			// float64 precision (2^53), e.g. activeDeadlineSeconds or runAsUser.
			name: "large integers survive the round trip",
			raw:  `{"spec":{"containers":null,"activeDeadlineSeconds":9007199254740993,"securityContext":{"runAsUser":1000000000000000001}}}`,
			want: `{"spec":{"activeDeadlineSeconds":9007199254740993,"securityContext":{"runAsUser":1000000000000000001}}}`,
		},
		{
			name: "typical patch with ordinary numbers is unchanged",
			raw:  `{"spec":{"containers":[{"name":"main","image":"nginx:2.0"}],"terminationGracePeriodSeconds":30}}`,
			want: `{"spec":{"containers":[{"name":"main","image":"nginx:2.0"}],"terminationGracePeriodSeconds":30}}`,
		},
		{
			name: "empty object is unchanged",
			raw:  `{}`,
			want: `{}`,
		},
		{
			name: "explicit null spec is unchanged",
			raw:  `{"metadata":{"labels":{"app":"test"}},"spec":null}`,
			want: `{"metadata":{"labels":{"app":"test"}},"spec":null}`,
		},
		{
			name: "spec not an object is unchanged",
			raw:  `{"spec":"invalid"}`,
			want: `{"spec":"invalid"}`,
		},
		{
			name: "non-object json root is unchanged",
			raw:  `["not","an","object"]`,
			want: `["not","an","object"]`,
		},
		{
			// An explicit empty list is a user-authored delete-all directive
			// and must not be confused with the marshaling null artifact.
			name: "empty containers list is kept as delete-all directive",
			raw:  `{"spec":{"containers":[]}}`,
			want: `{"spec":{"containers":[]}}`,
		},
		{
			name: "null containers dropped while other spec fields are kept",
			raw:  `{"spec":{"containers":null,"nodeSelector":{"zone":"a"}}}`,
			want: `{"spec":{"nodeSelector":{"zone":"a"}}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, string(sanitizeTemplatePatch([]byte(tt.raw))))
		})
	}
}

// TestApplySandboxPatch_AnnotationOnlyPatchKeepsContainers guards against the
// regression where marshaling a PodTemplateSpec that only sets ObjectMeta
// emits "spec":{"containers":null} (PodSpec.Containers has no omitempty) and
// the strategic merge patch treated it as a delete directive, wiping the
// required containers of the sandbox template.
func TestApplySandboxPatch_AnnotationOnlyPatchKeepsContainers(t *testing.T) {
	ops := &agentsv1alpha1.SandboxUpdateOps{
		ObjectMeta: metav1.ObjectMeta{Name: "ops-1", Namespace: "default"},
		Spec: agentsv1alpha1.SandboxUpdateOpsSpec{
			Patch: mustMarshalPatch(corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{"agents.kruise.io/upgrade-marker": "v2"},
				},
			}),
		},
	}
	sbx := &agentsv1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sbx-1",
			Namespace: "default",
			Labels:    map[string]string{"app": "test"},
		},
		Spec: agentsv1alpha1.SandboxSpec{
			EmbeddedSandboxTemplate: agentsv1alpha1.EmbeddedSandboxTemplate{
				Template: &corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "main", Image: "nginx:1.0"},
						},
					},
				},
			},
		},
	}

	r := newTestReconciler(sbx)
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(sbx), sbx))
	err := r.applySandboxPatch(context.Background(), sbx, ops)
	assert.NoError(t, err)

	updated := &agentsv1alpha1.Sandbox{}
	err = r.Get(context.Background(), types.NamespacedName{Name: "sbx-1", Namespace: "default"}, updated)
	assert.NoError(t, err)
	assert.Equal(t, "v2", updated.Spec.Template.Annotations["agents.kruise.io/upgrade-marker"])
	assert.Len(t, updated.Spec.Template.Spec.Containers, 1)
	assert.Equal(t, "nginx:1.0", updated.Spec.Template.Spec.Containers[0].Image)
}

func TestIsSandboxTemplateMatchPatch_AnnotationOnlyPatch(t *testing.T) {
	newSandbox := func(annotations map[string]string) *agentsv1alpha1.Sandbox {
		return &agentsv1alpha1.Sandbox{
			ObjectMeta: metav1.ObjectMeta{Name: "sbx-1", Namespace: "default"},
			Spec: agentsv1alpha1.SandboxSpec{
				EmbeddedSandboxTemplate: agentsv1alpha1.EmbeddedSandboxTemplate{
					Template: &corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Annotations: annotations},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{Name: "main", Image: "nginx:1.0"},
							},
						},
					},
				},
			},
		}
	}
	ops := &agentsv1alpha1.SandboxUpdateOps{
		Spec: agentsv1alpha1.SandboxUpdateOpsSpec{
			Patch: mustMarshalPatch(corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{"agents.kruise.io/upgrade-marker": "v2"},
				},
			}),
		},
	}

	assert.False(t, isSandboxTemplateMatchPatch(newSandbox(nil), ops),
		"sandbox without the annotation needs an upgrade")
	assert.True(t, isSandboxTemplateMatchPatch(newSandbox(map[string]string{"agents.kruise.io/upgrade-marker": "v2"}), ops),
		"sandbox already carrying the annotation must be skipped")
}
