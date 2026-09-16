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

package core

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	agentsv1alpha1 "github.com/openkruise/agents/api/v1alpha1"
	"github.com/openkruise/agents/pkg/utils"
	"github.com/openkruise/agents/pkg/utils/fieldindex"
	"github.com/openkruise/agents/pkg/utils/inplaceupdate"
)

// mockLifecycleHookFunc creates a mock LifecycleHookFunc for testing.
func mockLifecycleHookFunc(exitCode int32, stdout, stderr string, err error) LifecycleHookFunc {
	return func(ctx context.Context, box *agentsv1alpha1.Sandbox, action *agentsv1alpha1.UpgradeAction) (int32, string, string, error) {
		return exitCode, stdout, stderr, err
	}
}

func newUpgradeTestSandbox(lifecycle *agentsv1alpha1.SandboxLifecycle, upgradePolicy *agentsv1alpha1.SandboxUpgradePolicy) *agentsv1alpha1.Sandbox {
	// Default to Recreate policy if nil for backward compatibility in tests
	if upgradePolicy == nil {
		upgradePolicy = &agentsv1alpha1.SandboxUpgradePolicy{
			Type: agentsv1alpha1.SandboxUpgradePolicyRecreate,
		}
	}
	return &agentsv1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-sandbox",
			Namespace: "default",
			Annotations: map[string]string{
				agentsv1alpha1.AnnotationRuntimeURL:         "http://10.0.0.1:49983",
				agentsv1alpha1.AnnotationRuntimeAccessToken: "test-token",
				agentsv1alpha1.SandboxHashImmutablePart:     "old-hash",
			},
		},
		Spec: agentsv1alpha1.SandboxSpec{
			Lifecycle:     lifecycle,
			UpgradePolicy: upgradePolicy,
			EmbeddedSandboxTemplate: agentsv1alpha1.EmbeddedSandboxTemplate{
				Template: &corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "sandbox", Image: "test:v2"},
						},
					},
				},
			},
		},
		Status: agentsv1alpha1.SandboxStatus{
			Phase:     agentsv1alpha1.SandboxUpgrading,
			SandboxIp: "10.0.0.1",
		},
	}
}

func newRunningPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-sandbox",
			Namespace: "default",
			Labels: map[string]string{
				agentsv1alpha1.PodLabelTemplateHash: "old-revision",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "sandbox", Image: "test:v1"},
			},
		},
		Status: corev1.PodStatus{
			Phase:             corev1.PodRunning,
			PodIP:             "10.0.0.1",
			ContainerStatuses: []corev1.ContainerStatus{{Name: "sandbox", Image: "test:v1", ImageID: "img-old", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}},
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
}

func newTestCommonControl(hookFunc LifecycleHookFunc, objects ...client.Object) *commonControl {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = agentsv1alpha1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&agentsv1alpha1.Sandbox{}).WithObjects(objects...).Build()
	checkpointCtrl := NewCheckpointControl(fakeClient, record.NewFakeRecorder(100))
	podCtrl := NewPodControl(fakeClient, record.NewFakeRecorder(100), GeneratePodFromSandbox)
	initializer := &defaultSandboxInitializer{recorder: record.NewFakeRecorder(10)}
	control := &commonControl{
		Client:               fakeClient,
		recorder:             record.NewFakeRecorder(100),
		inplaceUpdateControl: inplaceupdate.NewInPlaceUpdateControl(fakeClient, inplaceupdate.DefaultGeneratePatchBodyFunc),
		rateLimiter:          NewRateLimiter(),
		checkpointControl:    checkpointCtrl,
		podControl:           podCtrl,
		lifecycleHookFunc:    hookFunc,
		initializer:          initializer,
	}
	control.upgradeControl = NewUpgradeControl(fakeClient, checkpointCtrl, podCtrl, record.NewFakeRecorder(100), hookFunc, initializer, defaultCommonSyncStatusFromPod, nil, control.inplaceUpdateControl)
	return control
}

// prepareUpgradeStepTest 准备已持久化预算和乐观锁版本的步骤测试数据。
func prepareUpgradeStepTest(t *testing.T, c client.Client, args EnsureFuncArgs) {
	t.Helper()
	// 步骤测试从已经持久化预算的位置进入；首轮预算初始化由专门用例覆盖。
	if args.NewStatus.UpgradeProgress == nil {
		now := metav1.Now()
		args.NewStatus.UpgradeProgress = &agentsv1alpha1.SandboxUpgradeProgress{
			OperationID: args.Box.Annotations[agentsv1alpha1.AnnotationUpgradeOperation],
			Revision:    args.NewStatus.UpdateRevision, StartedAt: now, Deadline: metav1.NewTime(now.Add(300 * time.Second)),
		}
	}
	stored := &agentsv1alpha1.Sandbox{}
	err := c.Get(t.Context(), client.ObjectKeyFromObject(args.Box), stored)
	if apierrors.IsNotFound(err) {
		stored = args.Box.DeepCopy()
		stored.Status = *args.NewStatus.DeepCopy()
		require.NoError(t, c.Create(t.Context(), stored))
	} else {
		require.NoError(t, err)
	}
	args.Box.ResourceVersion = stored.ResourceVersion
	args.Box.Status = *stored.Status.DeepCopy()
}

// TestUpgradePolicyPredicates covers the split of responsibilities between the
// three policy predicates: RequiresUpgradeSandbox decides whether the sandbox
// enters the Upgrading phase at all, RequiresPodReplacementUpgrade whether the
// UpgradePod step replaces the pod, and RequiresInplaceUpgrade whether it
// patches the pod in place.
func TestUpgradePolicyPredicates(t *testing.T) {
	boxWithPolicy := func(policy *agentsv1alpha1.SandboxUpgradePolicy) *agentsv1alpha1.Sandbox {
		return &agentsv1alpha1.Sandbox{
			Spec: agentsv1alpha1.SandboxSpec{UpgradePolicy: policy},
		}
	}
	tests := []struct {
		name               string
		box                *agentsv1alpha1.Sandbox
		wantUpgradeSandbox bool
		wantPodReplacement bool
		wantInplaceUpgrade bool
	}{
		{
			// The SandboxClaim path: no policy means the change is applied in place
			// from the Running phase, without the upgrade lifecycle.
			name:               "nil policy",
			box:                boxWithPolicy(nil),
			wantUpgradeSandbox: false,
			wantPodReplacement: false,
			wantInplaceUpgrade: false,
		},
		{
			name:               "empty type",
			box:                boxWithPolicy(&agentsv1alpha1.SandboxUpgradePolicy{}),
			wantUpgradeSandbox: false,
			wantPodReplacement: false,
			wantInplaceUpgrade: false,
		},
		{
			name:               "Recreate",
			box:                boxWithPolicy(&agentsv1alpha1.SandboxUpgradePolicy{Type: agentsv1alpha1.SandboxUpgradePolicyRecreate}),
			wantUpgradeSandbox: true,
			wantPodReplacement: true,
			wantInplaceUpgrade: false,
		},
		{
			name:               "CheckpointRestore",
			box:                boxWithPolicy(&agentsv1alpha1.SandboxUpgradePolicy{Type: agentsv1alpha1.SandboxUpgradePolicyCheckpointRestore}),
			wantUpgradeSandbox: true,
			wantPodReplacement: true,
			wantInplaceUpgrade: false,
		},
		{
			// InplaceUpdate runs the lifecycle but keeps the pod, so it must be in
			// the upgrade phase yet out of the pod-replacement path.
			name:               "InplaceUpdate",
			box:                boxWithPolicy(&agentsv1alpha1.SandboxUpgradePolicy{Type: agentsv1alpha1.SandboxUpgradePolicyInplaceUpdate}),
			wantUpgradeSandbox: true,
			wantPodReplacement: false,
			wantInplaceUpgrade: true,
		},
		{
			name:               "unknown type",
			box:                boxWithPolicy(&agentsv1alpha1.SandboxUpgradePolicy{Type: "SomethingElse"}),
			wantUpgradeSandbox: false,
			wantPodReplacement: false,
			wantInplaceUpgrade: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantUpgradeSandbox, RequiresUpgradeSandbox(tt.box), "RequiresUpgradeSandbox")
			assert.Equal(t, tt.wantPodReplacement, RequiresPodReplacementUpgrade(tt.box), "RequiresPodReplacementUpgrade")
			assert.Equal(t, tt.wantInplaceUpgrade, RequiresInplaceUpgrade(tt.box), "RequiresInplaceUpgrade")
		})
	}
}

func TestExecuteUpgradeAction(t *testing.T) {
	action := &agentsv1alpha1.UpgradeAction{
		Exec:           &corev1.ExecAction{Command: []string{"/bin/bash", "-c", "echo test"}},
		TimeoutSeconds: 30,
	}
	pod := newRunningPod()
	box := newUpgradeTestSandbox(&agentsv1alpha1.SandboxLifecycle{PreUpgrade: action}, nil)

	tests := []struct {
		name           string
		hookFunc       LifecycleHookFunc
		expectSuccess  bool
		expectContains string
	}{
		{
			name:           "error with stderr included in message",
			hookFunc:       mockLifecycleHookFunc(-1, "", "permission denied", fmt.Errorf("connection refused")),
			expectSuccess:  false,
			expectContains: "permission denied",
		},
		{
			name:           "error with stdout when stderr is empty",
			hookFunc:       mockLifecycleHookFunc(-1, "partial output", "", fmt.Errorf("timeout")),
			expectSuccess:  false,
			expectContains: "partial output",
		},
		{
			name:           "non-zero exit code with stderr included",
			hookFunc:       mockLifecycleHookFunc(1, "", "command not found", nil),
			expectSuccess:  false,
			expectContains: "command not found",
		},
		{
			name:           "message truncated when exceeding max length",
			hookFunc:       mockLifecycleHookFunc(-1, "", strings.Repeat("x", 1100), fmt.Errorf("exec failed")),
			expectSuccess:  false,
			expectContains: "...",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := newTestCommonControl(tt.hookFunc, box.DeepCopy(), pod.DeepCopy())
			result := ctrl.upgradeControl.executeUpgradeAction(context.Background(), pod, box, action)
			assert.Equal(t, tt.expectSuccess, result.Succeeded)
			assert.Contains(t, result.Message, tt.expectContains)
			// Verify truncation: message must not exceed MaxConditionMessageLen + len("...")
			assert.LessOrEqual(t, len(result.Message), utils.MaxConditionMessageLen+3)
		})
	}
}

func TestEnsureSandboxUpgraded(t *testing.T) {
	preUpgradeHook := &agentsv1alpha1.UpgradeAction{
		Exec:           &corev1.ExecAction{Command: []string{"/bin/bash", "-c", "echo backup"}},
		TimeoutSeconds: 30,
	}
	postUpgradeHook := &agentsv1alpha1.UpgradeAction{
		Exec:           &corev1.ExecAction{Command: []string{"/bin/bash", "-c", "echo restore"}},
		TimeoutSeconds: 30,
	}
	now := metav1.Now()

	tests := []struct {
		name            string
		pod             *corev1.Pod
		box             *agentsv1alpha1.Sandbox
		existingStatus  *agentsv1alpha1.SandboxStatus
		mockHookFunc    LifecycleHookFunc
		expectErr       bool
		expectPhase     agentsv1alpha1.SandboxPhase
		expectCondition map[string]metav1.ConditionStatus
	}{
		{
			name: "no lifecycle configured skips preUpgrade and proceeds to Phase 2",
			pod:  newRunningPod(),
			box:  newUpgradeTestSandbox(nil, nil),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase: agentsv1alpha1.SandboxUpgrading,
			},
			mockHookFunc:    mockLifecycleHookFunc(0, "", "", nil),
			expectErr:       false,
			expectPhase:     agentsv1alpha1.SandboxUpgrading,
			expectCondition: map[string]metav1.ConditionStatus{},
		},
		{
			name: "preUpgrade hook succeeds",
			pod:  newRunningPod(),
			box: newUpgradeTestSandbox(&agentsv1alpha1.SandboxLifecycle{
				PreUpgrade: preUpgradeHook,
			}, nil),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase: agentsv1alpha1.SandboxUpgrading,
			},
			mockHookFunc: mockLifecycleHookFunc(0, "ok", "", nil),
			expectErr:    false,
			expectPhase:  agentsv1alpha1.SandboxUpgrading,
			expectCondition: map[string]metav1.ConditionStatus{
				string(agentsv1alpha1.SandboxConditionUpgrading): metav1.ConditionFalse,
				string(agentsv1alpha1.SandboxConditionReady):     metav1.ConditionFalse,
			},
		},
		{
			name: "preUpgrade hook fails with non-zero exit code",
			pod:  newRunningPod(),
			box: newUpgradeTestSandbox(&agentsv1alpha1.SandboxLifecycle{
				PreUpgrade: preUpgradeHook,
			}, nil),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase: agentsv1alpha1.SandboxUpgrading,
			},
			mockHookFunc: mockLifecycleHookFunc(1, "", "error occurred", nil),
			expectErr:    false,
			expectPhase:  agentsv1alpha1.SandboxUpgrading,
			expectCondition: map[string]metav1.ConditionStatus{
				string(agentsv1alpha1.SandboxConditionReady):     metav1.ConditionFalse,
				string(agentsv1alpha1.SandboxConditionUpgrading): metav1.ConditionFalse,
			},
		},
		{
			name: "preUpgrade hook fails with executor error",
			pod:  newRunningPod(),
			box: newUpgradeTestSandbox(&agentsv1alpha1.SandboxLifecycle{
				PreUpgrade: preUpgradeHook,
			}, nil),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase: agentsv1alpha1.SandboxUpgrading,
			},
			mockHookFunc: mockLifecycleHookFunc(-1, "", "", fmt.Errorf("connection refused")),
			expectErr:    false,
			expectPhase:  agentsv1alpha1.SandboxUpgrading,
			expectCondition: map[string]metav1.ConditionStatus{
				string(agentsv1alpha1.SandboxConditionReady):     metav1.ConditionFalse,
				string(agentsv1alpha1.SandboxConditionUpgrading): metav1.ConditionFalse,
			},
		},
		{
			name: "preUpgrade hook fails when pod is nil",
			pod:  nil,
			box: newUpgradeTestSandbox(&agentsv1alpha1.SandboxLifecycle{
				PreUpgrade: preUpgradeHook,
			}, nil),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase: agentsv1alpha1.SandboxUpgrading,
			},
			mockHookFunc: mockLifecycleHookFunc(0, "", "", nil),
			expectErr:    false,
			expectPhase:  agentsv1alpha1.SandboxUpgrading,
			expectCondition: map[string]metav1.ConditionStatus{
				string(agentsv1alpha1.SandboxConditionReady):     metav1.ConditionFalse,
				string(agentsv1alpha1.SandboxConditionUpgrading): metav1.ConditionFalse,
			},
		},
		{
			name: "preUpgrade failed stops without replay",
			pod:  newRunningPod(),
			box: newUpgradeTestSandbox(&agentsv1alpha1.SandboxLifecycle{
				PreUpgrade: preUpgradeHook,
			}, nil),
			// 同轮已有失败 Condition，不再自动调用 hook。
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase:      agentsv1alpha1.SandboxUpgrading,
				Conditions: []metav1.Condition{{Type: string(agentsv1alpha1.SandboxConditionUpgrading), Status: metav1.ConditionFalse, Reason: agentsv1alpha1.SandboxUpgradingReasonPreUpgradeFailed}},
			},
			mockHookFunc: mockLifecycleHookFunc(1, "", "still failing", nil),
			expectErr:    false,
			expectPhase:  agentsv1alpha1.SandboxUpgrading,
			expectCondition: map[string]metav1.ConditionStatus{
				string(agentsv1alpha1.SandboxConditionReady):     metav1.ConditionFalse,
				string(agentsv1alpha1.SandboxConditionUpgrading): metav1.ConditionFalse,
			},
		},
		{
			name: "delete pod after preUpgrade succeeded (Phase 2)",
			pod:  newRunningPod(),
			box:  newUpgradeTestSandbox(nil, nil),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase: agentsv1alpha1.SandboxUpgrading,
				Conditions: []metav1.Condition{
					{
						Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
						Status:             metav1.ConditionFalse,
						Reason:             agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
						LastTransitionTime: now,
					},
				},
			},
			mockHookFunc: mockLifecycleHookFunc(0, "", "", nil),
			expectErr:    false,
			expectPhase:  agentsv1alpha1.SandboxUpgrading,
			expectCondition: map[string]metav1.ConditionStatus{
				string(agentsv1alpha1.SandboxConditionUpgrading): metav1.ConditionFalse,
			},
		},
		{
			name: "wait for pod deletion when pod is terminating",
			pod: func() *corev1.Pod {
				p := newRunningPod()
				p.DeletionTimestamp = &metav1.Time{Time: now.Time}
				p.Finalizers = []string{"fake-finalizer"}
				return p
			}(),
			box: newUpgradeTestSandbox(nil, nil),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase: agentsv1alpha1.SandboxUpgrading,
				Conditions: []metav1.Condition{
					{
						Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
						Status:             metav1.ConditionFalse,
						Reason:             agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
						LastTransitionTime: now,
					},
				},
			},
			mockHookFunc: mockLifecycleHookFunc(0, "", "", nil),
			expectErr:    false,
			expectPhase:  agentsv1alpha1.SandboxUpgrading,
			expectCondition: map[string]metav1.ConditionStatus{
				string(agentsv1alpha1.SandboxConditionUpgrading): metav1.ConditionFalse,
			},
		},
		{
			name: "create new pod when old pod deleted",
			pod:  nil,
			box:  newUpgradeTestSandbox(nil, nil),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase: agentsv1alpha1.SandboxUpgrading,
				Conditions: []metav1.Condition{
					{
						Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
						Status:             metav1.ConditionFalse,
						Reason:             agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
						LastTransitionTime: now,
					},
				},
			},
			mockHookFunc: mockLifecycleHookFunc(0, "", "", nil),
			expectErr:    false,
			expectPhase:  agentsv1alpha1.SandboxUpgrading,
			expectCondition: map[string]metav1.ConditionStatus{
				string(agentsv1alpha1.SandboxConditionUpgrading): metav1.ConditionFalse,
			},
		},
		{
			name: "wait for new pod to be ready before postUpgrade",
			pod: func() *corev1.Pod {
				p := newRunningPod()
				p.Status.Phase = corev1.PodPending
				p.Status.Conditions = nil
				return p
			}(),
			box: newUpgradeTestSandbox(nil, nil),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase: agentsv1alpha1.SandboxUpgrading,
				Conditions: []metav1.Condition{
					{
						Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
						Status:             metav1.ConditionFalse,
						Reason:             agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
						LastTransitionTime: now,
					},
				},
			},
			mockHookFunc: mockLifecycleHookFunc(0, "", "", nil),
			expectErr:    false,
			expectPhase:  agentsv1alpha1.SandboxUpgrading,
			expectCondition: map[string]metav1.ConditionStatus{
				string(agentsv1alpha1.SandboxConditionUpgrading): metav1.ConditionFalse,
			},
		},
		{
			name: "upgrade completed cleans up conditions (pod nil)",
			pod:  nil,
			box:  newUpgradeTestSandbox(nil, nil),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase: agentsv1alpha1.SandboxUpgrading,
				Conditions: []metav1.Condition{
					{
						Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
						Status:             metav1.ConditionTrue,
						Reason:             agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
						Message:            "upgrade completed",
						LastTransitionTime: now,
					},
				},
			},
			mockHookFunc: mockLifecycleHookFunc(0, "", "", nil),
			expectErr:    false,
			expectPhase:  agentsv1alpha1.SandboxUpgrading,
			expectCondition: map[string]metav1.ConditionStatus{
				string(agentsv1alpha1.SandboxConditionUpgrading): metav1.ConditionTrue,
				string(agentsv1alpha1.SandboxConditionReady):     metav1.ConditionFalse,
			},
		},
		{
			name: "postUpgrade failed blocks upgrade",
			pod:  newRunningPod(),
			box: newUpgradeTestSandbox(&agentsv1alpha1.SandboxLifecycle{
				PostUpgrade: postUpgradeHook,
			}, nil),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase: agentsv1alpha1.SandboxUpgrading,
				Conditions: []metav1.Condition{
					{
						Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
						Status:             metav1.ConditionFalse,
						Reason:             agentsv1alpha1.SandboxUpgradingReasonPostUpgradeFailed,
						Message:            "postUpgrade hook failed",
						LastTransitionTime: now,
					},
				},
			},
			// 已失败的 PostUpgrade 停止同轮自动重放。
			mockHookFunc: mockLifecycleHookFunc(1, "", "still failing", nil),
			expectErr:    false,
			expectPhase:  agentsv1alpha1.SandboxUpgrading,
			expectCondition: map[string]metav1.ConditionStatus{
				string(agentsv1alpha1.SandboxConditionUpgrading): metav1.ConditionFalse,
			},
		},
		{
			name: "terminal upgrade condition only refreshes pod readiness",
			pod:  newRunningPod(),
			box:  newUpgradeTestSandbox(nil, nil),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase: agentsv1alpha1.SandboxUpgrading,
				Conditions: []metav1.Condition{
					{
						Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
						Status:             metav1.ConditionTrue,
						Reason:             agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
						Message:            "upgrade completed",
						LastTransitionTime: now,
					},
				},
			},
			mockHookFunc: mockLifecycleHookFunc(0, "", "", nil),
			expectErr:    false,
			expectPhase:  agentsv1alpha1.SandboxUpgrading,
			expectCondition: map[string]metav1.ConditionStatus{
				string(agentsv1alpha1.SandboxConditionReady):     metav1.ConditionTrue,
				string(agentsv1alpha1.SandboxConditionUpgrading): metav1.ConditionTrue,
			},
		},
		{
			name: "upgrade completed cleans up conditions (with pod present for pod info)",
			pod:  nil,
			box:  newUpgradeTestSandbox(nil, nil),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase: agentsv1alpha1.SandboxUpgrading,
				Conditions: []metav1.Condition{
					{
						Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
						Status:             metav1.ConditionTrue,
						Reason:             agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
						Message:            "upgrade completed",
						LastTransitionTime: now,
					},
					{
						Type:               string(agentsv1alpha1.SandboxConditionReady),
						Status:             metav1.ConditionFalse,
						Reason:             "Upgrading",
						LastTransitionTime: now,
					},
				},
			},
			mockHookFunc: mockLifecycleHookFunc(0, "", "", nil),
			expectErr:    false,
			expectPhase:  agentsv1alpha1.SandboxUpgrading,
			expectCondition: map[string]metav1.ConditionStatus{
				string(agentsv1alpha1.SandboxConditionUpgrading): metav1.ConditionTrue,
				string(agentsv1alpha1.SandboxConditionReady):     metav1.ConditionFalse,
			},
		},
		{
			name: "new pod with matching revision completes upgrade without postUpgrade",
			pod: func() *corev1.Pod {
				p := newRunningPod()
				p.Spec.Containers[0].Image = "test:v2"
				p.Status.ContainerStatuses[0].Image = "test:v2"
				p.Labels[agentsv1alpha1.PodLabelTemplateHash] = "new-revision"
				return p
			}(),
			box: newUpgradeTestSandbox(nil, nil),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase:          agentsv1alpha1.SandboxUpgrading,
				UpdateRevision: "new-revision",
				Conditions: []metav1.Condition{
					{
						Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
						Status:             metav1.ConditionFalse,
						Reason:             agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
						LastTransitionTime: metav1.Now(),
					},
				},
			},
			mockHookFunc: mockLifecycleHookFunc(0, "", "", nil),
			expectErr:    false,
			expectPhase:  agentsv1alpha1.SandboxRunning,
			expectCondition: map[string]metav1.ConditionStatus{
				string(agentsv1alpha1.SandboxConditionReady): metav1.ConditionTrue,
			},
		},
		{
			name: "Recreate upgrade without lifecycle should still recreate pod",
			pod: func() *corev1.Pod {
				p := newRunningPod()
				p.Labels[agentsv1alpha1.PodLabelTemplateHash] = "old-revision"
				return p
			}(),
			box: newUpgradeTestSandbox(nil, &agentsv1alpha1.SandboxUpgradePolicy{
				Type: agentsv1alpha1.SandboxUpgradePolicyRecreate,
			}),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase:          agentsv1alpha1.SandboxUpgrading,
				UpdateRevision: "new-revision",
			},
			mockHookFunc:    mockLifecycleHookFunc(0, "", "", nil),
			expectErr:       false,
			expectPhase:     agentsv1alpha1.SandboxUpgrading,
			expectCondition: map[string]metav1.ConditionStatus{},
		},
		{
			name: "old pod with mismatching revision should be deleted in phase 2",
			pod: func() *corev1.Pod {
				p := newRunningPod()
				p.Labels[agentsv1alpha1.PodLabelTemplateHash] = "old-revision"
				return p
			}(),
			box: newUpgradeTestSandbox(nil, nil),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase:          agentsv1alpha1.SandboxUpgrading,
				UpdateRevision: "new-revision",
				Conditions: []metav1.Condition{
					{
						Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
						Status:             metav1.ConditionFalse,
						Reason:             agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
						LastTransitionTime: now,
					},
				},
			},
			mockHookFunc: mockLifecycleHookFunc(0, "", "", nil),
			expectErr:    false,
			expectPhase:  agentsv1alpha1.SandboxUpgrading,
			expectCondition: map[string]metav1.ConditionStatus{
				string(agentsv1alpha1.SandboxConditionUpgrading): metav1.ConditionFalse,
			},
		},
		{
			name: "ResumeSucceed with annotation removed and revision unchanged -> abandon to Running",
			pod:  newRunningPod(),
			box: func() *agentsv1alpha1.Sandbox {
				b := newUpgradeTestSandbox(nil, nil)
				b.Status.UpdateRevision = "same-revision"
				return b
			}(),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase:          agentsv1alpha1.SandboxUpgrading,
				UpdateRevision: "same-revision",
				Conditions: []metav1.Condition{
					{
						Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
						Status:             metav1.ConditionFalse,
						Reason:             agentsv1alpha1.SandboxUpgradingReasonResumeSucceed,
						LastTransitionTime: now,
					},
				},
			},
			mockHookFunc: mockLifecycleHookFunc(0, "", "", nil),
			expectErr:    false,
			expectPhase:  agentsv1alpha1.SandboxRunning,
			expectCondition: map[string]metav1.ConditionStatus{
				string(agentsv1alpha1.SandboxConditionReady): metav1.ConditionTrue,
			},
		},
		{
			name: "ResumeSucceed with annotation present stays Upgrading",
			pod:  newRunningPod(),
			box: func() *agentsv1alpha1.Sandbox {
				b := newUpgradeTestSandbox(nil, nil)
				b.Status.UpdateRevision = "same-revision"
				b.Annotations[agentsv1alpha1.AnnotationUpgradeResumeTrigger] = agentsv1alpha1.True
				return b
			}(),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase:          agentsv1alpha1.SandboxUpgrading,
				UpdateRevision: "same-revision",
				Conditions: []metav1.Condition{
					{
						Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
						Status:             metav1.ConditionFalse,
						Reason:             agentsv1alpha1.SandboxUpgradingReasonResumeSucceed,
						LastTransitionTime: now,
					},
				},
			},
			mockHookFunc: mockLifecycleHookFunc(0, "", "", nil),
			expectErr:    false,
			expectPhase:  agentsv1alpha1.SandboxUpgrading,
			expectCondition: map[string]metav1.ConditionStatus{
				string(agentsv1alpha1.SandboxConditionUpgrading): metav1.ConditionFalse,
				string(agentsv1alpha1.SandboxConditionReady):     metav1.ConditionTrue,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build objects for fake client
			var objects []client.Object
			if tt.pod != nil {
				objects = append(objects, tt.pod.DeepCopy())
			}

			control := newTestCommonControl(tt.mockHookFunc, objects...)

			// Prepare newStatus from existingStatus
			newStatus := tt.existingStatus.DeepCopy()

			args := EnsureFuncArgs{
				Pod:       tt.pod,
				Box:       tt.box,
				NewStatus: newStatus,
			}

			prepareUpgradeStepTest(t, control.Client, args)
			err := control.EnsureSandboxUpgraded(context.TODO(), args)

			// Check error
			if (err != nil) != tt.expectErr {
				t.Errorf("EnsureSandboxUpgraded() error = %v, wantErr %v", err, tt.expectErr)
				return
			}

			// Check phase
			if tt.expectPhase != "" && newStatus.Phase != tt.expectPhase {
				t.Errorf("Expected phase %q, got %q", tt.expectPhase, newStatus.Phase)
			}

			// Check conditions
			for condType, expectedStatus := range tt.expectCondition {
				cond := utils.GetSandboxCondition(newStatus, condType)
				if cond == nil {
					t.Errorf("Expected condition %q to exist, but it was not found", condType)
					continue
				}
				if cond.Status != expectedStatus {
					t.Errorf("Expected condition %q status to be %q, got %q (reason: %s, message: %s)",
						condType, expectedStatus, cond.Status, cond.Reason, cond.Message)
				}
			}

			// For upgrade in-progress tests (pod nil with UpgradePod reason), verify Upgrading condition is preserved
			if tt.name == "upgrade completed cleans up conditions (pod nil)" ||
				tt.name == "upgrade completed cleans up conditions (with pod present for pod info)" {
				upgradingCond := utils.GetSandboxCondition(newStatus, string(agentsv1alpha1.SandboxConditionUpgrading))
				if upgradingCond == nil {
					t.Errorf("Expected Upgrading condition to exist during in-progress upgrade, but it was removed")
				}
			}
		})
	}
}

func TestUpgradeBudgetInitialization(t *testing.T) {
	customTimeout := int32(42)
	for _, tt := range []struct {
		name    string
		timeout *int32
		want    time.Duration
	}{
		{name: "default", want: 300 * time.Second},
		{name: "configured", timeout: &customTimeout, want: 42 * time.Second},
	} {
		t.Run(tt.name, func(t *testing.T) {
			box := newUpgradeTestSandbox(&agentsv1alpha1.SandboxLifecycle{
				PreUpgrade: &agentsv1alpha1.UpgradeAction{Exec: &corev1.ExecAction{Command: []string{"pre"}}},
			}, &agentsv1alpha1.SandboxUpgradePolicy{Type: agentsv1alpha1.SandboxUpgradePolicyRecreate, TimeoutSeconds: tt.timeout})
			box.Annotations[agentsv1alpha1.AnnotationUpgradeOperation] = "budget-operation"
			pod := newRunningPod()
			pod.UID = "source-pod"
			calls := 0
			control := newTestCommonControl(func(context.Context, *agentsv1alpha1.Sandbox, *agentsv1alpha1.UpgradeAction) (int32, string, string, error) {
				calls++
				return 0, "", "", nil
			}, box.DeepCopy(), pod.DeepCopy())
			require.NoError(t, control.Get(t.Context(), client.ObjectKeyFromObject(box), box))
			status := box.Status.DeepCopy()
			status.UpdateRevision = "new-revision"
			before := time.Now()
			require.NoError(t, control.EnsureSandboxUpgraded(t.Context(), EnsureFuncArgs{Pod: pod, Box: box, NewStatus: status}))
			progress := status.UpgradeProgress
			require.NotNil(t, progress)
			require.Equal(t, "budget-operation", progress.OperationID)
			require.Equal(t, "new-revision", progress.Revision)
			require.Equal(t, string(pod.UID), progress.SourcePodUID)
			require.Equal(t, tt.want, progress.Deadline.Sub(progress.StartedAt.Time))
			require.False(t, progress.StartedAt.Before(&metav1.Time{Time: before}))
			require.Zero(t, calls)
			storedPod := &corev1.Pod{}
			require.NoError(t, control.Get(t.Context(), client.ObjectKeyFromObject(pod), storedPod))
			require.Equal(t, pod.Spec, storedPod.Spec)

			// 模拟外层落盘以及 Controller 重启，随后继续使用原预算。
			box.Status = *status.DeepCopy()
			require.NoError(t, control.Status().Update(t.Context(), box))
			reloaded := &agentsv1alpha1.Sandbox{}
			require.NoError(t, control.Get(t.Context(), client.ObjectKeyFromObject(box), reloaded))
			persisted := reloaded.Status.UpgradeProgress.DeepCopy()
			restarted := newTestCommonControl(control.lifecycleHookFunc, reloaded.DeepCopy(), storedPod.DeepCopy())
			status = reloaded.Status.DeepCopy()
			require.NoError(t, restarted.EnsureSandboxUpgraded(t.Context(), EnsureFuncArgs{Pod: storedPod, Box: reloaded, NewStatus: status}))
			require.Equal(t, 1, calls)
			require.Equal(t, persisted.StartedAt, status.UpgradeProgress.StartedAt)
			require.Equal(t, persisted.Deadline, status.UpgradeProgress.Deadline)
		})
	}
}

func TestUpgradeDeadline(t *testing.T) {
	for _, tt := range []struct {
		name, reason, wantReason                            string
		postSucceeded, ready, withinBudget, changedRevision bool
	}{
		{name: "pre timeout", reason: agentsv1alpha1.SandboxUpgradingReasonPreUpgrade, wantReason: agentsv1alpha1.SandboxUpgradingReasonPreUpgradeFailed},
		{name: "checkpoint timeout", reason: agentsv1alpha1.SandboxUpgradingReasonCheckpointing, wantReason: agentsv1alpha1.SandboxUpgradingReasonCheckpointFailed},
		{name: "configuration timeout", reason: agentsv1alpha1.SandboxUpgradingReasonUpgradePod, wantReason: agentsv1alpha1.SandboxUpgradingReasonUpgradePodFailed},
		{name: "post timeout", reason: agentsv1alpha1.SandboxUpgradingReasonPostUpgrade, wantReason: agentsv1alpha1.SandboxUpgradingReasonPostUpgradeFailed},
		{name: "resume timeout", reason: agentsv1alpha1.SandboxUpgradingReasonResuming, wantReason: agentsv1alpha1.SandboxUpgradingReasonUpgradePodFailed},
		{name: "final readiness timeout", reason: agentsv1alpha1.SandboxUpgradingReasonPostUpgrade, postSucceeded: true, wantReason: agentsv1alpha1.SandboxUpgradingReasonPostUpgradeFailed},
		{name: "completed hook and ready wins", reason: agentsv1alpha1.SandboxUpgradingReasonPostUpgrade, postSucceeded: true, ready: true, wantReason: agentsv1alpha1.SandboxUpgradingReasonSucceeded},
		{name: "wait within original budget", reason: agentsv1alpha1.SandboxUpgradingReasonPostUpgrade, postSucceeded: true, withinBudget: true, wantReason: agentsv1alpha1.SandboxUpgradingReasonPostUpgrade},
		{name: "same operation target change", reason: agentsv1alpha1.SandboxUpgradingReasonUpgradePod, withinBudget: true, changedRevision: true, wantReason: agentsv1alpha1.SandboxUpgradingReasonUpgradePodFailed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			box := newUpgradeTestSandbox(nil, nil)
			pod := newRunningPod()
			pod.UID = "original-pod"
			if !tt.ready {
				pod.Status.Conditions[0].Status = corev1.ConditionFalse
			}
			deadline := time.Now().Add(-time.Minute)
			if tt.withinBudget {
				deadline = time.Now().Add(time.Minute)
			}
			status := &agentsv1alpha1.SandboxStatus{
				Phase: agentsv1alpha1.SandboxUpgrading, UpdateRevision: "target",
				UpgradeProgress: &agentsv1alpha1.SandboxUpgradeProgress{OperationID: "operation", Revision: "target", StartedAt: metav1.NewTime(deadline.Add(-300 * time.Second)), Deadline: metav1.NewTime(deadline)},
				Conditions:      []metav1.Condition{{Type: string(agentsv1alpha1.SandboxConditionUpgrading), Status: metav1.ConditionFalse, Reason: tt.reason}},
			}
			if tt.postSucceeded {
				status.UpgradeProgress.PostUpgrade = "Succeeded"
			}
			if tt.changedRevision {
				status.UpdateRevision = "changed"
			}
			original := status.UpgradeProgress.DeepCopy()
			calls := 0
			control := newTestCommonControl(func(context.Context, *agentsv1alpha1.Sandbox, *agentsv1alpha1.UpgradeAction) (int32, string, string, error) {
				calls++
				return 0, "", "", nil
			}, pod.DeepCopy())
			args := EnsureFuncArgs{Pod: pod, Box: box, NewStatus: status}
			prepareUpgradeStepTest(t, control.Client, args)
			require.NoError(t, control.EnsureSandboxUpgraded(t.Context(), args))
			cond := utils.GetSandboxCondition(status, string(agentsv1alpha1.SandboxConditionUpgrading))
			require.Equal(t, tt.wantReason, cond.Reason)
			require.Equal(t, original, status.UpgradeProgress)
			require.Zero(t, calls)
			stored := &corev1.Pod{}
			require.NoError(t, control.Get(t.Context(), client.ObjectKeyFromObject(pod), stored))
			require.Equal(t, pod.UID, stored.UID)
			require.Equal(t, pod.Spec, stored.Spec)
			if tt.wantReason == agentsv1alpha1.SandboxUpgradingReasonSucceeded {
				require.Equal(t, agentsv1alpha1.SandboxRunning, status.Phase)
				require.Equal(t, metav1.ConditionTrue, cond.Status)
			} else {
				require.Equal(t, agentsv1alpha1.SandboxUpgrading, status.Phase)
				require.Equal(t, metav1.ConditionFalse, utils.GetSandboxCondition(status, string(agentsv1alpha1.SandboxConditionReady)).Status)
				if !tt.withinBudget {
					require.Contains(t, cond.Message, "deadline exceeded")
				}
			}
		})
	}
}

func TestUpgradeHookPersistence(t *testing.T) {
	for _, pre := range []bool{true, false} {
		for _, tt := range []struct {
			name, mark                     string
			failPatch, exitCode, wantCalls int
			wantErr, wantSuccess           bool
		}{
			{name: "success persists and skips replay", wantCalls: 1, wantSuccess: true},
			{name: "already succeeded", mark: "Succeeded", wantSuccess: true},
			{name: "unknown result stops", mark: "Running"},
			{name: "explicit failure stops", exitCode: 1, wantCalls: 1},
			{name: "intent write fails before execution", failPatch: 1, wantErr: true},
			{name: "success write fails after execution", failPatch: 2, wantCalls: 1, wantErr: true},
		} {
			t.Run(fmt.Sprintf("pre=%t/%s", pre, tt.name), func(t *testing.T) {
				action := &agentsv1alpha1.UpgradeAction{Exec: &corev1.ExecAction{Command: []string{"hook"}}}
				box := newUpgradeTestSandbox(&agentsv1alpha1.SandboxLifecycle{PreUpgrade: action, PostUpgrade: action.DeepCopy()}, nil)
				pod := newRunningPod()
				control := newTestCommonControl(nil, pod.DeepCopy())
				status := box.Status.DeepCopy()
				args := EnsureFuncArgs{Pod: pod, Box: box, NewStatus: status}
				prepareUpgradeStepTest(t, control.Client, args)
				mark := &status.UpgradeProgress.PostUpgrade
				if pre {
					mark = &status.UpgradeProgress.PreUpgrade
				}
				*mark = tt.mark
				box.Status = *status.DeepCopy()
				require.NoError(t, control.Status().Update(t.Context(), box))
				calls, patches := 0, 0
				control.upgradeControl.lifecycleHookFunc = func(ctx context.Context, _ *agentsv1alpha1.Sandbox, _ *agentsv1alpha1.UpgradeAction) (int32, string, string, error) {
					calls++
					stored := &agentsv1alpha1.Sandbox{}
					require.NoError(t, control.Get(ctx, client.ObjectKeyFromObject(box), stored))
					storedMark := stored.Status.UpgradeProgress.PostUpgrade
					if pre {
						storedMark = stored.Status.UpgradeProgress.PreUpgrade
					}
					require.Equal(t, "Running", storedMark, "执行 hook 前必须先落盘意图")
					return int32(tt.exitCode), "", "", nil
				}
				control.upgradeControl.Client = interceptor.NewClient(control.Client.(client.WithWatch), interceptor.Funcs{
					SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
						patches++
						if patches == tt.failPatch {
							return fmt.Errorf("injected status conflict")
						}
						return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
					},
				})
				result, err := control.upgradeControl.runUpgradeHook(t.Context(), args, pre)
				if tt.wantErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, tt.wantSuccess, result.Succeeded)
				require.Equal(t, tt.wantCalls, calls)
				stored := &agentsv1alpha1.Sandbox{}
				require.NoError(t, control.Get(t.Context(), client.ObjectKeyFromObject(box), stored))
				if tt.failPatch == 1 {
					require.Empty(t, *mark)
					return
				}
				// 从存储恢复，不依赖首次调用的内存标记，验证重启后不会重复执行。
				args.Box = stored
				args.NewStatus = stored.Status.DeepCopy()
				replay, err := control.upgradeControl.runUpgradeHook(t.Context(), args, pre)
				require.NoError(t, err)
				require.Equal(t, tt.wantSuccess, replay.Succeeded)
				require.Equal(t, tt.wantCalls, calls)
				if !tt.wantSuccess {
					require.Contains(t, replay.Message, "unknown")
				}
			})
		}
	}
}

func TestPostUpgradeWaitsForReadyWithoutReplay(t *testing.T) {
	for _, policy := range []agentsv1alpha1.SandboxUpgradePolicyType{agentsv1alpha1.SandboxUpgradePolicyInplaceUpdate, agentsv1alpha1.SandboxUpgradePolicyRecreate} {
		t.Run(string(policy), func(t *testing.T) {
			box := newUpgradeTestSandbox(&agentsv1alpha1.SandboxLifecycle{
				PostUpgrade: &agentsv1alpha1.UpgradeAction{Exec: &corev1.ExecAction{Command: []string{"post"}}},
			}, &agentsv1alpha1.SandboxUpgradePolicy{Type: policy})
			_, hash := HashSandbox(box)
			box.Annotations[agentsv1alpha1.SandboxHashImmutablePart] = hash
			pod := newRunningPod()
			pod.Labels[agentsv1alpha1.PodLabelTemplateHash] = "target"
			pod.Spec.Containers[0].Image = "test:v2"
			pod.Status.ContainerStatuses[0].Image = "test:v2"
			pod.Status.Conditions[0].Status = corev1.ConditionFalse
			calls := 0
			control := newTestCommonControl(func(context.Context, *agentsv1alpha1.Sandbox, *agentsv1alpha1.UpgradeAction) (int32, string, string, error) {
				calls++
				return 0, "", "", nil
			}, pod.DeepCopy())
			status := &agentsv1alpha1.SandboxStatus{Phase: agentsv1alpha1.SandboxUpgrading, UpdateRevision: "target",
				Conditions: []metav1.Condition{{Type: string(agentsv1alpha1.SandboxConditionUpgrading), Status: metav1.ConditionFalse, Reason: agentsv1alpha1.SandboxUpgradingReasonUpgradePod}}}
			args := EnsureFuncArgs{Pod: pod, Box: box, NewStatus: status}
			prepareUpgradeStepTest(t, control.Client, args)
			for range 2 {
				require.NoError(t, control.EnsureSandboxUpgraded(t.Context(), args))
				require.Equal(t, 1, calls)
				require.Equal(t, "Succeeded", status.UpgradeProgress.PostUpgrade)
				require.Equal(t, agentsv1alpha1.SandboxUpgradingReasonPostUpgrade, utils.GetSandboxCondition(status, string(agentsv1alpha1.SandboxConditionUpgrading)).Reason)
				require.Equal(t, metav1.ConditionFalse, utils.GetSandboxCondition(status, string(agentsv1alpha1.SandboxConditionReady)).Status)
			}
			pod.Status.Conditions[0].Status = corev1.ConditionTrue
			require.NoError(t, control.EnsureSandboxUpgraded(t.Context(), args))
			require.Equal(t, 1, calls)
			require.Equal(t, agentsv1alpha1.SandboxRunning, status.Phase)
		})
	}
}

func TestEnsureInplaceUpgrade(t *testing.T) {
	// Build a sandbox with correct immutable hash for inplace update tests
	newInplaceSandbox := func() *agentsv1alpha1.Sandbox {
		box := newUpgradeTestSandbox(nil, &agentsv1alpha1.SandboxUpgradePolicy{
			Type: agentsv1alpha1.SandboxUpgradePolicyInplaceUpdate,
		})
		// Compute and set the correct immutable hash so inplace update logic proceeds
		_, hashImmutablePart := HashSandbox(box)
		box.Annotations[agentsv1alpha1.SandboxHashImmutablePart] = hashImmutablePart
		return box
	}

	tests := []struct {
		name            string
		pod             *corev1.Pod
		box             *agentsv1alpha1.Sandbox
		existingStatus  *agentsv1alpha1.SandboxStatus
		mockHookFunc    LifecycleHookFunc
		expectErr       bool
		expectPhase     agentsv1alpha1.SandboxPhase
		expectCondition map[string]metav1.ConditionStatus
		expectMessage   string
	}{
		{
			name: "inplace upgrade - update done transitions to Running",
			pod: func() *corev1.Pod {
				p := newRunningPod()
				// spec、实际镜像与目标一致，且 Pod Ready，才能完成生命周期。
				p.Spec.Containers[0].Image = "test:v2"
				p.Status.ContainerStatuses[0].Image = "test:v2"
				p.Labels[agentsv1alpha1.PodLabelTemplateHash] = "new-revision"
				return p
			}(),
			box: newInplaceSandbox(),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase:          agentsv1alpha1.SandboxUpgrading,
				UpdateRevision: "new-revision",
				Conditions: []metav1.Condition{
					{
						Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
						Status:             metav1.ConditionFalse,
						Reason:             agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
						LastTransitionTime: metav1.Now(),
					},
				},
			},
			mockHookFunc: mockLifecycleHookFunc(0, "", "", nil),
			expectErr:    false,
			expectPhase:  agentsv1alpha1.SandboxRunning,
			expectCondition: map[string]metav1.ConditionStatus{
				string(agentsv1alpha1.SandboxConditionReady): metav1.ConditionTrue,
			},
		},
		{
			name: "inplace upgrade - update in progress stays Upgrading",
			pod: func() *corev1.Pod {
				p := newRunningPod()
				// spec 已下发，但容器尚未切换到目标镜像。
				p.Spec.Containers[0].Image = "test:v2"
				p.Labels[agentsv1alpha1.PodLabelTemplateHash] = "new-revision"
				// 保留旧 ImageID，验证配置生效等待不误报成功。
				if p.Annotations == nil {
					p.Annotations = map[string]string{}
				}
				p.Annotations[inplaceupdate.PodAnnotationInPlaceUpdateStateKey] =
					`{"revision":"new-revision","updateImages":true,"lastContainerStatuses":{"sandbox":{"imageID":"new-image-id","targetImage":"test:v2"}}}`
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{Name: "sandbox", ImageID: "new-image-id"},
				}
				return p
			}(),
			box: newInplaceSandbox(),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase:          agentsv1alpha1.SandboxUpgrading,
				UpdateRevision: "new-revision",
				Conditions: []metav1.Condition{
					{
						Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
						Status:             metav1.ConditionFalse,
						Reason:             agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
						LastTransitionTime: metav1.Now(),
					},
				},
			},
			mockHookFunc: mockLifecycleHookFunc(0, "", "", nil),
			expectErr:    false,
			// 正常等待时 Ready 跟随 Pod，但 Upgrading 不能提前成功。
			expectPhase: agentsv1alpha1.SandboxUpgrading,
			expectCondition: map[string]metav1.ConditionStatus{
				string(agentsv1alpha1.SandboxConditionReady):     metav1.ConditionTrue,
				string(agentsv1alpha1.SandboxConditionUpgrading): metav1.ConditionFalse,
			},
		},
		{
			name: "inplace upgrade - pod nil skips preUpgrade and creates pod via Recreate",
			pod:  nil,
			box:  newInplaceSandbox(),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase:          agentsv1alpha1.SandboxUpgrading,
				UpdateRevision: "new-revision",
			},
			mockHookFunc: mockLifecycleHookFunc(0, "", "", nil),
			expectErr:    false,
			// No lifecycle → skip preUpgrade → UpgradePod → performRecreateUpgrade creates pod → stays Upgrading
			expectPhase: agentsv1alpha1.SandboxUpgrading,
			expectCondition: map[string]metav1.ConditionStatus{
				string(agentsv1alpha1.SandboxConditionUpgrading): metav1.ConditionFalse,
				string(agentsv1alpha1.SandboxConditionReady):     metav1.ConditionFalse,
			},
		},
		{
			name: "inplace upgrade - pod nil after preUpgrade creates pod via Recreate",
			pod:  nil,
			box:  newInplaceSandbox(),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase:          agentsv1alpha1.SandboxUpgrading,
				UpdateRevision: "new-revision",
				Conditions: []metav1.Condition{
					{
						Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
						Status:             metav1.ConditionFalse,
						Reason:             agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
						LastTransitionTime: metav1.Now(),
					},
				},
			},
			mockHookFunc: mockLifecycleHookFunc(0, "", "", nil),
			expectErr:    false,
			// performRecreateUpgrade creates pod when pod=nil → stays Upgrading
			expectPhase: agentsv1alpha1.SandboxUpgrading,
			expectCondition: map[string]metav1.ConditionStatus{
				string(agentsv1alpha1.SandboxConditionUpgrading): metav1.ConditionFalse,
				string(agentsv1alpha1.SandboxConditionReady):     metav1.ConditionFalse,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var objects []client.Object
			if tt.pod != nil {
				objects = append(objects, tt.pod.DeepCopy())
			}

			control := newTestCommonControl(tt.mockHookFunc, objects...)
			newStatus := tt.existingStatus.DeepCopy()

			args := EnsureFuncArgs{
				Pod:       tt.pod,
				Box:       tt.box,
				NewStatus: newStatus,
			}

			prepareUpgradeStepTest(t, control.Client, args)
			err := control.EnsureSandboxUpgraded(context.TODO(), args)

			if (err != nil) != tt.expectErr {
				t.Errorf("EnsureSandboxUpgraded() error = %v, wantErr %v", err, tt.expectErr)
				return
			}

			if tt.expectPhase != "" && newStatus.Phase != tt.expectPhase {
				t.Errorf("Expected phase %q, got %q", tt.expectPhase, newStatus.Phase)
			}

			if tt.expectMessage != "" && newStatus.Message != tt.expectMessage {
				t.Errorf("Expected message %q, got %q", tt.expectMessage, newStatus.Message)
			}

			for condType, expectedStatus := range tt.expectCondition {
				cond := utils.GetSandboxCondition(newStatus, condType)
				if cond == nil {
					t.Errorf("Expected condition %q to exist, but it was not found", condType)
					continue
				}
				if cond.Status != expectedStatus {
					t.Errorf("Expected condition %q status to be %q, got %q (reason: %s, message: %s)",
						condType, expectedStatus, cond.Status, cond.Reason, cond.Message)
				}
			}
		})
	}
}

// newTestUpgradeControlForInplace builds a standalone UpgradeControl backed by
// a fake client and a real InPlaceUpdateControl, so the in-place UpgradePod
// branches are exercised against actual pod labels, annotations and statuses
// rather than a mocked handler.
func newTestUpgradeControlForInplace(objects ...client.Object) *UpgradeControl {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = agentsv1alpha1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&agentsv1alpha1.Sandbox{}).WithObjects(objects...).Build()
	checkpointCtrl := NewCheckpointControl(fakeClient, record.NewFakeRecorder(100))
	podCtrl := NewPodControl(fakeClient, record.NewFakeRecorder(100), GeneratePodFromSandbox)
	initializer := &defaultSandboxInitializer{recorder: record.NewFakeRecorder(10)}
	return NewUpgradeControl(
		fakeClient, checkpointCtrl, podCtrl, record.NewFakeRecorder(100),
		mockLifecycleHookFunc(0, "", "", nil),
		initializer, defaultCommonSyncStatusFromPod, nil,
		inplaceupdate.NewInPlaceUpdateControl(fakeClient, inplaceupdate.DefaultGeneratePatchBodyFunc),
	)
}

// TestExecuteUpgradePodStep_Branches covers the UpgradePod branches of
// executeUpgradePodStep for both the InplaceUpdate path (patching in place) and
// the Recreate path (pod replacement). The in-place branches are driven by real
// pod labels, annotations and container statuses, mirroring how the controller
// observes progress in a live cluster.
func TestExecuteUpgradePodStep_Branches(t *testing.T) {
	// box with a correct immutable-hash annotation so performInplaceUpgrade
	// proceeds past the hash guard.
	newInplaceBox := func() *agentsv1alpha1.Sandbox {
		box := newUpgradeTestSandbox(nil, &agentsv1alpha1.SandboxUpgradePolicy{
			Type: agentsv1alpha1.SandboxUpgradePolicyInplaceUpdate,
		})
		_, h := HashSandbox(box)
		box.Annotations[agentsv1alpha1.SandboxHashImmutablePart] = h
		return box
	}
	// box whose immutable-hash annotation does not match the computed hash, so
	// performInplaceUpgrade rejects the patch before touching the pod.
	newMismatchedBox := func() *agentsv1alpha1.Sandbox {
		box := newUpgradeTestSandbox(nil, &agentsv1alpha1.SandboxUpgradePolicy{
			Type: agentsv1alpha1.SandboxUpgradePolicyInplaceUpdate,
		})
		box.Annotations[agentsv1alpha1.SandboxHashImmutablePart] = "definitely-wrong-hash"
		return box
	}
	// pod that already carries the target revision label, optionally with an
	// in-place state annotation and container statuses.
	newTargetRevisionPod := func(stateJSON string, statuses ...corev1.ContainerStatus) *corev1.Pod {
		p := newRunningPod()
		p.Labels[agentsv1alpha1.PodLabelTemplateHash] = "new-revision"
		p.Spec.Containers[0].Image = "test:v2"
		if stateJSON != "" {
			p.Annotations = map[string]string{inplaceupdate.PodAnnotationInPlaceUpdateStateKey: stateJSON}
		}
		p.Status.ContainerStatuses = statuses
		return p
	}
	upgradingPodStatus := func() *agentsv1alpha1.SandboxStatus {
		return &agentsv1alpha1.SandboxStatus{
			Phase:          agentsv1alpha1.SandboxUpgrading,
			UpdateRevision: "new-revision",
			Conditions: []metav1.Condition{
				{
					Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
					Status:             metav1.ConditionFalse,
					Reason:             agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
					LastTransitionTime: metav1.Now(),
				},
			},
		}
	}
	condReason := func(s *agentsv1alpha1.SandboxStatus) string {
		c := utils.GetSandboxCondition(s, string(agentsv1alpha1.SandboxConditionUpgrading))
		if c == nil {
			return ""
		}
		return c.Reason
	}

	tests := []struct {
		name          string
		box           *agentsv1alpha1.Sandbox
		pod           *corev1.Pod
		nilControl    bool
		expectErr     bool
		expectReason  string
		expectPhase   agentsv1alpha1.SandboxPhase
		expectMsgPart string
	}{
		{
			// Hash-immutable-part mismatch fails the UpgradePod step terminally
			// before any patch is delivered.
			name:          "inplace hash mismatch fails terminally",
			box:           newMismatchedBox(),
			pod:           newRunningPod(),
			expectErr:     false,
			expectReason:  agentsv1alpha1.SandboxUpgradingReasonUpgradePodFailed,
			expectMsgPart: "container images, resources and template metadata",
		},
		{
			// A nil InPlaceUpdateControl (misconfiguration) surfaces a hard error.
			name:       "inplace nil control errors",
			box:        newInplaceBox(),
			pod:        newRunningPod(),
			nilControl: true,
			expectErr:  true,
		},
		{
			// A pod without the template-hash label cannot be tracked through an
			// in-place update; the step fails instead of silently succeeding.
			name: "inplace pod without template-hash label fails terminally",
			box:  newInplaceBox(),
			pod: func() *corev1.Pod {
				p := newRunningPod()
				delete(p.Labels, agentsv1alpha1.PodLabelTemplateHash)
				return p
			}(),
			expectErr:     false,
			expectReason:  agentsv1alpha1.SandboxUpgradingReasonUpgradePodFailed,
			expectMsgPart: "template-hash label",
		},
		{
			// First reconcile: the patch is delivered to the pod and the sandbox
			// stays in Upgrading until the kubelet applies it.
			name:         "inplace patch delivery stays Upgrading",
			box:          newInplaceBox(),
			pod:          newRunningPod(), // label old-revision != target
			expectErr:    false,
			expectReason: agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
		},
		{
			// The patch was delivered (pod label == target revision) but the image
			// has not been re-pulled yet: still in progress.
			name: "inplace waiting for image completion stays Upgrading",
			box:  newInplaceBox(),
			pod: newTargetRevisionPod(
				`{"revision":"new-revision","updateTimestamp":"2026-01-01T00:00:00Z","updateImages":true,"lastContainerStatuses":{"sandbox":{"imageID":"img-old"}}}`,
				corev1.ContainerStatus{Name: "sandbox", ImageID: "img-old"},
			),
			expectErr:    false,
			expectReason: agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
		},
		{
			// A corrupted in-place state annotation cannot self-heal; the step
			// fails terminally so the user can switch to Recreate.
			name:          "inplace corrupted state fails terminally",
			box:           newInplaceBox(),
			pod:           newTargetRevisionPod(`{corrupted`),
			expectErr:     false,
			expectReason:  agentsv1alpha1.SandboxUpgradingReasonUpgradePodFailed,
			expectMsgPart: "cannot determine in-place update progress",
		},
		{
			// The image was re-pulled (ImageID changed): the update is complete and
			// the state machine advances through PostUpgrade to Running.
			name: "inplace completed transitions to Running",
			box:  newInplaceBox(),
			pod: newTargetRevisionPod(
				`{"revision":"new-revision","updateTimestamp":"2026-01-01T00:00:00Z","updateImages":true,"lastContainerStatuses":{"sandbox":{"imageID":"img-old"}}}`,
				corev1.ContainerStatus{Name: "sandbox", ImageID: "img-new"},
			),
			expectErr:    false,
			expectPhase:  agentsv1alpha1.SandboxRunning,
			expectReason: agentsv1alpha1.SandboxUpgradingReasonSucceeded,
		},
		{
			// An infeasible resize reported by the kubelet is terminal for this
			// round: the UpgradePod step fails with the kubelet's message.
			name: "inplace infeasible resize fails terminally",
			box:  newInplaceBox(),
			pod: func() *corev1.Pod {
				p := newTargetRevisionPod(
					`{"revision":"new-revision","updateTimestamp":"2026-01-01T00:00:00Z","updateResources":true,"lastContainerStatuses":{}}`,
				)
				p.Generation = 2
				p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{
					ObservedGeneration: 2,
					Type:               corev1.PodResizePending,
					Status:             corev1.ConditionTrue,
					Reason:             corev1.PodReasonInfeasible,
					Message:            "insufficient cpu",
				})
				return p
			}(),
			expectErr:     false,
			expectReason:  agentsv1alpha1.SandboxUpgradingReasonUpgradePodFailed,
			expectMsgPart: "in-place pod update failed",
		},
		{
			// A resize that would change the pod's QoS class is rejected before
			// the patch is delivered.
			name: "inplace QoS change rejected fails terminally",
			box: func() *agentsv1alpha1.Sandbox {
				box := newUpgradeTestSandbox(nil, &agentsv1alpha1.SandboxUpgradePolicy{
					Type: agentsv1alpha1.SandboxUpgradePolicyInplaceUpdate,
				})
				// Template adds resources to a pod that has none: BestEffort -> Burstable.
				box.Spec.Template.Spec.Containers[0].Resources = corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
				}
				_, h := HashSandbox(box)
				box.Annotations[agentsv1alpha1.SandboxHashImmutablePart] = h
				return box
			}(),
			pod:           newRunningPod(),
			expectErr:     false,
			expectReason:  agentsv1alpha1.SandboxUpgradingReasonUpgradePodFailed,
			expectMsgPart: "QoS class",
		},
		{
			// Recreate path: a pod that is still being deleted keeps the sandbox
			// in Upgrading (done=false).
			name: "recreate pod deleting stays Upgrading",
			box: newUpgradeTestSandbox(nil, &agentsv1alpha1.SandboxUpgradePolicy{
				Type: agentsv1alpha1.SandboxUpgradePolicyRecreate,
			}),
			pod: func() *corev1.Pod {
				p := newRunningPod()
				ts := metav1.Now()
				p.DeletionTimestamp = &ts
				// A finalizer keeps the fake client happy: it refuses objects with a
				// deletionTimestamp but no finalizers.
				p.Finalizers = []string{"test-finalizer"}
				return p
			}(),
			expectErr:    false,
			expectReason: agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var objects []client.Object
			if tt.pod != nil {
				objects = append(objects, tt.pod.DeepCopy())
			}
			ctrl := newTestUpgradeControlForInplace(objects...)
			if tt.nilControl {
				ctrl.inplaceUpdateControl = nil
			}
			newStatus := upgradingPodStatus()
			args := EnsureFuncArgs{Pod: tt.pod, Box: tt.box, NewStatus: newStatus}
			prepareUpgradeStepTest(t, ctrl.Client, args)
			err := ctrl.EnsureSandboxUpgraded(context.TODO(), args)
			if (err != nil) != tt.expectErr {
				t.Fatalf("EnsureSandboxUpgraded() error = %v, wantErr %v", err, tt.expectErr)
			}
			if tt.expectPhase != "" && newStatus.Phase != tt.expectPhase {
				t.Errorf("phase = %q, want %q", newStatus.Phase, tt.expectPhase)
			}
			if tt.expectReason != "" && condReason(newStatus) != tt.expectReason {
				t.Errorf("Upgrading reason = %q, want %q", condReason(newStatus), tt.expectReason)
			}
			if tt.expectMsgPart != "" {
				c := utils.GetSandboxCondition(newStatus, string(agentsv1alpha1.SandboxConditionUpgrading))
				if c == nil || !strings.Contains(c.Message, tt.expectMsgPart) {
					t.Errorf("Upgrading message %q does not contain %q", c.Message, tt.expectMsgPart)
				}
			}
			// Contract check: the upgrade path never writes the InplaceUpdate
			// condition; the outcome lives only on the Upgrading condition.
			if c := utils.GetSandboxCondition(newStatus, string(agentsv1alpha1.SandboxConditionInplaceUpdate)); c != nil {
				t.Errorf("unexpected InplaceUpdate condition on the upgrade path: %+v", c)
			}
		})
	}
}

// 同轮失败停止执行；控制器接纳新 SUO 后，Recreate 与 inplace 均可补救。
// 新 UID 的接纳与旧状态清理由控制器级测试覆盖。
func TestInplaceUpgradeFailedRecovery(t *testing.T) {
	newFailedStatus := func(revision string) *agentsv1alpha1.SandboxStatus {
		return &agentsv1alpha1.SandboxStatus{
			Phase:          agentsv1alpha1.SandboxUpgrading,
			UpdateRevision: revision,
			Conditions: []metav1.Condition{
				{
					Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
					Status:             metav1.ConditionFalse,
					Reason:             agentsv1alpha1.SandboxUpgradingReasonUpgradePodFailed,
					Message:            "in-place upgrade only supports changing container images, resources and template metadata",
					LastTransitionTime: metav1.Now(),
				},
			},
		}
	}

	t.Run("new operation can recover with Recreate", func(t *testing.T) {
		// 先验证仅改变策略不会重放旧轮次，再模拟新操作已被接纳。
		box := newUpgradeTestSandbox(nil, &agentsv1alpha1.SandboxUpgradePolicy{
			Type: agentsv1alpha1.SandboxUpgradePolicyRecreate,
		})
		pod := newRunningPod() // label old-revision != target new-revision
		ctrl := newTestUpgradeControlForInplace(pod.DeepCopy())
		newStatus := newFailedStatus("new-revision")

		args := EnsureFuncArgs{Pod: pod, Box: box, NewStatus: newStatus}
		prepareUpgradeStepTest(t, ctrl.Client, args)
		err := ctrl.EnsureSandboxUpgraded(t.Context(), args)
		require.NoError(t, err)
		require.NoError(t, ctrl.Get(t.Context(), client.ObjectKeyFromObject(pod), &corev1.Pod{}))
		require.True(t, IsUpgradeFailed(utils.GetSandboxCondition(newStatus, string(agentsv1alpha1.SandboxConditionUpgrading))))

		// 模拟新 UID 接纳后的状态；首轮只创建预算，下一轮才删除旧 Pod。
		box.Annotations[agentsv1alpha1.AnnotationUpgradeOperation] = "replacement-operation"
		utils.RemoveSandboxCondition(newStatus, string(agentsv1alpha1.SandboxConditionUpgrading))
		newStatus.UpgradeProgress = nil
		require.NoError(t, ctrl.EnsureSandboxUpgraded(t.Context(), args))
		require.Equal(t, "replacement-operation", newStatus.UpgradeProgress.OperationID)
		require.NoError(t, ctrl.Get(t.Context(), client.ObjectKeyFromObject(pod), &corev1.Pod{}))
		err = ctrl.EnsureSandboxUpgraded(t.Context(), args)
		require.NoError(t, err)
		var gone corev1.Pod
		getErr := ctrl.Get(context.TODO(), types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}, &gone)
		assert.True(t, apierrors.IsNotFound(getErr), "old pod should be deleted by the recreate path")

		// Reconcile 2: with the pod gone, the recreate path creates a new pod
		// rendered from the current template (i.e. at the target revision).
		err = ctrl.EnsureSandboxUpgraded(context.TODO(), EnsureFuncArgs{Pod: nil, Box: box, NewStatus: newStatus})
		assert.NoError(t, err)
		var fresh corev1.Pod
		assert.NoError(t, ctrl.Get(context.TODO(), types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}, &fresh))
		assert.Equal(t, newStatus.UpdateRevision, fresh.Labels[agentsv1alpha1.PodLabelTemplateHash],
			"replacement pod should carry the target revision")
	})

	t.Run("new operation can recover with inplace rollback", func(t *testing.T) {
		// 原轮次在写入前拒绝；新操作目标与旧 Pod 一致时仍走完整生命周期。
		box := newUpgradeTestSandbox(nil, &agentsv1alpha1.SandboxUpgradePolicy{
			Type: agentsv1alpha1.SandboxUpgradePolicyInplaceUpdate,
		})
		_, h := HashSandbox(box)
		box.Annotations[agentsv1alpha1.SandboxHashImmutablePart] = h
		pod := newRunningPod()
		pod.UID = "unchanged-pod"
		box.Spec.Template.Spec.Containers[0].Image = "test:v1"
		ctrl := newTestUpgradeControlForInplace(pod.DeepCopy())
		// 回退目标与当前实际配置一致。
		newStatus := newFailedStatus("old-revision")

		args := EnsureFuncArgs{Pod: pod, Box: box, NewStatus: newStatus}
		prepareUpgradeStepTest(t, ctrl.Client, args)
		require.NoError(t, ctrl.EnsureSandboxUpgraded(t.Context(), args))
		require.True(t, IsUpgradeFailed(utils.GetSandboxCondition(newStatus, string(agentsv1alpha1.SandboxConditionUpgrading))))
		box.Annotations[agentsv1alpha1.AnnotationUpgradeOperation] = "rollback-operation"
		utils.RemoveSandboxCondition(newStatus, string(agentsv1alpha1.SandboxConditionUpgrading))
		newStatus.UpgradeProgress = nil
		require.NoError(t, ctrl.EnsureSandboxUpgraded(t.Context(), args))
		require.Equal(t, "rollback-operation", newStatus.UpgradeProgress.OperationID)
		require.NoError(t, ctrl.EnsureSandboxUpgraded(t.Context(), args))
		assert.Equal(t, agentsv1alpha1.SandboxRunning, newStatus.Phase)
		storedPod := &corev1.Pod{}
		require.NoError(t, ctrl.Get(t.Context(), client.ObjectKeyFromObject(pod), storedPod))
		require.Equal(t, pod.UID, storedPod.UID)
		c := utils.GetSandboxCondition(newStatus, string(agentsv1alpha1.SandboxConditionUpgrading))
		if assert.NotNil(t, c) {
			assert.Equal(t, agentsv1alpha1.SandboxUpgradingReasonSucceeded, c.Reason)
		}
	})
}

// 新操作已接纳后，即使旧镜像还在退避，也能通过 inplace 下发正确镜像。
// 回到原镜像时允许 ImageID 不变，但必须观察目标容器运行并等待最终 Ready。
func TestInplaceUpgradeRollbackWhileStuck(t *testing.T) {
	box := newUpgradeTestSandbox(nil, &agentsv1alpha1.SandboxUpgradePolicy{
		Type: agentsv1alpha1.SandboxUpgradePolicyInplaceUpdate,
	})
	// 新 SUO 将模板回退到原镜像。
	box.Annotations[agentsv1alpha1.AnnotationUpgradeOperation] = "corrected-operation"
	box.Spec.Template.Spec.Containers[0].Image = "test:v1"
	_, h := HashSandbox(box)
	box.Annotations[agentsv1alpha1.SandboxHashImmutablePart] = h

	// 旧镜像未拉取成功，Pod 尚未 Ready，且 ImageID 仍为原基线。
	pod := newRunningPod()
	pod.UID = "preserved-pod"
	pod.Status.Conditions[0].Status = corev1.ConditionFalse
	pod.Labels[agentsv1alpha1.PodLabelTemplateHash] = "bad-revision"
	pod.Spec.Containers[0].Image = "test:v2-bad"
	pod.Annotations = map[string]string{
		inplaceupdate.PodAnnotationInPlaceUpdateStateKey: `{"revision":"bad-revision","updateTimestamp":"2026-01-01T00:00:00Z","updateImages":true,"lastContainerStatuses":{"sandbox":{"imageID":"img-old"}}}`,
	}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:    "sandbox",
		ImageID: "img-old",
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
			Reason:  "ImagePullBackOff",
			Message: `Back-off pulling image "test:v2-bad"`,
		}},
	}}

	ctrl := newTestUpgradeControlForInplace(pod.DeepCopy())
	// 新操作目标是旧 Pod 原先使用的镜像。
	newStatus := &agentsv1alpha1.SandboxStatus{
		Phase:          agentsv1alpha1.SandboxUpgrading,
		UpdateRevision: "old-revision",
		Conditions: []metav1.Condition{
			{
				Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
				Status:             metav1.ConditionFalse,
				Reason:             agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
				LastTransitionTime: metav1.Now(),
			},
		},
	}

	prepareUpgradeStepTest(t, ctrl.Client, EnsureFuncArgs{Pod: pod, Box: box, NewStatus: newStatus})
	err := ctrl.EnsureSandboxUpgraded(context.TODO(), EnsureFuncArgs{Pod: pod, Box: box, NewStatus: newStatus})
	assert.NoError(t, err)

	// 正确目标已下发，Pod 不被删除重建。
	var patched corev1.Pod
	require.NoError(t, ctrl.Get(t.Context(), client.ObjectKeyFromObject(pod), &patched))
	require.Equal(t, "test:v1", patched.Spec.Containers[0].Image)
	require.Equal(t, "old-revision", patched.Labels[agentsv1alpha1.PodLabelTemplateHash])
	require.Equal(t, pod.UID, patched.UID)
	state, err := inplaceupdate.GetPodInPlaceUpdateState(&patched)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, "test:v1", state.LastContainerStatuses["sandbox"].TargetImage)
	// 下发不代表生效；仍将真实退避原因写入 Message。
	c := utils.GetSandboxCondition(newStatus, string(agentsv1alpha1.SandboxConditionUpgrading))
	if assert.NotNil(t, c) {
		assert.Equal(t, agentsv1alpha1.SandboxUpgradingReasonUpgradePod, c.Reason)
		assert.Contains(t, c.Message, "ImagePullBackOff", "the wait reason must be surfaced on the Upgrading condition")
	}

	// 同一个 ImageID 重新运行目标镜像后，配置阶段完成，仍等待 Pod Ready。
	patched.Status.ContainerStatuses[0].Image = "test:v1"
	patched.Status.ContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
	args := EnsureFuncArgs{Pod: &patched, Box: box, NewStatus: newStatus}
	require.NoError(t, ctrl.EnsureSandboxUpgraded(t.Context(), args))
	c = utils.GetSandboxCondition(newStatus, string(agentsv1alpha1.SandboxConditionUpgrading))
	require.Equal(t, agentsv1alpha1.SandboxUpgradingReasonPostUpgrade, c.Reason)
	require.Equal(t, metav1.ConditionFalse, c.Status)
	require.Equal(t, "Succeeded", newStatus.UpgradeProgress.PostUpgrade)
	patched.Status.Conditions[0].Status = corev1.ConditionTrue
	require.NoError(t, ctrl.EnsureSandboxUpgraded(t.Context(), args))
	require.Equal(t, agentsv1alpha1.SandboxRunning, newStatus.Phase)
	require.Equal(t, agentsv1alpha1.SandboxUpgradingReasonSucceeded,
		utils.GetSandboxCondition(newStatus, string(agentsv1alpha1.SandboxConditionUpgrading)).Reason)
}

// TestInplaceUpgradePatchTransientError verifies that a transient patch
// failure on the in-place path is returned as an error (so the reconcile is
// requeued and retried) instead of being recorded as a terminal step failure.
func TestInplaceUpgradePatchTransientError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = agentsv1alpha1.AddToScheme(scheme)

	box := newUpgradeTestSandbox(nil, &agentsv1alpha1.SandboxUpgradePolicy{
		Type: agentsv1alpha1.SandboxUpgradePolicyInplaceUpdate,
	})
	_, h := HashSandbox(box)
	box.Annotations[agentsv1alpha1.SandboxHashImmutablePart] = h
	pod := newRunningPod() // label old-revision != target, image differs from template

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod.DeepCopy()).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				return fmt.Errorf("simulated transient patch error")
			},
		}).Build()
	checkpointCtrl := NewCheckpointControl(fakeClient, record.NewFakeRecorder(100))
	podCtrl := NewPodControl(fakeClient, record.NewFakeRecorder(100), GeneratePodFromSandbox)
	initializer := &defaultSandboxInitializer{recorder: record.NewFakeRecorder(10)}
	ctrl := NewUpgradeControl(fakeClient, checkpointCtrl, podCtrl, record.NewFakeRecorder(100),
		mockLifecycleHookFunc(0, "", "", nil), initializer, defaultCommonSyncStatusFromPod, nil,
		inplaceupdate.NewInPlaceUpdateControl(fakeClient, inplaceupdate.DefaultGeneratePatchBodyFunc))

	newStatus := &agentsv1alpha1.SandboxStatus{
		Phase:          agentsv1alpha1.SandboxUpgrading,
		UpdateRevision: "new-revision",
		Conditions: []metav1.Condition{
			{
				Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
				Status:             metav1.ConditionFalse,
				Reason:             agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
				LastTransitionTime: metav1.Now(),
			},
		},
	}
	prepareUpgradeStepTest(t, ctrl.Client, EnsureFuncArgs{Pod: pod, Box: box, NewStatus: newStatus})
	err := ctrl.EnsureSandboxUpgraded(context.TODO(), EnsureFuncArgs{Pod: pod, Box: box, NewStatus: newStatus})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "simulated transient patch error")
	// The step is not marked as terminally failed: the retry may still succeed.
	c := utils.GetSandboxCondition(newStatus, string(agentsv1alpha1.SandboxConditionUpgrading))
	if assert.NotNil(t, c) {
		assert.Equal(t, agentsv1alpha1.SandboxUpgradingReasonUpgradePod, c.Reason)
	}
}

// TestExecuteUpgradePodStep_RecreateDeleteError covers the error-propagation
// branch of the Recreate path in executeUpgradePodStep: a pod-deletion failure
// must surface as an error rather than being silently swallowed.
func TestExecuteUpgradePodStep_RecreateDeleteError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = agentsv1alpha1.AddToScheme(scheme)

	upgradingPodStatus := func() *agentsv1alpha1.SandboxStatus {
		return &agentsv1alpha1.SandboxStatus{
			Phase:          agentsv1alpha1.SandboxUpgrading,
			UpdateRevision: "new-revision",
			Conditions: []metav1.Condition{
				{
					Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
					Status:             metav1.ConditionFalse,
					Reason:             agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
					LastTransitionTime: metav1.Now(),
				},
			},
		}
	}

	// A pod whose hash doesn't match UpdateRevision triggers deletion; the
	// injected Delete error surfaces through executeUpgradePodStep.
	pod := newRunningPod() // labels old-revision != UpdateRevision new-revision
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod.DeepCopy()).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				return fmt.Errorf("simulated delete error")
			},
		}).Build()
	checkpointCtrl := NewCheckpointControl(fakeClient, record.NewFakeRecorder(100))
	podCtrl := NewPodControl(fakeClient, record.NewFakeRecorder(100), GeneratePodFromSandbox)
	initializer := &defaultSandboxInitializer{recorder: record.NewFakeRecorder(10)}
	ctrl := NewUpgradeControl(fakeClient, checkpointCtrl, podCtrl, record.NewFakeRecorder(100),
		mockLifecycleHookFunc(0, "", "", nil), initializer, defaultCommonSyncStatusFromPod, nil, nil)

	box := newUpgradeTestSandbox(nil, &agentsv1alpha1.SandboxUpgradePolicy{Type: agentsv1alpha1.SandboxUpgradePolicyRecreate})
	newStatus := upgradingPodStatus()
	prepareUpgradeStepTest(t, ctrl.Client, EnsureFuncArgs{Pod: pod, Box: box, NewStatus: newStatus})
	err := ctrl.EnsureSandboxUpgraded(context.TODO(), EnsureFuncArgs{Pod: pod, Box: box, NewStatus: newStatus})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "simulated delete error")
}

// newTestCommonControlWithCheckpointIndex creates a commonControl with field index support
// for Checkpoint CRs, needed for CheckpointRestore upgrade tests.
func newTestCommonControlWithCheckpointIndex(hookFunc LifecycleHookFunc, objects ...client.Object) *commonControl {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = agentsv1alpha1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithIndex(&agentsv1alpha1.Checkpoint{}, fieldindex.IndexNameForOwnerRefUID, fieldindex.OwnerIndexFunc).
		WithStatusSubresource(&agentsv1alpha1.Checkpoint{}, &agentsv1alpha1.Sandbox{}).
		WithObjects(objects...).Build()
	checkpointCtrl := NewCheckpointControl(fakeClient, record.NewFakeRecorder(100))
	podCtrl := NewPodControl(fakeClient, record.NewFakeRecorder(100), GeneratePodFromSandbox)
	initializer := &defaultSandboxInitializer{recorder: record.NewFakeRecorder(10)}
	return &commonControl{
		Client:               fakeClient,
		recorder:             record.NewFakeRecorder(100),
		inplaceUpdateControl: inplaceupdate.NewInPlaceUpdateControl(fakeClient, inplaceupdate.DefaultGeneratePatchBodyFunc),
		rateLimiter:          NewRateLimiter(),
		checkpointControl:    checkpointCtrl,
		podControl:           podCtrl,
		lifecycleHookFunc:    hookFunc,
		initializer:          initializer,
		upgradeControl:       NewUpgradeControl(fakeClient, checkpointCtrl, podCtrl, record.NewFakeRecorder(100), hookFunc, initializer, defaultCommonSyncStatusFromPod, nil, nil),
	}
}

func newCheckpointRestoreSandbox(lifecycle *agentsv1alpha1.SandboxLifecycle) *agentsv1alpha1.Sandbox {
	box := newUpgradeTestSandbox(lifecycle, &agentsv1alpha1.SandboxUpgradePolicy{
		Type: agentsv1alpha1.SandboxUpgradePolicyCheckpointRestore,
	})
	box.UID = types.UID("sandbox-uid-001")
	return box
}

func newUpgradeCheckpoint(name string, box *agentsv1alpha1.Sandbox, phase agentsv1alpha1.CheckpointPhase) *agentsv1alpha1.Checkpoint {
	return &agentsv1alpha1.Checkpoint{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: box.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(box, sandboxControllerKind),
			},
			Labels: map[string]string{
				agentsv1alpha1.CheckpointLabelSandboxName: box.Name,
				agentsv1alpha1.CheckpointLabelType:        agentsv1alpha1.CheckpointPersistentContentFilesystem,
			},
		},
		Status: agentsv1alpha1.CheckpointStatus{
			Phase: phase,
		},
	}
}

func TestEnsureSandboxUpgraded_CheckpointRestore(t *testing.T) {
	now := metav1.Now()

	tests := []struct {
		name            string
		pod             *corev1.Pod
		box             *agentsv1alpha1.Sandbox
		existingStatus  *agentsv1alpha1.SandboxStatus
		existingCPs     []client.Object
		mockHookFunc    LifecycleHookFunc
		expectErr       bool
		expectPhase     agentsv1alpha1.SandboxPhase
		expectReason    string
		expectCondition map[string]metav1.ConditionStatus
	}{
		{
			name: "CheckpointRestore - PreUpgrade transitions to Checkpointing",
			pod:  newRunningPod(),
			box:  newCheckpointRestoreSandbox(nil),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase: agentsv1alpha1.SandboxUpgrading,
			},
			mockHookFunc: mockLifecycleHookFunc(0, "", "", nil),
			expectErr:    false,
			expectPhase:  agentsv1alpha1.SandboxUpgrading,
			expectReason: agentsv1alpha1.SandboxUpgradingReasonCheckpointing,
		},
		{
			name: "CheckpointRestore - Checkpointing in progress, waits",
			pod:  newRunningPod(),
			box:  newCheckpointRestoreSandbox(nil),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase: agentsv1alpha1.SandboxUpgrading,
				Conditions: []metav1.Condition{
					{
						Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
						Status:             metav1.ConditionFalse,
						Reason:             agentsv1alpha1.SandboxUpgradingReasonCheckpointing,
						LastTransitionTime: now,
					},
				},
			},
			existingCPs: []client.Object{
				newUpgradeCheckpoint("test-sandbox-cp1", newCheckpointRestoreSandbox(nil), agentsv1alpha1.CheckpointCreating),
			},
			mockHookFunc: mockLifecycleHookFunc(0, "", "", nil),
			expectErr:    false,
			expectPhase:  agentsv1alpha1.SandboxUpgrading,
			expectReason: agentsv1alpha1.SandboxUpgradingReasonCheckpointing,
		},
		{
			name: "CheckpointRestore - Checkpoint succeeded, transitions to UpgradePod",
			pod: func() *corev1.Pod {
				p := newRunningPod()
				p.Labels[agentsv1alpha1.PodLabelTemplateHash] = "old-revision"
				return p
			}(),
			box: newCheckpointRestoreSandbox(nil),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase:          agentsv1alpha1.SandboxUpgrading,
				UpdateRevision: "new-revision",
				Conditions: []metav1.Condition{
					{
						Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
						Status:             metav1.ConditionFalse,
						Reason:             agentsv1alpha1.SandboxUpgradingReasonCheckpointing,
						LastTransitionTime: now,
					},
				},
			},
			existingCPs: []client.Object{
				newUpgradeCheckpoint("test-sandbox-cp1", newCheckpointRestoreSandbox(nil), agentsv1alpha1.CheckpointSucceeded),
			},
			mockHookFunc: mockLifecycleHookFunc(0, "", "", nil),
			expectErr:    false,
			expectPhase:  agentsv1alpha1.SandboxUpgrading,
			expectReason: agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
		},
		{
			name: "CheckpointRestore - Checkpoint failed stops operation",
			pod:  newRunningPod(),
			box:  newCheckpointRestoreSandbox(nil),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase: agentsv1alpha1.SandboxUpgrading,
				Conditions: []metav1.Condition{
					{
						Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
						Status:             metav1.ConditionFalse,
						Reason:             agentsv1alpha1.SandboxUpgradingReasonCheckpointing,
						LastTransitionTime: now,
					},
				},
			},
			existingCPs: []client.Object{
				func() *agentsv1alpha1.Checkpoint {
					cp := newUpgradeCheckpoint("test-sandbox-cp1", newCheckpointRestoreSandbox(nil), agentsv1alpha1.CheckpointFailed)
					cp.Status.Message = "checkpoint timeout"
					return cp
				}(),
			},
			mockHookFunc: mockLifecycleHookFunc(0, "", "", nil),
			expectErr:    false,
			expectPhase:  agentsv1alpha1.SandboxUpgrading,
			expectReason: agentsv1alpha1.SandboxUpgradingReasonCheckpointFailed,
		},
		{
			name: "CheckpointRestore - PostUpgrade succeeds with cleanup",
			pod: func() *corev1.Pod {
				p := newRunningPod()
				p.Labels[agentsv1alpha1.PodLabelTemplateHash] = "new-revision"
				p.Spec.NodeName = "node-1"
				p.Status.PodIP = "10.0.0.2"
				return p
			}(),
			box: newCheckpointRestoreSandbox(nil),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase:          agentsv1alpha1.SandboxUpgrading,
				UpdateRevision: "new-revision",
				Conditions: []metav1.Condition{
					{
						Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
						Status:             metav1.ConditionFalse,
						Reason:             agentsv1alpha1.SandboxUpgradingReasonPostUpgrade,
						LastTransitionTime: now,
					},
				},
			},
			existingCPs: []client.Object{
				newUpgradeCheckpoint("test-sandbox-cp1", newCheckpointRestoreSandbox(nil), agentsv1alpha1.CheckpointSucceeded),
			},
			mockHookFunc: mockLifecycleHookFunc(0, "", "", nil),
			expectErr:    false,
			expectPhase:  agentsv1alpha1.SandboxRunning,
			expectReason: agentsv1alpha1.SandboxUpgradingReasonSucceeded,
			expectCondition: map[string]metav1.ConditionStatus{
				string(agentsv1alpha1.SandboxConditionReady):     metav1.ConditionTrue,
				string(agentsv1alpha1.SandboxConditionUpgrading): metav1.ConditionTrue,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var objects []client.Object
			if tt.pod != nil {
				objects = append(objects, tt.pod.DeepCopy())
			}
			objects = append(objects, tt.existingCPs...)

			control := newTestCommonControlWithCheckpointIndex(tt.mockHookFunc, objects...)
			newStatus := tt.existingStatus.DeepCopy()

			args := EnsureFuncArgs{
				Pod:       tt.pod,
				Box:       tt.box,
				NewStatus: newStatus,
			}

			prepareUpgradeStepTest(t, control.Client, args)
			err := control.EnsureSandboxUpgraded(context.TODO(), args)

			if (err != nil) != tt.expectErr {
				t.Errorf("EnsureSandboxUpgraded() error = %v, wantErr %v", err, tt.expectErr)
				return
			}

			if tt.expectPhase != "" && newStatus.Phase != tt.expectPhase {
				t.Errorf("Expected phase %q, got %q", tt.expectPhase, newStatus.Phase)
			}

			if tt.expectReason != "" {
				cond := utils.GetSandboxCondition(newStatus, string(agentsv1alpha1.SandboxConditionUpgrading))
				if cond == nil {
					t.Errorf("Expected Upgrading condition to exist")
				} else if cond.Reason != tt.expectReason {
					t.Errorf("Expected reason %q, got %q", tt.expectReason, cond.Reason)
				}
			}

			for condType, expectedStatus := range tt.expectCondition {
				cond := utils.GetSandboxCondition(newStatus, condType)
				if cond == nil {
					t.Errorf("Expected condition %q to exist", condType)
					continue
				}
				if cond.Status != expectedStatus {
					t.Errorf("Expected condition %q status %q, got %q", condType, expectedStatus, cond.Status)
				}
			}
		})
	}
}

func TestPerformRecreateUpgrade_ContainerStatuses(t *testing.T) {
	now := metav1.Now()

	tests := []struct {
		name            string
		pod             *corev1.Pod
		box             *agentsv1alpha1.Sandbox
		existingStatus  *agentsv1alpha1.SandboxStatus
		mockHookFunc    LifecycleHookFunc
		expectErr       bool
		expectPhase     agentsv1alpha1.SandboxPhase
		expectReason    string
		expectCondition map[string]metav1.ConditionStatus
	}{
		{
			name: "pod crash loop waits within deadline",
			pod: func() *corev1.Pod {
				p := newRunningPod()
				p.Labels[agentsv1alpha1.PodLabelTemplateHash] = "new-revision"
				p.Status.Phase = corev1.PodPending
				p.Status.Conditions = nil
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{
						Name: "sandbox",
						State: corev1.ContainerState{
							Waiting: &corev1.ContainerStateWaiting{
								Reason:  "CrashLoopBackOff",
								Message: "container is in crash loop",
							},
						},
					},
				}
				return p
			}(),
			box: newUpgradeTestSandbox(nil, nil),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase:          agentsv1alpha1.SandboxUpgrading,
				UpdateRevision: "new-revision",
				Conditions: []metav1.Condition{
					{
						Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
						Status:             metav1.ConditionFalse,
						Reason:             agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
						LastTransitionTime: now,
					},
				},
			},
			mockHookFunc: mockLifecycleHookFunc(0, "", "", nil),
			expectErr:    false,
			expectPhase:  agentsv1alpha1.SandboxUpgrading,
			expectReason: agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
			expectCondition: map[string]metav1.ConditionStatus{
				string(agentsv1alpha1.SandboxConditionUpgrading): metav1.ConditionFalse,
			},
		},
		{
			name: "terminated container waits for restart within deadline",
			pod: func() *corev1.Pod {
				p := newRunningPod()
				p.Labels[agentsv1alpha1.PodLabelTemplateHash] = "new-revision"
				p.Status.Phase = corev1.PodPending
				p.Status.Conditions = nil
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{
						Name: "sandbox",
						State: corev1.ContainerState{
							Terminated: &corev1.ContainerStateTerminated{
								Reason:   "Error",
								ExitCode: 1,
								Message:  "container exited with error",
							},
						},
					},
				}
				return p
			}(),
			box: newUpgradeTestSandbox(nil, nil),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase:          agentsv1alpha1.SandboxUpgrading,
				UpdateRevision: "new-revision",
				Conditions: []metav1.Condition{
					{
						Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
						Status:             metav1.ConditionFalse,
						Reason:             agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
						LastTransitionTime: now,
					},
				},
			},
			mockHookFunc: mockLifecycleHookFunc(0, "", "", nil),
			expectErr:    false,
			expectPhase:  agentsv1alpha1.SandboxUpgrading,
			expectReason: agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
			expectCondition: map[string]metav1.ConditionStatus{
				string(agentsv1alpha1.SandboxConditionUpgrading): metav1.ConditionFalse,
			},
		},
		{
			name: "pod not ready with PodInitializing (normal transient) does not set UpgradePodFailed",
			pod: func() *corev1.Pod {
				p := newRunningPod()
				p.Labels[agentsv1alpha1.PodLabelTemplateHash] = "new-revision"
				p.Status.Phase = corev1.PodPending
				p.Status.Conditions = nil
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{
						Name: "sandbox",
						State: corev1.ContainerState{
							Waiting: &corev1.ContainerStateWaiting{
								Reason:  WaitingReasonPodInitializing,
								Message: "pod is initializing",
							},
						},
					},
				}
				return p
			}(),
			box: newUpgradeTestSandbox(nil, nil),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase:          agentsv1alpha1.SandboxUpgrading,
				UpdateRevision: "new-revision",
				Conditions: []metav1.Condition{
					{
						Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
						Status:             metav1.ConditionFalse,
						Reason:             agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
						LastTransitionTime: now,
					},
				},
			},
			mockHookFunc: mockLifecycleHookFunc(0, "", "", nil),
			expectErr:    false,
			expectPhase:  agentsv1alpha1.SandboxUpgrading,
			expectReason: agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
			expectCondition: map[string]metav1.ConditionStatus{
				string(agentsv1alpha1.SandboxConditionUpgrading): metav1.ConditionFalse,
			},
		},
		{
			name: "pod not ready with ContainerCreating (normal transient) does not set UpgradePodFailed",
			pod: func() *corev1.Pod {
				p := newRunningPod()
				p.Labels[agentsv1alpha1.PodLabelTemplateHash] = "new-revision"
				p.Status.Phase = corev1.PodPending
				p.Status.Conditions = nil
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{
						Name: "sandbox",
						State: corev1.ContainerState{
							Waiting: &corev1.ContainerStateWaiting{
								Reason:  WaitingReasonContainerCreating,
								Message: "container is being created",
							},
						},
					},
				}
				return p
			}(),
			box: newUpgradeTestSandbox(nil, nil),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase:          agentsv1alpha1.SandboxUpgrading,
				UpdateRevision: "new-revision",
				Conditions: []metav1.Condition{
					{
						Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
						Status:             metav1.ConditionFalse,
						Reason:             agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
						LastTransitionTime: now,
					},
				},
			},
			mockHookFunc: mockLifecycleHookFunc(0, "", "", nil),
			expectErr:    false,
			expectPhase:  agentsv1alpha1.SandboxUpgrading,
			expectReason: agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
			expectCondition: map[string]metav1.ConditionStatus{
				string(agentsv1alpha1.SandboxConditionUpgrading): metav1.ConditionFalse,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var objects []client.Object
			if tt.pod != nil {
				objects = append(objects, tt.pod.DeepCopy())
			}

			control := newTestCommonControl(tt.mockHookFunc, objects...)
			newStatus := tt.existingStatus.DeepCopy()

			args := EnsureFuncArgs{
				Pod:       tt.pod,
				Box:       tt.box,
				NewStatus: newStatus,
			}

			prepareUpgradeStepTest(t, control.Client, args)
			err := control.EnsureSandboxUpgraded(context.TODO(), args)

			if (err != nil) != tt.expectErr {
				t.Errorf("EnsureSandboxUpgraded() error = %v, wantErr %v", err, tt.expectErr)
				return
			}

			if tt.expectPhase != "" && newStatus.Phase != tt.expectPhase {
				t.Errorf("Expected phase %q, got %q", tt.expectPhase, newStatus.Phase)
			}

			if tt.expectReason != "" {
				cond := utils.GetSandboxCondition(newStatus, string(agentsv1alpha1.SandboxConditionUpgrading))
				if cond == nil {
					t.Errorf("Expected Upgrading condition to exist")
				} else if cond.Reason != tt.expectReason {
					t.Errorf("Expected reason %q, got %q", tt.expectReason, cond.Reason)
				}
			}

			for condType, expectedStatus := range tt.expectCondition {
				cond := utils.GetSandboxCondition(newStatus, condType)
				if cond == nil {
					t.Errorf("Expected condition %q to exist", condType)
					continue
				}
				if cond.Status != expectedStatus {
					t.Errorf("Expected condition %q status %q, got %q", condType, expectedStatus, cond.Status)
				}
			}
		})
	}
}

func TestPerformRecreateUpgrade_CheckpointRestore_CreatePod(t *testing.T) {
	now := metav1.Now()

	// CheckpointRestore with pod=nil in UpgradePod state should create a new pod
	// with the checkpoint ID annotation.
	box := newCheckpointRestoreSandbox(nil)
	pod := newRunningPod()
	pod.Labels[agentsv1alpha1.PodLabelTemplateHash] = "old-revision"

	// Create a checkpoint carrying both the recorded delta and the ID, as a
	// real succeeded filesystem checkpoint does, so GetCheckpointResumeData
	// returns it.
	cp := newUpgradeCheckpoint("test-sandbox-cp1", box, agentsv1alpha1.CheckpointSucceeded)
	cp.Status.PodTemplateDelta = runtime.RawExtension{Raw: []byte(`{"spec":{"containers":[]}}`)}
	cp.Status.CheckpointId = "cp-id-restore-123"

	control := newTestCommonControlWithCheckpointIndex(
		mockLifecycleHookFunc(0, "", "", nil),
		pod.DeepCopy(),
		cp,
	)

	newStatus := &agentsv1alpha1.SandboxStatus{
		Phase:          agentsv1alpha1.SandboxUpgrading,
		UpdateRevision: "new-revision",
		Conditions: []metav1.Condition{
			{
				Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
				Status:             metav1.ConditionFalse,
				Reason:             agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
				LastTransitionTime: now,
			},
		},
	}

	// First call: pod has old-revision hash, so it gets deleted (Step 1)
	args := EnsureFuncArgs{
		Pod:       pod,
		Box:       box,
		NewStatus: newStatus,
	}
	prepareUpgradeStepTest(t, control.Client, args)
	err := control.EnsureSandboxUpgraded(context.TODO(), args)
	assert.NoError(t, err)

	// Second call: pod is nil (deleted), so it creates a new pod with checkpoint ID (Step 2)
	newStatus2 := &agentsv1alpha1.SandboxStatus{
		Phase:          agentsv1alpha1.SandboxUpgrading,
		UpdateRevision: "new-revision",
		Conditions: []metav1.Condition{
			{
				Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
				Status:             metav1.ConditionFalse,
				Reason:             agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
				LastTransitionTime: now,
			},
		},
	}
	args2 := EnsureFuncArgs{
		Pod:       nil,
		Box:       box,
		NewStatus: newStatus2,
	}
	newStatus2.UpgradeProgress = newStatus.UpgradeProgress.DeepCopy()
	err = control.EnsureSandboxUpgraded(context.TODO(), args2)
	assert.NoError(t, err)

	// Verify a new pod was created
	createdPod := &corev1.Pod{}
	err = control.Get(context.TODO(), types.NamespacedName{Namespace: box.Namespace, Name: box.Name}, createdPod)
	assert.NoError(t, err)
	assert.Equal(t, "new-revision", createdPod.Labels[agentsv1alpha1.PodLabelTemplateHash])
}

func TestPerformRecreateUpgrade_CheckpointRestore_MissingCheckpointID(t *testing.T) {
	now := metav1.Now()

	// A checkpoint that recorded a delta but lost its ID should never happen.
	// The upgrade must block instead of creating a pod that cannot restore
	// its writable layer.
	box := newCheckpointRestoreSandbox(nil)
	cp := newUpgradeCheckpoint("test-sandbox-cp1", box, agentsv1alpha1.CheckpointSucceeded)
	cp.Status.PodTemplateDelta = runtime.RawExtension{Raw: []byte(`{"spec":{"containers":[]}}`)}

	control := newTestCommonControlWithCheckpointIndex(
		mockLifecycleHookFunc(0, "", "", nil),
		cp,
	)

	newStatus := &agentsv1alpha1.SandboxStatus{
		Phase:          agentsv1alpha1.SandboxUpgrading,
		UpdateRevision: "new-revision",
		Conditions: []metav1.Condition{
			{
				Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
				Status:             metav1.ConditionFalse,
				Reason:             agentsv1alpha1.SandboxUpgradingReasonUpgradePod,
				LastTransitionTime: now,
			},
		},
	}

	// 直接准备已进入 UpgradePod 的持久化进度，验证创建路径的错误。
	prepareUpgradeStepTest(t, control.Client, EnsureFuncArgs{Box: box, NewStatus: newStatus})
	err := control.EnsureSandboxUpgraded(context.TODO(), EnsureFuncArgs{
		Pod:       nil,
		Box:       box,
		NewStatus: newStatus,
	})
	assert.ErrorContains(t, err, "checkpoint ID not found")

	// No pod may have been created.
	createdPod := &corev1.Pod{}
	getErr := control.Get(context.TODO(), types.NamespacedName{Namespace: box.Namespace, Name: box.Name}, createdPod)
	assert.Error(t, getErr)
}

func TestExecuteUpgradeAction_NilAction(t *testing.T) {
	ctrl := newTestCommonControl(mockLifecycleHookFunc(0, "", "", nil))
	box := newUpgradeTestSandbox(nil, nil)
	result := ctrl.upgradeControl.executeUpgradeAction(context.Background(), newRunningPod(), box, nil)
	assert.True(t, result.Succeeded)
	assert.Contains(t, result.Message, "no hook configured")
}

func TestExecuteUpgradeAction_NilPod(t *testing.T) {
	ctrl := newTestCommonControl(mockLifecycleHookFunc(0, "", "", nil))
	box := newUpgradeTestSandbox(nil, nil)
	action := &agentsv1alpha1.UpgradeAction{
		Exec:           &corev1.ExecAction{Command: []string{"/bin/bash", "-c", "echo test"}},
		TimeoutSeconds: 30,
	}
	result := ctrl.upgradeControl.executeUpgradeAction(context.Background(), nil, box, action)
	assert.False(t, result.Succeeded)
	assert.Contains(t, result.Message, "pod not found")
}

func TestHasUpgradeAction(t *testing.T) {
	tests := []struct {
		name     string
		box      *agentsv1alpha1.Sandbox
		pre      bool
		expected bool
	}{
		{
			name:     "nil lifecycle returns false",
			box:      &agentsv1alpha1.Sandbox{},
			pre:      true,
			expected: false,
		},
		{
			name: "lifecycle with nil preUpgrade action returns false",
			box: &agentsv1alpha1.Sandbox{
				Spec: agentsv1alpha1.SandboxSpec{
					Lifecycle: &agentsv1alpha1.SandboxLifecycle{},
				},
			},
			pre:      true,
			expected: false,
		},
		{
			name: "lifecycle with nil postUpgrade action returns false",
			box: &agentsv1alpha1.Sandbox{
				Spec: agentsv1alpha1.SandboxSpec{
					Lifecycle: &agentsv1alpha1.SandboxLifecycle{},
				},
			},
			pre:      false,
			expected: false,
		},
		{
			name: "lifecycle with preUpgrade action but nil exec returns false",
			box: &agentsv1alpha1.Sandbox{
				Spec: agentsv1alpha1.SandboxSpec{
					Lifecycle: &agentsv1alpha1.SandboxLifecycle{
						PreUpgrade: &agentsv1alpha1.UpgradeAction{},
					},
				},
			},
			pre:      true,
			expected: false,
		},
		{
			name: "lifecycle with preUpgrade action and empty command returns false",
			box: &agentsv1alpha1.Sandbox{
				Spec: agentsv1alpha1.SandboxSpec{
					Lifecycle: &agentsv1alpha1.SandboxLifecycle{
						PreUpgrade: &agentsv1alpha1.UpgradeAction{
							Exec: &corev1.ExecAction{},
						},
					},
				},
			},
			pre:      true,
			expected: false,
		},
		{
			name: "lifecycle with preUpgrade action and exec command returns true",
			box: &agentsv1alpha1.Sandbox{
				Spec: agentsv1alpha1.SandboxSpec{
					Lifecycle: &agentsv1alpha1.SandboxLifecycle{
						PreUpgrade: &agentsv1alpha1.UpgradeAction{
							Exec: &corev1.ExecAction{Command: []string{"echo", "test"}},
						},
					},
				},
			},
			pre:      true,
			expected: true,
		},
		{
			name: "lifecycle with postUpgrade action and exec command returns true",
			box: &agentsv1alpha1.Sandbox{
				Spec: agentsv1alpha1.SandboxSpec{
					Lifecycle: &agentsv1alpha1.SandboxLifecycle{
						PostUpgrade: &agentsv1alpha1.UpgradeAction{
							Exec: &corev1.ExecAction{Command: []string{"echo", "test"}},
						},
					},
				},
			},
			pre:      false,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, hasUpgradeAction(tt.box, tt.pre))
		})
	}
}

// TestEnsureSandboxUpgraded_Resuming covers the Resuming phase of the upgrade
// state machine, which handles paused sandboxes that need to be woken up before
// proceeding with the upgrade lifecycle.
func TestEnsureSandboxUpgraded_Resuming(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = agentsv1alpha1.AddToScheme(scheme)
	now := metav1.Now()
	oldPodIP := "10.0.0.1"
	newPodIP := "10.0.0.2"
	newPodUID := types.UID("new-pod-uid")

	readyPod := func() *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-sandbox",
				Namespace: "default",
				UID:       newPodUID,
				Labels: map[string]string{
					agentsv1alpha1.PodLabelTemplateHash: "old-revision",
				},
			},
			Spec: corev1.PodSpec{
				NodeName: "node-1",
				Containers: []corev1.Container{
					{Name: "sandbox", Image: "test:v1"},
				},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				PodIP: newPodIP,
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: now},
				},
			},
		}
	}

	notReadyPod := func() *corev1.Pod {
		p := readyPod()
		p.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionFalse, LastTransitionTime: now},
		}
		return p
	}

	baseBox := func() *agentsv1alpha1.Sandbox {
		return &agentsv1alpha1.Sandbox{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-sandbox",
				Namespace: "default",
			},
			Spec: agentsv1alpha1.SandboxSpec{
				UpgradePolicy: &agentsv1alpha1.SandboxUpgradePolicy{
					Type: agentsv1alpha1.SandboxUpgradePolicyRecreate,
				},
				EmbeddedSandboxTemplate: agentsv1alpha1.EmbeddedSandboxTemplate{
					Template: &corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{Name: "sandbox", Image: "test:v2"},
							},
						},
					},
				},
			},
		}
	}

	// boxWithPreUpgrade returns a sandbox with a PreUpgrade lifecycle hook,
	// which causes the controller to call Initialize during the Resuming stage.
	boxWithPreUpgrade := func() *agentsv1alpha1.Sandbox {
		box := baseBox()
		box.Spec.Lifecycle = &agentsv1alpha1.SandboxLifecycle{
			PreUpgrade: &agentsv1alpha1.UpgradeAction{
				Exec: &corev1.ExecAction{Command: []string{"/bin/echo", "pre"}},
			},
		}
		return box
	}

	pausedTrueCond := metav1.Condition{
		Type:               string(agentsv1alpha1.SandboxConditionPaused),
		Status:             metav1.ConditionTrue,
		Reason:             agentsv1alpha1.SandboxPausedReasonStopPauseSucceed,
		LastTransitionTime: now,
	}
	pausedFalseCond := metav1.Condition{
		Type:               string(agentsv1alpha1.SandboxConditionPaused),
		Status:             metav1.ConditionFalse,
		Reason:             agentsv1alpha1.SandboxPausedReasonPending,
		LastTransitionTime: now,
	}
	resumedTrueCond := metav1.Condition{
		Type:               string(agentsv1alpha1.SandboxConditionResumed),
		Status:             metav1.ConditionTrue,
		Reason:             agentsv1alpha1.SandboxResumeReasonCreatePod,
		LastTransitionTime: now,
	}
	resumingCond := metav1.Condition{
		Type:               string(agentsv1alpha1.SandboxConditionUpgrading),
		Status:             metav1.ConditionFalse,
		Reason:             agentsv1alpha1.SandboxUpgradingReasonResuming,
		LastTransitionTime: now,
	}

	// mockResume tracks resume calls and controls behavior.
	type mockResume struct {
		err        error
		setResumed bool
		called     int
	}

	// newControlWithResume creates an UpgradeControl with a mockable resumeFunc
	// and returns the mock initializer so tests can assert call counts.
	newControlWithResume := func(mr *mockResume, initErr error) (*UpgradeControl, *mockSandboxInitializer) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&agentsv1alpha1.Sandbox{}).Build()
		checkpointCtrl := NewCheckpointControl(fakeClient, record.NewFakeRecorder(100))
		podCtrl := NewPodControl(fakeClient, record.NewFakeRecorder(100), GeneratePodFromSandbox)
		initializer := &mockSandboxInitializer{err: initErr}
		resumeFn := func(ctx context.Context, args EnsureFuncArgs) error {
			mr.called++
			if mr.err != nil {
				return mr.err
			}
			if mr.setResumed {
				utils.SetSandboxCondition(args.NewStatus, metav1.Condition{
					Type:               string(agentsv1alpha1.SandboxConditionResumed),
					Status:             metav1.ConditionTrue,
					Reason:             agentsv1alpha1.SandboxResumeReasonCreatePod,
					LastTransitionTime: metav1.Now(),
				})
			}
			return nil
		}
		return NewUpgradeControl(fakeClient, checkpointCtrl, podCtrl, record.NewFakeRecorder(100), mockLifecycleHookFunc(0, "", "", nil), initializer, defaultCommonSyncStatusFromPod, resumeFn, nil), initializer
	}

	tests := []struct {
		name                string
		pod                 *corev1.Pod
		box                 *agentsv1alpha1.Sandbox
		existingStatus      *agentsv1alpha1.SandboxStatus
		resumeErr           error
		resumeSetResumed    bool
		expectResumeCalled  bool
		initErr             error
		expectInitCalled    bool
		expectErr           bool
		expectReason        string
		expectPausedRemoved bool
	}{
		{
			name: "paused sandbox first enters upgrade with Paused=False - initial reason is Resuming, waits",
			pod:  readyPod(),
			box:  baseBox(),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase: agentsv1alpha1.SandboxUpgrading,
				Conditions: []metav1.Condition{
					pausedFalseCond,
				},
			},
			expectResumeCalled: false,
			expectErr:          false,
			expectReason:       agentsv1alpha1.SandboxUpgradingReasonResuming,
		},
		{
			name: "Resuming with Paused=True, resumeFunc returns error",
			pod:  readyPod(),
			box:  baseBox(),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase: agentsv1alpha1.SandboxUpgrading,
				Conditions: []metav1.Condition{
					resumingCond,
					pausedTrueCond,
				},
			},
			resumeErr:          fmt.Errorf("resume failed"),
			expectResumeCalled: true,
			expectErr:          true,
			expectReason:       agentsv1alpha1.SandboxUpgradingReasonResuming,
		},
		{
			name: "Resuming with Paused=True, resumeFunc succeeds but Resumed not set - waits",
			pod:  readyPod(),
			box:  baseBox(),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase: agentsv1alpha1.SandboxUpgrading,
				Conditions: []metav1.Condition{
					resumingCond,
					pausedTrueCond,
				},
			},
			expectResumeCalled: true,
			expectErr:          false,
			expectReason:       agentsv1alpha1.SandboxUpgradingReasonResuming,
		},
		{
			name: "Resuming with Paused=True, Resumed=True, PodReady=False - waits",
			pod:  notReadyPod(),
			box:  baseBox(),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase: agentsv1alpha1.SandboxUpgrading,
				Conditions: []metav1.Condition{
					resumingCond,
					pausedTrueCond,
					resumedTrueCond,
				},
			},
			resumeSetResumed:   true,
			expectResumeCalled: true,
			expectErr:          false,
			expectReason:       agentsv1alpha1.SandboxUpgradingReasonResuming,
		},
		{
			name: "Resuming with Paused=True, Resumed=True, PodReady=True, Initialize fails",
			pod:  readyPod(),
			box:  boxWithPreUpgrade(),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase:     agentsv1alpha1.SandboxUpgrading,
				SandboxIp: oldPodIP,
				PodInfo: agentsv1alpha1.PodInfo{
					PodIP:  oldPodIP,
					PodUID: types.UID("old-pod-uid"),
				},
				Conditions: []metav1.Condition{
					resumingCond,
					pausedTrueCond,
					resumedTrueCond,
				},
			},
			resumeSetResumed:   true,
			expectResumeCalled: true,
			initErr:            fmt.Errorf("init failed"),
			expectInitCalled:   true,
			expectErr:          true,
			expectReason:       agentsv1alpha1.SandboxUpgradingReasonResuming,
		},
		{
			name: "Resuming with Paused=True, Resumed=True, PodReady=True, Initialize succeeds - transitions to ResumeSucceed and removes Paused",
			pod:  readyPod(),
			box:  boxWithPreUpgrade(),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase:     agentsv1alpha1.SandboxUpgrading,
				SandboxIp: oldPodIP,
				PodInfo: agentsv1alpha1.PodInfo{
					PodIP:  oldPodIP,
					PodUID: types.UID("old-pod-uid"),
				},
				Conditions: []metav1.Condition{
					resumingCond,
					pausedTrueCond,
					resumedTrueCond,
				},
			},
			resumeSetResumed:    true,
			expectResumeCalled:  true,
			expectInitCalled:    true,
			expectErr:           false,
			expectReason:        agentsv1alpha1.SandboxUpgradingReasonResumeSucceed,
			expectPausedRemoved: true,
		},
		{
			name: "Resuming with Paused=True, Resumed=True, PodReady=True, no PreUpgrade hook - skips Initialize, transitions to ResumeSucceed",
			pod:  readyPod(),
			box:  baseBox(),
			existingStatus: &agentsv1alpha1.SandboxStatus{
				Phase: agentsv1alpha1.SandboxUpgrading,
				Conditions: []metav1.Condition{
					resumingCond,
					pausedTrueCond,
					resumedTrueCond,
				},
			},
			resumeSetResumed:    true,
			expectResumeCalled:  true,
			expectInitCalled:    false,
			expectErr:           false,
			expectReason:        agentsv1alpha1.SandboxUpgradingReasonResumeSucceed,
			expectPausedRemoved: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr := &mockResume{err: tt.resumeErr, setResumed: tt.resumeSetResumed}
			ctrl, init := newControlWithResume(mr, tt.initErr)

			newStatus := tt.existingStatus.DeepCopy()
			args := EnsureFuncArgs{
				Pod:       tt.pod,
				Box:       tt.box,
				NewStatus: newStatus,
			}

			prepareUpgradeStepTest(t, ctrl.Client, args)
			err := ctrl.EnsureSandboxUpgraded(context.TODO(), args)

			if (err != nil) != tt.expectErr {
				t.Errorf("EnsureSandboxUpgraded() error = %v, wantErr %v", err, tt.expectErr)
			}

			if tt.expectResumeCalled && mr.called == 0 {
				t.Error("expected resumeFunc to be called, but it was not")
			}
			if !tt.expectResumeCalled && mr.called > 0 {
				t.Error("expected resumeFunc NOT to be called, but it was")
			}

			if tt.expectInitCalled {
				if init.called == 0 {
					t.Error("expected Initialize to be called, but it was not")
				}
				if init.initializedStatus == nil {
					t.Error("expected Initialize to receive sandbox status")
				} else {
					assert.Equal(t, newPodIP, init.initializedStatus.SandboxIp, "SandboxIp passed to Initialize")
					assert.Equal(t, newPodIP, init.initializedStatus.PodInfo.PodIP, "PodInfo.PodIP passed to Initialize")
					assert.Equal(t, newPodUID, init.initializedStatus.PodInfo.PodUID, "PodInfo.PodUID passed to Initialize")
				}
				assert.Equal(t, newPodIP, newStatus.SandboxIp, "SandboxIp in newStatus")
				assert.Equal(t, newPodIP, newStatus.PodInfo.PodIP, "PodInfo.PodIP in newStatus")
				assert.Equal(t, newPodUID, newStatus.PodInfo.PodUID, "PodInfo.PodUID in newStatus")
			}
			if !tt.expectInitCalled && init.called > 0 {
				t.Error("expected Initialize NOT to be called, but it was")
			}

			upgradingCond := utils.GetSandboxCondition(newStatus, string(agentsv1alpha1.SandboxConditionUpgrading))
			if upgradingCond == nil {
				t.Error("expected Upgrading condition to exist")
			} else if upgradingCond.Reason != tt.expectReason {
				t.Errorf("expected Upgrading reason %q, got %q", tt.expectReason, upgradingCond.Reason)
			}

			if tt.expectPausedRemoved {
				pausedCond := utils.GetSandboxCondition(newStatus, string(agentsv1alpha1.SandboxConditionPaused))
				if pausedCond != nil {
					t.Error("expected Paused condition to be removed, but it still exists")
				}
			}
		})
	}
}
