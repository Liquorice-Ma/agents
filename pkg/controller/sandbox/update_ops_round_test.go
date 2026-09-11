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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	agentsv1alpha1 "github.com/openkruise/agents/api/v1alpha1"
	"github.com/openkruise/agents/pkg/controller/sandbox/core"
)

const testPendingRevision = "2026-08-12T00:00:00Z/ops-a"

// newRoundTerminalSandbox builds a Running sandbox carrying an in-flight
// update-ops round (pending record + upgrade policy), the state
// ensureUpdateOpsRoundTerminal settles.
func newRoundTerminalSandbox(mods ...func(*agentsv1alpha1.Sandbox)) *agentsv1alpha1.Sandbox {
	box := &agentsv1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "round-sbx",
			Namespace: "default",
			Annotations: map[string]string{
				agentsv1alpha1.AnnotationUpdateOpsPendingRevision: testPendingRevision,
			},
		},
		Spec: agentsv1alpha1.SandboxSpec{
			UpgradePolicy: &agentsv1alpha1.SandboxUpgradePolicy{
				Type: agentsv1alpha1.SandboxUpgradePolicyRecreate,
			},
			EmbeddedSandboxTemplate: agentsv1alpha1.EmbeddedSandboxTemplate{
				Template: &corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "main", Image: "busybox:1.0"},
						},
					},
				},
			},
		},
		Status: agentsv1alpha1.SandboxStatus{
			Phase: agentsv1alpha1.SandboxRunning,
		},
	}
	for _, mod := range mods {
		mod(box)
	}
	return box
}

func withUpgradingCondition(reason string, status metav1.ConditionStatus) func(*agentsv1alpha1.Sandbox) {
	return func(box *agentsv1alpha1.Sandbox) {
		box.Status.Conditions = append(box.Status.Conditions, metav1.Condition{
			Type:   string(agentsv1alpha1.SandboxConditionUpgrading),
			Status: status,
			Reason: reason,
		})
	}
}

// withMatchingUpdateRevision records the sandbox's current template hash as
// the observed update revision, i.e. the round never bumped the template.
func withMatchingUpdateRevision() func(*agentsv1alpha1.Sandbox) {
	return func(box *agentsv1alpha1.Sandbox) {
		hash, _ := core.HashSandbox(box)
		box.Status.UpdateRevision = hash
	}
}

// withObservedCurrentSpec marks the persisted status as having observed the
// current spec (generation observed, template hash current), satisfying the
// round-ownership gate so terminal conditions are attributed to this round.
func withObservedCurrentSpec() func(*agentsv1alpha1.Sandbox) {
	return func(box *agentsv1alpha1.Sandbox) {
		box.Generation = 2
		box.Status.ObservedGeneration = 2
		hash, _ := core.HashSandbox(box)
		box.Status.UpdateRevision = hash
	}
}

func TestEnsureUpdateOpsRoundTerminal(t *testing.T) {
	tests := []struct {
		name          string
		box           *agentsv1alpha1.Sandbox
		wantPatched   bool
		wantPending   string
		wantStamp     string
		wantPolicyNil bool
	}{
		{
			name:        "no pending record is a no-op",
			box:         newRoundTerminalSandbox(func(box *agentsv1alpha1.Sandbox) { box.Annotations = nil }),
			wantPatched: false,
		},
		{
			name: "succeeded upgrade promotes pending record to stamp and clears policy",
			box: newRoundTerminalSandbox(withObservedCurrentSpec(),
				withUpgradingCondition(agentsv1alpha1.SandboxUpgradingReasonSucceeded, metav1.ConditionTrue)),
			wantPatched:   true,
			wantStamp:     testPendingRevision,
			wantPolicyNil: true,
		},
		{
			name: "stale succeeded condition does not settle an unobserved new round",
			box: newRoundTerminalSandbox(
				withUpgradingCondition(agentsv1alpha1.SandboxUpgradingReasonSucceeded, metav1.ConditionTrue),
				func(box *agentsv1alpha1.Sandbox) {
					// The new round's round-start patch bumped the generation; the
					// status still carries the previous round's observation.
					box.Generation = 3
					box.Status.ObservedGeneration = 2
				}),
			wantPatched: false,
			wantPending: testPendingRevision,
		},
		{
			name: "stale succeeded condition with stale template hash does not settle the round",
			box: newRoundTerminalSandbox(
				withUpgradingCondition(agentsv1alpha1.SandboxUpgradingReasonSucceeded, metav1.ConditionTrue),
				func(box *agentsv1alpha1.Sandbox) {
					box.Generation = 2
					box.Status.ObservedGeneration = 2
					box.Status.UpdateRevision = "previous-round-hash"
				}),
			wantPatched: false,
			wantPending: testPendingRevision,
		},
		{
			name: "patch-mode pending promotes without stamping",
			box: newRoundTerminalSandbox(withObservedCurrentSpec(),
				withUpgradingCondition(agentsv1alpha1.SandboxUpgradingReasonSucceeded, metav1.ConditionTrue),
				func(box *agentsv1alpha1.Sandbox) {
					box.Annotations[agentsv1alpha1.AnnotationUpdateOpsPendingRevision] = agentsv1alpha1.UpdateOpsPatchPendingPrefix + testPendingRevision
				}),
			wantPatched:   true,
			wantStamp:     "",
			wantPolicyNil: true,
		},
		{
			name: "patch-mode pending is cleared on terminal failure without stamping",
			box: newRoundTerminalSandbox(withObservedCurrentSpec(),
				withUpgradingCondition(agentsv1alpha1.SandboxUpgradingReasonUpgradePodFailed, metav1.ConditionFalse),
				func(box *agentsv1alpha1.Sandbox) {
					box.Annotations[agentsv1alpha1.AnnotationUpdateOpsPendingRevision] = agentsv1alpha1.UpdateOpsPatchPendingPrefix + testPendingRevision
				}),
			wantPatched: true,
			wantStamp:   "",
		},
		{
			name: "terminal failure clears pending record without stamping",
			box: newRoundTerminalSandbox(withObservedCurrentSpec(),
				withUpgradingCondition(agentsv1alpha1.SandboxUpgradingReasonUpgradePodFailed, metav1.ConditionFalse)),
			wantPatched: true,
		},
		{
			name: "checkpoint failure clears pending record without stamping",
			box: newRoundTerminalSandbox(withObservedCurrentSpec(),
				withUpgradingCondition(agentsv1alpha1.SandboxUpgradingReasonCheckpointFailed, metav1.ConditionFalse)),
			wantPatched: true,
		},
		{
			name: "stale terminal failure does not clear an unobserved new round's pending record",
			box: newRoundTerminalSandbox(
				withUpgradingCondition(agentsv1alpha1.SandboxUpgradingReasonUpgradePodFailed, metav1.ConditionFalse),
				func(box *agentsv1alpha1.Sandbox) {
					box.Generation = 3
					box.Status.ObservedGeneration = 2
				}),
			wantPatched: false,
			wantPending: testPendingRevision,
		},
		{
			name: "in-flight upgrade keeps the pending record",
			box: newRoundTerminalSandbox(withObservedCurrentSpec(),
				withUpgradingCondition(agentsv1alpha1.SandboxUpgradingReasonUpgradePod, metav1.ConditionFalse)),
			wantPatched: false,
			wantPending: testPendingRevision,
		},
		{
			name:        "orphaned round with unchanged template revision is cleared",
			box:         newRoundTerminalSandbox(withMatchingUpdateRevision()),
			wantPatched: true,
		},
		{
			name: "pending resume trigger is not an orphan",
			box: newRoundTerminalSandbox(withMatchingUpdateRevision(), func(box *agentsv1alpha1.Sandbox) {
				box.Annotations[agentsv1alpha1.AnnotationUpgradeResumeTrigger] = agentsv1alpha1.True
			}),
			wantPatched: false,
			wantPending: testPendingRevision,
		},
		{
			name: "changed template revision is not an orphan",
			box: newRoundTerminalSandbox(func(box *agentsv1alpha1.Sandbox) {
				box.Status.UpdateRevision = "stale-revision"
			}),
			wantPatched: false,
			wantPending: testPendingRevision,
		},
		{
			name: "transitional phase is not an orphan",
			box: newRoundTerminalSandbox(withMatchingUpdateRevision(), func(box *agentsv1alpha1.Sandbox) {
				box.Status.Phase = agentsv1alpha1.SandboxResuming
			}),
			wantPatched: false,
			wantPending: testPendingRevision,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, clientgoscheme.AddToScheme(scheme))
			require.NoError(t, agentsv1alpha1.AddToScheme(scheme))
			cli := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.box).
				Build()
			r := &SandboxReconciler{Client: cli, Scheme: scheme}

			patched, err := r.ensureUpdateOpsRoundTerminal(context.Background(), tt.box)
			require.NoError(t, err)
			assert.Equal(t, tt.wantPatched, patched)

			updated := &agentsv1alpha1.Sandbox{}
			require.NoError(t, cli.Get(context.Background(), client.ObjectKeyFromObject(tt.box), updated))
			assert.Equal(t, tt.wantPending, updated.Annotations[agentsv1alpha1.AnnotationUpdateOpsPendingRevision])
			assert.Equal(t, tt.wantStamp, updated.Annotations[agentsv1alpha1.AnnotationUpdateOpsRevision])
			if tt.wantPolicyNil {
				assert.Nil(t, updated.Spec.UpgradePolicy)
			}
		})
	}
}
