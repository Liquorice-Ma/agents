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

package sandboxupdateops

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	agentsv1alpha1 "github.com/openkruise/agents/api/v1alpha1"
)

// newTemplateOps builds a template-mode ops targeting the busybox:2.0
// template (the newSandbox helper creates sandboxes at busybox:1.0).
func newTemplateOps(name, ns string, phase agentsv1alpha1.SandboxUpdateOpsPhase) *agentsv1alpha1.SandboxUpdateOps {
	ops := newSandboxUpdateOps(name, ns, phase, false, nil)
	ops.Spec.Template = &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "main", Image: "busybox:2.0"},
			},
		},
	}
	return ops
}

// sandboxTemplate returns the template the newSandbox helper creates, for
// building ops whose target already matches the sandbox.
func sandboxTemplate() *corev1.PodTemplateSpec {
	return &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "main", Image: "busybox:1.0"},
			},
		},
	}
}

func TestOpsRevision_TotalOrder(t *testing.T) {
	older := newTemplateOps("zeta", "default", "")
	older.CreationTimestamp = metav1.NewTime(time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC))
	newer := newTemplateOps("alpha", "default", "")
	newer.CreationTimestamp = metav1.NewTime(time.Date(2026, 8, 12, 10, 0, 1, 0, time.UTC))

	assert.Equal(t, "2026-08-12T10:00:00Z/zeta", opsRevision(older))
	// Timestamp dominates: a later creation is newer regardless of name.
	assert.Less(t, opsRevision(older), opsRevision(newer))

	// Same-second tie-break is lexicographic by name.
	tieA := newTemplateOps("alpha", "default", "")
	tieA.CreationTimestamp = older.CreationTimestamp
	assert.Less(t, opsRevision(tieA), opsRevision(older))
}

func TestClassifyTemplateSandbox(t *testing.T) {
	ops := newTemplateOps("test-ops", "default", agentsv1alpha1.SandboxUpdateOpsUpdating)
	myRev := opsRevision(ops)
	newerRev := "2027-01-01T00:00:00Z/newer-ops"
	olderRev := "0001-01-01T00:00:00Z/aaa-ops"

	upgradingCond := func(reason string, status metav1.ConditionStatus) []metav1.Condition {
		return []metav1.Condition{{
			Type:   string(agentsv1alpha1.SandboxConditionUpgrading),
			Status: status,
			Reason: reason,
		}}
	}

	tests := []struct {
		name string
		mod  func(*agentsv1alpha1.Sandbox)
		want sandboxUpdateState
	}{
		{
			name: "foreign pending record waits",
			mod: func(sbx *agentsv1alpha1.Sandbox) {
				sbx.Annotations = map[string]string{agentsv1alpha1.AnnotationUpdateOpsPendingRevision: newerRev}
			},
			want: sandboxWaiting,
		},
		{
			name: "foreign patch-mode pending record waits",
			mod: func(sbx *agentsv1alpha1.Sandbox) {
				sbx.Annotations = map[string]string{agentsv1alpha1.AnnotationUpdateOpsPendingRevision: agentsv1alpha1.UpdateOpsPatchPendingPrefix + newerRev}
			},
			want: sandboxWaiting,
		},
		{
			name: "own pending record is updating",
			mod: func(sbx *agentsv1alpha1.Sandbox) {
				sbx.Annotations = map[string]string{agentsv1alpha1.AnnotationUpdateOpsPendingRevision: myRev}
				sbx.Status.Phase = agentsv1alpha1.SandboxResuming
			},
			want: sandboxUpdating,
		},
		{
			name: "own pending record with ResumeSucceed enters phase 2",
			mod: func(sbx *agentsv1alpha1.Sandbox) {
				sbx.Annotations = map[string]string{agentsv1alpha1.AnnotationUpdateOpsPendingRevision: myRev}
				sbx.Status.Conditions = upgradingCond(agentsv1alpha1.SandboxUpgradingReasonResumeSucceed, metav1.ConditionTrue)
			},
			want: sandboxResumeSucceed,
		},
		{
			name: "own pending record, resumed Running without condition and old template enters phase 2",
			mod: func(sbx *agentsv1alpha1.Sandbox) {
				sbx.Annotations = map[string]string{agentsv1alpha1.AnnotationUpdateOpsPendingRevision: myRev}
			},
			want: sandboxResumeSucceed,
		},
		{
			name: "own stamp settles as updated",
			mod: func(sbx *agentsv1alpha1.Sandbox) {
				sbx.Annotations = map[string]string{agentsv1alpha1.AnnotationUpdateOpsRevision: myRev}
			},
			want: sandboxUpdated,
		},
		{
			name: "own stamp is not drift-chased after manual template edits",
			mod: func(sbx *agentsv1alpha1.Sandbox) {
				sbx.Annotations = map[string]string{agentsv1alpha1.AnnotationUpdateOpsRevision: myRev}
				sbx.Spec.Template.Spec.Containers[0].Image = "busybox:9.9"
			},
			want: sandboxUpdated,
		},
		{
			name: "newer stamp settles as superseded",
			mod: func(sbx *agentsv1alpha1.Sandbox) {
				sbx.Annotations = map[string]string{agentsv1alpha1.AnnotationUpdateOpsRevision: newerRev}
			},
			want: sandboxSuperseded,
		},
		{
			name: "older stamp is re-updated",
			mod: func(sbx *agentsv1alpha1.Sandbox) {
				sbx.Annotations = map[string]string{agentsv1alpha1.AnnotationUpdateOpsRevision: olderRev}
			},
			want: sandboxCandidate,
		},
		{
			name: "own label with terminal failure and cleared pending record is failed",
			mod: func(sbx *agentsv1alpha1.Sandbox) {
				sbx.Labels[agentsv1alpha1.LabelSandboxUpdateOps] = ops.Name
				sbx.Status.Conditions = upgradingCond(agentsv1alpha1.SandboxUpgradingReasonUpgradePodFailed, metav1.ConditionFalse)
			},
			want: sandboxFailed,
		},
		{
			name: "phase outside state filter is skipped",
			mod: func(sbx *agentsv1alpha1.Sandbox) {
				sbx.Status.Phase = agentsv1alpha1.SandboxPaused
			},
			want: sandboxNoNeedUpdate,
		},
		{
			name: "matching template on a steady sandbox takes the fast path",
			mod: func(sbx *agentsv1alpha1.Sandbox) {
				sbx.Spec.Template = ops.Spec.Template.DeepCopy()
			},
			want: sandboxFastPath,
		},
		{
			name: "matching template mid-upgrade waits for steady state",
			mod: func(sbx *agentsv1alpha1.Sandbox) {
				sbx.Spec.Template = ops.Spec.Template.DeepCopy()
				sbx.Status.Phase = agentsv1alpha1.SandboxUpgrading
			},
			want: sandboxWaiting,
		},
		{
			name: "matching template not yet observed waits for steady state",
			mod: func(sbx *agentsv1alpha1.Sandbox) {
				sbx.Spec.Template = ops.Spec.Template.DeepCopy()
				sbx.Generation = 2
				sbx.Status.ObservedGeneration = 1
			},
			want: sandboxWaiting,
		},
		{
			name: "unstamped sandbox with different template is a candidate",
			mod:  func(sbx *agentsv1alpha1.Sandbox) {},
			want: sandboxCandidate,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sbx := newSandbox("sbx-1", "default", "", agentsv1alpha1.SandboxRunning, nil)
			tt.mod(sbx)
			assert.Equal(t, tt.want, classifyTemplateSandbox(sbx, ops, myRev))
		})
	}
}

func TestTemplateMatchesOps(t *testing.T) {
	templateOps := newTemplateOps("t-ops", "default", "")
	refOps := newSandboxUpdateOps("r-ops", "default", "", false, nil)
	refOps.Spec.TemplateRef = &agentsv1alpha1.SandboxTemplateRef{Name: "tpl-a"}

	tests := []struct {
		name string
		ops  *agentsv1alpha1.SandboxUpdateOps
		mod  func(*agentsv1alpha1.Sandbox)
		want bool
	}{
		{
			name: "inline template equal",
			ops:  templateOps,
			mod: func(sbx *agentsv1alpha1.Sandbox) {
				sbx.Spec.Template = templateOps.Spec.Template.DeepCopy()
			},
			want: true,
		},
		{
			name: "inline template differs",
			ops:  templateOps,
			mod:  func(sbx *agentsv1alpha1.Sandbox) {},
			want: false,
		},
		{
			name: "carrier field mismatch: ops inline vs sandbox ref",
			ops:  templateOps,
			mod: func(sbx *agentsv1alpha1.Sandbox) {
				sbx.Spec.Template = templateOps.Spec.Template.DeepCopy()
				sbx.Spec.TemplateRef = &agentsv1alpha1.SandboxTemplateRef{Name: "tpl-a"}
			},
			want: false,
		},
		{
			name: "template ref equal",
			ops:  refOps,
			mod: func(sbx *agentsv1alpha1.Sandbox) {
				sbx.Spec.Template = nil
				sbx.Spec.TemplateRef = &agentsv1alpha1.SandboxTemplateRef{Name: "tpl-a"}
			},
			want: true,
		},
		{
			name: "template ref differs",
			ops:  refOps,
			mod: func(sbx *agentsv1alpha1.Sandbox) {
				sbx.Spec.Template = nil
				sbx.Spec.TemplateRef = &agentsv1alpha1.SandboxTemplateRef{Name: "tpl-b"}
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sbx := newSandbox("sbx-1", "default", "", agentsv1alpha1.SandboxRunning, nil)
			tt.mod(sbx)
			assert.Equal(t, tt.want, templateMatchesOps(sbx, tt.ops))
		})
	}
}

func TestBlockedByModeBarrier(t *testing.T) {
	t0 := metav1.NewTime(time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC))
	t1 := metav1.NewTime(time.Date(2026, 8, 12, 10, 1, 0, 0, time.UTC))

	patchOps := func(name string, created metav1.Time, phase agentsv1alpha1.SandboxUpdateOpsPhase) *agentsv1alpha1.SandboxUpdateOps {
		ops := newSandboxUpdateOps(name, "default", phase, false, nil)
		ops.CreationTimestamp = created
		return ops
	}
	templateOps := func(name string, created metav1.Time, phase agentsv1alpha1.SandboxUpdateOpsPhase) *agentsv1alpha1.SandboxUpdateOps {
		ops := newTemplateOps(name, "default", phase)
		ops.CreationTimestamp = created
		return ops
	}

	tests := []struct {
		name        string
		self        *agentsv1alpha1.SandboxUpdateOps
		other       *agentsv1alpha1.SandboxUpdateOps
		wantBlocked bool
	}{
		{
			name:        "patch ops blocked by older active patch ops",
			self:        patchOps("self", t1, ""),
			other:       patchOps("other", t0, agentsv1alpha1.SandboxUpdateOpsUpdating),
			wantBlocked: true,
		},
		{
			name:        "template ops run concurrently with older template ops",
			self:        templateOps("self", t1, ""),
			other:       templateOps("other", t0, agentsv1alpha1.SandboxUpdateOpsUpdating),
			wantBlocked: false,
		},
		{
			name:        "template ops blocked by older active patch ops",
			self:        templateOps("self", t1, ""),
			other:       patchOps("other", t0, agentsv1alpha1.SandboxUpdateOpsUpdating),
			wantBlocked: true,
		},
		{
			name:        "patch ops blocked by older active template ops",
			self:        patchOps("self", t1, ""),
			other:       templateOps("other", t0, agentsv1alpha1.SandboxUpdateOpsUpdating),
			wantBlocked: true,
		},
		{
			name:        "older ops proceeds past a newer conflicting ops",
			self:        patchOps("self", t0, agentsv1alpha1.SandboxUpdateOpsUpdating),
			other:       patchOps("other", t1, ""),
			wantBlocked: false,
		},
		{
			name:        "terminal ops does not block",
			self:        patchOps("self", t1, ""),
			other:       patchOps("other", t0, agentsv1alpha1.SandboxUpdateOpsCompleted),
			wantBlocked: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestReconciler(tt.self, tt.other)
			blocked, err := r.blockedByModeBarrier(context.Background(), tt.self)
			require.NoError(t, err)
			assert.Equal(t, tt.wantBlocked, blocked)

			if tt.wantBlocked {
				// A blocked ops surfaces as (non-terminal) Pending so the
				// webhook keeps rejecting further conflicting creations.
				updated := &agentsv1alpha1.SandboxUpdateOps{}
				require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: tt.self.Name, Namespace: "default"}, updated))
				assert.Equal(t, agentsv1alpha1.SandboxUpdateOpsPending, updated.Status.Phase)
			}
		})
	}
}

func TestReconcileTemplateMode_FastPathAndCompletion(t *testing.T) {
	ops := newTemplateOps("test-ops", "default", "")
	ops.Spec.Template = sandboxTemplate()
	sbx := newSandbox("sbx-1", "default", "", agentsv1alpha1.SandboxRunning, nil)
	r := newTestReconciler(ops, sbx)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-ops", Namespace: "default"}}

	// First reconcile stamps the already-matching sandbox directly.
	result, err := r.Reconcile(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)

	updatedSbx := &agentsv1alpha1.Sandbox{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "sbx-1", Namespace: "default"}, updatedSbx))
	assert.Equal(t, opsRevision(ops), updatedSbx.Annotations[agentsv1alpha1.AnnotationUpdateOpsRevision])
	// The fast path is metadata-only: no round, no occupancy, no ownership.
	assert.Empty(t, updatedSbx.Annotations[agentsv1alpha1.AnnotationUpdateOpsPendingRevision])
	assert.Empty(t, updatedSbx.Labels[agentsv1alpha1.LabelSandboxUpdateOps])
	assert.Nil(t, updatedSbx.Spec.UpgradePolicy)

	// Second reconcile observes the stamp and completes.
	_, err = r.Reconcile(context.Background(), req)
	require.NoError(t, err)

	updatedOps := &agentsv1alpha1.SandboxUpdateOps{}
	require.NoError(t, r.Get(context.Background(), req.NamespacedName, updatedOps))
	assert.Equal(t, agentsv1alpha1.SandboxUpdateOpsCompleted, updatedOps.Status.Phase)
	assert.Equal(t, int32(1), updatedOps.Status.UpdatedReplicas)
}

func TestReconcileTemplateMode_RoundStartIsAtomic(t *testing.T) {
	ops := newTemplateOps("test-ops", "default", "")
	sbx := newSandbox("sbx-1", "default", "", agentsv1alpha1.SandboxRunning, nil)
	r := newTestReconciler(ops, sbx)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-ops", Namespace: "default"}}

	_, err := r.Reconcile(context.Background(), req)
	require.NoError(t, err)

	// One patch carries template, pending record, label, and policy together.
	updatedSbx := &agentsv1alpha1.Sandbox{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "sbx-1", Namespace: "default"}, updatedSbx))
	require.NotNil(t, updatedSbx.Spec.Template)
	assert.Equal(t, "busybox:2.0", updatedSbx.Spec.Template.Spec.Containers[0].Image)
	assert.Equal(t, opsRevision(ops), updatedSbx.Annotations[agentsv1alpha1.AnnotationUpdateOpsPendingRevision])
	assert.Equal(t, "test-ops", updatedSbx.Labels[agentsv1alpha1.LabelSandboxUpdateOps])
	require.NotNil(t, updatedSbx.Spec.UpgradePolicy)
	assert.Equal(t, agentsv1alpha1.SandboxUpgradePolicyRecreate, updatedSbx.Spec.UpgradePolicy.Type)
	// No stamp before the round terminates.
	assert.Empty(t, updatedSbx.Annotations[agentsv1alpha1.AnnotationUpdateOpsRevision])

	updatedOps := &agentsv1alpha1.SandboxUpdateOps{}
	require.NoError(t, r.Get(context.Background(), req.NamespacedName, updatedOps))
	assert.Equal(t, agentsv1alpha1.SandboxUpdateOpsUpdating, updatedOps.Status.Phase)
}

func TestReconcileTemplateMode_WaitingPollsForOccupiedSandbox(t *testing.T) {
	ops := newTemplateOps("test-ops", "default", agentsv1alpha1.SandboxUpdateOpsUpdating)
	sbx := newSandbox("sbx-1", "default", "", agentsv1alpha1.SandboxRunning, nil)
	sbx.Annotations = map[string]string{
		agentsv1alpha1.AnnotationUpdateOpsPendingRevision: "2027-01-01T00:00:00Z/other-ops",
	}
	r := newTestReconciler(ops, sbx)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-ops", Namespace: "default"}}

	result, err := r.Reconcile(context.Background(), req)
	require.NoError(t, err)
	// The occupied sandbox's events route to the owning ops, so this ops
	// polls to re-read stamps and occupancy.
	assert.Equal(t, ctrl.Result{RequeueAfter: templateModePollInterval}, result)

	// The occupied sandbox is never re-targeted.
	updatedSbx := &agentsv1alpha1.Sandbox{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "sbx-1", Namespace: "default"}, updatedSbx))
	assert.Equal(t, "busybox:1.0", updatedSbx.Spec.Template.Spec.Containers[0].Image)

	updatedOps := &agentsv1alpha1.SandboxUpdateOps{}
	require.NoError(t, r.Get(context.Background(), req.NamespacedName, updatedOps))
	assert.Equal(t, int32(1), updatedOps.Status.WaitingReplicas)
	assert.Equal(t, agentsv1alpha1.SandboxUpdateOpsUpdating, updatedOps.Status.Phase)
}

func TestReconcileTemplateMode_NewerStampSettlesAsSuperseded(t *testing.T) {
	ops := newTemplateOps("test-ops", "default", agentsv1alpha1.SandboxUpdateOpsUpdating)
	sbx := newSandbox("sbx-1", "default", "", agentsv1alpha1.SandboxRunning, nil)
	sbx.Annotations = map[string]string{
		agentsv1alpha1.AnnotationUpdateOpsRevision: "2027-01-01T00:00:00Z/newer-ops",
	}
	r := newTestReconciler(ops, sbx)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-ops", Namespace: "default"}}

	_, err := r.Reconcile(context.Background(), req)
	require.NoError(t, err)

	// Immediate settlement: no round started, ops completes.
	updatedOps := &agentsv1alpha1.SandboxUpdateOps{}
	require.NoError(t, r.Get(context.Background(), req.NamespacedName, updatedOps))
	assert.Equal(t, agentsv1alpha1.SandboxUpdateOpsCompleted, updatedOps.Status.Phase)
	assert.Equal(t, int32(1), updatedOps.Status.SupersededReplicas)
	assert.Equal(t, int32(0), updatedOps.Status.UpdatedReplicas)

	updatedSbx := &agentsv1alpha1.Sandbox{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "sbx-1", Namespace: "default"}, updatedSbx))
	assert.Equal(t, "busybox:1.0", updatedSbx.Spec.Template.Spec.Containers[0].Image)
	assert.Empty(t, updatedSbx.Annotations[agentsv1alpha1.AnnotationUpdateOpsPendingRevision])
}

func TestReconcileTemplateMode_PausedSandboxPhase1DefersTemplate(t *testing.T) {
	ops := newTemplateOps("test-ops", "default", "")
	ops.Spec.StateFilter = &agentsv1alpha1.UpgradeStateFilter{
		States: []agentsv1alpha1.SandboxPhase{agentsv1alpha1.SandboxRunning, agentsv1alpha1.SandboxPaused},
	}
	sbx := newSandbox("sbx-1", "default", "", agentsv1alpha1.SandboxPaused, []metav1.Condition{{
		Type:   string(agentsv1alpha1.SandboxConditionPaused),
		Status: metav1.ConditionTrue,
		Reason: "PauseSucceed",
	}})
	sbx.Spec.Paused = true
	r := newTestReconciler(ops, sbx)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-ops", Namespace: "default"}}

	_, err := r.Reconcile(context.Background(), req)
	require.NoError(t, err)

	// Phase 1 claims occupancy and triggers the resume, but the template
	// write is deferred to phase 2.
	updatedSbx := &agentsv1alpha1.Sandbox{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "sbx-1", Namespace: "default"}, updatedSbx))
	assert.Equal(t, "busybox:1.0", updatedSbx.Spec.Template.Spec.Containers[0].Image)
	assert.Equal(t, opsRevision(ops), updatedSbx.Annotations[agentsv1alpha1.AnnotationUpdateOpsPendingRevision])
	assert.Equal(t, agentsv1alpha1.True, updatedSbx.Annotations[agentsv1alpha1.AnnotationUpgradeResumeTrigger])
	assert.Equal(t, "test-ops", updatedSbx.Labels[agentsv1alpha1.LabelSandboxUpdateOps])
	require.NotNil(t, updatedSbx.Spec.UpgradePolicy)
}

func TestReconcileTemplateMode_PausedStillRunsPhase2(t *testing.T) {
	ops := newTemplateOps("test-ops", "default", agentsv1alpha1.SandboxUpdateOpsUpdating)
	ops.Spec.Paused = true
	// The sandbox finished phase 1: it carries this ops' pending record and
	// has resumed with the old template (ResumeSucceed).
	sbx := newSandbox("sbx-1", "default", "test-ops", agentsv1alpha1.SandboxRunning, []metav1.Condition{{
		Type:   string(agentsv1alpha1.SandboxConditionUpgrading),
		Status: metav1.ConditionTrue,
		Reason: agentsv1alpha1.SandboxUpgradingReasonResumeSucceed,
	}})
	sbx.Annotations = map[string]string{
		agentsv1alpha1.AnnotationUpdateOpsPendingRevision: opsRevision(ops),
		agentsv1alpha1.AnnotationUpgradeResumeTrigger:     agentsv1alpha1.True,
	}
	// A second sandbox eligible for a new round must NOT be started while paused.
	sbx2 := newSandbox("sbx-2", "default", "", agentsv1alpha1.SandboxRunning, nil)
	r := newTestReconciler(ops, sbx, sbx2)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-ops", Namespace: "default"}}

	_, err := r.Reconcile(context.Background(), req)
	require.NoError(t, err)

	// spec.paused only brakes new rounds; the in-flight two-phase flow is
	// finished: target template written, resume trigger dropped.
	updatedSbx := &agentsv1alpha1.Sandbox{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "sbx-1", Namespace: "default"}, updatedSbx))
	assert.Equal(t, "busybox:2.0", updatedSbx.Spec.Template.Spec.Containers[0].Image)
	assert.Empty(t, updatedSbx.Annotations[agentsv1alpha1.AnnotationUpgradeResumeTrigger])
	// The pending record stays: round terminal handling is owned by the
	// sandbox controller.
	assert.Equal(t, opsRevision(ops), updatedSbx.Annotations[agentsv1alpha1.AnnotationUpdateOpsPendingRevision])

	untouchedSbx := &agentsv1alpha1.Sandbox{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "sbx-2", Namespace: "default"}, untouchedSbx))
	assert.Equal(t, "busybox:1.0", untouchedSbx.Spec.Template.Spec.Containers[0].Image)
	assert.Empty(t, untouchedSbx.Annotations[agentsv1alpha1.AnnotationUpdateOpsPendingRevision])
}

func TestReconcileTemplateMode_FastPathFailureStaysNonTerminal(t *testing.T) {
	ops := newTemplateOps("test-ops", "default", agentsv1alpha1.SandboxUpdateOpsUpdating)
	ops.Spec.Template = sandboxTemplate()
	sbx := newSandbox("sbx-1", "default", "", agentsv1alpha1.SandboxRunning, nil)

	fakeClient := fake.NewClientBuilder().
		WithScheme(testScheme).
		WithStatusSubresource(&agentsv1alpha1.SandboxUpdateOps{}, &agentsv1alpha1.Sandbox{}).
		WithRuntimeObjects(ops, sbx).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if _, ok := obj.(*agentsv1alpha1.Sandbox); ok {
					return apierrors.NewConflict(schema.GroupResource{Group: "agents.kruise.io", Resource: "sandboxes"}, obj.GetName(), fmt.Errorf("simulated conflict"))
				}
				return c.Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()
	r := &Reconciler{Client: fakeClient, Scheme: testScheme, Recorder: record.NewFakeRecorder(100)}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-ops", Namespace: "default"}}

	// The stamp write conflicts; the error surfaces so the reconcile retries.
	_, err := r.Reconcile(context.Background(), req)
	require.Error(t, err)

	// The failed stamp target counts towards total but no settled category,
	// so the ops cannot reach a terminal phase.
	updatedOps := &agentsv1alpha1.SandboxUpdateOps{}
	require.NoError(t, r.Get(context.Background(), req.NamespacedName, updatedOps))
	assert.Equal(t, agentsv1alpha1.SandboxUpdateOpsUpdating, updatedOps.Status.Phase)
	assert.Equal(t, int32(1), updatedOps.Status.Replicas)
	assert.Equal(t, int32(0), updatedOps.Status.UpdatedReplicas)

	// No stamp was written.
	updatedSbx := &agentsv1alpha1.Sandbox{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: "sbx-1", Namespace: "default"}, updatedSbx))
	assert.Empty(t, updatedSbx.Annotations[agentsv1alpha1.AnnotationUpdateOpsRevision])
}
