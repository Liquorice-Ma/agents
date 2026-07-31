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

package tracing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// withTracingEnabled flips the enabled flag for the duration of a test.
func withTracingEnabled(t *testing.T, enabled bool) {
	t.Helper()
	prev := enabledFlag.Load()
	enabledFlag.Store(enabled)
	t.Cleanup(func() { enabledFlag.Store(prev) })
}

func TestNewWriteTrackingClient_DisabledReturnsOriginal(t *testing.T) {
	withTracingEnabled(t, false)
	base := clientfake.NewClientBuilder().Build()
	assert.Same(t, base, NewWriteTrackingClient(base),
		"with tracing disabled the original client must be returned unwrapped")
}

func TestNewWriteTrackingClient_EnabledWraps(t *testing.T) {
	withTracingEnabled(t, true)
	base := clientfake.NewClientBuilder().Build()
	assert.NotSame(t, client.Client(base), NewWriteTrackingClient(base),
		"with tracing enabled the client must be wrapped")
}

// TestWriteTrackingClient_Verbs verifies every write verb marks the
// per-Reconcile write flag (regardless of the call outcome) and read verbs
// never do.
func TestWriteTrackingClient_Verbs(t *testing.T) {
	withTracingEnabled(t, true)

	pod := func() *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"}}
	}

	tests := []struct {
		name      string
		call      func(ctx context.Context, c client.Client)
		wantWrite bool
	}{
		{
			name:      "Create marks write",
			call:      func(ctx context.Context, c client.Client) { _ = c.Create(ctx, pod()) },
			wantWrite: true,
		},
		{
			name:      "Update marks write even on error",
			call:      func(ctx context.Context, c client.Client) { _ = c.Update(ctx, pod()) },
			wantWrite: true,
		},
		{
			name: "Patch marks write even on error",
			call: func(ctx context.Context, c client.Client) {
				_ = c.Patch(ctx, pod(), client.MergeFrom(pod()))
			},
			wantWrite: true,
		},
		{
			name:      "Delete marks write even when object is absent",
			call:      func(ctx context.Context, c client.Client) { _ = c.Delete(ctx, pod()) },
			wantWrite: true,
		},
		{
			name:      "DeleteAllOf marks write",
			call:      func(ctx context.Context, c client.Client) { _ = c.DeleteAllOf(ctx, pod()) },
			wantWrite: true,
		},
		{
			name: "Status Update marks write even on error",
			call: func(ctx context.Context, c client.Client) {
				_ = c.Status().Update(ctx, pod())
			},
			wantWrite: true,
		},
		{
			name: "Status Patch marks write even on error",
			call: func(ctx context.Context, c client.Client) {
				_ = c.Status().Patch(ctx, pod(), client.MergeFrom(pod()))
			},
			wantWrite: true,
		},
		{
			name: "SubResource Update marks write even on error",
			call: func(ctx context.Context, c client.Client) {
				_ = c.SubResource("status").Update(ctx, pod())
			},
			wantWrite: true,
		},
		{
			name: "Get does not mark write",
			call: func(ctx context.Context, c client.Client) {
				_ = c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "p"}, pod())
			},
			wantWrite: false,
		},
		{
			name: "List does not mark write",
			call: func(ctx context.Context, c client.Client) {
				_ = c.List(ctx, &corev1.PodList{})
			},
			wantWrite: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cli := NewWriteTrackingClient(clientfake.NewClientBuilder().Build())
			ctx := withWriteFlag(context.Background())
			tt.call(ctx, cli)
			assert.Equal(t, tt.wantWrite, hasWrite(ctx))
		})
	}
}

// TestWriteTrackingClient_NoFlagContext verifies writes outside a Reconcile
// (no write flag in ctx) are safe no-ops for tracking.
func TestWriteTrackingClient_NoFlagContext(t *testing.T) {
	withTracingEnabled(t, true)
	cli := NewWriteTrackingClient(clientfake.NewClientBuilder().Build())
	ctx := context.Background()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"}}
	assert.NoError(t, cli.Create(ctx, pod))
	assert.False(t, hasWrite(ctx))
}
