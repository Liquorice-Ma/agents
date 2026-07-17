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
	"sync/atomic"
)

// writeFlagKey is the context key for the per-Reconcile write flag.
type writeFlagKey struct{}

// writeFlag tracks whether any real write operation (e.g. CreatePod, DeletePod,
// status Patch, finalizer removal) occurred during a single Reconcile iteration.
// It is shared across the whole Reconcile call tree via context so that the
// Reconcile Span (and its EnsureSandbox* child Spans) can be marked as no-op and
// dropped by FilteringSpanProcessor when nothing was actually written.
type writeFlag struct {
	written atomic.Bool
}

// WithWriteFlag returns a context carrying a fresh write flag. It must be called
// once at the start of each Reconcile iteration (in StartReconcileSpan) so that
// downstream write operations can mark it via MarkWrite.
func WithWriteFlag(ctx context.Context) context.Context {
	return context.WithValue(ctx, writeFlagKey{}, &writeFlag{})
}

// MarkWrite records that a real write operation happened in the current Reconcile.
// It is a no-op if the context carries no write flag (e.g. tracing disabled or
// called outside a Reconcile). Safe for concurrent use.
func MarkWrite(ctx context.Context) {
	if f, ok := ctx.Value(writeFlagKey{}).(*writeFlag); ok {
		f.written.Store(true)
	}
}

// HasWrite reports whether MarkWrite was called for the current Reconcile.
// Returns false if the context carries no write flag.
func HasWrite(ctx context.Context) bool {
	if f, ok := ctx.Value(writeFlagKey{}).(*writeFlag); ok {
		return f.written.Load()
	}
	return false
}
