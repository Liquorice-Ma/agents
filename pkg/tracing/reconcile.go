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

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// traceIDKey is the context key for storing the trace ID extracted from
// the Reconcile span. Callers can use TraceIDFromContext to retrieve it and
// inject it into their logging framework (e.g., klog.FromContext).
type traceIDKey struct{}

// TraceIDFromContext returns the trace ID stored in ctx by StartReconcileSpan.
// Returns empty string if no trace ID is present (e.g., tracing disabled or
// StartReconcileSpan was not called).
func TraceIDFromContext(ctx context.Context) string {
	if id, ok := ctx.Value(traceIDKey{}).(string); ok {
		return id
	}
	return ""
}

// StartReconcileSpan creates a Span for a controller-runtime Reconcile iteration.
// It extracts the trace context from the CR's annotations to establish a parent-child
// relationship with the sandbox-manager root Span. Multiple Reconcile iterations
// for the same user operation produce sibling Spans (same TraceID, different SpanID).
//
// If no trace-context annotation exists (e.g., kubectl-created sandbox), the Span
// starts a new root trace — still useful for manual search via sandbox UID attribute.
//
// IMPORTANT: The caller must invoke this AFTER all early-return paths that indicate
// "no work to do" (e.g., Sandbox not found, terminal state, expectation unsatisfied).
// This avoids creating noise Spans for no-op Reconciles.
func StartReconcileSpan(ctx context.Context, obj client.Object, controllerName string) (context.Context, trace.Span) {
	// Extract trace context from CR annotations.
	annotations := obj.GetAnnotations()
	ctx = ExtractTraceContext(ctx, annotations)

	// Attach a fresh write flag so downstream write operations can mark this
	// Reconcile as having performed real work. Reconcile and EnsureSandbox*
	// Spans with no write are dropped by FilteringSpanProcessor (see
	// EndSpanWithWriteCheck).
	ctx = WithWriteFlag(ctx)

	tracer := Tracer(controllerName)
	attrs := []attribute.KeyValue{
		attribute.String(AttrSandboxID, string(obj.GetUID())),
		attribute.String(AttrSandboxNamespace, obj.GetNamespace()),
		attribute.String(AttrSandboxName, obj.GetName()),
	}
	ctx, span := tracer.Start(ctx, SpanControllerReconcile,
		trace.WithAttributes(attrs...),
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	// Store trace ID in context so callers can inject it into their logger
	// for unified trace-log correlation across manager and controller.
	if span.SpanContext().TraceID().IsValid() {
		ctx = context.WithValue(ctx, traceIDKey{}, span.SpanContext().TraceID().String())
	}
	return ctx, span
}

// StartChildSpan creates a child Span within a Reconcile for specific IO operations
// (e.g., CreatePod, UpdateStatus, Checkpoint).
//
// If the context does not carry a valid parent span (e.g., tracing is disabled or
// the Reconcile Span was a noop), StartChildSpan returns a noop span to avoid
// creating orphan root spans that would pollute trace data.
func StartChildSpan(ctx context.Context, spanName string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	// If no valid parent span exists in ctx, return noop to avoid orphan spans.
	if !trace.SpanFromContext(ctx).SpanContext().IsValid() {
		return ctx, trace.SpanFromContext(context.Background())
	}

	// A write-operation Span (e.g. CreatePod, DeletePod, updateSandboxStatus)
	// means this Reconcile did real work; mark it so the enclosing Reconcile and
	// EnsureSandbox* Spans are retained rather than filtered as no-op.
	if writeSpanNames[spanName] {
		MarkWrite(ctx)
	}

	tracer := Tracer("sandbox-controller")
	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindInternal),
	}
	if len(attrs) > 0 {
		opts = append(opts, trace.WithAttributes(attrs...))
	}
	return tracer.Start(ctx, spanName, opts...)
}

// EndSpanWithWriteCheck ends span, first marking it no-op when the current
// Reconcile performed no write operation (see MarkWrite/HasWrite). No-op Spans
// are dropped by FilteringSpanProcessor so that empty, write-free Reconcile
// iterations (and their EnsureSandbox* children) stay out of the trace, while
// any Reconcile that created/deleted a Pod or patched status is fully retained.
func EndSpanWithWriteCheck(ctx context.Context, span trace.Span) {
	if !HasWrite(ctx) {
		span.SetAttributes(attribute.Bool(AttrReconcileNoop, true))
	}
	span.End()
}
