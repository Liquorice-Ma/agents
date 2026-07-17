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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestHTTPMiddleware_PassesRequestThrough(t *testing.T) {
	cleanup := initNoopTracer()
	defer cleanup()

	called := false
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})

	wrapped := HTTPMiddleware(handler, "test-service")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/sandboxes", nil)
	wrapped.ServeHTTP(rec, req)

	assert.True(t, called, "handler should be called")
	assert.Equal(t, http.StatusOK, rec.Code, "status code should be 200")
}

func TestHTTPMiddleware_CreatesSpan(t *testing.T) {
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	defer func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	}()

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	var spanFromCtx trace.Span
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		spanFromCtx = trace.SpanFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	wrapped := HTTPMiddleware(handler, "test-service")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/sandboxes", nil)
	wrapped.ServeHTTP(rec, req)

	assert.NotNil(t, spanFromCtx, "span should be in request context")
	assert.True(t, spanFromCtx.SpanContext().IsValid(),
		"span context should be valid when tracing is enabled")
}

func TestHTTPMiddleware_NoopProviderNoCrash(t *testing.T) {
	cleanup := initNoopTracer()
	defer cleanup()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify span context is not valid with noop tracer.
		span := trace.SpanFromContext(r.Context())
		assert.False(t, span.SpanContext().IsValid(),
			"noop tracer should produce invalid span context")
		w.WriteHeader(http.StatusOK)
	})

	wrapped := HTTPMiddleware(handler, "test-service")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/sandboxes/test-id", nil)
	wrapped.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHTTPMiddleware_PropagatesTraceContext(t *testing.T) {
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	defer func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	}()

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	// Create a parent span and inject its context into headers.
	tracer := tp.Tracer("test")
	ctx, parentSpan := tracer.Start(context.Background(), "parent")
	headers := make(map[string]string)
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(headers))

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		childSpan := trace.SpanFromContext(r.Context())
		assert.True(t, childSpan.SpanContext().IsValid(),
			"child span should be valid")
		assert.Equal(t, parentSpan.SpanContext().TraceID(), childSpan.SpanContext().TraceID(),
			"child span should share parent trace ID")
		w.WriteHeader(http.StatusOK)
	})

	wrapped := HTTPMiddleware(handler, "test-service")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/sandboxes", nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	wrapped.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	parentSpan.End()
}
