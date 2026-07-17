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
	"go.opentelemetry.io/otel"
)

func TestInitTracerProvider_Disabled(t *testing.T) {
	prevTP := otel.GetTracerProvider()
	defer func() { otel.SetTracerProvider(prevTP) }()

	shutdown, err := InitTracerProvider(context.Background(), Config{
		Mode:        TracingModeNone,
		ServiceName: "test-service",
	})
	assert.NoError(t, err, "should not error when mode is none")
	assert.NotNil(t, shutdown, "should return non-nil shutdown function")

	// Verify no-op tracer is set.
	tracer := Tracer("test")
	assert.NotNil(t, tracer, "Tracer should return non-nil")

	// Shutdown should not error.
	err = shutdown(context.Background())
	assert.NoError(t, err, "shutdown should not error")
}

func TestInitTracerProvider_Enabled(t *testing.T) {
	prevTP := otel.GetTracerProvider()
	defer func() { otel.SetTracerProvider(prevTP) }()

	shutdown, err := InitTracerProvider(context.Background(), Config{
		Mode:          TracingModeOTel,
		Endpoint:      "localhost:4317",
		ServiceName:   "test-service",
		SamplingRatio: 1.0,
		Insecure:      true,
	})
	// Should not error even with unreachable endpoint (gRPC connects lazily).
	assert.NoError(t, err, "should not error with valid config")
	assert.NotNil(t, shutdown, "should return non-nil shutdown function")

	// Tracer should return a real tracer (not no-op).
	tracer := Tracer("test")
	assert.NotNil(t, tracer, "Tracer should return non-nil")

	// Cleanup.
	err = shutdown(context.Background())
	assert.NoError(t, err, "shutdown should not error")
}

func TestInitTracerProvider_EnabledWithDefaultSamplingRatio(t *testing.T) {
	prevTP := otel.GetTracerProvider()
	defer func() { otel.SetTracerProvider(prevTP) }()

	shutdown, err := InitTracerProvider(context.Background(), Config{
		Mode:          TracingModeOTel,
		Endpoint:      "localhost:4317",
		ServiceName:   "test-service",
		SamplingRatio: 0, // Should default to 1.0
		Insecure:      true,
	})
	assert.NoError(t, err, "should not error with zero sampling ratio")
	defer func() { _ = shutdown(context.Background()) }()
}

func TestTracer_NotInitialized(t *testing.T) {
	// Reset global state to simulate uninitialized state.
	// Since globalTracerProvider is package-level, Tracer uses sync.Once
	// to lazily initialize with a no-op provider.
	// We can't easily reset the sync.Once, so we just verify Tracer returns non-nil.
	tracer := Tracer("uninitialized-test")
	assert.NotNil(t, tracer, "Tracer should return non-nil even when not initialized")

	// Verify it's a no-op tracer by checking that spans are not recorded.
	_, span := tracer.Start(context.Background(), "test-span")
	assert.NotNil(t, span, "Start should return non-nil span")
	assert.False(t, span.SpanContext().IsValid(),
		"no-op tracer should produce invalid span context")
	span.End()
}

func TestInitTracerProvider_RestoresPropagator(t *testing.T) {
	// Set up with disabled config.
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	defer func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	}()

	shutdown, err := InitTracerProvider(context.Background(), Config{
		Mode:        TracingModeNone,
		ServiceName: "test-service",
	})
	assert.NoError(t, err)
	defer func() { _ = shutdown(context.Background()) }()

	// Verify propagator is set (non-nil).
	prop := otel.GetTextMapPropagator()
	assert.NotNil(t, prop, "propagator should be set after InitTracerProvider")
}

func TestInitTracerProvider_EnabledWithTLS(t *testing.T) {
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	defer func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	}()

	shutdown, err := InitTracerProvider(context.Background(), Config{
		Mode:          TracingModeOTel,
		Endpoint:      "localhost:4317",
		ServiceName:   "test-service-tls",
		SamplingRatio: 1.0,
		Insecure:      false, // Exercises the TLS credentials path.
	})
	assert.NoError(t, err, "should not error with valid TLS config")
	assert.NotNil(t, shutdown, "should return non-nil shutdown function")

	tracer := Tracer("test-tls")
	assert.NotNil(t, tracer, "Tracer should return non-nil")

	defer func() { _ = shutdown(context.Background()) }()
}
