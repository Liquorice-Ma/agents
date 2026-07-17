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
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// TracingMode selects the distributed tracing backend.
type TracingMode string

const (
	// TracingModeOTel uses OpenTelemetry with OTLP gRPC exporter.
	TracingModeOTel TracingMode = "otel"

	// TracingModeNone disables tracing; a no-op tracer is installed.
	TracingModeNone TracingMode = "none"
)

// DefaultEndpoint is the default OTLP gRPC endpoint for tracing export.
// Enterprise deployments may override this via inner_provider.go init().
var DefaultEndpoint = "otel-collector:4317"

// Config holds the configuration for distributed tracing.
type Config struct {
	Mode          TracingMode
	Endpoint      string  // OTLP gRPC endpoint, e.g., "otel-collector:4317"
	ServiceName   string  // e.g., "sandbox-controller" or "sandbox-manager"
	SamplingRatio float64 // 0.0 to 1.0, default 1.0
	Insecure      bool    // Use insecure gRPC (dev environment)
}

// InitTracerProvider initializes the global TracerProvider and returns a shutdown function.
// Must be called once at startup, before any controller or HTTP server starts.
// If cfg.Mode is not TracingModeOTel, sets up a no-op tracer and returns a no-op shutdown function.
func InitTracerProvider(ctx context.Context, cfg Config) (func(context.Context) error, error) {
	if cfg.Mode != TracingModeOTel {
		noopTP := trace.NewNoopTracerProvider()
		otel.SetTracerProvider(noopTP)
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		))
		return func(context.Context) error { return nil }, nil
	}

	// Set default sampling ratio.
	if cfg.SamplingRatio <= 0 {
		cfg.SamplingRatio = 1.0
	}

	// Create OTLP gRPC exporter.
	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(cfg.Endpoint),
	}
	if cfg.Insecure {
		opts = append(opts, otlptracegrpc.WithTLSCredentials(insecure.NewCredentials()))
	} else {
		opts = append(opts, otlptracegrpc.WithTLSCredentials(credentials.NewTLS(nil)))
	}

	exporter, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP gRPC exporter: %w", err)
	}

	// Create resource with service attributes.
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion("0.1.0"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Create TracerProvider with a FilteringSpanProcessor wrapping the batch
	// processor. The filter drops Reconcile Spans marked no-op (no write
	// operation), keeping empty Reconcile iterations out of exported traces.
	// Use custom RequestIDGenerator so that TraceID equals the request ID,
	// enabling unified trace-log correlation.
	batcher := sdktrace.NewBatchSpanProcessor(exporter)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(NewFilteringSpanProcessor(batcher)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SamplingRatio))),
		sdktrace.WithIDGenerator(&RequestIDGenerator{}),
	)

	// Set global tracer provider and propagator.
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}

// Tracer returns a tracer for the specified instrumentation scope.
// Uses the global OTel TracerProvider, which is set by InitTracerProvider.
// If InitTracerProvider has not been called, the OTel SDK returns a no-op tracer.
func Tracer(name string) trace.Tracer {
	return otel.GetTracerProvider().Tracer(name)
}
