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
	"crypto/rand"
	"encoding/hex"

	"go.opentelemetry.io/otel/trace"
)

// requestIDKey is the context key for storing the request ID.
// The custom IDGenerator reads it to produce a TraceID equal to the request ID,
// enabling unified trace-log correlation without manual span context construction.
type requestIDKey struct{}

// WithRequestID stores the request ID in the context so that the custom
// IDGenerator can use it as the TraceID when creating a new root span.
// The requestID must be a 32-char hex string (UUID without hyphens) to match
// the OTel TraceID format, ensuring requestID == traceID.
func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, requestID)
}

// RequestIDGenerator implements sdktrace.IDGenerator.
// When the context carries a request ID (via WithRequestID), it is converted
// to the TraceID. Otherwise, a random TraceID is generated as fallback.
type RequestIDGenerator struct{}

// NewIDs returns a new TraceID and SpanID.
// If the context contains a valid request ID (UUID), it is used as the TraceID.
func (g *RequestIDGenerator) NewIDs(ctx context.Context) (trace.TraceID, trace.SpanID) {
	var traceID trace.TraceID
	if requestID, ok := ctx.Value(requestIDKey{}).(string); ok {
		if len(requestID) == 32 {
			if bytes, err := hex.DecodeString(requestID); err == nil && len(bytes) == 16 {
				copy(traceID[:], bytes)
				return traceID, g.NewSpanID(ctx, traceID)
			}
		}
	}
	// Fallback: random trace ID
	_, _ = rand.Read(traceID[:])
	return traceID, g.NewSpanID(ctx, traceID)
}

// NewSpanID returns a new random SpanID.
func (g *RequestIDGenerator) NewSpanID(_ context.Context, _ trace.TraceID) trace.SpanID {
	var spanID trace.SpanID
	_, _ = rand.Read(spanID[:])
	return spanID
}
