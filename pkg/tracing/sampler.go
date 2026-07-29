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
	crand "crypto/rand"
	"encoding/binary"
	"fmt"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// randomRatioSampler samples root spans with the configured probability using
// a server-controlled random draw instead of the TraceID.
//
// With the custom RequestIDGenerator, the TraceID equals the caller-provided
// request ID. The standard TraceIDRatioBased sampler derives its decision
// deterministically from the TraceID, so a caller could choose request IDs
// that always (or never) fall inside the sampled range, defeating the
// configured rate as a capacity/cost limit and making sampling
// caller-controllable. Drawing from crypto/rand keeps the decision
// unpredictable to callers; the cost of one 8-byte read per root span is
// negligible next to span creation itself.
//
// ratio semantics: >= 1 always samples, 0 never samples (values outside
// [0, 1] are rejected by InitTracerProvider before this sampler is built).
type randomRatioSampler struct {
	ratio float64
}

// ShouldSample implements sdktrace.Sampler.
func (s randomRatioSampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	psc := trace.SpanContextFromContext(p.ParentContext)
	decision := sdktrace.Drop
	if s.ratio >= 1 || randomFloat64() < s.ratio {
		decision = sdktrace.RecordAndSample
	}
	return sdktrace.SamplingResult{
		Decision:   decision,
		Tracestate: psc.TraceState(),
	}
}

// randomFloat64 returns a uniform float64 in [0, 1) drawn from crypto/rand,
// using the top 53 bits for full mantissa precision.
func randomFloat64() float64 {
	var b [8]byte
	// crypto/rand.Read does not fail on supported platforms.
	_, _ = crand.Read(b[:])
	return float64(binary.BigEndian.Uint64(b[:])>>11) / (1 << 53)
}

// Description implements sdktrace.Sampler.
func (s randomRatioSampler) Description() string {
	return fmt.Sprintf("RandomRatioBased{%g}", s.ratio)
}
