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
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// sampleWith runs ShouldSample n times against fresh root parameters carrying
// the given TraceID and returns how many decisions were RecordAndSample.
func sampleWith(s sdktrace.Sampler, traceID trace.TraceID, n int) int {
	sampled := 0
	for i := 0; i < n; i++ {
		res := s.ShouldSample(sdktrace.SamplingParameters{
			ParentContext: context.Background(),
			TraceID:       traceID,
			Name:          "op",
		})
		if res.Decision == sdktrace.RecordAndSample {
			sampled++
		}
	}
	return sampled
}

func TestRandomRatioSampler_Boundaries(t *testing.T) {
	traceID := trace.TraceID{0x01}

	tests := []struct {
		name        string
		ratio       float64
		n           int
		wantSampled int
	}{
		{name: "ratio 0 never samples", ratio: 0, n: 1000, wantSampled: 0},
		{name: "ratio 1 always samples", ratio: 1, n: 1000, wantSampled: 1000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sampleWith(randomRatioSampler{ratio: tt.ratio}, traceID, tt.n)
			assert.Equal(t, tt.wantSampled, got)
		})
	}
}

// TestRandomRatioSampler_NotTraceIDDeterministic verifies the decision is not
// a deterministic function of the TraceID: with the request-ID-as-TraceID
// scheme, a caller must not be able to pick an ID that is always sampled.
// With ratio 0.5 and 400 draws on a FIXED TraceID, a TraceID-deterministic
// sampler would return exactly 0 or 400; the random sampler lands in between
// (the probability of hitting either extreme is 2^-400).
func TestRandomRatioSampler_NotTraceIDDeterministic(t *testing.T) {
	fixedTraceID := trace.TraceID{0xde, 0xad, 0xbe, 0xef}
	const n = 400

	got := sampleWith(randomRatioSampler{ratio: 0.5}, fixedTraceID, n)
	assert.Greater(t, got, 0, "decision must not be deterministically negative for a fixed TraceID")
	assert.Less(t, got, n, "decision must not be deterministically positive for a fixed TraceID")
}
