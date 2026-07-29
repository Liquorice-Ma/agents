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
	"crypto/rand"
	"encoding/hex"
	"sync/atomic"
)

// enabledFlag records whether a real (non-noop) TracerProvider was installed
// by InitTracerProvider. It gates request-ID validation at the web framework
// boundary: only when tracing is on must a caller-provided request ID be
// usable as an OTel TraceID.
var enabledFlag atomic.Bool

// Enabled returns true when tracing was initialized with a real exporter
// (mode "otel", "std" or "file"), and false for mode "none", before
// InitTracerProvider is called, or after shutdown.
func Enabled() bool {
	return enabledFlag.Load()
}

// NewRequestID returns a server-generated request ID directly in the
// representation required by the tracing scheme: 32 lowercase hex characters
// (16 random bytes), usable as an OTel TraceID as-is. Generating the required
// form up front means caller-visible request IDs never need rewriting.
func NewRequestID() string {
	b := make([]byte, 16)
	// crypto/rand.Read does not fail on supported platforms; the OTel SDK
	// relies on the same source for its random IDs.
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// IsValidRequestID reports whether id can serve as an OTel TraceID:
// exactly 32 hex characters and not all-zero (an all-zero trace ID is
// invalid per the W3C Trace Context and OTel specifications). Both cases
// of hex digits are accepted; the API layer lowercases accepted IDs to the
// canonical TraceID string form before use.
func IsValidRequestID(id string) bool {
	if len(id) != 32 {
		return false
	}
	b, err := hex.DecodeString(id)
	if err != nil {
		return false
	}
	for _, x := range b {
		if x != 0 {
			return true
		}
	}
	return false
}
