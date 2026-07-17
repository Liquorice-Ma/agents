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
	"net/http"
	"strings"

	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// HTTPMiddleware wraps an http.Handler with OpenTelemetry instrumentation.
// It extracts (or generates) the request ID and injects it into the request
// context BEFORE otelhttp.NewHandler creates the root Span. This ensures the
// custom RequestIDGenerator produces a TraceID equal to the request ID,
// enabling unified trace-log correlation (TraceID = request ID).
func HTTPMiddleware(handler http.Handler, serviceName string) http.Handler {
	otelHandler := otelhttp.NewHandler(handler, serviceName)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get("X-Request-ID")
		if requestID == "" {
			requestID = uuid.NewString()
		}
		// Normalize to hex-only format (no hyphens) so that requestID == traceID,
		// enabling direct trace-log correlation without any format conversion.
		requestID = strings.ReplaceAll(requestID, "-", "")
		r.Header.Set("X-Request-ID", requestID)
		ctx := WithRequestID(r.Context(), requestID)
		r = r.WithContext(ctx)
		otelHandler.ServeHTTP(w, r)
	})
}
