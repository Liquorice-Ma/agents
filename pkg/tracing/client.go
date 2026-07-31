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

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NewWriteTrackingClient wraps c so that every Kubernetes write issued through
// it (Create, Update, Patch, Delete, DeleteAllOf, and subresource writes such
// as Status().Patch) marks the current Reconcile iteration as having performed
// real work. This is the ONLY mechanism that marks a Reconcile as a write:
// instrumentation authors never deal with write marking — any write that goes
// through the client is tracked automatically, and Spans (StartControllerSpan
// + EndSpan) are purely observational.
//
// A write is counted when the write method is invoked, regardless of the
// result: the request reached the API server, so the iteration did real work
// worth retaining (a failed write additionally retains the iteration via the
// failed flag when its error ends a Span or the Reconcile).
//
// When tracing is disabled the original client is returned unwrapped, so the
// call path is structurally identical to a build without tracing — the same
// zero-overhead philosophy as the no-op filter and sampler, which are not
// installed at all in mode "none". This requires InitTracerProvider to have
// run before controllers are assembled, which is its documented contract.
//
// Reads (Get, List, Watch) are forwarded without interception.
func NewWriteTrackingClient(c client.Client) client.Client {
	if !Enabled() {
		return c
	}
	return &writeTrackingClient{Client: c}
}

// writeTrackingClient decorates client.Client, marking the per-Reconcile write
// flag on every write-verb call. markWrite is a no-op when the context carries
// no write flag (e.g. outside a Reconcile), so sharing the wrapped client with
// non-Reconcile code paths is safe.
type writeTrackingClient struct {
	client.Client
}

func (c *writeTrackingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	markWrite(ctx)
	return c.Client.Create(ctx, obj, opts...)
}

func (c *writeTrackingClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	markWrite(ctx)
	return c.Client.Update(ctx, obj, opts...)
}

func (c *writeTrackingClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	markWrite(ctx)
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func (c *writeTrackingClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	markWrite(ctx)
	return c.Client.Delete(ctx, obj, opts...)
}

func (c *writeTrackingClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	markWrite(ctx)
	return c.Client.DeleteAllOf(ctx, obj, opts...)
}

func (c *writeTrackingClient) Status() client.SubResourceWriter {
	return &writeTrackingSubResourceWriter{writer: c.Client.Status()}
}

func (c *writeTrackingClient) SubResource(subResource string) client.SubResourceClient {
	return &writeTrackingSubResourceClient{SubResourceClient: c.Client.SubResource(subResource)}
}

// writeTrackingSubResourceWriter marks writes issued via Status().
type writeTrackingSubResourceWriter struct {
	writer client.SubResourceWriter
}

func (w *writeTrackingSubResourceWriter) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	markWrite(ctx)
	return w.writer.Create(ctx, obj, subResource, opts...)
}

func (w *writeTrackingSubResourceWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	markWrite(ctx)
	return w.writer.Update(ctx, obj, opts...)
}

func (w *writeTrackingSubResourceWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	markWrite(ctx)
	return w.writer.Patch(ctx, obj, patch, opts...)
}

// writeTrackingSubResourceClient marks writes issued via SubResource(...);
// its read method (Get) is forwarded by the embedded client untouched.
type writeTrackingSubResourceClient struct {
	client.SubResourceClient
}

func (c *writeTrackingSubResourceClient) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	markWrite(ctx)
	return c.SubResourceClient.Create(ctx, obj, subResource, opts...)
}

func (c *writeTrackingSubResourceClient) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	markWrite(ctx)
	return c.SubResourceClient.Update(ctx, obj, opts...)
}

func (c *writeTrackingSubResourceClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	markWrite(ctx)
	return c.SubResourceClient.Patch(ctx, obj, patch, opts...)
}
