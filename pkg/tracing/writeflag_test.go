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
)

func TestWriteFlag(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(ctx context.Context) context.Context
		markWrite bool
		wantWrite bool
	}{
		{
			name:      "no write flag in context returns false",
			setup:     func(ctx context.Context) context.Context { return ctx },
			markWrite: false,
			wantWrite: false,
		},
		{
			name:      "write flag present but not marked returns false",
			setup:     func(ctx context.Context) context.Context { return WithWriteFlag(ctx) },
			markWrite: false,
			wantWrite: false,
		},
		{
			name:      "write flag present and marked returns true",
			setup:     func(ctx context.Context) context.Context { return WithWriteFlag(ctx) },
			markWrite: true,
			wantWrite: true,
		},
		{
			name:      "MarkWrite without write flag is a no-op",
			setup:     func(ctx context.Context) context.Context { return ctx },
			markWrite: true,
			wantWrite: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.setup(context.Background())
			if tt.markWrite {
				MarkWrite(ctx)
			}
			assert.Equal(t, tt.wantWrite, HasWrite(ctx))
		})
	}
}

func TestMarkWrite_Idempotent(t *testing.T) {
	ctx := WithWriteFlag(context.Background())
	assert.False(t, HasWrite(ctx), "should be false before MarkWrite")

	MarkWrite(ctx)
	assert.True(t, HasWrite(ctx), "should be true after first MarkWrite")

	MarkWrite(ctx)
	assert.True(t, HasWrite(ctx), "should remain true after second MarkWrite")
}

func TestWithWriteFlag_IndependentFlags(t *testing.T) {
	ctx1 := WithWriteFlag(context.Background())
	ctx2 := WithWriteFlag(context.Background())

	MarkWrite(ctx1)
	assert.True(t, HasWrite(ctx1), "ctx1 should be marked")
	assert.False(t, HasWrite(ctx2), "ctx2 should not be affected by marking ctx1")
}
