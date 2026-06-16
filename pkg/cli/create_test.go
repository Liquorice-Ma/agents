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

package cli

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	agentsv1alpha1 "github.com/openkruise/agents/api/v1alpha1"
	"github.com/openkruise/agents/client/clientset/versioned/fake"
)

func TestCreateSuo(t *testing.T) {
	makeSandbox := func(name string, labels map[string]string, containers []corev1.Container) *agentsv1alpha1.Sandbox {
		return &agentsv1alpha1.Sandbox{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "default",
				Labels:    labels,
			},
			Spec: agentsv1alpha1.SandboxSpec{
				EmbeddedSandboxTemplate: agentsv1alpha1.EmbeddedSandboxTemplate{
					Template: &corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: containers,
						},
					},
				},
			},
		}
	}

	tests := []struct {
		name          string
		selector      string
		imageArgs     []string
		seedSandboxes []*agentsv1alpha1.Sandbox
		expectError   string
	}{
		{
			name:     "update single container via SUO",
			selector: "app=my-app",
			imageArgs: []string{"main=nginx:2.0"},
			seedSandboxes: []*agentsv1alpha1.Sandbox{
				makeSandbox("sbx-1", map[string]string{"app": "my-app"}, []corev1.Container{
					{Name: "main", Image: "nginx:1.0"},
					{Name: "sidecar", Image: "envoy:1.0"},
				}),
			},
		},
		{
			name:     "update multiple containers via SUO",
			selector: "app=my-app",
			imageArgs: []string{"main=nginx:2.0", "sidecar=envoy:2.0"},
			seedSandboxes: []*agentsv1alpha1.Sandbox{
				makeSandbox("sbx-1", map[string]string{"app": "my-app"}, []corev1.Container{
					{Name: "main", Image: "nginx:1.0"},
					{Name: "sidecar", Image: "envoy:1.0"},
				}),
			},
		},
		{
			name:          "missing selector",
			selector:      "",
			imageArgs:     []string{"main=nginx:2.0"},
			seedSandboxes: nil,
			expectError:   "--selector (-l) is required",
		},
		{
			name:          "no matching sandboxes",
			selector:      "app=nonexistent",
			imageArgs:     []string{"main=nginx:2.0"},
			seedSandboxes: nil,
			expectError:   "no sandboxes found",
		},
		{
			name:     "container not found in sandbox",
			selector: "app=my-app",
			imageArgs: []string{"nonexistent=foo:1.0"},
			seedSandboxes: []*agentsv1alpha1.Sandbox{
				makeSandbox("sbx-1", map[string]string{"app": "my-app"}, []corev1.Container{
					{Name: "main", Image: "nginx:1.0"},
				}),
			},
			expectError: "container \"nonexistent\" not found",
		},
		{
			name:          "invalid image argument format",
			selector:      "app=my-app",
			imageArgs:     []string{"bad-format"},
			seedSandboxes: nil,
			expectError:   "invalid container=image argument",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := fake.NewSimpleClientset()
			for _, sbx := range tt.seedSandboxes {
				_, err := cs.ApiV1alpha1().Sandboxes(sbx.Namespace).Create(
					context.TODO(), sbx, metav1.CreateOptions{},
				)
				assert.NoError(t, err)
			}

			o := &createSuoOptions{
				global: &GlobalOptions{
					Namespace: "default",
				},
				selector: tt.selector,
			}

			err := runCreateSuoWithClient(cs.ApiV1alpha1(), o, tt.imageArgs)

			if tt.expectError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectError)
			} else {
				assert.NoError(t, err)

				// Verify SandboxUpdateOps was created
				suoList, listErr := cs.ApiV1alpha1().Sandboxupdateops("default").List(
					context.TODO(), metav1.ListOptions{},
				)
				assert.NoError(t, listErr)
				assert.Len(t, suoList.Items, 1, "expected exactly one SandboxUpdateOps")

				suo := suoList.Items[0]
				assert.NotNil(t, suo.Spec.Selector)
				assert.NotEmpty(t, suo.Spec.Patch.Raw, "patch should not be empty")
			}
		})
	}
}

func TestBuildSuoImagePatch(t *testing.T) {
	tests := []struct {
		name     string
		images   map[string]string
		contains string
	}{
		{
			name:     "single container",
			images:   map[string]string{"app": "nginx:2.0"},
			contains: `"name":"app"`,
		},
		{
			name:     "multiple containers",
			images:   map[string]string{"app": "nginx:2.0", "sidecar": "envoy:2.0"},
			contains: `"name":"app"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := buildSuoImagePatch(tt.images)
			assert.NoError(t, err)
			assert.Contains(t, string(data), tt.contains)
			assert.Contains(t, string(data), `"containers"`)
			assert.NotContains(t, string(data), `"template"`, "patch should not contain 'template' layer - SUO patch is applied directly to PodTemplateSpec")
		})
	}
}

func TestParseSuoSelectorToMap(t *testing.T) {
	tests := []struct {
		name     string
		selector string
		expected map[string]string
	}{
		{
			name:     "single pair",
			selector: "app=my-app",
			expected: map[string]string{"app": "my-app"},
		},
		{
			name:     "multiple pairs",
			selector: "app=my-app,env=prod",
			expected: map[string]string{"app": "my-app", "env": "prod"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseSuoSelectorToMap(tt.selector)
			assert.Equal(t, tt.expected, result)
		})
	}
}
