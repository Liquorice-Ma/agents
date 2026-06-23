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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"

	agentsv1alpha1 "github.com/openkruise/agents/api/v1alpha1"
	"github.com/openkruise/agents/client/clientset/versioned/fake"
)

func TestStatusSbsRunEInvalidConfig(t *testing.T) {
	origFn := inClusterConfigFn
	defer func() { inClusterConfigFn = origFn }()
	inClusterConfigFn = func() (*rest.Config, error) {
		return nil, fmt.Errorf("not in cluster")
	}

	cmd := NewStatusCommand(&GlobalOptions{
		KubeConfig: "/nonexistent/config",
		Namespace:  "default",
	})
	cmd.SetArgs([]string{"sbs", "test-sbs"})
	err := cmd.Execute()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to build kubeconfig")
}

func TestStatusSuoRunEInvalidConfig(t *testing.T) {
	origFn := inClusterConfigFn
	defer func() { inClusterConfigFn = origFn }()
	inClusterConfigFn = func() (*rest.Config, error) {
		return nil, fmt.Errorf("not in cluster")
	}

	cmd := NewStatusCommand(&GlobalOptions{
		KubeConfig: "/nonexistent/config",
		Namespace:  "default",
	})
	cmd.SetArgs([]string{"suo", "test-suo"})
	err := cmd.Execute()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to build kubeconfig")
}

func TestStatusSbsSandboxsetAlias(t *testing.T) {
	origFn := inClusterConfigFn
	defer func() { inClusterConfigFn = origFn }()
	inClusterConfigFn = func() (*rest.Config, error) {
		return nil, fmt.Errorf("not in cluster")
	}

	// Both "sbs" and "sandboxset" should resolve to the same RunE
	cmd := NewStatusCommand(&GlobalOptions{
		KubeConfig: "/nonexistent/config",
		Namespace:  "default",
	})
	cmd.SetArgs([]string{"sandboxset", "test-sbs"})
	err := cmd.Execute()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to build kubeconfig")
}

func TestWaitForSuoCompleteFailedPhase(t *testing.T) {
	suo := &agentsv1alpha1.SandboxUpdateOps{
		ObjectMeta: metav1.ObjectMeta{Name: "suo-fail", Namespace: "default"},
		Status: agentsv1alpha1.SandboxUpdateOpsStatus{
			Phase:            agentsv1alpha1.SandboxUpdateOpsFailed,
			Replicas:         3,
			UpdatedReplicas:  1,
			UpdatingReplicas: 0,
			FailedReplicas:   2,
		},
	}

	cs := fake.NewSimpleClientset()
	_, err := cs.ApiV1alpha1().Sandboxupdateops("default").Create(
		context.TODO(), suo, metav1.CreateOptions{},
	)
	assert.NoError(t, err)

	err = waitForSuoComplete(cs.ApiV1alpha1(), context.TODO(), "default", "suo-fail", suo)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "update failed")
	assert.Contains(t, err.Error(), "2 failed")
}

func TestWaitForSuoCompleteGetError(t *testing.T) {
	// Start with an Updating SUO, but the fake clientset doesn't have it,
	// so the poll loop's Get call will fail.
	suo := &agentsv1alpha1.SandboxUpdateOps{
		ObjectMeta: metav1.ObjectMeta{Name: "suo-missing", Namespace: "default"},
		Status: agentsv1alpha1.SandboxUpdateOpsStatus{
			Phase:            agentsv1alpha1.SandboxUpdateOpsUpdating,
			Replicas:         2,
			UpdatedReplicas:  0,
			UpdatingReplicas: 1,
		},
	}

	// Empty clientset: first printSuoStatus works on the initial object,
	// but the Get inside the loop will fail.
	cs := fake.NewSimpleClientset()

	err := waitForSuoComplete(cs.ApiV1alpha1(), context.TODO(), "default", "suo-missing", suo)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get sandboxupdateops")
}
