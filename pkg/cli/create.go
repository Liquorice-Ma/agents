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
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	agentsv1alpha1 "github.com/openkruise/agents/api/v1alpha1"
	apiv1alpha1 "github.com/openkruise/agents/client/clientset/versioned/typed/api/v1alpha1"
)

type createSuoOptions struct {
	global   *GlobalOptions
	selector string
}

// NewCreateCommand returns the "create" command with its subcommands.
func NewCreateCommand(globalOpts *GlobalOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create SUBCOMMAND",
		Short: "Create a resource",
		Long: `Create OpenKruise Agents resources.

Currently supports creating SandboxUpdateOps for batch updating claimed sandboxes.`,
	}
	cmd.AddCommand(newCreateSuoCommand(globalOpts))
	return cmd
}

func newCreateSuoCommand(globalOpts *GlobalOptions) *cobra.Command {
	o := &createSuoOptions{global: globalOpts}

	cmd := &cobra.Command{
		Use:     "suo -l SELECTOR CONTAINER=IMAGE [CONTAINER=IMAGE ...]",
		Aliases: []string{"sandboxupdateops"},
		Short:   "Create a SandboxUpdateOps to update claimed sandbox images",
		Long: `Create a SandboxUpdateOps resource to batch update container images of claimed sandboxes.

This command creates a SandboxUpdateOps that applies a Strategic Merge Patch to all
sandboxes matching the label selector. Only claimed sandboxes (not controlled by
SandboxSet) can be updated this way.`,
		Example: `  # Update the gateway container image for all claimed sandboxes with app=openclaw
  okactl create suo -l app=openclaw gateway=nginx:1.27

  # Update multiple container images
  okactl create suo -l app=openclaw gateway=nginx:1.27 sidecar=envoy:1.28

  # Update in a specific namespace
  okactl -n production create suo -l app=openclaw gateway=nginx:1.27`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.run(args)
		},
	}
	cmd.Flags().StringVarP(&o.selector, "selector", "l", "", "Label selector to match target sandboxes (required)")
	_ = cmd.MarkFlagRequired("selector")
	return cmd
}

func (o *createSuoOptions) run(imageArgs []string) error {
	if o.selector == "" {
		return fmt.Errorf("--selector (-l) is required")
	}

	client, err := o.global.AgentsClient()
	if err != nil {
		return err
	}
	return runCreateSuoWithClient(client, o, imageArgs)
}

func runCreateSuoWithClient(client apiv1alpha1.ApiV1alpha1Interface, o *createSuoOptions, imageArgs []string) error {
	if o.selector == "" {
		return fmt.Errorf("--selector (-l) is required")
	}

	images, err := parseImageArgs(imageArgs)
	if err != nil {
		return err
	}

	ctx := context.TODO()
	ns := o.global.Namespace

	// Parse the label selector
	sel, err := labels.Parse(o.selector)
	if err != nil {
		return fmt.Errorf("invalid label selector %q: %w", o.selector, err)
	}

	// List all sandboxes and filter by label selector
	sbxList, err := client.Sandboxes(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list sandboxes: %w", err)
	}

	var matched []agentsv1alpha1.Sandbox
	for i := range sbxList.Items {
		if sel.Matches(labels.Set(sbxList.Items[i].Labels)) {
			matched = append(matched, sbxList.Items[i])
		}
	}
	if len(matched) == 0 {
		return fmt.Errorf("no sandboxes found matching selector %q in namespace %q", o.selector, ns)
	}

	// Validate container names against all matching sandboxes
	if err := validateSuoImageContainers(matched, images); err != nil {
		return err
	}

	patchData, err := buildSuoImagePatch(images)
	if err != nil {
		return fmt.Errorf("failed to build patch: %w", err)
	}

	// Delete any active (non-terminal) SUO before creating a new one
	if err := deleteActiveSandboxUpdateOps(client, ns); err != nil {
		return err
	}

	labelSelector, err := metav1.ParseToLabelSelector(o.selector)
	if err != nil {
		return fmt.Errorf("invalid label selector %q: %w", o.selector, err)
	}

	suo := &agentsv1alpha1.SandboxUpdateOps{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "suo-",
			Namespace:    ns,
		},
		Spec: agentsv1alpha1.SandboxUpdateOpsSpec{
			Selector: labelSelector,
			Patch:    runtime.RawExtension{Raw: patchData},
		},
	}

	created, err := client.Sandboxupdateops(ns).Create(ctx, suo, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create SandboxUpdateOps: %w", err)
	}

	fmt.Printf("sandboxupdateops.agents.kruise.io/%s created (selector: %s, images: %s)\n",
		created.Name, o.selector, strings.Join(formatSuoImagePairs(images), ", "))
	return nil
}

// buildSuoImagePatch generates a Strategic Merge Patch JSON for container image updates.
// The patch is applied to the sandbox's spec.template (PodTemplateSpec),
// so the structure must be relative to PodTemplateSpec (spec.containers),
// NOT relative to SandboxSpec (spec.template.spec.containers).
func buildSuoImagePatch(images map[string]string) ([]byte, error) {
	names := make([]string, 0, len(images))
	for name := range images {
		names = append(names, name)
	}
	sort.Strings(names)

	containers := make([]map[string]string, 0, len(images))
	for _, name := range names {
		containers = append(containers, map[string]string{
			"name":  name,
			"image": images[name],
		})
	}

	patch := map[string]interface{}{
		"spec": map[string]interface{}{
			"containers": containers,
		},
	}
	return json.Marshal(patch)
}

// validateSuoImageContainers checks that container names in images exist in the matched sandboxes.
// If a container name is missing from ALL sandboxes, it returns an error (likely a typo).
// If a container name is missing from SOME sandboxes, it prints a warning but does not fail,
// because different sandboxes may use different template versions with varying container sets.
func validateSuoImageContainers(sandboxes []agentsv1alpha1.Sandbox, images map[string]string) error {
	missingCount := make(map[string]int)
	for i := range sandboxes {
		sbx := &sandboxes[i]
		known := make(map[string]bool)
		if sbx.Spec.Template != nil {
			for _, c := range sbx.Spec.Template.Spec.Containers {
				known[c.Name] = true
			}
			for _, c := range sbx.Spec.Template.Spec.InitContainers {
				known[c.Name] = true
			}
		}
		for name := range images {
			if !known[name] {
				missingCount[name]++
				fmt.Printf("Warning: container %q not found in sandbox %q\n", name, sbx.Name)
			}
		}
	}

	// If a container is missing from ALL sandboxes, it is likely a typo
	for name, count := range missingCount {
		if count == len(sandboxes) {
			return fmt.Errorf("container %q not found in any of the %d matching sandboxes", name, len(sandboxes))
		}
	}
	return nil
}

// formatSuoImagePairs formats a map of container=image pairs as a slice of "container=image" strings.
func formatSuoImagePairs(images map[string]string) []string {
	names := make([]string, 0, len(images))
	for name := range images {
		names = append(names, name)
	}
	sort.Strings(names)

	pairs := make([]string, 0, len(images))
	for _, name := range names {
		pairs = append(pairs, name+"="+images[name])
	}
	return pairs
}

// deleteActiveSandboxUpdateOps deletes existing SUOs that are still active (Pending or Updating)
// in the namespace to avoid conflicts. Completed and Failed SUOs are preserved for historical reference.
// It first removes the finalizer (if present) to ensure the SUO can be deleted immediately,
// even if the SUO controller is not running.
func deleteActiveSandboxUpdateOps(client apiv1alpha1.ApiV1alpha1Interface, ns string) error {
	list, err := client.Sandboxupdateops(ns).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list SandboxUpdateOps: %w", err)
	}

	var deleted []string
	for i := range list.Items {
		suo := &list.Items[i]
		phase := suo.Status.Phase

		// Only delete SUOs that are Pending or Updating (including empty phase for newly created SUOs).
		// Completed and Failed SUOs are left for historical reference.
		if phase != "" && phase != agentsv1alpha1.SandboxUpdateOpsPending && phase != agentsv1alpha1.SandboxUpdateOpsUpdating {
			continue
		}

		// Remove finalizer first to allow immediate deletion
		// The SUO controller may not be running, so finalizer cleanup won't happen automatically
		if err := removeSUOFinalizer(client, ns, suo.Name); err != nil {
			return fmt.Errorf("failed to remove finalizer from SandboxUpdateOps %q: %w", suo.Name, err)
		}

		// Now delete the SUO (should be immediate without finalizer)
		if err := client.Sandboxupdateops(ns).Delete(context.TODO(), suo.Name, metav1.DeleteOptions{}); err != nil {
			return fmt.Errorf("failed to delete SandboxUpdateOps %q: %w", suo.Name, err)
		}
		fmt.Printf("sandboxupdateops.agents.kruise.io/%s deleted (was %s)\n", suo.Name, phase)
		deleted = append(deleted, suo.Name)
	}

	// Wait for deleted SUOs to be fully removed (should be near-instant now)
	for _, name := range deleted {
		if err := waitForSUODeletion(client, ns, name); err != nil {
			return err
		}
	}
	return nil
}

// removeSUOFinalizer removes the finalizer from a SandboxUpdateOps via JSON patch.
// This allows the SUO to be deleted immediately without waiting for the controller to process it.
func removeSUOFinalizer(client apiv1alpha1.ApiV1alpha1Interface, ns, name string) error {
	patch := []byte(`[{"op":"replace","path":"/metadata/finalizers","value":[]}]`)
	_, err := client.Sandboxupdateops(ns).Patch(context.TODO(), name, types.JSONPatchType, patch, metav1.PatchOptions{})
	return err
}

// waitForSUODeletion polls until the SUO is fully removed from the API server.
// After finalizer removal, deletion should be near-instant.
func waitForSUODeletion(client apiv1alpha1.ApiV1alpha1Interface, ns, name string) error {
	const maxWait = 10 * time.Second
	const pollInterval = 500 * time.Millisecond

	for elapsed := time.Duration(0); elapsed < maxWait; elapsed += pollInterval {
		_, err := client.Sandboxupdateops(ns).Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("failed to check SandboxUpdateOps %q deletion: %w", name, err)
		}
		time.Sleep(pollInterval)
	}
	return fmt.Errorf("timeout waiting for SandboxUpdateOps %q to be deleted (finalizer may be stuck)", name)
}
