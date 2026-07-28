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

package e2e

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	agentsv1alpha1 "github.com/openkruise/agents/api/v1alpha1"
	"github.com/openkruise/agents/pkg/tracing"
)

const (
	controllerNamespace      = "sandbox-system"
	controllerDeploymentName = "sandbox-controller-manager"
	controllerPodSelector    = "control-plane=sandbox-controller-manager"
	controllerContainerName  = "manager"
)

// The Tracing Stdout suite verifies the controller-side half of the tracing
// pipeline using the "std" exporter: trace-context extraction from the CR
// annotation and span export on the controller's stdout. The traceparent is
// constructed by the test itself to simulate the annotation a sandbox-manager
// would inject; the manager → CR annotation → controller chain and the
// requestID == TraceID contract are NOT covered here (the workflow deploys
// only the controller). It requires the controller to run with
// --tracing-mode=std and skips itself otherwise, so it is a no-op in
// workflows that don't enable tracing.
var _ = Describe("Tracing Stdout", func() {
	var (
		ctx       = context.Background()
		namespace string
		sandbox   *agentsv1alpha1.Sandbox
	)

	BeforeEach(func() {
		deploy := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Namespace: controllerNamespace,
			Name:      controllerDeploymentName,
		}, deploy)).To(Succeed())
		if !deploymentHasArg(deploy, "--tracing-mode=std") {
			Skip("controller is not running with --tracing-mode=std")
		}
		namespace = createNamespace(ctx)
	})

	AfterEach(func() {
		if sandbox != nil {
			_ = k8sClient.Delete(ctx, sandbox)
		}
	})

	It("should extract trace context from the annotation and export controller spans to stdout", func() {
		By("Building a W3C traceparent simulating the annotation a sandbox-manager would inject")
		traceID := randomHex(16)
		parentSpanID := randomHex(8)
		traceparent := fmt.Sprintf("00-%s-%s-01", traceID, parentSpanID)

		By("Creating a Sandbox carrying the trace-context annotation")
		sandbox = &agentsv1alpha1.Sandbox{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("tracing-e2e-%d", time.Now().UnixNano()),
				Namespace: namespace,
				Annotations: map[string]string{
					tracing.TraceContextAnnotationKey: traceparent,
				},
			},
			Spec: agentsv1alpha1.SandboxSpec{
				EmbeddedSandboxTemplate: agentsv1alpha1.EmbeddedSandboxTemplate{
					Template: &corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "test-container",
									Image: "nginx:stable-alpine3.20",
								},
							},
							RestartPolicy: corev1.RestartPolicyNever,
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, sandbox)).To(Succeed())

		By("Waiting for the sandbox to reach Running")
		Eventually(func() agentsv1alpha1.SandboxPhase {
			_ = k8sClient.Get(ctx, types.NamespacedName{
				Name:      sandbox.Name,
				Namespace: sandbox.Namespace,
			}, sandbox)
			return sandbox.Status.Phase
		}, time.Minute*5, time.Millisecond*500).Should(Equal(agentsv1alpha1.SandboxRunning))

		By("Verifying spans show up on controller stdout with the propagated trace ID")
		// The BatchSpanProcessor exports in batches (up to a few seconds of
		// delay), so poll the logs instead of reading them once. The stdout
		// exporter pretty-prints spans as JSON, hence the `"Name": "..."`
		// assertions below cannot collide with regular klog output.
		Eventually(func(g Gomega) {
			logs := controllerLogs(ctx)
			g.Expect(logs).To(ContainSubstring(`"Name": "` + tracing.SpanControllerReconcile + `"`))
			g.Expect(logs).To(ContainSubstring(`"Name": "` + tracing.SpanControllerCreatePod + `"`))
			g.Expect(logs).To(ContainSubstring(`"Name": "` + tracing.SpanControllerUpdateStatus + `"`))
			// Spans triggered by this sandbox must carry the trace ID extracted
			// from the annotation, proving controller-side trace-context
			// extraction works.
			g.Expect(logs).To(ContainSubstring(`"TraceID": "` + traceID + `"`))
		}, time.Minute*2, time.Second*5).Should(Succeed())
	})
})

// deploymentHasArg reports whether any container of the deployment carries the
// given command-line argument.
func deploymentHasArg(deploy *appsv1.Deployment, arg string) bool {
	for _, c := range deploy.Spec.Template.Spec.Containers {
		for _, a := range c.Args {
			if a == arg {
				return true
			}
		}
	}
	return false
}

// randomHex returns n random bytes hex-encoded (2n characters), suitable for
// building W3C trace-context trace and span IDs.
func randomHex(n int) string {
	b := make([]byte, n)
	_, err := rand.Read(b)
	Expect(err).NotTo(HaveOccurred())
	return hex.EncodeToString(b)
}

// controllerLogs returns the concatenated stdout of all controller manager
// pods, where the std tracing exporter writes its spans.
func controllerLogs(ctx context.Context) string {
	pods, err := clientset.CoreV1().Pods(controllerNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: controllerPodSelector,
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(pods.Items).NotTo(BeEmpty(), "no controller manager pods found")
	var sb strings.Builder
	for i := range pods.Items {
		raw, err := clientset.CoreV1().Pods(controllerNamespace).
			GetLogs(pods.Items[i].Name, &corev1.PodLogOptions{Container: controllerContainerName}).
			DoRaw(ctx)
		Expect(err).NotTo(HaveOccurred())
		sb.Write(raw)
	}
	return sb.String()
}
