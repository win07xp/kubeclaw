/*
Copyright 2026 The Kaalm Authors.

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

package gateway

import (
	"context"

	corev1 "k8s.io/api/core/v1"

	"k8s.io/apimachinery/pkg/runtime/schema"

	kaalmv1beta1 "github.com/win07xp/kaalm/api/v1beta1"
)

// Store is the gateway's read surface over cluster state. The production
// implementation wraps a controller-runtime informer cache; tests use a
// map-backed fake.
type Store interface {
	AgentByName(ctx context.Context, namespace, name string) (*kaalmv1beta1.Agent, bool)
	TaskByName(ctx context.Context, namespace, name string) (*kaalmv1beta1.AgentTask, bool)
	ClassByName(ctx context.Context, name string) (*kaalmv1beta1.AgentClass, bool)
	ProviderByName(ctx context.Context, name string) (*kaalmv1beta1.ModelProvider, bool)
	// Credential resolves the provider's credential Secret key value.
	Credential(ctx context.Context, provider *kaalmv1beta1.ModelProvider) (string, error)
	// ToolProviderByName looks up a ToolProvider for the MCP broker.
	ToolProviderByName(ctx context.Context, name string) (*kaalmv1beta1.ToolProvider, bool)
	// ToolCredential resolves the tool provider's credential Secret key
	// value. A nil credentialsRef (an unauthenticated server) yields "".
	ToolCredential(ctx context.Context, provider *kaalmv1beta1.ToolProvider) (string, error)
	// PodByIP resolves a source IP to a Pod for the cross-check and the
	// Mode 2 ownership precheck. ok is false when no Pod matches.
	PodByIP(ctx context.Context, ip string) (*corev1.Pod, bool)
	// PodByIPLive resolves a source IP to a Pod with a live apiserver List,
	// narrowed to one namespace. It is the report-path fallback for the
	// new-Pod window where the informer has not observed the Pod yet
	// (docs/src/gateways/api/task-complete.md, cross-check step 2). The
	// namespace comes from the caller's certificate SAN, so the live query
	// never searches beyond the namespace the certificate attests.
	PodByIPLive(ctx context.Context, namespace, ip string) (*corev1.Pod, bool)
	// ChannelByPath resolves a webhook path to its AgentChannel. Only
	// channels with Ready=True are returned: Ready gates routing admission.
	ChannelByPath(ctx context.Context, path string) (*kaalmv1beta1.AgentChannel, bool)
	// SecretValue reads one key of a Secret in a user namespace (the
	// per-channel scoped Role is what grants this in production).
	SecretValue(ctx context.Context, namespace, name, key string) (string, error)
}

// isKaalmManagedPod reports whether the Pod belongs to an Agent or AgentTask
// (ownerRef to either kind, or the Kaalm-managed label set). Such Pods must
// use mTLS; the Mode 2 precheck rejects their bearer tokens. The ownerRef is
// matched by API group, not group and version: a Pod created before the
// v0.6.0 graduation names kaalm.io/v1alpha1 and is just as managed.
func isKaalmManagedPod(pod *corev1.Pod) bool {
	for _, ref := range pod.OwnerReferences {
		gv, err := schema.ParseGroupVersion(ref.APIVersion)
		if err == nil && gv.Group == kaalmv1beta1.GroupVersion.Group &&
			(ref.Kind == "Agent" || ref.Kind == "AgentTask") {
			return true
		}
	}
	if _, ok := pod.Labels["kaalm.io/workload"]; ok {
		return true
	}
	return false
}
