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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kaalmv1beta1 "github.com/win07xp/kaalm/api/v1beta1"
)

// gatewayScheme builds a scheme carrying the core and kaalm types.
func gatewayScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := kaalmv1beta1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

// kubeClientWith builds a fake reader seeded with objs and a status.podIP index.
func kubeClientWith(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(gatewayScheme(t)).
		WithObjects(objs...).
		WithIndex(&corev1.Pod{}, PodIPIndex, func(o client.Object) []string {
			ip := o.(*corev1.Pod).Status.PodIP
			if ip == "" {
				return nil
			}
			return []string{ip}
		}).
		Build()
}

func TestKubeStore_AgentTaskClassProvider(t *testing.T) {
	agent := &kaalmv1beta1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "sup", Namespace: "team-a"}}
	task := &kaalmv1beta1.AgentTask{ObjectMeta: metav1.ObjectMeta{Name: "fix", Namespace: "team-a"}}
	class := &kaalmv1beta1.AgentClass{ObjectMeta: metav1.ObjectMeta{Name: "std"}}
	prov := &kaalmv1beta1.ModelProvider{ObjectMeta: metav1.ObjectMeta{Name: "prov"}}

	k := &KubeStore{Reader: kubeClientWith(t, agent, task, class, prov), OperatorNamespace: "kaalm-system"}
	ctx := context.Background()

	if a, ok := k.AgentByName(ctx, "team-a", "sup"); !ok || a.Name != "sup" {
		t.Errorf("AgentByName miss: %v %v", a, ok)
	}
	if _, ok := k.AgentByName(ctx, "team-a", "nope"); ok {
		t.Error("AgentByName should miss unknown agent")
	}
	if tk, ok := k.TaskByName(ctx, "team-a", "fix"); !ok || tk.Name != "fix" {
		t.Errorf("TaskByName miss: %v %v", tk, ok)
	}
	if _, ok := k.TaskByName(ctx, "team-a", "nope"); ok {
		t.Error("TaskByName should miss unknown task")
	}
	if c, ok := k.ClassByName(ctx, "std"); !ok || c.Name != "std" {
		t.Errorf("ClassByName miss: %v %v", c, ok)
	}
	if _, ok := k.ClassByName(ctx, "nope"); ok {
		t.Error("ClassByName should miss unknown class")
	}
	if p, ok := k.ProviderByName(ctx, "prov"); !ok || p.Name != "prov" {
		t.Errorf("ProviderByName miss: %v %v", p, ok)
	}
	if _, ok := k.ProviderByName(ctx, "nope"); ok {
		t.Error("ProviderByName should miss unknown provider")
	}
}

func TestKubeStore_Credential(t *testing.T) {
	prov := &kaalmv1beta1.ModelProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "prov"},
		Spec: kaalmv1beta1.ModelProviderSpec{
			CredentialsRef: kaalmv1beta1.SecretKeyReference{Name: "prov-secret", Key: "api-key"},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "prov-secret", Namespace: "kaalm-system"},
		Data:       map[string][]byte{"api-key": []byte("sk-live")},
	}
	k := &KubeStore{Reader: kubeClientWith(t, prov, secret), OperatorNamespace: "kaalm-system"}
	ctx := context.Background()

	got, err := k.Credential(ctx, prov)
	if err != nil || got != "sk-live" {
		t.Fatalf("Credential = %q err=%v", got, err)
	}

	// Missing key in the Secret.
	provBadKey := prov.DeepCopy()
	provBadKey.Spec.CredentialsRef.Key = "absent"
	if _, err := k.Credential(ctx, provBadKey); err == nil {
		t.Error("missing key must error")
	}

	// Missing Secret entirely.
	provNoSecret := prov.DeepCopy()
	provNoSecret.Spec.CredentialsRef.Name = "ghost"
	if _, err := k.Credential(ctx, provNoSecret); err == nil {
		t.Error("missing Secret must error")
	}

	// Empty value is treated as missing.
	emptySec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "empty", Namespace: "kaalm-system"},
		Data:       map[string][]byte{"api-key": {}},
	}
	k2 := &KubeStore{Reader: kubeClientWith(t, emptySec), OperatorNamespace: "kaalm-system"}
	provEmpty := prov.DeepCopy()
	provEmpty.Spec.CredentialsRef.Name = "empty"
	if _, err := k2.Credential(ctx, provEmpty); err == nil {
		t.Error("empty credential value must error")
	}
}

func TestKubeStore_SecretValue(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "chan-secret", Namespace: "team-a"},
		Data:       map[string][]byte{"token": []byte("hunter2")},
	}
	k := &KubeStore{Reader: kubeClientWith(t, secret), OperatorNamespace: "kaalm-system"}
	ctx := context.Background()

	if v, err := k.SecretValue(ctx, "team-a", "chan-secret", "token"); err != nil || v != "hunter2" {
		t.Fatalf("SecretValue = %q err=%v", v, err)
	}
	if _, err := k.SecretValue(ctx, "team-a", "chan-secret", "absent"); err == nil {
		t.Error("missing key must error")
	}
	if _, err := k.SecretValue(ctx, "team-a", "ghost", "token"); err == nil {
		t.Error("missing Secret must error")
	}
}

// readyChannel builds an AgentChannel at path, optionally Ready.
func readyChannel(ns, name, path string, ready bool) *kaalmv1beta1.AgentChannel {
	ch := &kaalmv1beta1.AgentChannel{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: kaalmv1beta1.AgentChannelSpec{
			Webhook: &kaalmv1beta1.AgentChannelWebhook{Path: path},
		},
	}
	if ready {
		ch.Status.Conditions = []metav1.Condition{
			{Type: kaalmv1beta1.ConditionReady, Status: "True", Reason: "Ready",
				LastTransitionTime: metav1.Now()},
		}
	}
	return ch
}

func TestKubeStore_ChannelByPath(t *testing.T) {
	ctx := context.Background()

	ready := readyChannel("team-a", "ch", "/channels/team-a/hook", true)
	notReady := readyChannel("team-b", "cb", "/channels/team-b/hook2", false)
	// Path does not begin with the channel's own namespace prefix (rule 15).
	spoofed := readyChannel("team-c", "cc", "/channels/team-a/spoof", true)

	k := &KubeStore{Reader: kubeClientWith(t, ready, notReady, spoofed), OperatorNamespace: "kaalm-system"}

	if ch, ok := k.ChannelByPath(ctx, "/channels/team-a/hook"); !ok || ch.Name != "ch" {
		t.Errorf("Ready channel not found: %v %v", ch, ok)
	}
	if _, ok := k.ChannelByPath(ctx, "/channels/team-b/hook2"); ok {
		t.Error("non-Ready channel must not resolve")
	}
	if _, ok := k.ChannelByPath(ctx, "/channels/team-a/spoof"); ok {
		t.Error("channel whose path escapes its namespace prefix must be rejected")
	}
	if _, ok := k.ChannelByPath(ctx, "/channels/team-a/unknown"); ok {
		t.Error("unknown path must miss")
	}
}

func TestChannelPathAllowed(t *testing.T) {
	ok := readyChannel("team-a", "ch", "/channels/team-a/x", false)
	if !channelPathAllowed(ok) {
		t.Error("in-namespace prefix must be allowed")
	}
	bad := readyChannel("team-a", "ch", "/channels/team-b/x", false)
	if channelPathAllowed(bad) {
		t.Error("cross-namespace prefix must be rejected")
	}
}

func TestKubeStore_PodByIP(t *testing.T) {
	ctx := context.Background()

	running := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "sup-abc", Namespace: "team-a"},
		Status:     corev1.PodStatus{PodIP: "10.0.0.5", Phase: corev1.PodRunning},
	}
	// A terminated Pod sharing a recycled IP must be skipped.
	dead := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "old", Namespace: "team-a"},
		Status:     corev1.PodStatus{PodIP: "10.0.0.9", Phase: corev1.PodSucceeded},
	}
	k := &KubeStore{Reader: kubeClientWith(t, running, dead), OperatorNamespace: "kaalm-system"}

	if p, ok := k.PodByIP(ctx, "10.0.0.5"); !ok || p.Name != "sup-abc" {
		t.Errorf("PodByIP hit failed: %v %v", p, ok)
	}
	if _, ok := k.PodByIP(ctx, "10.0.0.9"); ok {
		t.Error("terminated Pod must be skipped")
	}
	if _, ok := k.PodByIP(ctx, "10.0.0.250"); ok {
		t.Error("unknown IP must miss")
	}
}

func TestKubeStore_PodByIPLive(t *testing.T) {
	ctx := context.Background()

	running := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "fix-pod", Namespace: "team-a"},
		Status:     corev1.PodStatus{PodIP: "10.0.0.5", Phase: corev1.PodRunning},
	}
	dead := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "old", Namespace: "team-a"},
		Status:     corev1.PodStatus{PodIP: "10.0.0.9", Phase: corev1.PodFailed},
	}
	live := kubeClientWith(t, running, dead)

	// Nil APIReader disables the fallback.
	k := &KubeStore{Reader: kubeClientWith(t), OperatorNamespace: "kaalm-system"}
	if _, ok := k.PodByIPLive(ctx, "team-a", "10.0.0.5"); ok {
		t.Error("nil APIReader must miss")
	}

	k = &KubeStore{Reader: kubeClientWith(t), APIReader: live, OperatorNamespace: "kaalm-system"}
	if p, ok := k.PodByIPLive(ctx, "team-a", "10.0.0.5"); !ok || p.Name != "fix-pod" {
		t.Errorf("PodByIPLive hit failed: %v %v", p, ok)
	}
	// The query is namespace-narrowed: the same IP misses for another
	// namespace.
	if _, ok := k.PodByIPLive(ctx, "team-b", "10.0.0.5"); ok {
		t.Error("live lookup must not match across namespaces")
	}
	if _, ok := k.PodByIPLive(ctx, "team-a", "10.0.0.9"); ok {
		t.Error("terminated Pod must be skipped")
	}
	if _, ok := k.PodByIPLive(ctx, "team-a", "10.0.0.250"); ok {
		t.Error("unknown IP must miss")
	}
}
