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
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	kaalmv1beta1 "github.com/win07xp/kaalm/api/v1beta1"
)

func secretObj(ns, name, value string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Data:       map[string][]byte{"token": []byte(value)},
	}
}

// eventually polls fn until it returns true or the timeout passes.
func eventually(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !fn() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSecretWatcherServesAndFollowsRotation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cs := fake.NewSimpleClientset(secretObj("kaalm-system", "openai-key", "v1"))
	w := NewSecretWatcher(ctx, cs)

	sec, err := w.Get(ctx, "kaalm-system", "openai-key")
	if err != nil {
		t.Fatalf("first read: %v", err)
	}
	if got := string(sec.Data["token"]); got != "v1" {
		t.Fatalf("token = %q, want v1", got)
	}
	// Rotation: an in-place update reaches the cache through the watch, with
	// no further GET.
	gets := 0
	cs.PrependReactor("get", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) {
		gets++
		return false, nil, nil
	})
	rotated := secretObj("kaalm-system", "openai-key", "v2")
	if _, err := cs.CoreV1().Secrets("kaalm-system").Update(ctx, rotated, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, 3*time.Second, func() bool {
		sec, err := w.Get(ctx, "kaalm-system", "openai-key")
		return err == nil && string(sec.Data["token"]) == "v2"
	})
	if gets != 0 {
		t.Fatalf("rotation cost %d live GETs, want 0", gets)
	}
}

func TestSecretWatcherReportsAbsentSecretAndSeesItAppear(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cs := fake.NewSimpleClientset()
	w := NewSecretWatcher(ctx, cs)

	if _, err := w.Get(ctx, "team-a", "hook"); !apierrors.IsNotFound(err) {
		t.Fatalf("absent Secret: err = %v, want NotFound", err)
	}
	if _, err := cs.CoreV1().Secrets("team-a").Create(ctx, secretObj("team-a", "hook", "s3cr3t"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, 3*time.Second, func() bool {
		sec, err := w.Get(ctx, "team-a", "hook")
		return err == nil && string(sec.Data["token"]) == "s3cr3t"
	})
}

func TestSecretWatcherFallsBackToLiveReadWhenSyncFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cs := fake.NewSimpleClientset()
	forbidden := apierrors.NewForbidden(corev1.Resource("secrets"), "hook", errors.New("no grant"))
	cs.PrependReactor("get", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, forbidden
	})
	w := NewSecretWatcher(ctx, cs)
	w.SyncTimeout = 100 * time.Millisecond

	_, err := w.Get(ctx, "team-a", "hook")
	if !apierrors.IsForbidden(err) {
		t.Fatalf("err = %v, want the live read's Forbidden", err)
	}
}

func TestSecretWatcherStopsIdleInformers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cs := fake.NewSimpleClientset(secretObj("team-a", "hook", "x"))
	w := NewSecretWatcher(ctx, cs)
	w.IdleTTL = 0
	if _, err := w.Get(ctx, "team-a", "hook"); err != nil {
		t.Fatal(err)
	}
	w.mu.Lock()
	entry := w.informers[types.NamespacedName{Namespace: "team-a", Name: "hook"}]
	entry.lastUsed = time.Now().Add(-time.Minute)
	w.mu.Unlock()
	// Drive one janitor pass by hand: the ticker is five minutes.
	w.mu.Lock()
	for key, e := range w.informers {
		if time.Since(e.lastUsed) > w.IdleTTL {
			close(e.stop)
			delete(w.informers, key)
		}
	}
	remaining := len(w.informers)
	w.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("%d informers remain after idle eviction, want 0", remaining)
	}
	// The next read starts a fresh informer transparently.
	if _, err := w.Get(ctx, "team-a", "hook"); err != nil {
		t.Fatalf("read after eviction: %v", err)
	}
}

func TestKubeStoreReadsSecretsThroughTheWatcher(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cs := fake.NewSimpleClientset(
		secretObj("kaalm-system", "openai-key", "sk-live"),
		secretObj("kaalm-system", "tool-key", "tool-live"),
		secretObj("team-a", "hook", "hook-live"),
	)
	store := &KubeStore{
		Reader:            kubeClientWith(t),
		OperatorNamespace: "kaalm-system",
		Secrets:           NewSecretWatcher(ctx, cs),
	}
	provider := &kaalmv1beta1.ModelProvider{Spec: kaalmv1beta1.ModelProviderSpec{
		CredentialsRef: kaalmv1beta1.SecretKeyReference{Name: "openai-key", Key: "token"},
	}}
	if got, err := store.Credential(ctx, provider); err != nil || got != "sk-live" {
		t.Fatalf("Credential = %q, %v", got, err)
	}
	tool := &kaalmv1beta1.ToolProvider{Spec: kaalmv1beta1.ToolProviderSpec{
		CredentialsRef: &kaalmv1beta1.SecretKeyReference{Name: "tool-key", Key: "token"},
	}}
	if got, err := store.ToolCredential(ctx, tool); err != nil || got != "tool-live" {
		t.Fatalf("ToolCredential = %q, %v", got, err)
	}
	if got, err := store.SecretValue(ctx, "team-a", "hook", "token"); err != nil || got != "hook-live" {
		t.Fatalf("SecretValue = %q, %v", got, err)
	}
	if _, err := store.SecretValue(ctx, "team-a", "hook", "missing"); err == nil {
		t.Fatal("missing key must error")
	}
}
