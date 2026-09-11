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
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// SecretWatcher serves Secret reads from single-object informers. The gateway
// holds get and watch on Secrets (the operator namespace outright, channel
// Secrets through the reconciler's resourceNames-scoped grants) but never
// list, so an ordinary informer would hang on a forbidden LIST. Each
// referenced Secret therefore gets its own list-watch whose "list" is a GET
// wrapped as a one-item list and whose watch is filtered to metadata.name. A
// read is then a cache hit and a rotation lands on the watch event, which is
// the contract docs/src/security/credentials.md describes. Without a watcher
// KubeStore reads live, one GET per request through the client's rate
// limiter, which is what capped every replica at 20 requests per second
// (#170).
type SecretWatcher struct {
	client kubernetes.Interface
	ctx    context.Context

	// SyncTimeout bounds how long a first read waits for its informer's
	// initial GET before reading live instead; a misconfigured grant then
	// surfaces as the API error it always was, never as a hang.
	SyncTimeout time.Duration
	// IdleTTL is how long an informer nobody has read outlives its last use.
	// Channel Secrets come and go with their channels; the janitor keeps the
	// watch count bounded by the Secrets still in use.
	IdleTTL time.Duration

	mu        sync.Mutex
	informers map[types.NamespacedName]*secretInformer
}

type secretInformer struct {
	informer cache.SharedInformer
	stop     chan struct{}
	lastUsed time.Time
}

// NewSecretWatcher builds a watcher whose informers stop when ctx ends.
func NewSecretWatcher(ctx context.Context, client kubernetes.Interface) *SecretWatcher {
	w := &SecretWatcher{
		client:      client,
		ctx:         ctx,
		SyncTimeout: 2 * time.Second,
		IdleTTL:     time.Hour,
		informers:   map[types.NamespacedName]*secretInformer{},
	}
	go w.janitor()
	return w
}

// Get returns the current Secret from its informer, starting the informer on
// first use. If the informer has not completed its initial GET within
// SyncTimeout the read goes live, so the caller sees the same error a direct
// read would.
func (w *SecretWatcher) Get(ctx context.Context, namespace, name string) (*corev1.Secret, error) {
	entry := w.entryFor(namespace, name)
	if !waitSynced(ctx, entry.informer, w.SyncTimeout) {
		return w.client.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	}
	obj, exists, err := entry.informer.GetStore().GetByKey(namespace + "/" + name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, apierrors.NewNotFound(corev1.Resource("secrets"), name)
	}
	sec, ok := obj.(*corev1.Secret)
	if !ok {
		return nil, apierrors.NewNotFound(corev1.Resource("secrets"), name)
	}
	return sec, nil
}

func (w *SecretWatcher) entryFor(namespace, name string) *secretInformer {
	w.mu.Lock()
	defer w.mu.Unlock()
	key := types.NamespacedName{Namespace: namespace, Name: name}
	if entry, ok := w.informers[key]; ok {
		entry.lastUsed = time.Now()
		return entry
	}
	lw := &cache.ListWatch{
		ListFunc: func(metav1.ListOptions) (runtime.Object, error) {
			sec, err := w.client.CoreV1().Secrets(namespace).Get(w.ctx, name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				// An absent Secret is a valid, empty state: the watch below
				// delivers it when it appears.
				return &corev1.SecretList{}, nil
			}
			if err != nil {
				return nil, err
			}
			return &corev1.SecretList{
				ListMeta: metav1.ListMeta{ResourceVersion: sec.ResourceVersion},
				Items:    []corev1.Secret{*sec},
			}, nil
		},
		WatchFunc: func(opts metav1.ListOptions) (watch.Interface, error) {
			opts.FieldSelector = fields.OneTermEqualSelector("metadata.name", name).String()
			return w.client.CoreV1().Secrets(namespace).Watch(w.ctx, opts)
		},
	}
	entry := &secretInformer{
		informer: cache.NewSharedInformer(lw, &corev1.Secret{}, 0),
		stop:     make(chan struct{}),
		lastUsed: time.Now(),
	}
	go entry.informer.Run(entry.stop)
	w.informers[key] = entry
	return entry
}

// waitSynced polls HasSynced closely (the standard helper polls at 100 ms,
// which would tax the very first read) until synced, the timeout, or ctx.
func waitSynced(ctx context.Context, inf cache.SharedInformer, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for !inf.HasSynced() {
		if time.Now().After(deadline) || ctx.Err() != nil {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
	return true
}

// janitor stops informers idle past IdleTTL and every informer when the
// watcher's context ends.
func (w *SecretWatcher) janitor() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-w.ctx.Done():
			w.mu.Lock()
			for key, entry := range w.informers {
				close(entry.stop)
				delete(w.informers, key)
			}
			w.mu.Unlock()
			return
		case <-ticker.C:
			w.mu.Lock()
			for key, entry := range w.informers {
				if time.Since(entry.lastUsed) > w.IdleTTL {
					close(entry.stop)
					delete(w.informers, key)
				}
			}
			w.mu.Unlock()
		}
	}
}
