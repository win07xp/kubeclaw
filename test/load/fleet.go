//go:build loadtest

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

package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	cmapi "github.com/cert-manager/cert-manager/pkg/apis/certmanager/v1"
	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kaalmv1beta1 "github.com/win07xp/kaalm/api/v1beta1"
)

// Every object the harness creates carries this label with the phase that
// made it, so a phase can tear down exactly its own objects and a re-run on a
// dirty cluster starts by clearing them.
const (
	phaseLabel         = "load.kaalm.io/phase"
	hookSecretName     = "load-hook"          // the channels' inbound bearer (testdata/infra.yaml)
	callbackSecretName = "load-callback-hmac" // the callback signing key (testdata/infra.yaml)
	activeClassName    = "load-active"
	churnClassName     = "load-churn"
	// loadgenAgentName is the Agent whose identity the mTLS gateway legs
	// borrow: the controller issues its certificate (SAN
	// {name}.{ns}.svc.cluster.local) and the loadgen Job presents it, so
	// the gateway sees a Kaalm-managed agent calling from its own namespace,
	// which is the primary path every agent Pod takes.
	loadgenAgentName = "loadgen-agent"
)

func loadgenAgentObj(ns, class, image string, providers []string) *kaalmv1beta1.Agent {
	a := agentObj(ns, loadgenAgentName, class, image, phaseGateway, false)
	for _, p := range providers {
		a.Spec.Providers = append(a.Spec.Providers, kaalmv1beta1.AgentProviderReference{
			ProviderRef: kaalmv1beta1.LocalObjectReference{Name: p},
		})
	}
	return a
}

// activeClass is the max-active fleet's class: the same BestEffort shape as
// the e2e class, with every lifecycle timer at zero so nothing ever idles.
// It admits the three load providers so the loadgen agent identity can call
// them (an empty allowedProviders admits none).
func activeClass() *kaalmv1beta1.AgentClass {
	c := &kaalmv1beta1.AgentClass{
		ObjectMeta: metav1.ObjectMeta{Name: activeClassName},
		Spec: kaalmv1beta1.AgentClassSpec{
			Runtime: kaalmv1beta1.AgentClassRuntime{Backend: "pod"},
			Image: kaalmv1beta1.AgentClassImage{
				AllowedImages: []string{"registry.test/agents/*"},
				PullPolicy:    corev1.PullIfNotPresent,
			},
		},
	}
	for _, p := range gatewayProviders {
		c.Spec.AllowedProviders = append(c.Spec.AllowedProviders, kaalmv1beta1.LocalObjectReference{Name: p})
	}
	return c
}

// churnClass permits hibernation with short timers. The floor on a
// Running-to-Hibernated cycle is the controller's 15s activity cache, not
// these values; they only need to be shorter than that.
func churnClass(idle, delay time.Duration) *kaalmv1beta1.AgentClass {
	c := activeClass()
	c.Name = churnClassName
	c.Spec.Persistence = kaalmv1beta1.AgentClassPersistence{Enabled: true, DefaultSizeGi: 1}
	c.Spec.Lifecycle = kaalmv1beta1.AgentClassLifecycle{
		HibernationAllowed:      true,
		DefaultIdleTimeout:      metav1.Duration{Duration: idle},
		DefaultHibernationDelay: metav1.Duration{Duration: delay},
	}
	return c
}

func agentObj(ns, name, class, image, phase string, persistent bool) *kaalmv1beta1.Agent {
	a := &kaalmv1beta1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: map[string]string{phaseLabel: phase}},
		Spec: kaalmv1beta1.AgentSpec{
			AgentClassRef: kaalmv1beta1.LocalObjectReference{Name: class},
			Image:         image,
			Lifecycle:     kaalmv1beta1.AgentLifecycle{ActivitySource: "gatewayTraffic"},
		},
	}
	if persistent {
		a.Spec.Persistence = kaalmv1beta1.AgentPersistence{Enabled: true}
		a.Spec.Lifecycle.HibernationEnabled = true
	}
	return a
}

// channelObj is an async webhook channel whose replies push to the in-cluster
// mock's /callback receiver. Polling would leave one response ConfigMap per
// message in the operator namespace for an hour; at load that would turn the
// run into an apiserver stress test the design never claimed to pass.
func channelObj(ns, agent, phase, callbackURL string) *kaalmv1beta1.AgentChannel {
	cb := callbackURL
	return &kaalmv1beta1.AgentChannel{
		ObjectMeta: metav1.ObjectMeta{Name: agent, Namespace: ns, Labels: map[string]string{phaseLabel: phase}},
		Spec: kaalmv1beta1.AgentChannelSpec{
			AgentRef: kaalmv1beta1.LocalObjectReference{Name: agent},
			Type:     channelTypeWebhook,
			Webhook: &kaalmv1beta1.AgentChannelWebhook{
				Path:         "/channels/" + ns + "/" + agent,
				ResponseMode: "async",
				CallbackURL:  &cb,
				CallbackAuth: &kaalmv1beta1.ChannelAuth{
					Type: "hmac",
					HMAC: &kaalmv1beta1.ChannelHMAC{
						Header:    "X-Kaalm-Signature",
						Algorithm: "sha256",
						SecretRef: kaalmv1beta1.SecretKeyReference{Name: callbackSecretName, Key: "key"},
					},
				},
				Auth: kaalmv1beta1.ChannelAuth{
					Type:      "bearer",
					SecretRef: &kaalmv1beta1.SecretKeyReference{Name: hookSecretName, Key: hookSecretKey},
				},
				MaxPendingAsyncResponses: 1000,
			},
		},
	}
}

func taskObj(ns, name, class, image string) *kaalmv1beta1.AgentTask {
	ttl := int32(3600)
	return &kaalmv1beta1.AgentTask{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: map[string]string{phaseLabel: phaseTasks}},
		Spec: kaalmv1beta1.AgentTaskSpec{
			AgentClassRef: kaalmv1beta1.LocalObjectReference{Name: class},
			Image:         image,
			Env:           []corev1.EnvVar{{Name: "KAALM_TASK_AUTOCOMPLETE", Value: "success"}},
			Completion: kaalmv1beta1.AgentTaskCompletion{
				Condition: "agentReported",
				Timeout:   metav1.Duration{Duration: 5 * time.Minute},
				OnTimeout: "Fail",
			},
			TTLSecondsAfterFinished: &ttl,
		},
	}
}

// ensureClass creates the class or updates its spec in place.
func (k *cluster) ensureClass(ctx context.Context, desired *kaalmv1beta1.AgentClass) error {
	var existing kaalmv1beta1.AgentClass
	err := k.c.Get(ctx, client.ObjectKeyFromObject(desired), &existing)
	if apierrors.IsNotFound(err) {
		return k.c.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	existing.Spec = desired.Spec
	return k.c.Update(ctx, &existing)
}

// createAll creates objects with bounded parallelism; AlreadyExists is fine.
func (k *cluster) createAll(ctx context.Context, objs []client.Object) error {
	sem := make(chan struct{}, 16)
	errCh := make(chan error, len(objs))
	var wg sync.WaitGroup
	for _, o := range objs {
		wg.Add(1)
		sem <- struct{}{}
		go func(o client.Object) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := k.c.Create(ctx, o); err != nil && !apierrors.IsAlreadyExists(err) {
				errCh <- fmt.Errorf("create %T %s: %w", o, o.GetName(), err)
			}
		}(o)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		return err
	}
	return nil
}

// agentTiming is what the API objects say about one agent's provisioning:
// cert-manager's Ready on its Certificate, the kubelet's start on its Pod,
// and the reconciler's Ready on the Agent. All at one-second granularity.
type agentTiming struct {
	Name       string
	Created    time.Time
	CertReady  *time.Time
	PodStarted *time.Time
	Ready      *time.Time
	ReadyNow   bool
	Phase      string
	Hibernated *time.Time
	Restarts   int32
}

func (k *cluster) agentTimings(ctx context.Context, ns, phase string) ([]agentTiming, error) {
	var agents kaalmv1beta1.AgentList
	if err := k.c.List(ctx, &agents, client.InNamespace(ns), client.MatchingLabels{phaseLabel: phase}); err != nil {
		return nil, err
	}
	var certs cmapi.CertificateList
	if err := k.c.List(ctx, &certs, client.InNamespace(ns)); err != nil {
		return nil, err
	}
	certReady := map[string]time.Time{}
	for i := range certs.Items {
		for _, cond := range certs.Items[i].Status.Conditions {
			ready := cond.Type == cmapi.CertificateConditionReady && cond.Status == cmmeta.ConditionTrue
			if ready && cond.LastTransitionTime != nil {
				certReady[certs.Items[i].Name] = cond.LastTransitionTime.Time
			}
		}
	}
	var pods corev1.PodList
	agentPods := client.MatchingLabels{"kaalm.io/workload": workloadAgent}
	if err := k.c.List(ctx, &pods, client.InNamespace(ns), agentPods); err != nil {
		return nil, err
	}
	podStart := map[string]time.Time{}
	restarts := map[string]int32{}
	for i := range pods.Items {
		p := &pods.Items[i]
		agent := p.Labels["kaalm.io/agent"]
		if p.Status.StartTime != nil {
			if prev, ok := podStart[agent]; !ok || p.Status.StartTime.After(prev) {
				podStart[agent] = p.Status.StartTime.Time
			}
		}
		for _, cs := range p.Status.ContainerStatuses {
			restarts[agent] += cs.RestartCount
		}
	}
	out := make([]agentTiming, 0, len(agents.Items))
	for i := range agents.Items {
		a := &agents.Items[i]
		if !a.DeletionTimestamp.IsZero() {
			continue // a trimmed agent on its way out is not part of the fleet
		}
		t := agentTiming{
			Name: a.Name, Created: a.CreationTimestamp.Time, Phase: string(a.Status.Phase), Restarts: restarts[a.Name],
		}
		if c := meta.FindStatusCondition(a.Status.Conditions, kaalmv1beta1.ConditionReady); c != nil {
			t.ReadyNow = c.Status == metav1.ConditionTrue
			if t.ReadyNow {
				ts := c.LastTransitionTime.Time
				t.Ready = &ts
			}
		}
		if ts, ok := certReady[a.Name+"-tls"]; ok {
			t.CertReady = &ts
		}
		if ts, ok := podStart[a.Name]; ok {
			t.PodStarted = &ts
		}
		if a.Status.HibernatedAt != nil {
			ts := a.Status.HibernatedAt.Time
			t.Hibernated = &ts
		}
		out = append(out, t)
	}
	return out, nil
}

// cameUp reports whether an agent has reached Ready at least once: it is Ready
// now, or it has moved on to a phase only a Ready agent reaches. A churn
// fleet with short idle timers hibernates its early agents before its late
// ones are Ready, so "all Ready at once" never happens there.
func cameUp(t agentTiming) bool {
	if t.ReadyNow {
		return true
	}
	switch kaalmv1beta1.AgentPhase(t.Phase) {
	case kaalmv1beta1.AgentIdle, kaalmv1beta1.AgentHibernating, kaalmv1beta1.AgentHibernated, kaalmv1beta1.AgentResuming:
		return true
	}
	return false
}

func countReady(ts []agentTiming) int {
	n := 0
	for _, t := range ts {
		if t.ReadyNow {
			n++
		}
	}
	return n
}

func countPhase(ts []agentTiming, phase kaalmv1beta1.AgentPhase) int {
	n := 0
	for _, t := range ts {
		if t.Phase == string(phase) {
			n++
		}
	}
	return n
}

// secondsBetween collects b-a in seconds for every timing where both exist.
func secondsBetween(ts []agentTiming, a, b func(agentTiming) *time.Time) []float64 {
	var out []float64
	for _, t := range ts {
		from, to := a(t), b(t)
		if from == nil || to == nil {
			continue
		}
		out = append(out, to.Sub(*from).Seconds())
	}
	return out
}

func created(t agentTiming) *time.Time { return &t.Created }
func certReadyAt(t agentTiming) *time.Time {
	return t.CertReady
}
func podStartedAt(t agentTiming) *time.Time { return t.PodStarted }
func readyAt(t agentTiming) *time.Time      { return t.Ready }

// channelsActive counts a phase's channels the reconciler has marked Ready.
func (k *cluster) channelsActive(ctx context.Context, ns, phase string) (int, int, error) {
	var list kaalmv1beta1.AgentChannelList
	if err := k.c.List(ctx, &list, client.InNamespace(ns), client.MatchingLabels{phaseLabel: phase}); err != nil {
		return 0, 0, err
	}
	active := 0
	for i := range list.Items {
		ch := &list.Items[i]
		ready := meta.IsStatusConditionTrue(ch.Status.Conditions, kaalmv1beta1.ConditionReady)
		if ch.Status.Phase == kaalmv1beta1.ChannelActive || ready {
			active++
		}
	}
	return active, len(list.Items), nil
}

// deleteAgents removes the named agents and their channels and waits until
// their pods are gone. The ramp uses it to trim a wave that hit the
// environment's ceiling, so the fleet that stays is the clean maximal set.
func (k *cluster) deleteAgents(ctx context.Context, ns string, names map[string]bool, timeout time.Duration) error {
	for name := range names {
		for _, obj := range []client.Object{
			&kaalmv1beta1.AgentChannel{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}},
			&kaalmv1beta1.Agent{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}},
		} {
			err := k.c.Delete(ctx, obj, client.PropagationPolicy(metav1.DeletePropagationBackground))
			if err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}
	agentPods := client.MatchingLabels{"kaalm.io/workload": workloadAgent}
	return pollUntil(ctx, timeout, 3*time.Second, func() (bool, error) {
		var agents kaalmv1beta1.AgentList
		if err := k.c.List(ctx, &agents, client.InNamespace(ns)); err != nil {
			return false, err
		}
		for i := range agents.Items {
			if names[agents.Items[i].Name] {
				return false, nil
			}
		}
		var pods corev1.PodList
		if err := k.c.List(ctx, &pods, client.InNamespace(ns), agentPods); err != nil {
			return false, err
		}
		for i := range pods.Items {
			if names[pods.Items[i].Labels["kaalm.io/agent"]] {
				return false, nil
			}
		}
		return true, nil
	})
}

// deletePhase removes everything a phase created, channels before agents so
// nothing dangles, and returns how long until the workload pods were gone.
func (k *cluster) deletePhase(
	ctx context.Context, ns, phase, podPrefix string, timeout time.Duration,
) (time.Duration, error) {
	start := time.Now()
	sel := []client.DeleteAllOfOption{
		client.InNamespace(ns),
		client.MatchingLabels{phaseLabel: phase},
		client.PropagationPolicy(metav1.DeletePropagationBackground),
	}
	for _, obj := range []client.Object{&kaalmv1beta1.AgentChannel{}, &kaalmv1beta1.AgentTask{}, &kaalmv1beta1.Agent{}} {
		if err := k.c.DeleteAllOf(ctx, obj, sel...); err != nil && !apierrors.IsNotFound(err) {
			return 0, err
		}
	}
	err := pollUntil(ctx, timeout, 3*time.Second, func() (bool, error) {
		var agents kaalmv1beta1.AgentList
		if err := k.c.List(ctx, &agents, client.InNamespace(ns), client.MatchingLabels{phaseLabel: phase}); err != nil {
			return false, err
		}
		var tasks kaalmv1beta1.AgentTaskList
		if err := k.c.List(ctx, &tasks, client.InNamespace(ns), client.MatchingLabels{phaseLabel: phase}); err != nil {
			return false, err
		}
		if len(agents.Items)+len(tasks.Items) > 0 {
			return false, nil
		}
		var pods corev1.PodList
		if err := k.c.List(ctx, &pods, client.InNamespace(ns)); err != nil {
			return false, err
		}
		for i := range pods.Items {
			name := pods.Items[i].Labels["kaalm.io/agent"] + pods.Items[i].Labels["kaalm.io/task"]
			if strings.HasPrefix(name, podPrefix) {
				return false, nil
			}
		}
		return true, nil
	})
	return time.Since(start), err
}

var errTimeout = errors.New("timed out")

// pollUntil calls fn every interval until it reports done or the timeout
// passes (errTimeout, so callers can treat a timeout as a result). A few
// consecutive errors are tolerated and logged: under load the k3d
// apiserver behind its load balancer drops a connection now and then, and
// a phase must not lose its numbers to one lost handshake.
func pollUntil(ctx context.Context, timeout, interval time.Duration, fn func() (bool, error)) error {
	const tolerated = 5
	deadline := time.Now().Add(timeout)
	consecutive := 0
	for {
		done, err := fn()
		if err != nil {
			consecutive++
			if consecutive > tolerated {
				return err
			}
			fmt.Printf("%s  transient error %d/%d: %v\n", time.Now().Format("15:04:05"), consecutive, tolerated, err)
		} else {
			consecutive = 0
		}
		if done {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%w after %s", errTimeout, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}
