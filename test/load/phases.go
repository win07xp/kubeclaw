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
	"math"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kaalmv1beta1 "github.com/win07xp/kaalm/api/v1beta1"
)

// The phases, in the order `load run` executes them. Each records its own
// block on the summary and cleans up its own objects, so a failed phase
// leaves the earlier numbers intact and the cluster reusable.

type gatewayResult struct {
	Legs []gatewayLeg `json:"legs"`
}

type gatewayLeg struct {
	Name              string      `json:"name"`
	Provider          string      `json:"provider"`
	MTLS              bool        `json:"mtls"`
	Concurrency       int         `json:"concurrency"`
	Client            *loadResult `json:"client"`
	GatewaySideMs     stats       `json:"gatewaySideMs"`
	GatewayRequests   float64     `json:"gatewayRequests"`
	SpendUSD          float64     `json:"spendUsd"`
	BudgetUtilization float64     `json:"budgetUtilization"`
	GatewayUsageMax   usage       `json:"gatewayUsageMax"`
}

type rampResult struct {
	Target           int          `json:"target"`
	WaveSize         int          `json:"waveSize"`
	Waves            []waveResult `json:"waves"`
	Achieved         int          `json:"achieved"`
	Saturation       string       `json:"saturation,omitempty"`
	SaturationWave   int          `json:"saturationWave,omitempty"`
	HostMemBeforeMiB float64      `json:"hostMemBeforeMiB"`
	MemPerAgentMiB   float64      `json:"memPerAgentMiB"`
}

type waveResult struct {
	Index           int                `json:"index"`
	Requested       int                `json:"requested"`
	ReadyInWave     int                `json:"readyInWave"`
	FleetReady      int                `json:"fleetReady"`
	WallSec         float64            `json:"wallSec"`
	TimeToReadySec  stats              `json:"timeToReadySec"`
	CertIssueSec    stats              `json:"certIssueSec"`
	PodStartSec     stats              `json:"podStartSec"`
	StartToReadySec stats              `json:"startToReadySec"`
	HostMemAvailMiB float64            `json:"hostMemAvailMiB"`
	Controller      usage              `json:"controller"`
	Gateway         usage              `json:"gateway"`
	ReconcileMs     stats              `json:"reconcileMs"`
	WorkqueueDepth  float64            `json:"workqueueDepth"`
	NodeMemMiB      map[string]float64 `json:"nodeMemMiB"`
}

type holdResult struct {
	Agents            int                `json:"agents"`
	Channels          int                `json:"channels"`
	ChannelsActiveSec float64            `json:"channelsActiveSec"`
	Client            *loadResult        `json:"client"`
	MessagesByStatus  map[string]float64 `json:"messagesByStatus"`
	MessageDurationMs stats              `json:"messageDurationMs"`
	DeliveryAttempts  map[string]float64 `json:"deliveryAttempts"`
	Callbacks         float64            `json:"callbacks"`
	ReadyBefore       int                `json:"readyBefore"`
	ReadyAfter        int                `json:"readyAfter"`
	RestartsBefore    int32              `json:"restartsBefore"`
	RestartsAfter     int32              `json:"restartsAfter"`
	Flaps             int                `json:"flaps"`
	GatewayUsageMax   usage              `json:"gatewayUsageMax"`
	ControllerUsage   usage              `json:"controllerUsageMax"`
}

type teardownResult struct {
	Agents  int     `json:"agents"`
	Seconds float64 `json:"seconds"`
}

type churnResult struct {
	Agents              int                `json:"agents"`
	TimeToReadySec      stats              `json:"timeToReadySec"`
	AllReadySec         float64            `json:"allReadySec"`
	FirstHibernationSec stats              `json:"firstHibernationSec"`
	AllHibernatedSec    float64            `json:"allHibernatedSec"`
	Client              *loadResult        `json:"client"`
	MessagesByStatus    map[string]float64 `json:"messagesByStatus"`
	WakeDurationMs      stats              `json:"wakeDurationMs"`
	WakesByResult       map[string]float64 `json:"wakesByResult"`
	WakesTotal          float64            `json:"wakesTotal"`
	HibernationsTotal   float64            `json:"hibernationsTotal"`
	Callbacks           float64            `json:"callbacks"`
	TeardownSec         float64            `json:"teardownSec"`
}

type tasksResult struct {
	Submitted        int            `json:"submitted"`
	SubmitSec        float64        `json:"submitSec"`
	Phases           map[string]int `json:"phases"`
	ProvisionSec     stats          `json:"provisionSec"`
	RunSec           stats          `json:"runSec"`
	TotalSec         stats          `json:"totalSec"`
	MakespanSec      float64        `json:"makespanSec"`
	ThroughputPerMin float64        `json:"throughputPerMin"`
	Retries          int32          `json:"retries"`
	TeardownSec      float64        `json:"teardownSec"`
}

func (h *harness) scrapeGateway(ctx context.Context) (*snapshot, error) {
	return h.k.scrapeComponent(ctx, "kaalm-system", "gateway", "9090")
}

func (h *harness) scrapeController(ctx context.Context) (*snapshot, error) {
	return h.k.scrapeComponent(ctx, "kaalm-system", "controller", "8080")
}

// usageSampler records the peak metrics-API usage per chart component while
// a load job runs.
type usageSampler struct {
	mu   sync.Mutex
	peak map[string]usage
	stop chan struct{}
	done chan struct{}
}

func (h *harness) sampleUsage(ctx context.Context) *usageSampler {
	s := &usageSampler{peak: map[string]usage{}, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(s.done)
		for {
			if u, err := h.k.podUsage(ctx, "kaalm-system"); err == nil {
				s.mu.Lock()
				for name, cur := range u {
					p := s.peak[name]
					p.CPUMilli = math.Max(p.CPUMilli, cur.CPUMilli)
					p.MemMiB = math.Max(p.MemMiB, cur.MemMiB)
					s.peak[name] = p
				}
				s.mu.Unlock()
			}
			select {
			case <-s.stop:
				return
			case <-time.After(5 * time.Second):
			}
		}
	}()
	return s
}

func (s *usageSampler) finish() map[string]usage {
	close(s.stop)
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]usage{}
	for k, v := range s.peak {
		out[k] = usage{CPUMilli: round3(v.CPUMilli), MemMiB: round3(v.MemMiB)}
	}
	return out
}

// ---- gateway ----

var gatewayProviders = []string{providerFast, providerSlow, providerHard}

func (h *harness) runGateway(ctx context.Context) error {
	// The token tier is the gateway-only adoption path (a ServiceAccount
	// token validated once and cached); the mTLS legs are the primary path
	// every Kaalm-managed agent takes, presented here through a borrowed
	// agent identity. Upstream latency and budget mode vary on that path.
	legs := []struct {
		name, provider string
		mtls           bool
	}{
		{"token tier: soft budget, 0 ms upstream", providerFast, false},
		{"mtls: soft budget, 0 ms upstream", providerFast, true},
		{"mtls: soft budget, 50 ms upstream", providerSlow, true},
		{"mtls: hard budget, 0 ms upstream", providerHard, true},
	}
	if err := h.k.ensureClass(ctx, activeClass()); err != nil {
		return err
	}
	agent := loadgenAgentObj(h.cfg.Namespace, activeClassName, h.cfg.AgentImage, gatewayProviders)
	if err := h.k.createAll(ctx, []client.Object{agent}); err != nil {
		return err
	}
	defer func() { _ = h.k.c.Delete(context.Background(), agent) }()
	mtlsSecret := loadgenAgentName + "-tls"
	if err := pollUntil(ctx, 3*time.Minute, 2*time.Second, func() (bool, error) {
		sec, err := h.k.cs.CoreV1().Secrets(h.cfg.Namespace).Get(ctx, mtlsSecret, metav1.GetOptions{})
		if err != nil {
			return false, nil //nolint:nilerr // absent until cert-manager issues it
		}
		return len(sec.Data["tls.crt"]) > 0 && len(sec.Data["tls.key"]) > 0, nil
	}); err != nil {
		return fmt.Errorf("waiting for the loadgen agent certificate: %w", err)
	}

	res := &gatewayResult{}
	for i, leg := range legs {
		h.logf("gateway leg %d/%d: %s (%d callers, %s)",
			i+1, len(legs), leg.name, h.cfg.GatewayConcurrency, h.cfg.GatewayDuration)
		before, err := h.scrapeGateway(ctx)
		if err != nil {
			return err
		}
		args := []string{
			flagMode, modeGateway,
			"-model", leg.provider + "/mock-model",
			"-concurrency", strconv.Itoa(h.cfg.GatewayConcurrency),
			flagDuration, h.cfg.GatewayDuration.String(),
		}
		jobName, secret := "loadgen-gateway-token-"+leg.provider, ""
		if leg.mtls {
			args = append(args, "-cert-file", "/var/run/mtls/tls.crt", "-key-file", "/var/run/mtls/tls.key")
			jobName, secret = "loadgen-gateway-mtls-"+leg.provider, mtlsSecret
		}
		sampler := h.sampleUsage(ctx)
		client, err := h.k.runLoadgen(ctx, h.cfg.Namespace, jobName, h.cfg.LoadgenImage, args, secret,
			h.cfg.GatewayDuration+3*time.Minute)
		peak := sampler.finish()
		if err != nil {
			return fmt.Errorf("gateway leg %s: %w", leg.name, err)
		}
		after, err := h.scrapeGateway(ctx)
		if err != nil {
			return err
		}
		want := map[string]string{"provider": leg.provider}
		res.Legs = append(res.Legs, gatewayLeg{
			Name:              leg.name,
			Provider:          leg.provider,
			MTLS:              leg.mtls,
			Concurrency:       h.cfg.GatewayConcurrency,
			Client:            client,
			GatewaySideMs:     histStats(histogramDelta(before, after, "kaalm_llm_request_duration_seconds", want)),
			GatewayRequests:   counterDelta(before, after, "kaalm_llm_requests_total", want),
			SpendUSD:          round3(counterDelta(before, after, "kaalm_llm_spend_usd_total", want)),
			BudgetUtilization: after.gauge("kaalm_llm_budget_utilization", want),
			GatewayUsageMax:   peak["kaalm-gateway"],
		})
		h.logf("  client: %d requests, %.1f rps, p50 %.1f ms, p99 %.1f ms, statuses %v",
			client.Requests, client.RPS, client.LatencyMs.P50, client.LatencyMs.P99, client.Statuses)
	}
	h.sum.Gateway = res
	return nil
}

// ---- ramp ----

// saturation names the first environmental limit the ramp has hit, or "".
func (h *harness) saturation(ctx context.Context) (string, error) {
	mem, err := hostMemAvailableMiB()
	if err != nil {
		return "", err
	}
	if mem < float64(h.cfg.MemFloorMiB) {
		return fmt.Sprintf("host MemAvailable %.0f MiB is below the %d MiB floor", mem, h.cfg.MemFloorMiB), nil
	}
	var nodes corev1.NodeList
	if err := h.k.c.List(ctx, &nodes); err != nil {
		return "", err
	}
	for i := range nodes.Items {
		for _, c := range nodes.Items[i].Status.Conditions {
			if c.Type == corev1.NodeMemoryPressure && c.Status == corev1.ConditionTrue {
				return "node " + nodes.Items[i].Name + " reports MemoryPressure", nil
			}
		}
	}
	var pods corev1.PodList
	if err := h.k.c.List(ctx, &pods, client.InNamespace(h.cfg.Namespace)); err != nil {
		return "", err
	}
	// Agent pods that crash-loop are the environment giving out from the
	// inside: probes time out before any node reports pressure or the
	// scheduler refuses anything. The first baseline run saw exactly that,
	// with 3.9 GiB of "available" host memory and 422 MiB actually free.
	crashLooping := 0
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Labels["kaalm.io/workload"] == workloadAgent {
			for _, cs := range p.Status.ContainerStatuses {
				if cs.State.Waiting != nil && cs.State.Waiting.Reason == "CrashLoopBackOff" {
					crashLooping++
				}
			}
		}
		if p.Status.Phase != corev1.PodPending {
			continue
		}
		for _, c := range p.Status.Conditions {
			if c.Type == corev1.PodScheduled && c.Status == corev1.ConditionFalse && c.Reason == corev1.PodReasonUnschedulable {
				return "scheduler: " + c.Message, nil
			}
		}
	}
	if crashLooping > 0 {
		return fmt.Sprintf("agent pods failing probes: %d in CrashLoopBackOff", crashLooping), nil
	}
	return "", nil
}

func (h *harness) runRamp(ctx context.Context) error {
	cfg := h.cfg
	if err := h.k.ensureClass(ctx, activeClass()); err != nil {
		return err
	}
	memBefore, err := hostMemAvailableMiB()
	if err != nil {
		return err
	}
	res := &rampResult{Target: cfg.RampTarget, WaveSize: cfg.WaveSize, HostMemBeforeMiB: round3(memBefore)}
	prevCtl, err := h.scrapeController(ctx)
	if err != nil {
		return err
	}
	for wave := 0; wave*cfg.WaveSize < cfg.RampTarget; wave++ {
		lo, hi := wave*cfg.WaveSize, (wave+1)*cfg.WaveSize
		if hi > cfg.RampTarget {
			hi = cfg.RampTarget
		}
		names := map[string]bool{}
		objs := make([]client.Object, 0, 2*(hi-lo))
		for i := lo; i < hi; i++ {
			name := fmt.Sprintf("ramp-%04d", i)
			names[name] = true
			// The channel is created with its agent so the channel
			// reconciler's work overlaps the waves instead of stacking up
			// in front of the hold phase.
			objs = append(objs,
				agentObj(cfg.Namespace, name, activeClassName, cfg.AgentImage, phaseRamp, false),
				channelObj(cfg.Namespace, name, phaseRamp, h.callbackURL()))
		}
		h.logf("ramp wave %d: creating agents %d..%d with their channels", wave, lo, hi-1)
		waveStart := time.Now()
		if err := h.k.createAll(ctx, objs); err != nil {
			return err
		}
		var sat string
		var timings []agentTiming
		err := pollUntil(ctx, cfg.WaveTimeout, 3*time.Second, func() (bool, error) {
			ts, err := h.k.agentTimings(ctx, cfg.Namespace, phaseRamp)
			if err != nil {
				return false, err
			}
			timings = ts
			if readyAmong(ts, names) == len(names) {
				return true, nil
			}
			s, err := h.saturation(ctx)
			if err != nil {
				return false, err
			}
			if s != "" {
				sat = s
				return true, nil
			}
			return false, nil
		})
		if errors.Is(err, errTimeout) {
			sat = fmt.Sprintf("wave %d: %d of %d agents Ready within %s",
				wave, readyAmong(timings, names), len(names), cfg.WaveTimeout)
		} else if err != nil {
			return err
		}
		wall := time.Since(waveStart)

		inWave := filterTimings(timings, names)
		w := waveResult{
			Index:           wave,
			Requested:       len(names),
			ReadyInWave:     countReady(inWave),
			FleetReady:      countReady(timings),
			WallSec:         round3(wall.Seconds()),
			TimeToReadySec:  summarize(secondsBetween(inWave, created, readyAt)),
			CertIssueSec:    summarize(secondsBetween(inWave, created, certReadyAt)),
			PodStartSec:     summarize(secondsBetween(inWave, created, podStartedAt)),
			StartToReadySec: summarize(secondsBetween(inWave, podStartedAt, readyAt)),
			NodeMemMiB:      map[string]float64{},
		}
		if mem, err := hostMemAvailableMiB(); err == nil {
			w.HostMemAvailMiB = round3(mem)
		}
		if u, err := h.k.podUsage(ctx, "kaalm-system"); err == nil {
			w.Controller, w.Gateway = u["kaalm-controller"], u["kaalm-gateway"]
		}
		if nu, err := h.k.nodeUsage(ctx); err == nil {
			for name, u := range nu {
				w.NodeMemMiB[name] = round3(u.MemMiB)
			}
		}
		if ctl, err := h.scrapeController(ctx); err == nil {
			agentReconciles := map[string]string{"controller": agentControllerName}
			w.ReconcileMs = histStats(histogramDelta(prevCtl, ctl, "controller_runtime_reconcile_time_seconds", agentReconciles))
			w.WorkqueueDepth = ctl.gauge("workqueue_depth", map[string]string{"name": "agent"})
			prevCtl = ctl
		}
		res.Waves = append(res.Waves, w)
		res.Achieved = w.FleetReady
		h.logf("  wave %d: %d/%d Ready in %.0fs (fleet %d Ready), time-to-Ready p50 %.0fs p95 %.0fs, host avail %.0f MiB",
			wave, w.ReadyInWave, w.Requested, w.WallSec, w.FleetReady,
			w.TimeToReadySec.P50, w.TimeToReadySec.P95, w.HostMemAvailMiB)
		if sat != "" {
			res.Saturation = sat
			res.SaturationWave = wave
			h.logf("  ramp stops: %s", sat)
			// The wave that hit the ceiling is trimmed, so the fleet the
			// later phases run on is the largest one that came up clean.
			h.logf("  trimming wave %d (%d agents)", wave, len(names))
			if err := h.k.deleteAgents(ctx, cfg.Namespace, names, 5*time.Minute); err != nil {
				h.note("trimming wave %d: %v", wave, err)
			}
			if ts, err := h.k.agentTimings(ctx, cfg.Namespace, phaseRamp); err == nil {
				res.Achieved = countReady(ts)
			}
			break
		}
	}
	if res.Achieved > 0 {
		if memNow, err := hostMemAvailableMiB(); err == nil {
			res.MemPerAgentMiB = round3((memBefore - memNow) / float64(res.Achieved))
		}
	}
	h.sum.Ramp = res
	return nil
}

func readyAmong(ts []agentTiming, names map[string]bool) int {
	n := 0
	for _, t := range ts {
		if names[t.Name] && t.ReadyNow {
			n++
		}
	}
	return n
}

func filterTimings(ts []agentTiming, names map[string]bool) []agentTiming {
	var out []agentTiming
	for _, t := range ts {
		if names[t.Name] {
			out = append(out, t)
		}
	}
	return out
}

// ---- hold ----

func (h *harness) runHold(ctx context.Context) error {
	cfg := h.cfg
	timings, err := h.k.agentTimings(ctx, cfg.Namespace, phaseRamp)
	if err != nil {
		return err
	}
	if len(timings) == 0 {
		return errors.New("hold needs the ramp fleet; run the ramp phase first")
	}
	// Messages go to the longest run of Ready agents from index 0, so a
	// partially failed final wave never turns delivery errors into noise.
	readySet := map[string]bool{}
	for _, t := range timings {
		if t.ReadyNow {
			readySet[t.Name] = true
		}
	}
	count := 0
	for readySet[fmt.Sprintf("ramp-%04d", count)] {
		count++
	}
	if count == 0 {
		return errors.New("hold: no Ready ramp agents")
	}

	objs := make([]client.Object, 0, len(timings))
	for _, t := range timings {
		objs = append(objs, channelObj(cfg.Namespace, t.Name, phaseRamp, h.callbackURL()))
	}
	h.logf("hold: creating %d channels", len(objs))
	chStart := time.Now()
	if err := h.k.createAll(ctx, objs); err != nil {
		return err
	}
	var active, total int
	if err := pollUntil(ctx, 10*time.Minute, 3*time.Second, func() (bool, error) {
		var err error
		active, total, err = h.k.channelsActive(ctx, cfg.Namespace, phaseRamp)
		return active == total, err
	}); err != nil && !errors.Is(err, errTimeout) {
		return err
	}
	res := &holdResult{Agents: len(timings), Channels: total, ChannelsActiveSec: round3(time.Since(chStart).Seconds())}
	h.logf("  %d/%d channels active after %.0fs", active, total, res.ChannelsActiveSec)

	res.ReadyBefore = countReady(timings)
	res.RestartsBefore = sumRestarts(timings)
	before, err := h.scrapeGateway(ctx)
	if err != nil {
		return err
	}
	rate := float64(count) / cfg.HoldPerAgentInterval.Seconds()
	h.logf("hold: %d agents, %.2f msg/s for %s (one message per agent per %s)",
		count, rate, cfg.HoldDuration, cfg.HoldPerAgentInterval)
	holdStart := time.Now()
	sampler := h.sampleUsage(ctx)
	client, err := h.k.runLoadgen(ctx, cfg.Namespace, "loadgen-hold", cfg.LoadgenImage, []string{
		flagMode, modeChannels,
		"-path-prefix", "/channels/" + cfg.Namespace + "/ramp-",
		"-count", strconv.Itoa(count),
		"-pad", "4",
		"-rate", strconv.FormatFloat(rate, 'f', 4, 64),
		flagDuration, cfg.HoldDuration.String(),
	}, "", cfg.HoldDuration+3*time.Minute)
	peak := sampler.finish()
	if err != nil {
		return err
	}
	res.Client = client
	// Let detached deliveries and callbacks settle before reading the counters.
	time.Sleep(30 * time.Second)
	after, err := h.scrapeGateway(ctx)
	if err != nil {
		return err
	}
	webhook := map[string]string{"channel_type": channelTypeWebhook}
	res.MessagesByStatus = counterByLabel(before, after, "kaalm_channel_messages_total", "status", webhook)
	res.MessageDurationMs = histStats(histogramDelta(before, after, "kaalm_channel_message_duration_seconds", webhook))
	res.DeliveryAttempts = counterByLabel(before, after, "kaalm_channel_delivery_attempts_total", "outcome", nil)
	res.Callbacks = counterDelta(before, after, "kaalm_channel_callback_total", nil)
	res.GatewayUsageMax = peak["kaalm-gateway"]
	res.ControllerUsage = peak["kaalm-controller"]

	afterTimings, err := h.k.agentTimings(ctx, cfg.Namespace, phaseRamp)
	if err != nil {
		return err
	}
	res.ReadyAfter = countReady(afterTimings)
	res.RestartsAfter = sumRestarts(afterTimings)
	for _, t := range afterTimings {
		if t.Ready != nil && t.Ready.After(holdStart) {
			res.Flaps++
		}
	}
	h.logf("  delivery attempts by outcome %v", res.DeliveryAttempts)
	h.logf("  %d messages accepted, statuses %v, callbacks %.0f, Ready %d -> %d, restarts %d -> %d, flaps %d",
		client.Requests, res.MessagesByStatus, res.Callbacks, res.ReadyBefore, res.ReadyAfter,
		res.RestartsBefore, res.RestartsAfter, res.Flaps)
	h.sum.Hold = res
	return nil
}

func sumRestarts(ts []agentTiming) int32 {
	var n int32
	for _, t := range ts {
		n += t.Restarts
	}
	return n
}

func (h *harness) callbackURL() string {
	return "https://mock-provider." + h.cfg.Namespace + ".svc:8443/callback"
}

// ---- teardown ----

func (h *harness) runTeardown(ctx context.Context) error {
	timings, err := h.k.agentTimings(ctx, h.cfg.Namespace, phaseRamp)
	if err != nil {
		return err
	}
	h.logf("teardown: deleting %d ramp agents and their channels", len(timings))
	took, err := h.k.deletePhase(ctx, h.cfg.Namespace, phaseRamp, "ramp-", 15*time.Minute)
	if err != nil {
		return err
	}
	h.sum.Teardown = &teardownResult{Agents: len(timings), Seconds: round3(took.Seconds())}
	h.logf("  gone after %.0fs", took.Seconds())
	return nil
}

// ---- churn ----

func (h *harness) runChurn(ctx context.Context) error {
	cfg := h.cfg
	if err := h.k.ensureClass(ctx, churnClass(10*time.Second, 5*time.Second)); err != nil {
		return err
	}
	objs := make([]client.Object, 0, 2*cfg.ChurnAgents)
	for i := 0; i < cfg.ChurnAgents; i++ {
		name := fmt.Sprintf("churn-%04d", i)
		objs = append(objs,
			agentObj(cfg.Namespace, name, churnClassName, cfg.AgentImage, phaseChurn, true),
			channelObj(cfg.Namespace, name, phaseChurn, h.callbackURL()))
	}
	h.logf("churn: creating %d persistence-enabled agents with channels", cfg.ChurnAgents)
	start := time.Now()
	if err := h.k.createAll(ctx, objs); err != nil {
		return err
	}
	res := &churnResult{Agents: cfg.ChurnAgents}
	// Ready times are collected across polls: an agent that hibernates
	// before the fleet is complete loses its Ready condition, and with it
	// the transition time, so each is recorded the first time it is seen.
	readyTimes := map[string]time.Time{}
	createdTimes := map[string]time.Time{}
	var timings []agentTiming
	up := 0
	if err := pollUntil(ctx, 10*time.Minute, 3*time.Second, func() (bool, error) {
		ts, err := h.k.agentTimings(ctx, cfg.Namespace, phaseChurn)
		if err != nil {
			return false, err
		}
		timings = ts
		up = 0
		for _, t := range ts {
			createdTimes[t.Name] = t.Created
			if t.Ready != nil {
				if _, seen := readyTimes[t.Name]; !seen {
					readyTimes[t.Name] = *t.Ready
				}
			}
			if cameUp(t) {
				up++
			}
		}
		return up == cfg.ChurnAgents, nil
	}); err != nil {
		return fmt.Errorf("churn fleet never fully came up: %w (%d of %d)", err, up, cfg.ChurnAgents)
	}
	res.AllReadySec = round3(time.Since(start).Seconds())
	var ttr []float64
	for name, ready := range readyTimes {
		ttr = append(ttr, ready.Sub(createdTimes[name]).Seconds())
	}
	res.TimeToReadySec = summarize(ttr)
	h.logf("  all %d Ready after %.0fs; waiting for the first hibernation of every agent",
		cfg.ChurnAgents, res.AllReadySec)
	hibStart := time.Now()
	if err := pollUntil(ctx, 8*time.Minute, 5*time.Second, func() (bool, error) {
		ts, err := h.k.agentTimings(ctx, cfg.Namespace, phaseChurn)
		if err != nil {
			return false, err
		}
		timings = ts
		return countPhase(ts, kaalmv1beta1.AgentHibernated) == cfg.ChurnAgents, nil
	}); err != nil {
		return fmt.Errorf("churn fleet never fully Hibernated: %w (%d of %d)",
			err, countPhase(timings, kaalmv1beta1.AgentHibernated), cfg.ChurnAgents)
	}
	res.AllHibernatedSec = round3(time.Since(hibStart).Seconds())
	var firstHib []float64
	for _, t := range timings {
		if t.Hibernated != nil {
			if r, ok := readyTimes[t.Name]; ok {
				firstHib = append(firstHib, t.Hibernated.Sub(r).Seconds())
			}
		}
	}
	res.FirstHibernationSec = summarize(firstHib)
	h.logf("  all Hibernated after %.0fs (Ready-to-Hibernated p50 %.0fs p95 %.0fs)",
		res.AllHibernatedSec, res.FirstHibernationSec.P50, res.FirstHibernationSec.P95)

	gwBefore, err := h.scrapeGateway(ctx)
	if err != nil {
		return err
	}
	ctlBefore, err := h.scrapeController(ctx)
	if err != nil {
		return err
	}
	rate := float64(cfg.ChurnAgents) / cfg.ChurnCycle.Seconds()
	h.logf("churn: %.2f msg/s for %s (one message per agent per %s)", rate, cfg.ChurnDuration, cfg.ChurnCycle)
	client, err := h.k.runLoadgen(ctx, cfg.Namespace, "loadgen-churn", cfg.LoadgenImage, []string{
		flagMode, modeChannels,
		"-path-prefix", "/channels/" + cfg.Namespace + "/churn-",
		"-count", strconv.Itoa(cfg.ChurnAgents),
		"-pad", "4",
		"-rate", strconv.FormatFloat(rate, 'f', 4, 64),
		flagDuration, cfg.ChurnDuration.String(),
	}, "", cfg.ChurnDuration+3*time.Minute)
	if err != nil {
		return err
	}
	res.Client = client
	// The last messages' wakes may take the full wake budget to resolve.
	time.Sleep(90 * time.Second)
	gwAfter, err := h.scrapeGateway(ctx)
	if err != nil {
		return err
	}
	ctlAfter, err := h.scrapeController(ctx)
	if err != nil {
		return err
	}
	ns := map[string]string{"namespace": cfg.Namespace}
	webhook := map[string]string{"channel_type": channelTypeWebhook}
	res.MessagesByStatus = counterByLabel(gwBefore, gwAfter, "kaalm_channel_messages_total", "status", webhook)
	res.WakeDurationMs = histStats(histogramDelta(gwBefore, gwAfter, "kaalm_channel_wake_duration_seconds", ns))
	res.WakesByResult = histogramCountByLabel(gwBefore, gwAfter, "kaalm_channel_wake_duration_seconds", "result", ns)
	res.WakesTotal = counterDelta(ctlBefore, ctlAfter, "kaalm_wakes_total", ns)
	res.HibernationsTotal = counterDelta(ctlBefore, ctlAfter, "kaalm_hibernations_total", ns)
	res.Callbacks = counterDelta(gwBefore, gwAfter, "kaalm_channel_callback_total", nil)
	h.logf("  %d messages, wakes %.0f (by result %v), hibernations %.0f, wake p50 %.0f ms p95 %.0f ms, callbacks %.0f",
		client.Requests, res.WakesTotal, res.WakesByResult, res.HibernationsTotal,
		res.WakeDurationMs.P50, res.WakeDurationMs.P95, res.Callbacks)

	took, err := h.k.deletePhase(ctx, cfg.Namespace, phaseChurn, "churn-", 15*time.Minute)
	if err != nil {
		return err
	}
	res.TeardownSec = round3(took.Seconds())
	h.sum.Churn = res
	return nil
}

// histogramCountByLabel returns the delta of a histogram's sample count per
// distinct value of one label (the wake histogram's result label).
func histogramCountByLabel(before, after *snapshot, name, label string, want map[string]string) map[string]float64 {
	out := map[string]float64{}
	if after == nil {
		return out
	}
	for _, smp := range after.metrics[name] {
		if !match(want)(smp.labels) || smp.hist == nil {
			continue
		}
		v := smp.hist.count
		if before != nil {
			if b := before.find(name, smp.labels); b != nil && b.hist != nil {
				v -= b.hist.count
			}
		}
		out[smp.labels[label]] += v
	}
	return out
}

// ---- tasks ----

func (h *harness) runTasks(ctx context.Context) error {
	cfg := h.cfg
	if err := h.k.ensureClass(ctx, activeClass()); err != nil {
		return err
	}
	objs := make([]client.Object, 0, cfg.Tasks)
	for i := 0; i < cfg.Tasks; i++ {
		objs = append(objs, taskObj(cfg.Namespace, fmt.Sprintf("task-%04d", i), activeClassName, cfg.AgentImage))
	}
	h.logf("tasks: submitting %d AgentTasks at once", cfg.Tasks)
	start := time.Now()
	if err := h.k.createAll(ctx, objs); err != nil {
		return err
	}
	res := &tasksResult{Submitted: cfg.Tasks, SubmitSec: round3(time.Since(start).Seconds()), Phases: map[string]int{}}
	var list kaalmv1beta1.AgentTaskList
	terminal := map[kaalmv1beta1.AgentTaskPhase]bool{
		kaalmv1beta1.TaskSucceeded: true, kaalmv1beta1.TaskFailed: true, kaalmv1beta1.TaskTimedOut: true,
	}
	taskLabels := client.MatchingLabels{phaseLabel: phaseTasks}
	err := pollUntil(ctx, cfg.TaskTimeout, 3*time.Second, func() (bool, error) {
		if err := h.k.c.List(ctx, &list, client.InNamespace(cfg.Namespace), taskLabels); err != nil {
			return false, err
		}
		done := 0
		for i := range list.Items {
			if terminal[list.Items[i].Status.Phase] {
				done++
			}
		}
		return done == len(list.Items) && len(list.Items) == cfg.Tasks, nil
	})
	if err != nil && !errors.Is(err, errTimeout) {
		return err
	}
	var provision, run, total []float64
	var first, last time.Time
	for i := range list.Items {
		t := &list.Items[i]
		res.Phases[string(t.Status.Phase)]++
		res.Retries += t.Status.Retries
		c := t.CreationTimestamp.Time
		if first.IsZero() || c.Before(first) {
			first = c
		}
		if t.Status.StartTime != nil {
			provision = append(provision, t.Status.StartTime.Sub(c).Seconds())
		}
		if t.Status.CompletionTime != nil {
			total = append(total, t.Status.CompletionTime.Sub(c).Seconds())
			if t.Status.CompletionTime.After(last) {
				last = t.Status.CompletionTime.Time
			}
			if t.Status.StartTime != nil {
				run = append(run, t.Status.CompletionTime.Sub(t.Status.StartTime.Time).Seconds())
			}
		}
	}
	res.ProvisionSec = summarize(provision)
	res.RunSec = summarize(run)
	res.TotalSec = summarize(total)
	if !last.IsZero() {
		res.MakespanSec = round3(last.Sub(first).Seconds())
		if res.MakespanSec > 0 {
			res.ThroughputPerMin = round3(float64(len(total)) * 60 / res.MakespanSec)
		}
	}
	if errors.Is(err, errTimeout) {
		h.note("tasks: not every task settled within %s; phases %v", cfg.TaskTimeout, res.Phases)
	}
	h.logf("  phases %v, makespan %.0fs, %.1f tasks/min, created-to-completion p50 %.0fs p95 %.0fs, retries %d",
		res.Phases, res.MakespanSec, res.ThroughputPerMin, res.TotalSec.P50, res.TotalSec.P95, res.Retries)
	took, err := h.k.deletePhase(ctx, cfg.Namespace, phaseTasks, "task-", 10*time.Minute)
	if err != nil {
		return err
	}
	res.TeardownSec = round3(took.Seconds())
	h.sum.Tasks = res
	return nil
}
