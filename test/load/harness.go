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
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type harness struct {
	cfg config
	k   *cluster
	sum *summary
}

type summary struct {
	Commit      string          `json:"commit"`
	StartedAt   time.Time       `json:"startedAt"`
	FinishedAt  time.Time       `json:"finishedAt"`
	Config      config          `json:"config"`
	Environment environment     `json:"environment"`
	Gateway     *gatewayResult  `json:"gateway,omitempty"`
	Ramp        *rampResult     `json:"ramp,omitempty"`
	Hold        *holdResult     `json:"hold,omitempty"`
	Teardown    *teardownResult `json:"teardown,omitempty"`
	Churn       *churnResult    `json:"churn,omitempty"`
	Tasks       *tasksResult    `json:"tasks,omitempty"`
	Notes       []string        `json:"notes,omitempty"`
}

type environment struct {
	HostCPUs          int                      `json:"hostCpus"`
	HostMemTotalMiB   float64                  `json:"hostMemTotalMiB"`
	Kernel            string                   `json:"kernel"`
	K3dVersion        string                   `json:"k3dVersion"`
	KubernetesVersion string                   `json:"kubernetesVersion"`
	Nodes             []nodeInfo               `json:"nodes"`
	Components        map[string]componentInfo `json:"components"`
}

type nodeInfo struct {
	Name              string  `json:"name"`
	AllocatablePods   int64   `json:"allocatablePods"`
	AllocatableCPU    string  `json:"allocatableCpu"`
	AllocatableMemMiB float64 `json:"allocatableMemMiB"`
	Kubelet           string  `json:"kubelet"`
}

type componentInfo struct {
	Replicas int32  `json:"replicas"`
	Image    string `json:"image"`
}

func (h *harness) logf(format string, args ...any) {
	fmt.Printf("%s  %s\n", time.Now().Format("15:04:05"), fmt.Sprintf(format, args...))
}

func (h *harness) note(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	h.sum.Notes = append(h.sum.Notes, msg)
	h.logf("note: %s", msg)
}

func runHarness(args []string) error {
	cfg, err := parseRunFlags(args)
	if err != nil {
		return err
	}
	if cfg.HostPreflight {
		if err := preflightHost(); err != nil {
			return err
		}
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	k, err := connect(cfg.Context)
	if err != nil {
		return err
	}
	h := &harness{cfg: cfg, k: k, sum: &summary{StartedAt: time.Now(), Config: cfg, Commit: gitCommit()}}
	if cfg.Out == "" {
		cfg.Out = filepath.Join("test", "load", "results", h.sum.StartedAt.Format("2006-01-02T15-04-05")+".json")
		h.cfg.Out = cfg.Out
	}

	h.logf("load harness against context %s, phases %s", cfg.Context, strings.Join(cfg.Phases, ","))
	if err := h.preflightCluster(ctx); err != nil {
		return err
	}
	if err := h.applyInfra(ctx); err != nil {
		return err
	}
	h.cleanLeftovers(ctx)

	runErr := h.runPhases(ctx)
	h.sum.FinishedAt = time.Now()
	if err := h.write(); err != nil {
		return err
	}
	h.printTable()
	if runErr != nil {
		return runErr
	}
	h.logf("summary written to %s", cfg.Out)
	return nil
}

func (h *harness) runPhases(ctx context.Context) error {
	for _, p := range h.cfg.Phases {
		var err error
		start := time.Now()
		switch p {
		case phaseGateway:
			err = h.runGateway(ctx)
		case phaseRamp:
			err = h.runRamp(ctx)
		case phaseHold:
			err = h.runHold(ctx)
		case phaseTeardown:
			err = h.runTeardown(ctx)
		case phaseChurn:
			err = h.runChurn(ctx)
		case phaseTasks:
			err = h.runTasks(ctx)
		}
		if err != nil {
			h.note("phase %s failed after %s: %v", p, time.Since(start).Round(time.Second), err)
			return fmt.Errorf("phase %s: %w", p, err)
		}
		h.logf("phase %s done in %s", p, time.Since(start).Round(time.Second))
	}
	return nil
}

// preflightHost fails fast on the host limits that kill a k3d cluster at a
// few hundred pods with confusing symptoms. The k3d nodes share this kernel.
func preflightHost() error {
	checks := []struct {
		path string
		min  int
		fix  string
	}{
		{"/proc/sys/fs/inotify/max_user_instances", 512, "sudo sysctl -w fs.inotify.max_user_instances=8192"},
		{"/proc/sys/fs/inotify/max_user_watches", 524288, "sudo sysctl -w fs.inotify.max_user_watches=1048576"},
	}
	for _, c := range checks {
		raw, err := os.ReadFile(c.path)
		if err != nil {
			continue // not Linux; nothing to check
		}
		v, err := strconv.Atoi(strings.TrimSpace(string(raw)))
		if err != nil {
			continue
		}
		if v < c.min {
			return fmt.Errorf("preflight: %s is %d, need at least %d for a few hundred pods on k3d; run: %s",
				c.path, v, c.min, c.fix)
		}
	}
	return nil
}

func (h *harness) preflightCluster(ctx context.Context) error {
	env := environment{HostCPUs: runtime.NumCPU(), Components: map[string]componentInfo{}}
	if mem, err := hostMemTotalMiB(); err == nil {
		env.HostMemTotalMiB = round3(mem)
	}
	if out, err := exec.Command("uname", "-r").Output(); err == nil {
		env.Kernel = strings.TrimSpace(string(out))
	}
	if out, err := exec.Command("k3d", "version").Output(); err == nil {
		env.K3dVersion = strings.TrimSpace(strings.SplitN(string(out), "\n", 2)[0])
	}
	if v, err := h.k.cs.Discovery().ServerVersion(); err == nil {
		env.KubernetesVersion = v.GitVersion
	}
	var nodes corev1.NodeList
	if err := h.k.c.List(ctx, &nodes); err != nil {
		return fmt.Errorf("list nodes on %s: %w", h.cfg.Context, err)
	}
	for i := range nodes.Items {
		n := &nodes.Items[i]
		env.Nodes = append(env.Nodes, nodeInfo{
			Name:              n.Name,
			AllocatablePods:   n.Status.Allocatable.Pods().Value(),
			AllocatableCPU:    n.Status.Allocatable.Cpu().String(),
			AllocatableMemMiB: round3(n.Status.Allocatable.Memory().AsApproximateFloat64() / (1 << 20)),
			Kubelet:           n.Status.NodeInfo.KubeletVersion,
		})
	}
	for _, name := range []string{"kaalm-controller", "kaalm-gateway"} {
		if err := h.k.kubectl("rollout", "status", "deploy/"+name, "-n", "kaalm-system", "--timeout=180s"); err != nil {
			return fmt.Errorf("chart not ready: %w", err)
		}
		d, err := h.k.cs.AppsV1().Deployments("kaalm-system").Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		info := componentInfo{Replicas: d.Status.ReadyReplicas}
		if len(d.Spec.Template.Spec.Containers) > 0 {
			info.Image = d.Spec.Template.Spec.Containers[0].Image
		}
		env.Components[name] = info
	}
	h.sum.Environment = env
	h.logf("cluster: %s, %d nodes, controller x%d, gateway x%d", env.KubernetesVersion, len(env.Nodes),
		env.Components["kaalm-controller"].Replicas, env.Components["kaalm-gateway"].Replicas)
	return nil
}

func (h *harness) applyInfra(ctx context.Context) error {
	if err := h.k.kubectl("apply", "-f", h.cfg.Infra); err != nil {
		return err
	}
	err := h.k.kubectl("rollout", "status", "deploy/mock-provider", "-n", h.cfg.Namespace, "--timeout=180s")
	if err != nil {
		return fmt.Errorf("mock provider not ready: %w", err)
	}
	// The channel callbacks and the loadgen's CA mount both need the trust
	// bundle in the fleet namespace; trust-manager distributes it on
	// namespace creation, usually within seconds.
	return pollUntil(ctx, 2*time.Minute, 2*time.Second, func() (bool, error) {
		_, err := h.k.cs.CoreV1().ConfigMaps(h.cfg.Namespace).Get(ctx, "kaalm-ca", metav1.GetOptions{})
		return err == nil, nil
	})
}

// cleanLeftovers clears a previous run's fleet so an inner-loop re-run on the
// same cluster starts from nothing.
func (h *harness) cleanLeftovers(ctx context.Context) {
	phases := []struct{ phase, prefix string }{{phaseRamp, "ramp-"}, {phaseChurn, "churn-"}, {phaseTasks, "task-"}}
	for _, p := range phases {
		if _, err := h.k.deletePhase(ctx, h.cfg.Namespace, p.phase, p.prefix, 10*time.Minute); err != nil {
			h.note("cleaning leftover %s objects: %v", p.phase, err)
		}
	}
}

func (h *harness) write() error {
	if err := os.MkdirAll(filepath.Dir(h.cfg.Out), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(h.sum, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(h.cfg.Out, data, 0o644)
}

func gitCommit() string {
	out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func (h *harness) printTable() {
	s := h.sum
	fmt.Println()
	fmt.Printf("== load summary (%s, %s) ==\n", s.Commit, s.FinishedAt.Sub(s.StartedAt).Round(time.Second))
	fmt.Printf("host: %d CPUs, %.0f MiB; %d nodes; %s\n",
		s.Environment.HostCPUs, s.Environment.HostMemTotalMiB, len(s.Environment.Nodes), s.Environment.KubernetesVersion)
	if g := s.Gateway; g != nil {
		fmt.Println("\ngateway (client-observed ms | gateway-side ms):")
		for _, l := range g.Legs {
			fmt.Printf("  %-36s %6.1f rps  p50 %6.1f | %6.1f  p95 %6.1f | %6.1f  p99 %6.1f | %6.1f",
				l.Name, l.Client.RPS, l.Client.LatencyMs.P50, l.GatewaySideMs.P50,
				l.Client.LatencyMs.P95, l.GatewaySideMs.P95, l.Client.LatencyMs.P99, l.GatewaySideMs.P99)
			fmt.Printf("  statuses %v  gw peak %.0f mCPU %.0f MiB\n",
				l.Client.Statuses, l.GatewayUsageMax.CPUMilli, l.GatewayUsageMax.MemMiB)
		}
	}
	if r := s.Ramp; r != nil {
		fmt.Printf("\nramp: %d agents Ready (target %d)", r.Achieved, r.Target)
		if r.Saturation != "" {
			fmt.Printf("; stopped: %s", r.Saturation)
		}
		fmt.Printf("; %.1f MiB host memory per agent\n", r.MemPerAgentMiB)
		fmt.Println("  wave  fleet  wall(s)  ready p50/p95/max(s)  cert p50(s)  podstart p50(s)" +
			"  reconcile p50/p99(ms)  ctl MiB  gw MiB  host avail MiB")
		for _, w := range r.Waves {
			fmt.Printf("  %4d  %5d  %7.0f  %5.0f/%5.0f/%5.0f  %11.0f  %15.0f",
				w.Index, w.FleetReady, w.WallSec, w.TimeToReadySec.P50, w.TimeToReadySec.P95, w.TimeToReadySec.Max,
				w.CertIssueSec.P50, w.PodStartSec.P50)
			fmt.Printf("  %10.1f/%6.1f  %7.0f  %6.0f  %14.0f\n",
				w.ReconcileMs.P50, w.ReconcileMs.P99, w.Controller.MemMiB, w.Gateway.MemMiB, w.HostMemAvailMiB)
		}
	}
	if hd := s.Hold; hd != nil {
		fmt.Printf("\nhold: %d agents, %d messages at %.1f msg/s; gateway statuses %v; callbacks %.0f",
			hd.Agents, hd.Client.Requests, hd.Client.RPS, hd.MessagesByStatus, hd.Callbacks)
		fmt.Printf("; message p50 %.0f ms p95 %.0f ms; Ready %d->%d; restarts %d->%d; flaps %d\n",
			hd.MessageDurationMs.P50, hd.MessageDurationMs.P95,
			hd.ReadyBefore, hd.ReadyAfter, hd.RestartsBefore, hd.RestartsAfter, hd.Flaps)
	}
	if t := s.Teardown; t != nil {
		fmt.Printf("\nteardown: %d agents gone in %.0fs\n", t.Agents, t.Seconds)
	}
	if c := s.Churn; c != nil {
		fmt.Printf("\nchurn: %d agents; all Ready %.0fs; first hibernation p50 %.0fs p95 %.0fs (all %.0fs); %d messages",
			c.Agents, c.AllReadySec, c.FirstHibernationSec.P50, c.FirstHibernationSec.P95, c.AllHibernatedSec, c.Client.Requests)
		fmt.Printf("; wakes %.0f %v; hibernations %.0f; wake p50/p95/p99 %.0f/%.0f/%.0f ms; callbacks %.0f; statuses %v\n",
			c.WakesTotal, c.WakesByResult, c.HibernationsTotal, c.WakeDurationMs.P50, c.WakeDurationMs.P95,
			c.WakeDurationMs.P99, c.Callbacks, c.MessagesByStatus)
	}
	if t := s.Tasks; t != nil {
		fmt.Printf("\ntasks: %d submitted in %.0fs; phases %v; makespan %.0fs (%.1f/min)",
			t.Submitted, t.SubmitSec, t.Phases, t.MakespanSec, t.ThroughputPerMin)
		fmt.Printf("; provision p50 %.0fs; run p50 %.0fs; total p50/p95 %.0f/%.0fs; retries %d\n",
			t.ProvisionSec.P50, t.RunSec.P50, t.TotalSec.P50, t.TotalSec.P95, t.Retries)
	}
	for _, n := range s.Notes {
		fmt.Println("note:", n)
	}
}
