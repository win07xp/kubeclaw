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
	"flag"
	"fmt"
	"strings"
	"time"
)

// config holds the run's knobs. Every default is the baseline shape the
// design book's numbers were taken with; override through the load-* Makefile
// variables (LOAD_FLAGS) rather than editing the defaults, so a re-run of the
// baseline stays one command.
type config struct {
	Context      string
	Namespace    string
	AgentImage   string
	LoadgenImage string
	Infra        string
	Out          string
	Phases       []string
	// HostPreflight checks the host sysctls a few hundred pods need. Off is
	// for partial runs (a gateway leg on a laptop) that never build a fleet.
	HostPreflight bool

	// The max-active ramp: waves of agents that are never retired, until the
	// target is reached or the environment saturates.
	RampTarget  int
	WaveSize    int
	WaveTimeout time.Duration
	// MemFloorMiB stops the ramp when the host's MemAvailable drops below it.
	// BestEffort pods carry no requests, so nothing else stands between the
	// ramp and the kernel's OOM killer.
	MemFloorMiB int

	// Hold-and-serve: message load across the whole active fleet.
	HoldDuration         time.Duration
	HoldPerAgentInterval time.Duration

	// Hibernation churn on a persistence-enabled subset.
	ChurnAgents   int
	ChurnCycle    time.Duration
	ChurnDuration time.Duration

	// Concurrent AgentTasks.
	Tasks       int
	TaskTimeout time.Duration

	// Gateway steady state.
	GatewayConcurrency int
	GatewayDuration    time.Duration
}

// Phase names, in run order.
const (
	phaseGateway  = "gateway"
	phaseRamp     = "ramp"
	phaseHold     = "hold"
	phaseTeardown = "teardown"
	phaseChurn    = "churn"
	phaseTasks    = "tasks"
)

// Strings shared between the orchestrator and the in-cluster load generator.
const (
	modeGateway         = "gateway"
	modeChannels        = "channels"
	flagMode            = "-mode"
	flagDuration        = "-duration"
	loadgenName         = "loadgen" // the subcommand, the ServiceAccount, and the Job label
	tokenVolume         = "token"
	hookSecretKey       = "token"
	channelTypeWebhook  = "webhook"
	workloadAgent       = "agent" // the kaalm.io/workload label value on agent Pods
	agentControllerName = "agent" // the controller label on controller-runtime metrics
)

// The three ModelProviders testdata/infra.yaml defines, told apart by the
// mock's endpoint prefix: immediate, 50 ms, and immediate under hard budget.
const (
	providerFast = "load-fast"
	providerSlow = "load-slow"
	providerHard = "load-hard"
)

var allPhases = []string{phaseGateway, phaseRamp, phaseHold, phaseTeardown, phaseChurn, phaseTasks}

func parseRunFlags(args []string) (config, error) {
	var c config
	var phases string
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.StringVar(&c.Context, "context", "k3d-kaalm-load", "kubeconfig context of the load cluster")
	fs.StringVar(&c.Namespace, "namespace", "load", "namespace the fleet lives in (must match testdata/infra.yaml)")
	fs.StringVar(&c.AgentImage, "agent-image", "registry.test/agents/starter-go:e2e", "agent image for every workload")
	fs.StringVar(&c.LoadgenImage, "loadgen-image", "registry.test/load/loadgen:load", "in-cluster load generator image")
	fs.StringVar(&c.Infra, "infra", "test/load/testdata/infra.yaml",
		"in-cluster infrastructure manifest applied before any phase")
	fs.StringVar(&c.Out, "out", "", "summary JSON path (default test/load/results/<timestamp>.json)")
	fs.StringVar(&phases, "phases", strings.Join(allPhases, ","),
		"comma-separated phases to run, in this order: "+strings.Join(allPhases, ","))
	fs.BoolVar(&c.HostPreflight, "host-preflight", true,
		"fail fast when the host's inotify sysctls are too low for a few hundred pods")

	fs.IntVar(&c.RampTarget, "ramp-target", 500, "agents to reach in the max-active ramp")
	fs.IntVar(&c.WaveSize, "wave-size", 50, "agents created per ramp wave")
	fs.DurationVar(&c.WaveTimeout, "wave-timeout", 6*time.Minute,
		"how long a wave may take to reach Ready before the ramp stops")
	fs.IntVar(&c.MemFloorMiB, "mem-floor-mib", 3072, "stop the ramp when host MemAvailable drops below this many MiB")

	fs.DurationVar(&c.HoldDuration, "hold-duration", 3*time.Minute,
		"how long to drive messages across the whole active fleet")
	fs.DurationVar(&c.HoldPerAgentInterval, "hold-interval", time.Minute,
		"each active agent receives one message per this interval")

	fs.IntVar(&c.ChurnAgents, "churn-agents", 100, "persistence-enabled agents in the hibernation churn")
	fs.DurationVar(&c.ChurnCycle, "churn-cycle", 90*time.Second,
		"each churn agent receives one message per this interval "+
			"(longer than a hibernation cycle, so most messages are cold wakes)")
	fs.DurationVar(&c.ChurnDuration, "churn-duration", 10*time.Minute, "how long to drive the churn")

	fs.IntVar(&c.Tasks, "tasks", 200, "AgentTasks submitted at once")
	fs.DurationVar(&c.TaskTimeout, "task-timeout", 15*time.Minute, "how long the whole task batch may take to settle")

	fs.IntVar(&c.GatewayConcurrency, "gateway-concurrency", 32, "concurrent LLM callers per gateway leg")
	fs.DurationVar(&c.GatewayDuration, "gateway-duration", time.Minute, "duration of each gateway leg")

	if err := fs.Parse(args); err != nil {
		return c, err
	}
	for _, p := range strings.Split(phases, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		known := false
		for _, k := range allPhases {
			if k == p {
				known = true
			}
		}
		if !known {
			return c, fmt.Errorf("unknown phase %q (known: %s)", p, strings.Join(allPhases, ","))
		}
		c.Phases = append(c.Phases, p)
	}
	if c.WaveSize <= 0 || c.RampTarget <= 0 {
		return c, fmt.Errorf("ramp-target and wave-size must be positive")
	}
	return c, nil
}
