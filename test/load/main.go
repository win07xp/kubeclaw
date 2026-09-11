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

// Command load is the scale proof (#140): a repeatable harness that stands up
// a Kaalm fleet on a dedicated k3d cluster, drives load through the shipped
// chart, and emits a machine-readable summary. `make load` runs it end to end;
// the first run's numbers are the baseline the design book's
// operations/load-and-scale.md page publishes and later releases compare
// against.
//
// One binary, two entrypoints:
//
//	load run      the host-side orchestrator: creates the fleet, waits on the
//	              API objects, scrapes the controller and gateway metrics,
//	              writes the summary
//	load loadgen  the in-cluster load generator the orchestrator runs as a
//	              Job, so request traffic reaches the gateway the way a
//	              workload's would and never crosses a port-forward
//
// The loadtest build tag keeps the package out of `go build ./...`, the lint
// run, and the coverage gate: it drives a live cluster and is not a unit
// under test. Lint it explicitly with `--build-tags loadtest`.
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "run":
		err = runHarness(os.Args[2:])
	case loadgenName:
		err = runLoadgen(os.Args[2:])
	default:
		printUsage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "load:", err)
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "usage: load run [flags] | load loadgen [flags]")
}
