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

// Package profiling is the off-by-default pprof listener the gateway and
// the controller can expose for profiling under load. It is a debugging
// aid: the listener is unauthenticated, serves on its own port, and is
// meant to be reached through a port-forward, never through a Service.
package profiling

import (
	"errors"
	"net/http"
	"net/http/pprof"
	"runtime"
	"time"
)

const (
	// mutexFraction samples one in this many lock contention events.
	mutexFraction = 10
	// blockRateNanos samples one blocking event per this much time spent
	// blocked, so events shorter than 100 microseconds are mostly skipped.
	blockRateNanos = 100_000
)

// Handler serves the net/http/pprof endpoints under /debug/pprof/ on a mux
// of its own, so the profiles never share a listener with metrics or the
// request paths. Importing net/http/pprof also registers the same handlers
// on http.DefaultServeMux; neither binary serves the default mux, so this
// listener is the only way to reach them.
func Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

// EnableSampling turns on the mutex and block profilers, which Go leaves off
// because sampling costs a little on every contended lock and every blocking
// operation. Call it only when a pprof listener is configured; without it
// the mutex and block profiles are empty.
func EnableSampling() {
	runtime.SetMutexProfileFraction(mutexFraction)
	runtime.SetBlockProfileRate(blockRateNanos)
}

// Start opens the pprof listener on addr in the background and turns
// sampling on. An empty addr does nothing and reports false, which is the
// default configuration. A listener failure is passed to onError.
func Start(addr string, onError func(error)) bool {
	if addr == "" {
		return false
	}
	EnableSampling()
	go func() {
		srv := &http.Server{Addr: addr, Handler: Handler(), ReadHeaderTimeout: 10 * time.Second}
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			onError(err)
		}
	}()
	return true
}
