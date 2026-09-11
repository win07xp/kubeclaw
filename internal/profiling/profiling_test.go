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

package profiling

import (
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
)

func TestHandlerServesProfiles(t *testing.T) {
	h := Handler()
	for _, path := range []string{"/debug/pprof/", "/debug/pprof/heap", "/debug/pprof/goroutine", "/debug/pprof/cmdline"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Errorf("%s: status %d, want 200", path, rec.Code)
		}
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("/metrics on the pprof mux: status %d, want 404 (the mux serves profiles only)", rec.Code)
	}
}

func TestEnableSamplingTurnsOnMutexAndBlockProfiles(t *testing.T) {
	prevMutex := runtime.SetMutexProfileFraction(0)
	runtime.SetBlockProfileRate(0)
	t.Cleanup(func() {
		runtime.SetMutexProfileFraction(prevMutex)
		runtime.SetBlockProfileRate(0)
	})
	EnableSampling()
	if got := runtime.SetMutexProfileFraction(-1); got != mutexFraction {
		t.Errorf("mutex profile fraction = %d, want %d", got, mutexFraction)
	}
}

func TestStartIsOffWithoutAnAddress(t *testing.T) {
	prevMutex := runtime.SetMutexProfileFraction(0)
	t.Cleanup(func() { runtime.SetMutexProfileFraction(prevMutex) })
	if Start("", func(error) { t.Error("onError called") }) {
		t.Fatal("Start with an empty address reported a listener")
	}
	if got := runtime.SetMutexProfileFraction(-1); got != 0 {
		t.Errorf("mutex sampling turned on without a listener (fraction %d)", got)
	}
	if !Start("127.0.0.1:0", func(err error) { t.Errorf("listener: %v", err) }) {
		t.Fatal("Start with an address reported no listener")
	}
}
