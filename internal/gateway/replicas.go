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
	"sync"
	"time"
)

// CachedCount wraps a counter so it is re-evaluated at most once per ttl.
// Concurrent callers inside a window share one evaluation; the wrapped
// function is never called concurrently with itself.
func CachedCount(ttl time.Duration, f func() int) func() int {
	var (
		mu    sync.Mutex
		value int
		until time.Time
	)
	return func() int {
		mu.Lock()
		defer mu.Unlock()
		if now := time.Now(); now.After(until) {
			value = f()
			until = now.Add(ttl)
		}
		return value
	}
}
