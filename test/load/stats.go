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
	"math"
	"sort"
)

// stats summarizes a sample of durations. Units are whatever the caller fed
// in (the harness uses seconds for API-derived timings and milliseconds for
// request latencies) and the JSON field name says which.
type stats struct {
	Count int     `json:"count"`
	Mean  float64 `json:"mean"`
	P50   float64 `json:"p50"`
	P90   float64 `json:"p90"`
	P95   float64 `json:"p95"`
	P99   float64 `json:"p99"`
	Max   float64 `json:"max"`
}

func summarize(samples []float64) stats {
	if len(samples) == 0 {
		return stats{}
	}
	s := append([]float64(nil), samples...)
	sort.Float64s(s)
	var sum float64
	for _, v := range s {
		sum += v
	}
	return stats{
		Count: len(s),
		Mean:  round3(sum / float64(len(s))),
		P50:   round3(percentile(s, 0.50)),
		P90:   round3(percentile(s, 0.90)),
		P95:   round3(percentile(s, 0.95)),
		P99:   round3(percentile(s, 0.99)),
		Max:   round3(s[len(s)-1]),
	}
}

// percentile is the nearest-rank percentile of an ascending sample.
func percentile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(q*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

func round3(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return math.Round(v*1000) / 1000
}
