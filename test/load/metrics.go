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
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// The operator's metrics are replica-local: each gateway replica owns its own
// histograms and only the leading controller replica reconciles. A snapshot
// therefore scrapes every pod of a component and merges: counters and
// histograms sum, gauges take the max (the cache-derived fleet gauges are
// identical on every controller replica, and summing would double count).

type sample struct {
	labels map[string]string
	value  float64
	hist   *histogram
}

type histogram struct {
	buckets []bucket // ascending upper bounds, cumulative counts
	count   float64
	sum     float64
}

type bucket struct {
	le    float64
	count float64
}

type snapshot struct {
	at      time.Time
	metrics map[string][]sample // metric name -> samples (one per label set)
}

// scrapeComponent snapshots one chart component's /metrics across all its
// pods, retrying a few times: a scrape is a pod list plus a port-forward per
// pod, and either can lose a connection through the k3d load balancer while
// the fleet is large.
func (k *cluster) scrapeComponent(ctx context.Context, namespace, component, port string) (*snapshot, error) {
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		snap, err := k.scrapeComponentOnce(ctx, namespace, component, port)
		if err == nil {
			return snap, nil
		}
		lastErr = err
		fmt.Printf("%s  scrape %s attempt %d failed: %v\n", time.Now().Format("15:04:05"), component, attempt+1, err)
		time.Sleep(time.Duration(attempt+1) * 5 * time.Second)
	}
	return nil, lastErr
}

func (k *cluster) scrapeComponentOnce(ctx context.Context, namespace, component, port string) (*snapshot, error) {
	var pods corev1.PodList
	if err := k.c.List(ctx, &pods, client.InNamespace(namespace), client.MatchingLabels{
		"app.kubernetes.io/name":      "kaalm",
		"app.kubernetes.io/component": component,
	}); err != nil {
		return nil, err
	}
	snap := &snapshot{at: time.Now(), metrics: map[string][]sample{}}
	scraped := 0
	for _, pod := range pods.Items {
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}
		families, err := k.scrapePod(namespace, pod.Name, port)
		if err != nil {
			return nil, fmt.Errorf("scrape %s: %w", pod.Name, err)
		}
		snap.merge(families)
		scraped++
	}
	if scraped == 0 {
		return nil, fmt.Errorf("no running %s pods to scrape", component)
	}
	return snap, nil
}

func (k *cluster) scrapePod(namespace, pod, port string) (map[string]*dto.MetricFamily, error) {
	local, stop, err := k.portForward(namespace, "pod/"+pod, port)
	if err != nil {
		return nil, err
	}
	defer stop()
	cli := &http.Client{Timeout: 20 * time.Second}
	var body []byte
	for attempt := 0; attempt < 5; attempt++ {
		resp, err := cli.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", local))
		if err == nil {
			body, err = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if err == nil && resp.StatusCode == http.StatusOK {
				break
			}
		}
		time.Sleep(time.Second)
	}
	if len(body) == 0 {
		return nil, fmt.Errorf("empty metrics body from %s", pod)
	}
	var parser expfmt.TextParser
	return parser.TextToMetricFamilies(strings.NewReader(string(body)))
}

func (s *snapshot) merge(families map[string]*dto.MetricFamily) {
	for name, mf := range families {
		for _, m := range mf.Metric {
			labels := map[string]string{}
			for _, lp := range m.Label {
				labels[lp.GetName()] = lp.GetValue()
			}
			var incoming sample
			incoming.labels = labels
			switch mf.GetType() {
			case dto.MetricType_COUNTER:
				incoming.value = m.Counter.GetValue()
			case dto.MetricType_GAUGE:
				incoming.value = m.Gauge.GetValue()
			case dto.MetricType_HISTOGRAM:
				h := &histogram{count: float64(m.Histogram.GetSampleCount()), sum: m.Histogram.GetSampleSum()}
				for _, b := range m.Histogram.Bucket {
					h.buckets = append(h.buckets, bucket{le: b.GetUpperBound(), count: float64(b.GetCumulativeCount())})
				}
				incoming.hist = h
			default:
				continue
			}
			existing := s.find(name, labels)
			if existing == nil {
				s.metrics[name] = append(s.metrics[name], incoming)
				continue
			}
			switch mf.GetType() {
			case dto.MetricType_COUNTER:
				existing.value += incoming.value
			case dto.MetricType_GAUGE:
				existing.value = math.Max(existing.value, incoming.value)
			case dto.MetricType_HISTOGRAM:
				existing.hist.add(incoming.hist)
			}
		}
	}
}

func (s *snapshot) find(name string, labels map[string]string) *sample {
	for i := range s.metrics[name] {
		if labelsEqual(s.metrics[name][i].labels, labels) {
			return &s.metrics[name][i]
		}
	}
	return nil
}

func labelsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func (h *histogram) add(o *histogram) {
	h.count += o.count
	h.sum += o.sum
	if len(h.buckets) != len(o.buckets) {
		return // same binary, same buckets; anything else is a scrape mismatch
	}
	for i := range h.buckets {
		h.buckets[i].count += o.buckets[i].count
	}
}

// sub returns h minus o (a delta between two snapshots of the same series).
func (h *histogram) sub(o *histogram) *histogram {
	if o == nil {
		return h
	}
	out := &histogram{count: h.count - o.count, sum: h.sum - o.sum}
	for i, b := range h.buckets {
		c := b.count
		if i < len(o.buckets) {
			c -= o.buckets[i].count
		}
		out.buckets = append(out.buckets, bucket{le: b.le, count: c})
	}
	return out
}

// quantile estimates a quantile from cumulative buckets with linear
// interpolation inside the bucket, the same estimate Prometheus's
// histogram_quantile makes.
func (h *histogram) quantile(q float64) float64 {
	if h == nil || h.count == 0 || len(h.buckets) == 0 {
		return 0
	}
	rank := q * h.count
	prevLE, prevCount := 0.0, 0.0
	for i, b := range h.buckets {
		if b.count >= rank {
			if math.IsInf(b.le, 1) {
				if i == 0 {
					return 0
				}
				return h.buckets[i-1].le
			}
			if b.count == prevCount {
				return b.le
			}
			return prevLE + (b.le-prevLE)*(rank-prevCount)/(b.count-prevCount)
		}
		prevLE, prevCount = b.le, b.count
	}
	return h.buckets[len(h.buckets)-1].le
}

// histStats renders a histogram delta in milliseconds as a stats block; the
// Prometheus buckets are seconds.
func histStats(h *histogram) stats {
	if h == nil || h.count == 0 {
		return stats{}
	}
	return stats{
		Count: int(h.count),
		Mean:  round3(h.sum / h.count * 1000),
		P50:   round3(h.quantile(0.50) * 1000),
		P90:   round3(h.quantile(0.90) * 1000),
		P95:   round3(h.quantile(0.95) * 1000),
		P99:   round3(h.quantile(0.99) * 1000),
		Max:   round3(maxObservedBucket(h) * 1000),
	}
}

// maxObservedBucket is the upper bound of the highest non-empty finite
// bucket: the tightest statement a histogram can make about its maximum.
func maxObservedBucket(h *histogram) float64 {
	var prev float64
	for _, b := range h.buckets {
		if math.IsInf(b.le, 1) {
			if b.count > prev {
				return h.buckets[len(h.buckets)-2].le // observations beyond the last finite bucket
			}
			break
		}
		prev = b.count
	}
	best := 0.0
	var prevCount float64
	for _, b := range h.buckets {
		if math.IsInf(b.le, 1) {
			break
		}
		if b.count > prevCount {
			best = b.le
		}
		prevCount = b.count
	}
	return best
}

// match selects samples whose labels include every given pair.
func match(want map[string]string) func(map[string]string) bool {
	return func(have map[string]string) bool {
		for k, v := range want {
			if have[k] != v {
				return false
			}
		}
		return true
	}
}

// counter sums matching counter samples in a snapshot.
func (s *snapshot) counter(name string, want map[string]string) float64 {
	if s == nil {
		return 0
	}
	var total float64
	for _, smp := range s.metrics[name] {
		if match(want)(smp.labels) && smp.hist == nil {
			total += smp.value
		}
	}
	return total
}

// counterDelta is after minus before for matching counters.
func counterDelta(before, after *snapshot, name string, want map[string]string) float64 {
	return after.counter(name, want) - before.counter(name, want)
}

// counterByLabel returns after-minus-before per distinct value of one label.
func counterByLabel(before, after *snapshot, name, label string, want map[string]string) map[string]float64 {
	out := map[string]float64{}
	if after == nil {
		return out
	}
	for _, smp := range after.metrics[name] {
		if !match(want)(smp.labels) || smp.hist != nil {
			continue
		}
		v := smp.value
		if before != nil {
			if b := before.find(name, smp.labels); b != nil {
				v -= b.value
			}
		}
		out[smp.labels[label]] += v
	}
	return out
}

// histogramDelta merges every matching histogram series and subtracts the
// before snapshot's matching series.
func histogramDelta(before, after *snapshot, name string, want map[string]string) *histogram {
	if after == nil {
		return nil
	}
	var merged *histogram
	for _, smp := range after.metrics[name] {
		if !match(want)(smp.labels) || smp.hist == nil {
			continue
		}
		delta := smp.hist
		if before != nil {
			if b := before.find(name, smp.labels); b != nil && b.hist != nil {
				delta = smp.hist.sub(b.hist)
			}
		}
		if merged == nil {
			copied := *delta
			copied.buckets = append([]bucket(nil), delta.buckets...)
			merged = &copied
			continue
		}
		merged.add(delta)
	}
	return merged
}

// gauge returns the max of matching gauge samples.
func (s *snapshot) gauge(name string, want map[string]string) float64 {
	if s == nil {
		return 0
	}
	best := 0.0
	for _, smp := range s.metrics[name] {
		if match(want)(smp.labels) && smp.hist == nil {
			best = math.Max(best, smp.value)
		}
	}
	return best
}
