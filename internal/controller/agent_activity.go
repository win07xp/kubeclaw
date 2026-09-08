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

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/win07xp/kaalm/internal/tlsutil"
)

// AgentActivity is one replica's view of one agent's two signal sources.
type AgentActivity struct {
	GatewayTraffic *time.Time `json:"gatewayTraffic"`
	Heartbeat      *time.Time `json:"heartbeat"`
}

// ReplicaActivity is one gateway replica's /v1/activity response.
type ReplicaActivity struct {
	StartedAt time.Time                `json:"replicaStartedAt"`
	Agents    map[string]AgentActivity `json:"agents"`
}

// ActivityClient fetches per-namespace activity from the gateway fleet.
// reachable holds one entry per replica that answered; total is how many
// replicas were enumerated. total == 0 or len(reachable) == 0 both mean the
// controller has no activity data and must not make idle transitions.
type ActivityClient interface {
	NamespaceActivity(ctx context.Context, namespace string) (reachable []ReplicaActivity, total int, err error)
}

// activityCacheWindow is the fixed per-namespace cache window: a controller
// constant, not a Helm tunable, well below any practical idleTimeout.
const activityCacheWindow = 15 * time.Second

// GatewayActivityClient is the production ActivityClient: it enumerates
// gateway Pod IPs via the cache, dials each in parallel with the controller's
// client cert (ServerName pinned to the gateway Service DNS so SAN
// verification passes against Pod-IP targets), and caches the merged result
// per namespace for 15 seconds. See
// docs/src/gateways/user/activation-and-activity.md.
type GatewayActivityClient struct {
	Reader            client.Reader
	OperatorNamespace string
	// CertFile/KeyFile/CAFile are the controller's client identity
	// (kaalm-controller-tls) and trust bundle.
	CertFile, KeyFile, CAFile string
	// Port is the gateway cluster listener port (default 8443).
	Port int

	mu     sync.Mutex
	client *http.Client
	cache  map[string]activityCacheEntry
}

type activityCacheEntry struct {
	fetched   time.Time
	reachable []ReplicaActivity
	total     int
}

func (g *GatewayActivityClient) httpClient() (*http.Client, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.client != nil {
		return g.client, nil
	}
	c, err := newGatewayMTLSClient(g.CertFile, g.KeyFile, g.CAFile, g.OperatorNamespace)
	if err != nil {
		return nil, err
	}
	g.client = c
	return g.client, nil
}

// newGatewayMTLSClient builds the controller's client for gateway Pod-IP
// dials: its own certificate as the client identity, the Kaalm CA as trust,
// and SAN verification pinned to the gateway Service DNS. Certificate and
// pool are re-read per connection so rotation applies without a controller
// restart (#149).
func newGatewayMTLSClient(certFile, keyFile, caFile, operatorNamespace string) (*http.Client, error) {
	loader := &tlsutil.CertLoader{CertFile: certFile, KeyFile: keyFile, CAFile: caFile}
	if _, err := loader.Certificate(); err != nil {
		return nil, err
	}
	if _, err := loader.CAPool(); err != nil {
		return nil, err
	}
	// Pod-IP dials: SAN verification runs against the Service DNS.
	serverName := fmt.Sprintf("%s.%s.svc.cluster.local", gatewayServiceName, operatorNamespace)
	return &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{DialTLSContext: loader.DialTLSContext(serverName)},
	}, nil
}

// gatewayFanOut dials every gateway Pod IP in parallel at path (with the
// namespace query attached) and decodes each 200 as a T. total is how many
// replicas were enumerated; reachable holds the ones that answered.
// Unreachable replicas are skipped, never failed on.
func gatewayFanOut[T any](
	ctx context.Context, reader client.Reader, operatorNamespace string,
	httpClient *http.Client, port int, path, namespace string,
) (reachable []T, total int, err error) {
	var pods corev1.PodList
	if err := reader.List(ctx, &pods, client.InNamespace(operatorNamespace),
		client.MatchingLabels(gatewayPodLabels)); err != nil {
		return nil, 0, err
	}
	if port == 0 {
		port = gatewayPort
	}
	type result struct {
		replica T
		ok      bool
	}
	var targets []string
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Status.PodIP != "" && p.DeletionTimestamp.IsZero() {
			targets = append(targets, p.Status.PodIP)
		}
	}
	results := make(chan result, len(targets))
	for _, ip := range targets {
		go func(ip string) {
			url := fmt.Sprintf("https://%s:%d%s?namespace=%s", ip, port, path, namespace)
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				results <- result{}
				return
			}
			resp, err := httpClient.Do(req)
			if err != nil {
				results <- result{}
				return
			}
			defer func() { _ = resp.Body.Close() }()
			var replica T
			if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&replica) != nil {
				results <- result{}
				return
			}
			results <- result{replica: replica, ok: true}
		}(ip)
	}
	for range targets {
		if r := <-results; r.ok {
			reachable = append(reachable, r.replica)
		}
	}
	return reachable, len(targets), nil
}

// NamespaceActivity fans out to every gateway Pod IP, skipping unreachable
// replicas, with a 15-second per-namespace cache in front.
func (g *GatewayActivityClient) NamespaceActivity(ctx context.Context, namespace string) ([]ReplicaActivity, int, error) {
	g.mu.Lock()
	if g.cache == nil {
		g.cache = map[string]activityCacheEntry{}
	}
	if entry, ok := g.cache[namespace]; ok && time.Since(entry.fetched) < activityCacheWindow {
		g.mu.Unlock()
		return entry.reachable, entry.total, nil
	}
	g.mu.Unlock()

	httpClient, err := g.httpClient()
	if err != nil {
		return nil, 0, err
	}
	reachable, total, err := gatewayFanOut[ReplicaActivity](ctx, g.Reader, g.OperatorNamespace, httpClient, g.Port,
		"/v1/activity", namespace)
	if err != nil {
		return nil, 0, err
	}

	g.mu.Lock()
	g.cache[namespace] = activityCacheEntry{fetched: time.Now(), reachable: reachable, total: total}
	g.mu.Unlock()
	return reachable, total, nil
}

// mergedActivity merges one agent's timestamps across replicas (most recent
// per source), then applies the activitySource filter. The order is
// load-bearing: merge first, filter second; the controller owns the policy.
func mergedActivity(reachable []ReplicaActivity, agentName, source string) *time.Time {
	var traffic, heartbeat *time.Time
	newer := func(current, candidate *time.Time) *time.Time {
		if candidate == nil {
			return current
		}
		if current == nil || candidate.After(*current) {
			return candidate
		}
		return current
	}
	for _, replica := range reachable {
		if a, ok := replica.Agents[agentName]; ok {
			traffic = newer(traffic, a.GatewayTraffic)
			heartbeat = newer(heartbeat, a.Heartbeat)
		}
	}
	switch source {
	case "agentHeartbeat":
		return heartbeat
	case "both":
		return newer(traffic, heartbeat)
	default: // gatewayTraffic
		return traffic
	}
}
