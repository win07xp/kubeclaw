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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/win07xp/kaalm/internal/tlsutil"
)

// NewProbeClient builds the HTTP client both health checkers use when the
// chart configures probe CA trust (docs/src/operations/deployment.md,
// controller.trustClusterCAForProbes and controller.probeCA): the given
// bundles are merged into the system roots, mirroring the gateway's
// upstream trust pool. The client carries no Timeout of its own; each probe
// is bounded by its healthCheck.timeoutSeconds context.
func NewProbeClient(caFiles []string) *http.Client {
	return &http.Client{
		Transport: &caReloadingTransport{
			loader: &tlsutil.CAPoolLoader{Files: caFiles, Additive: true},
		},
		// The same refusal the default probe client applies (#153): Go's
		// cross-host header stripping does not cover x-api-key, and a
		// redirecting endpoint is not a healthy one.
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("provider health probes do not follow redirects")
		},
	}
}

// caReloadingTransport rebuilds its inner transport when the probe trust
// pool changes on disk, so a rotated CA needs no controller restart, the
// same contract every other outbound trust pool in the system keeps. The
// loader is mtime-cached, so the per-request cost is a stat per bundle.
type caReloadingTransport struct {
	loader *tlsutil.CAPoolLoader

	mu    sync.Mutex
	pool  *x509.CertPool
	inner *http.Transport
}

func (t *caReloadingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	pool, err := t.loader.Load()
	if err != nil {
		return nil, fmt.Errorf("probe trust pool: %w", err)
	}
	t.mu.Lock()
	if t.inner == nil || pool != t.pool {
		if t.inner != nil {
			t.inner.CloseIdleConnections()
		}
		t.inner = &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}
		t.pool = pool
	}
	inner := t.inner
	t.mu.Unlock()
	return inner.RoundTrip(req)
}
