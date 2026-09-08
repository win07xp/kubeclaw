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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kaalmv1beta1 "github.com/win07xp/kaalm/api/v1beta1"
	"github.com/win07xp/kaalm/internal/tlsutil"
)

// ActivatorServer serves the controller's :9443 endpoints: kubelet probes
// (cert-less) and POST /v1/activate/{namespace}/{agentName} (gateway SAN
// required). It runs on EVERY controller replica, not only the leader: the
// handler is deliberately thin, patching kaalm.io/wake=true on the target
// Agent so the leader's existing watch drives the actual wake. See
// docs/src/gateways/user/activation-and-activity.md (The Activator).
type ActivatorServer struct {
	Client            client.Client
	OperatorNamespace string
	Addr              string
	CertFile          string
	KeyFile           string
	CAFile            string
}

// NeedLeaderElection returns false: the activator runs on every replica.
func (s *ActivatorServer) NeedLeaderElection() bool { return false }

// Start serves until ctx is cancelled. It satisfies manager.Runnable.
func (s *ActivatorServer) Start(ctx context.Context) error {
	// The serving cert is re-read per handshake and ClientCAs rebuilt per
	// connection, so cert-manager rotation applies without a restart (#149).
	// Probes present no cert; the activate handler enforces per-path.
	loader := &tlsutil.CertLoader{CertFile: s.CertFile, KeyFile: s.KeyFile, CAFile: s.CAFile}
	tlsCfg, err := loader.ServerMTLSConfig(tls.VerifyClientCertIfGiven)
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	ok := func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }
	mux.HandleFunc("/healthz", ok)
	mux.HandleFunc("/readyz", ok)
	mux.HandleFunc("/v1/activate/", s.handleActivate)

	server := &http.Server{
		Addr:              s.Addr,
		Handler:           mux,
		TLSConfig:         tlsCfg,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() { errCh <- server.ListenAndServeTLS("", "") }()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// handleActivate authorizes the gateway SAN and patches the wake annotation.
// It does no lifecycle work itself; the apiserver is the message bus to the
// leader's reconciler.
func (s *ActivatorServer) handleActivate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		http.Error(w, "client certificate required", http.StatusUnauthorized)
		return
	}
	if !s.isGatewayCert(r.TLS.PeerCertificates[0]) {
		http.Error(w, "this path requires the gateway identity", http.StatusForbidden)
		return
	}

	rest := strings.TrimPrefix(r.URL.Path, "/v1/activate/")
	parts := strings.Split(rest, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		http.Error(w, "path must be /v1/activate/{namespace}/{agentName}", http.StatusBadRequest)
		return
	}
	namespace, name := parts[0], parts[1]

	var agent kaalmv1beta1.Agent
	if err := s.Client.Get(r.Context(), types.NamespacedName{Namespace: namespace, Name: name}, &agent); err != nil {
		if apierrors.IsNotFound(err) {
			http.Error(w, "agent not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	patch := fmt.Appendf(nil, `{"metadata":{"annotations":{%q:%q}}}`, kaalmv1beta1.AnnotationWake, kaalmv1beta1.AnnotationTrue)
	if err := s.Client.Patch(r.Context(), &agent, client.RawPatch(types.MergePatchType, patch)); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// 202 confirms only that the annotation was written; the wake itself is
	// watch-driven on the leader.
	w.WriteHeader(http.StatusAccepted)
}

func (s *ActivatorServer) isGatewayCert(cert *x509.Certificate) bool {
	long := fmt.Sprintf("%s.%s.svc.cluster.local", gatewayServiceName, s.OperatorNamespace)
	short := fmt.Sprintf("%s.%s.svc", gatewayServiceName, s.OperatorNamespace)
	for _, san := range cert.DNSNames {
		if san == long || san == short {
			return true
		}
	}
	return false
}
