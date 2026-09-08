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
	"context"
	"crypto/x509"
	"net"
	"net/http"
)

// caller is the authenticated identity attached to a request after the
// per-path middleware has run.
type caller struct {
	Namespace string
	// Workload is set only in Mode 1 (mTLS); gateway-only callers carry a
	// namespace but no workload identity.
	Workload *Identity
}

type callerKey struct{}

func callerFrom(ctx context.Context) *caller {
	c, _ := ctx.Value(callerKey{}).(*caller)
	return c
}

// Authenticator implements the per-path client-auth regimes on the single
// VerifyClientCertIfGiven socket. The TLS handshake is deliberately the
// weakest link; every real decision lives here. A routing bug in this file
// would let agent-cert holders reach controller-only paths, so this mapping
// is the most security-load-bearing detail in the gateway.
// See docs/src/gateways/llm/listener-tls.md.
type Authenticator struct {
	Store             Store
	Tokens            *TokenAuthenticator
	OperatorNamespace string
	// DisableSourceIPCheck skips the source-IP-to-Pod cross-check. Dev/test
	// only: the check is defense in depth and must stay on in-cluster.
	DisableSourceIPCheck bool
}

// peerCert returns the verified leaf client certificate, if one was presented.
func peerCert(r *http.Request) *x509.Certificate {
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return nil
	}
	return r.TLS.PeerCertificates[0]
}

func sourceIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// crossCheck confirms the Pod at the request's source IP is in the
// authenticated namespace. It runs after authentication, never instead of it.
func (a *Authenticator) crossCheck(r *http.Request, namespace string) bool {
	if a.DisableSourceIPCheck {
		return true
	}
	pod, ok := a.Store.PodByIP(r.Context(), sourceIP(r))
	if !ok {
		return false
	}
	return pod.Namespace == namespace
}

// crossCheckLive is crossCheck with the task-complete fallback: an informer
// miss falls back to a live List narrowed to the SAN-derived namespace, so a
// Pod completing inside the new-Pod informer-lag window is not rejected with
// a terminal 401 (workload-identity.md, Informer-lag fallback; #148). The
// spec scopes the fallback to /v1/task/complete alone: heartbeats are
// periodic and recover on the next tick, and giving them the fallback would
// turn an informer resync into a fleet-wide live-List stampede.
func (a *Authenticator) crossCheckLive(r *http.Request, namespace string) bool {
	if a.DisableSourceIPCheck {
		return true
	}
	ip := sourceIP(r)
	pod, ok := a.Store.PodByIP(r.Context(), ip)
	if !ok {
		pod, ok = a.Store.PodByIPLive(r.Context(), namespace, ip)
	}
	if !ok {
		return false
	}
	return pod.Namespace == namespace
}

// DualModePaths authenticates the caller-identity profile shared by the LLM
// proxy and the MCP broker: Mode 1 when a client cert is present (any bearer
// header is ignored; mTLS wins), Mode 2 (TokenReview bearer, the gateway-only
// tier) when not. The profile establishes only WHO is calling; each plane's
// authorization (the provider chain, the tool grant chain) lives with its
// handler, and nothing plane-specific may be added here.
func (a *Authenticator) DualModePaths(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if cert := peerCert(r); cert != nil {
			id, err := ParseWorkloadSAN(cert)
			if err != nil {
				forbidden(w, errInvalidCert, err.Error())
				return
			}
			if !a.crossCheck(r, id.Namespace) {
				unauthorized(w, "source IP does not match the authenticated namespace")
				return
			}
			next(w, r.WithContext(context.WithValue(r.Context(), callerKey{},
				&caller{Namespace: id.Namespace, Workload: &id})))
			return
		}

		token, ok := bearerToken(r.Header.Get("Authorization"))
		if !ok {
			unauthorized(w, "no client certificate or bearer token presented")
			return
		}

		// Pod-ownership precheck, uncached and BEFORE any token validation:
		// Kaalm-managed Pods must use mTLS and cannot fall back to their
		// ServiceAccount token as a second credential.
		if !a.DisableSourceIPCheck {
			if pod, found := a.Store.PodByIP(r.Context(), sourceIP(r)); found && isKaalmManagedPod(pod) {
				unauthorized(w, "Kaalm-managed Pods must authenticate with mTLS")
				return
			}
		}

		ns, ok, err := a.Tokens.Authenticate(r.Context(), token)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable,
				errorBody{Type: errInternalUnavailable, Message: "TokenReview unavailable", Retryable: true}, 1)
			return
		}
		if !ok {
			unauthorized(w, "token rejected")
			return
		}
		if !a.crossCheck(r, ns) {
			unauthorized(w, "source IP does not match the authenticated namespace")
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), callerKey{}, &caller{Namespace: ns})))
	}
}

// AgentReportPaths authenticates the mTLS-only report endpoints
// (/v1/agent/heartbeat, /v1/task/complete). There is no bearer fallback; the
// Agent-vs-AgentTask split is enforced by requiredKind at the handler level.
// AgentTask callers (task-complete) get the live cross-check fallback; Agent
// callers (heartbeat) stay informer-only per the spec (see crossCheckLive).
func (a *Authenticator) AgentReportPaths(requiredKind WorkloadKind, next http.HandlerFunc) http.HandlerFunc {
	crossCheck := a.crossCheck
	if requiredKind == KindAgentTask {
		crossCheck = a.crossCheckLive
	}
	return func(w http.ResponseWriter, r *http.Request) {
		cert := peerCert(r)
		if cert == nil {
			unauthorized(w, "client certificate required")
			return
		}
		id, err := ParseWorkloadSAN(cert)
		if err != nil {
			forbidden(w, errInvalidCert, err.Error())
			return
		}
		if id.Kind != requiredKind {
			forbidden(w, errAccessDenied, string(id.Kind)+" callers are not accepted on this path")
			return
		}
		if !crossCheck(r, id.Namespace) {
			unauthorized(w, "source IP does not match the authenticated namespace")
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), callerKey{},
			&caller{Namespace: id.Namespace, Workload: &id})))
	}
}

// ControllerPaths authenticates the controller-only endpoints (/v1/activity,
// /v1/channels/health): a client cert whose SAN matches the controller
// Service DNS. Agent and AgentTask certs are valid CA-signed certs but their
// SANs do not match, so they are rejected 403.
func (a *Authenticator) ControllerPaths(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cert := peerCert(r)
		if cert == nil {
			unauthorized(w, "client certificate required")
			return
		}
		if !IsControllerCert(cert, a.OperatorNamespace) {
			forbidden(w, errAccessDenied, "this path requires the controller identity")
			return
		}
		next(w, r)
	}
}

// ConsolePaths authenticates the console-only endpoint (/v1/test-chat): a
// client cert whose SAN matches the console Service DNS. The gateway does not
// re-authorize the human behind the request; the console runs TokenReview and
// SubjectAccessReview before calling, and possession of the console SAN
// carries that authorization (docs/src/security/rbac.md, Internal Endpoint
// Authentication).
func (a *Authenticator) ConsolePaths(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cert := peerCert(r)
		if cert == nil {
			unauthorized(w, "client certificate required")
			return
		}
		if !IsConsoleCert(cert, a.OperatorNamespace) {
			forbidden(w, errAccessDenied, "this path requires the console identity")
			return
		}
		next(w, r)
	}
}
