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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/win07xp/kaalm/internal/callbackpolicy"
	"github.com/win07xp/kaalm/internal/tlsutil"
)

// Config carries the gateway's runtime settings.
type Config struct {
	// OperatorNamespace hosts the gateway (kaalm-system); the controller
	// SAN check and the gateway endpoint derivation use it.
	OperatorNamespace string
	// ListenAddr is the cluster listener (default :8443): the LLM proxy, the
	// MCP tool broker, and the internal endpoints.
	ListenAddr string
	// HealthAddr serves /healthz and /readyz over TLS with no client auth on
	// a dedicated port, outside the listener auth profiles.
	HealthAddr string
	// CertFile/KeyFile are the serving cert (kaalm-gateway-tls), reloaded
	// from disk on rotation.
	CertFile string
	KeyFile  string
	// CAFile is the Kaalm CA bundle used for the inbound ClientCAs pool.
	CAFile string
	// MaxBodyBytes caps inbound LLM request bodies (default 4 MiB).
	MaxBodyBytes int64
	// MaxFallbackDepth bounds the total providers attempted per request,
	// including the primary (default 3).
	MaxFallbackDepth int
	// Replicas returns the live gateway replica count for rate-limit
	// division; nil means a single replica.
	Replicas func() int
	// UpstreamTimeout bounds each upstream provider call.
	UpstreamTimeout time.Duration
	// UpstreamCAFiles are upstream trust bundles, merged and added to the system
	// roots and reloaded when any mtime changes so a rotated kaalm-upstream-ca
	// needs no restart. More than one entry lets the cluster CA and an
	// operator-supplied bundle be trusted together. Takes precedence over
	// UpstreamCAs.
	UpstreamCAFiles []string
	// UpstreamCAs is a prebuilt upstream pool, used when UpstreamCAFiles is
	// empty (tests inject one directly).
	UpstreamCAs *x509.CertPool
	// MCPMaxBodyBytes caps brokered MCP request and response bodies; zero
	// falls back to MaxBodyBytes.
	MCPMaxBodyBytes int64
	// MCPUpstreamTimeout bounds each brokered tool call; zero falls back to
	// UpstreamTimeout.
	MCPUpstreamTimeout time.Duration
	// SessionKey is the gateway-shared HMAC key binding MCP session ids to
	// caller identities (docs/src/gateways/tool-plane.md, The Broker).
	SessionKey []byte
	// DisableSourceIPCheck skips the source-IP cross-check (dev/test only).
	DisableSourceIPCheck bool

	// UserListenAddr is the User Gateway listener (default :8080).
	UserListenAddr string
	// MaxMessageBodyBytes caps inbound webhook bodies (default 1 MiB).
	MaxMessageBodyBytes int64
	// MaxResponseBodyBytes caps agent replies (default 900 KiB, headroom
	// under the ~1 MiB ConfigMap object cap).
	MaxResponseBodyBytes int64
	// AgentReadTimeout bounds each delivery attempt (default 10s).
	AgentReadTimeout time.Duration
	// AgentConnectTimeout is the hibernation-detection connect bound (1s).
	AgentConnectTimeout time.Duration
	// SyncDeliveryDeadline bounds sync-mode wall-clock (default 30s).
	SyncDeliveryDeadline time.Duration
	// DeliveryBackoff is the agent-delivery retry schedule (1s, 5s, 25s).
	DeliveryBackoff []time.Duration
	// CallbackBackoff is the callback retry schedule (1s, 5s, 25s).
	CallbackBackoff []time.Duration
	// ChannelHealthWindow is the rolling health window (default 5m).
	ChannelHealthWindow time.Duration
	// AgentServiceHostOverride / AgentServicePortOverride redirect agent
	// delivery dials (dev/test only).
	AgentServiceHostOverride string
	AgentServicePortOverride int32
	// InsecureSkipAgentVerify disables agent cert verification (dev only).
	InsecureSkipAgentVerify bool
	// CallbackCAFiles are callback trust bundles, with the same merge and
	// reload contract as UpstreamCAFiles. Takes precedence over CallbackCAs.
	CallbackCAFiles []string
	// CallbackCAs is a prebuilt callback pool, used when CallbackCAFiles is
	// empty (tests inject one directly).
	CallbackCAs *x509.CertPool
	// CallbackPolicy decides which callbackUrl targets may receive async
	// responses. The zero value denies internal address space; entries come
	// from gateway.callbackUrl.allowlist. See internal/callbackpolicy.
	CallbackPolicy callbackpolicy.Policy
	// DiscordAPIBaseURL and WhatsAppAPIBaseURL are where the platform
	// adapters reply (gateway.platforms.<type>.apiBaseUrl). Operator-set and
	// trusted like a provider endpoint; empty means the platform's default.
	DiscordAPIBaseURL  string
	WhatsAppAPIBaseURL string
}

// Server is the Kaalm Gateway's :8443 surface.
type Server struct {
	Config        Config
	Store         Store
	Auth          *Authenticator
	Spend         SpendRecorder
	Activity      *ActivityStore
	Budget        *BudgetLedger
	ChannelHealth *ChannelHealthStore
	RateLimiter   *RateLimiter
	Metrics       *Metrics
	// Tracing is the OpenTelemetry wiring; nil (the default install) means
	// no spans are created and no trace context is forwarded.
	Tracing *Tracing
	// Recorder emits Kubernetes Events (runtime FallbackIneligible,
	// CredentialsInvalid). A nil recorder no-ops.
	Recorder EventRecorder
	// Activator wakes hibernated Agents via the controller (nil in tests or
	// when the controller identity is not configured).
	Activator ActivatorClient
	// Async persists async webhook response records.
	Async AsyncRecords
	// Completions patches per-task completion mailboxes.
	Completions CompletionWriter

	mcpClientOnce sync.Once
	mcpClient     *http.Client

	upstreamOnce   sync.Once
	upstreamClient *http.Client

	outboundCAOnce sync.Once
	upstreamCAs    *tlsutil.CAPoolLoader
	callbackCAs    *tlsutil.CAPoolLoader

	agentClientOnce   sync.Once
	agentClientLoader *tlsutil.CertLoader
	agentClientErr    error
}

// initOutboundCAs builds the file-backed outbound trust loaders once.
func (s *Server) initOutboundCAs() {
	s.outboundCAOnce.Do(func() {
		if len(s.Config.UpstreamCAFiles) > 0 {
			s.upstreamCAs = &tlsutil.CAPoolLoader{Files: s.Config.UpstreamCAFiles, Additive: true}
		}
		if len(s.Config.CallbackCAFiles) > 0 {
			s.callbackCAs = &tlsutil.CAPoolLoader{Files: s.Config.CallbackCAFiles, Additive: true}
		}
	})
}

// upstreamCAPool returns the current upstream trust pool, re-reading the bundle
// when it has rotated. Falls back to the injected pool, then to nil (system
// roots only).
func (s *Server) upstreamCAPool() (*x509.CertPool, error) {
	s.initOutboundCAs()
	if s.upstreamCAs != nil {
		return s.upstreamCAs.Load()
	}
	return s.Config.UpstreamCAs, nil
}

// callbackCAPool is upstreamCAPool's counterpart for channel callbackUrl TLS.
func (s *Server) callbackCAPool() (*x509.CertPool, error) {
	s.initOutboundCAs()
	if s.callbackCAs != nil {
		return s.callbackCAs.Load()
	}
	return s.Config.CallbackCAs, nil
}

// NewServer wires a Server from its parts, applying defaults.
func NewServer(cfg Config, store Store, tokens *TokenAuthenticator, spend SpendRecorder) *Server {
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":8443"
	}
	if cfg.HealthAddr == "" {
		cfg.HealthAddr = ":8081"
	}
	if cfg.MaxBodyBytes == 0 {
		cfg.MaxBodyBytes = 4 << 20
	}
	if cfg.UpstreamTimeout == 0 {
		cfg.UpstreamTimeout = 120 * time.Second
	}
	if cfg.UserListenAddr == "" {
		cfg.UserListenAddr = ":8080"
	}
	if cfg.MaxMessageBodyBytes == 0 {
		cfg.MaxMessageBodyBytes = 1 << 20
	}
	if cfg.MaxResponseBodyBytes == 0 {
		cfg.MaxResponseBodyBytes = 900 << 10
	}
	if cfg.AgentReadTimeout == 0 {
		cfg.AgentReadTimeout = 10 * time.Second
	}
	if cfg.AgentConnectTimeout == 0 {
		cfg.AgentConnectTimeout = time.Second
	}
	if cfg.SyncDeliveryDeadline == 0 {
		cfg.SyncDeliveryDeadline = 30 * time.Second
	}
	if cfg.DeliveryBackoff == nil {
		cfg.DeliveryBackoff = []time.Duration{time.Second, 5 * time.Second, 25 * time.Second}
	}
	if cfg.CallbackBackoff == nil {
		cfg.CallbackBackoff = []time.Duration{time.Second, 5 * time.Second, 25 * time.Second}
	}
	if cfg.MaxFallbackDepth == 0 {
		cfg.MaxFallbackDepth = 3
	}
	s := &Server{
		Config: cfg,
		Store:  store,
		Auth: &Authenticator{
			Store: store, Tokens: tokens,
			OperatorNamespace:    cfg.OperatorNamespace,
			DisableSourceIPCheck: cfg.DisableSourceIPCheck,
		},
		Spend:         spend,
		Activity:      NewActivityStore(),
		Budget:        NewBudgetLedger(),
		ChannelHealth: NewChannelHealthStore(cfg.ChannelHealthWindow),
		RateLimiter:   NewRateLimiter(cfg.Replicas),
	}
	// The effective boundary margin extrapolates observed traffic across the
	// live replica count (hard budget enforcement).
	s.Budget.SetReplicas(cfg.Replicas)
	return s
}

// Handler builds the :8443 mux with the per-path auth regimes. The mapping
// mirrors the listener profile table in docs/src/gateways/overview.md.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// LLM proxy paths: dual-mode (mTLS SAN or bearer token).
	mux.HandleFunc("/v1/messages", s.Auth.DualModePaths(s.handleLLMProxy))
	mux.HandleFunc("/v1/chat/completions", s.Auth.DualModePaths(s.handleLLMProxy))
	mux.HandleFunc("/v1/completions", s.Auth.DualModePaths(s.handleLLMProxy))
	// The tool plane's broker surface, on the shared dual-mode
	// caller-identity profile (docs/src/gateways/tool-plane.md, The Broker).
	mux.HandleFunc("/v1/mcp/", s.Auth.DualModePaths(s.handleMCPBroker))

	// Agent-report paths: mTLS-only, kind split at the handler. The
	// task-complete body lands with the user-gateway phase.
	mux.HandleFunc("/v1/agent/heartbeat", s.Auth.AgentReportPaths(KindAgent, s.handleHeartbeat))
	mux.HandleFunc("/v1/task/complete", s.Auth.AgentReportPaths(KindAgentTask, s.handleTaskComplete))

	// Controller-only paths: controller SAN required.
	mux.HandleFunc("/v1/activity", s.Auth.ControllerPaths(s.handleActivity))
	mux.HandleFunc("/v1/channels/health", s.Auth.ControllerPaths(s.handleChannelsHealth))

	// Console-only paths: console SAN required
	// (docs/src/gateways/api/internal-endpoints.md).
	mux.HandleFunc("/v1/test-chat", s.Auth.ConsolePaths(s.handleTestChat))
	mux.HandleFunc("/v1/spend", s.Auth.ConsolePaths(s.handleSpend))

	// Anything else on the cluster listener is an unrecognized path.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		badRequest(w, "unrecognized path "+r.URL.Path)
	})
	return mux
}

// TLSConfig builds the listener TLS configuration: VerifyClientCertIfGiven so
// bearer-token callers complete the handshake, with the serving cert and CA
// pool reloaded from disk on rotation (kubelet swaps the projected volume).
func (s *Server) TLSConfig() (*tls.Config, error) {
	loader := &tlsutil.CertLoader{CertFile: s.Config.CertFile, KeyFile: s.Config.KeyFile, CAFile: s.Config.CAFile}
	if _, err := loader.Certificate(); err != nil {
		return nil, err
	}
	pool, err := loader.CAPool()
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		ClientAuth: tls.VerifyClientCertIfGiven,
		ClientCAs:  pool,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return loader.Certificate()
		},
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			// Rebuild ClientCAs when the CA bundle rotates: a CA change must
			// refresh the inbound trust pool, not only the serving cert.
			pool, err := loader.CAPool()
			if err != nil {
				return nil, err
			}
			return &tls.Config{
				MinVersion: tls.VersionTLS12,
				ClientAuth: tls.VerifyClientCertIfGiven,
				ClientCAs:  pool,
				GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
					return loader.Certificate()
				},
			}, nil
		},
	}, nil
}

// Run serves the cluster listener and the health port until ctx is cancelled.
func (s *Server) Run(ctx context.Context) error {
	tlsCfg, err := s.TLSConfig()
	if err != nil {
		return err
	}
	main := &http.Server{
		Addr: s.Config.ListenAddr, Handler: s.Handler(), TLSConfig: tlsCfg,
		ReadHeaderTimeout: 10 * time.Second,
	}
	healthMux := http.NewServeMux()
	ok := func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }
	healthMux.HandleFunc("/healthz", ok)
	healthMux.HandleFunc("/readyz", ok)
	health := &http.Server{
		Addr: s.Config.HealthAddr, Handler: healthMux,
		TLSConfig:         &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: tlsCfg.GetCertificate},
		ReadHeaderTimeout: 10 * time.Second,
	}

	user := &http.Server{
		Addr: s.Config.UserListenAddr, Handler: s.UserHandler(),
		TLSConfig:         &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: tlsCfg.GetCertificate},
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 3)
	go func() { errCh <- main.ListenAndServeTLS("", "") }()
	go func() { errCh <- health.ListenAndServeTLS("", "") }()
	go func() { errCh <- user.ListenAndServeTLS("", "") }()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = main.Shutdown(shutdownCtx)
		_ = health.Shutdown(shutdownCtx)
		_ = user.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// mcpHTTPClient shares the provider-facing transport (same trust pool and
// rotation behavior) but never follows redirects, and leaves timing to the
// per-call request context.
func (s *Server) mcpHTTPClient() *http.Client {
	s.mcpClientOnce.Do(func() {
		s.mcpClient = &http.Client{
			Transport: s.upstream().Transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return errNoRedirects
			},
		}
	})
	return s.mcpClient
}

func (s *Server) mcpUpstreamTimeout() time.Duration {
	if s.Config.MCPUpstreamTimeout > 0 {
		return s.Config.MCPUpstreamTimeout
	}
	return s.Config.UpstreamTimeout
}

// upstream returns the shared provider-facing HTTP client.
func (s *Server) upstream() *http.Client {
	s.upstreamOnce.Do(func() {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		if len(s.Config.UpstreamCAFiles) > 0 || s.Config.UpstreamCAs != nil {
			// Build the TLS config per dial rather than once, so a rotated
			// bundle is picked up without a restart. Pooled connections keep
			// their existing config until they are recycled; new connections
			// verify against the current pool.
			transport.TLSClientConfig = nil
			transport.DialTLSContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
				pool, err := s.upstreamCAPool()
				if err != nil {
					return nil, err
				}
				host, _, splitErr := net.SplitHostPort(addr)
				if splitErr != nil {
					host = addr
				}
				dialer := &tls.Dialer{Config: &tls.Config{
					MinVersion: tls.VersionTLS12, RootCAs: pool, ServerName: host,
				}}
				return dialer.DialContext(ctx, network, addr)
			}
		}
		s.upstreamClient = &http.Client{
			Timeout: s.Config.UpstreamTimeout, Transport: transport,
			// A refused redirect surfaces as a connect-class failure, so the
			// fallback walk continues past it (#153).
			CheckRedirect: func(*http.Request, []*http.Request) error { return errNoRedirects },
		}
	})
	return s.upstreamClient
}

// The rotation-aware certificate and CA-bundle loaders live in
// internal/tlsutil, shared with the console.
