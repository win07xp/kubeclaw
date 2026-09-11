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
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	kaalmv1beta1 "github.com/win07xp/kaalm/api/v1beta1"
	"github.com/win07xp/kaalm/internal/tlsutil"
)

// ActivatorClient asks the controller to wake a hibernated Agent. Injected so
// tests need no controller.
type ActivatorClient interface {
	Wake(ctx context.Context, namespace, name string) error
}

// ControllerActivator is the production ActivatorClient: POST
// /v1/activate/{ns}/{name} on the controller's :9443 over mTLS.
type ControllerActivator struct {
	// BaseURL is the controller activator base, e.g.
	// https://kaalm-controller.kaalm-system.svc.cluster.local:9443.
	BaseURL string
	Client  *http.Client
}

// Wake posts the activation request. Any non-202 is an error.
func (c *ControllerActivator) Wake(ctx context.Context, namespace, name string) error {
	url := fmt.Sprintf("%s/v1/activate/%s/%s", strings.TrimSuffix(c.BaseURL, "/"), namespace, name)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return err
	}
	resp, err := c.Client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("activator returned %d", resp.StatusCode)
	}
	return nil
}

// UserHandler builds the :8080 mux: webhook intake under /channels/ and the
// async polling endpoint. Everything else is 401, matching the
// path-not-registered posture (no path-existence leaks).
func (s *Server) UserHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/channels/", s.handleWebhook)
	mux.HandleFunc("/v1/channels/responses/", s.handlePoll)
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		unauthorized(w, "unknown path")
	})
	return mux
}

// handleWebhook is the inbound intake: size cap first (on the raw frame,
// before path resolution, so 413 never leaks which paths exist), then channel
// lookup, auth, normalization, and mode dispatch.
func (s *Server) handleWebhook(w http.ResponseWriter, r *http.Request) {
	// The size check runs on the raw frame before path resolution, so an
	// oversized POST answers 413 whether or not the path exists. A GET has no
	// body; only the WhatsApp verification handshake uses one, and whether a
	// GET is meaningful is the adapter's call after the route is known.
	var body []byte
	if r.Method == http.MethodPost {
		var err error
		body, err = io.ReadAll(http.MaxBytesReader(w, r.Body, s.Config.MaxMessageBodyBytes))
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				writeError(w, http.StatusRequestEntityTooLarge, errorBody{
					Type:    errRequestTooLarge,
					Message: fmt.Sprintf("request body exceeds %d bytes", s.Config.MaxMessageBodyBytes)}, 0)
				return
			}
			badRequest(w, "reading request body: "+err.Error())
			return
		}
	}

	channel, ok := s.Store.ChannelByPath(r.Context(), r.URL.Path)
	if !ok {
		unauthorized(w, "auth failed or path not registered")
		return
	}
	// Write gate: a Terminating channel accepts no new work, which is what
	// makes the delete-time finalizer sweep race-free.
	if channel.Status.Phase == kaalmv1beta1.ChannelTerminating {
		unauthorized(w, "auth failed or path not registered")
		return
	}

	// Platform channels (since v0.7.0) hand the whole inbound half to their
	// adapter; the webhook path below is the generic receiver.
	if adapter, ok := s.platformAdapterFor(channel); ok {
		s.handlePlatform(w, r, channel, body, adapter)
		return
	}
	if r.Method != http.MethodPost || channel.Spec.Webhook == nil {
		unauthorized(w, "unknown path")
		return
	}

	if !s.authenticateWebhook(r.Context(), channel, r, body) {
		s.ChannelHealth.RecordFailure(channel.Spec.Path(), healthReasonAuthFailed,
			"webhook auth validation failed: 401 Unauthorized")
		unauthorized(w, "auth failed or path not registered")
		return
	}

	env, err := normalize(channel, r, body)
	if err != nil {
		badRequest(w, err.Error())
		return
	}

	agent, ok := s.Store.AgentByName(r.Context(), channel.Namespace, channel.Spec.AgentRef.Name)
	if !ok {
		s.ChannelHealth.RecordFailure(channel.Spec.Path(), healthReasonAgentNotReady,
			"referenced Agent not found")
		writeError(w, http.StatusBadGateway, errorBody{
			Type: errDeliveryFailed, Message: "referenced Agent not found"}, 0)
		return
	}

	// channel.receive covers the synchronous half of handling (through the
	// 202 for async mode; the delivery span it parents stays connected
	// either way). Root unless the caller sent W3C trace context.
	ctx, endSpan := s.Tracing.Start(s.Tracing.Extract(r.Context(), r.Header), "channel.receive",
		trace.SpanKindServer,
		attribute.String("kaalm.channel_type", env.ChannelType),
		attribute.String("kaalm.namespace", channel.Namespace),
		attribute.String("kaalm.agent", channel.Spec.AgentRef.Name),
		attribute.String("kaalm.message_id", env.MessageID))
	defer endSpan(nil)
	r = r.WithContext(ctx)

	if channel.Spec.Webhook.ResponseMode == "async" {
		s.handleAsyncAccept(w, r, channel, agent, env)
		return
	}
	s.handleSyncDelivery(w, ctx, channel, agent, env)
}

// handleSyncDelivery runs the wake-then-deliver pipeline with the caller
// attached, bounded by syncDeliveryDeadline.
func (s *Server) handleSyncDelivery(
	w http.ResponseWriter, ctx context.Context,
	channel *kaalmv1beta1.AgentChannel, agent *kaalmv1beta1.Agent, env MessageEnvelope,
) {
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(s.Config.SyncDeliveryDeadline))
	defer cancel()

	respBody, errType, err := s.wakeAndDeliver(ctx, channel.Spec.Path(), agent, env)
	if err != nil {
		spanError(ctx, errType)
		s.writeSyncError(w, ctx, agent.Namespace, errType, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(respBody)
}

// writeSyncError maps a wakeAndDeliver failure onto the sync-mode HTTP
// contract, shared by the sync webhook path and test-chat.
func (s *Server) writeSyncError(w http.ResponseWriter, ctx context.Context, namespace, errType string, err error) {
	if ctx.Err() != nil {
		// The sync wall-clock budget fired before the pipeline settled.
		writeError(w, http.StatusGatewayTimeout, errorBody{
			Type: errSyncDeadline, Retryable: true,
			Message: fmt.Sprintf("sync-mode wall-clock exceeded %s", s.Config.SyncDeliveryDeadline)}, 0)
		return
	}
	switch errType {
	case errControllerDown:
		writeError(w, http.StatusGatewayTimeout, errorBody{
			Type: errType, Retryable: true,
			Message: "controller activator endpoint unreachable; wake could not be triggered"}, 5)
	case errWakeTimeout:
		writeError(w, http.StatusGatewayTimeout, errorBody{
			Type: errType, Message: err.Error()}, 0)
	case errResponseTooLarge:
		s.Metrics.ResponseTooLarge(namespace, "sync")
		writeError(w, http.StatusRequestEntityTooLarge, errorBody{
			Type: errType, Message: err.Error()}, 0)
	default:
		writeError(w, http.StatusBadGateway, errorBody{
			Type: errDeliveryFailed, Message: err.Error()}, 0)
	}
}

// wakeAndDeliver wakes a hibernated agent when needed, then runs the bounded
// delivery pipeline. Returns the raw agent response body on success.
// healthPath is the channel webhook path for channel-health recording; the
// test-chat path passes "" so /console/... traffic never enters a channel
// health report. The delivery outcome and duration are metered here for
// every caller (sync, async, test-chat).
func (s *Server) wakeAndDeliver(
	ctx context.Context, healthPath string,
	agent *kaalmv1beta1.Agent, env MessageEnvelope,
) (respBody []byte, errType string, err error) {
	start := time.Now()
	defer func() {
		status := "delivered"
		if errType != "" {
			status = errType
		}
		s.Metrics.ChannelMessage(env.ChannelType, agent.Namespace, status)
		s.Metrics.ChannelMessageDuration(env.ChannelType, time.Since(start).Seconds())
	}()

	if agent.Status.Phase == kaalmv1beta1.AgentHibernated {
		if s.Activator == nil {
			s.recordChannelFailure(healthPath, healthReasonAgentNotReady,
				"agent hibernated and no activator configured")
			return nil, errControllerDown, fmt.Errorf("no activator configured")
		}
		s.Metrics.ChannelWake(agent.Namespace)
		wakeStart := time.Now()
		if err := s.Activator.Wake(ctx, agent.Namespace, agent.Name); err != nil {
			s.Metrics.ChannelWakeDuration(agent.Namespace, "controller_unavailable", time.Since(wakeStart).Seconds())
			s.recordChannelFailure(healthPath, healthReasonAgentNotReady,
				"activator unreachable: "+err.Error())
			return nil, errControllerDown, err
		}
		if err := s.waitAgentReachable(ctx, agent); err != nil {
			s.Metrics.ChannelWakeDuration(agent.Namespace, "wake_timeout", time.Since(wakeStart).Seconds())
			return nil, errWakeTimeout, fmt.Errorf(
				"agent did not become ready within wakeTimeout (%s)", s.wakeTimeout(agent))
		}
		s.Metrics.ChannelWakeDuration(agent.Namespace, "ready", time.Since(wakeStart).Seconds())
	}
	dctx, endDeliver := s.Tracing.Start(ctx, "agent.deliver", trace.SpanKindClient,
		attribute.String("kaalm.namespace", agent.Namespace),
		attribute.String("kaalm.agent", agent.Name),
		attribute.String("kaalm.message_id", env.MessageID))
	respBody, err = s.deliverToAgent(dctx, agent, env)
	endDeliver(err)
	if err != nil {
		if strings.Contains(err.Error(), "response body exceeded") {
			return nil, errResponseTooLarge, err
		}
		s.recordChannelFailure(healthPath, healthReasonDispatchFailed, err.Error())
		return nil, errDeliveryFailed, err
	}
	// A successful delivery is agent traffic for idle detection: the channel
	// half of the gatewayTraffic contract on GET /v1/activity.
	if s.Activity != nil {
		s.Activity.RecordTraffic(agent.Namespace, agent.Name)
	}
	if healthPath != "" {
		s.ChannelHealth.RecordSuccess(healthPath)
	}
	return respBody, "", nil
}

// recordChannelFailure records a channel-health failure unless the delivery
// has no backing channel (test-chat).
func (s *Server) recordChannelFailure(healthPath, reason, message string) {
	if healthPath == "" {
		return
	}
	s.ChannelHealth.RecordFailure(healthPath, reason, message)
}

func (s *Server) wakeTimeout(agent *kaalmv1beta1.Agent) time.Duration {
	if d := agent.Spec.Lifecycle.WakeTimeout.Duration; d > 0 {
		return d
	}
	return 120 * time.Second
}

// waitAgentReachable polls the agent Service with TCP connects until it
// accepts, bounded by wakeTimeout. Connect failure is the whole hibernation
// detection mechanism; connect success is the readiness signal.
func (s *Server) waitAgentReachable(ctx context.Context, agent *kaalmv1beta1.Agent) error {
	deadline := time.Now().Add(s.wakeTimeout(agent))
	addr := net.JoinHostPort(s.agentServiceHost(agent), fmt.Sprintf("%d", s.agentServicePort(agent)))
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		conn, err := net.DialTimeout("tcp", addr, s.Config.AgentConnectTimeout)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return fmt.Errorf("wake timeout")
}

func (s *Server) agentServiceHost(agent *kaalmv1beta1.Agent) string {
	if s.Config.AgentServiceHostOverride != "" {
		return s.Config.AgentServiceHostOverride
	}
	return fmt.Sprintf("%s.%s.svc.cluster.local", agent.Name, agent.Namespace)
}

func (s *Server) agentServicePort(agent *kaalmv1beta1.Agent) int32 {
	if s.Config.AgentServicePortOverride != 0 {
		return s.Config.AgentServicePortOverride
	}
	if agent.Spec.Service != nil && agent.Spec.Service.Port != 0 {
		return agent.Spec.Service.Port
	}
	return 8080
}

// deliverToAgent runs the bounded agent-delivery pipeline: POST /v1/message
// over mTLS, retried on the 1s/5s/25s schedule (4 attempts total), each
// attempt bounded by agentReadTimeout. The same messageId is reused across
// attempts; agents deduplicate. A 200 with a malformed envelope (missing or
// non-string content) counts as a failed attempt.
func (s *Server) deliverToAgent(
	ctx context.Context, agent *kaalmv1beta1.Agent, env MessageEnvelope,
) ([]byte, error) {
	payload, err := json.Marshal(env)
	if err != nil {
		return nil, err
	}
	url := fmt.Sprintf("https://%s:%d/v1/message", s.agentServiceHost(agent), s.agentServicePort(agent))

	var lastErr error
	backoff := append([]time.Duration{0}, s.Config.DeliveryBackoff...)
	for attempt, delay := range backoff {
		if delay > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}
		respBody, err := s.deliverOnce(ctx, url, agent, payload)
		if err == nil {
			s.Metrics.ChannelDeliveryAttempt(agent.Namespace, deliveryOutcomeOK)
			return respBody, nil
		}
		lastErr = err
		outcome := deliveryOutcome(err)
		s.Metrics.ChannelDeliveryAttempt(agent.Namespace, outcome)
		slog.Warn("agent delivery attempt failed",
			"namespace", agent.Namespace, "agent", agent.Name, "messageId", env.MessageID,
			"attempt", attempt+1, "of", len(backoff), "outcome", outcome, "error", err.Error())
		if outcome == deliveryOutcomeTooLarge {
			return nil, err // size violations are not retried
		}
	}
	return nil, fmt.Errorf("failed to deliver message to agent after %d attempts: %w", len(backoff), lastErr)
}

// Delivery attempt outcomes, the "outcome" label of
// kaalm_channel_delivery_attempts_total. The failure classes name the layer
// that failed so a retry rate can be read back to a cause (#172).
const (
	deliveryOutcomeOK        = "ok"
	deliveryOutcomeDNS       = "dns"       // name resolution failed or timed out
	deliveryOutcomeConnect   = "connect"   // TCP connect refused, reset, or unreachable
	deliveryOutcomeTLS       = "tls"       // handshake or certificate verification
	deliveryOutcomeTimeout   = "timeout"   // the per-attempt deadline elapsed
	deliveryOutcomeStatus    = "status"    // the agent answered outside 2xx
	deliveryOutcomeMalformed = "malformed" // 2xx with an unusable envelope
	deliveryOutcomeTooLarge  = "too_large" // reply over the body cap; not retried
	deliveryOutcomeCanceled  = "canceled"  // the delivery's own context ended
	deliveryOutcomeOther     = "other"
)

// deliveryOutcome classifies one failed attempt. Go's net errors are
// nested (url.Error over net.OpError over the syscall or DNS error), so the
// classes are read from the chain first and from the message last.
func deliveryOutcome(err error) string {
	var dnsErr *net.DNSError
	switch {
	case errors.As(err, &dnsErr):
		return deliveryOutcomeDNS
	case errors.Is(err, context.Canceled):
		return deliveryOutcomeCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return deliveryOutcomeTimeout
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "response body exceeded"):
		return deliveryOutcomeTooLarge
	case strings.Contains(msg, "malformed response envelope"):
		return deliveryOutcomeMalformed
	case strings.HasPrefix(msg, "agent returned "):
		return deliveryOutcomeStatus
	case strings.Contains(msg, "tls:") || strings.Contains(msg, "x509:"):
		return deliveryOutcomeTLS
	case strings.Contains(msg, "connection refused") || strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "no route to host") || strings.Contains(msg, "network is unreachable"):
		return deliveryOutcomeConnect
	case strings.Contains(msg, "i/o timeout") || strings.Contains(msg, "Client.Timeout"):
		return deliveryOutcomeTimeout
	}
	return deliveryOutcomeOther
}

func (s *Server) deliverOnce(ctx context.Context, url string, agent *kaalmv1beta1.Agent, payload []byte) ([]byte, error) {
	attemptCtx, cancel := context.WithTimeout(ctx, s.Config.AgentReadTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(attemptCtx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	// The agent hop: the runtime attaches this context to every gateway
	// call made while handling the message (runtime contract).
	s.Tracing.Inject(attemptCtx, req.Header)
	client, err := s.agentHTTPClient(agent)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("agent returned %d", resp.StatusCode)
	}
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, s.Config.MaxResponseBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(respBody)) > s.Config.MaxResponseBodyBytes {
		return nil, fmt.Errorf("agent response body exceeded %d bytes; externalize large outputs and reference by URL",
			s.Config.MaxResponseBodyBytes)
	}
	var envelope ResponseEnvelope
	if err := json.Unmarshal(respBody, &envelope); err != nil || envelope.Content == nil {
		return nil, fmt.Errorf("agent returned 200 with a malformed response envelope")
	}
	return respBody, nil
}

// agentHTTPClient builds (once) the mTLS client for gateway-to-agent
// delivery: the gateway presents its own cert, verifies the agent's against
// the Kaalm CA, and pins ServerName to the agent's Service DNS.
func (s *Server) agentHTTPClient(agent *kaalmv1beta1.Agent) (*http.Client, error) {
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: fmt.Sprintf("%s.%s.svc.cluster.local", agent.Name, agent.Namespace),
	}
	if s.Config.InsecureSkipAgentVerify {
		tlsCfg.InsecureSkipVerify = true // dev/test only
	}
	// A missing TLS identity (dev/test) sends no client cert; production
	// always configures the gateway cert for the bidirectional mTLS contract.
	if s.Config.CertFile != "" {
		s.agentClientOnce.Do(func() {
			loader := &tlsutil.CertLoader{CertFile: s.Config.CertFile, KeyFile: s.Config.KeyFile, CAFile: s.Config.CAFile}
			if _, err := loader.Certificate(); err != nil {
				s.agentClientErr = err
				return
			}
			if _, err := loader.CAPool(); err != nil {
				s.agentClientErr = err
				return
			}
			s.agentClientLoader = loader
		})
		if s.agentClientErr != nil {
			return nil, s.agentClientErr
		}
		cert, err := s.agentClientLoader.Certificate()
		if err != nil {
			return nil, err
		}
		pool, err := s.agentClientLoader.CAPool()
		if err != nil {
			return nil, err
		}
		tlsCfg.Certificates = []tls.Certificate{*cert}
		tlsCfg.RootCAs = pool
	}
	return &http.Client{Transport: &http.Transport{TLSClientConfig: tlsCfg}}, nil
}

// NewControllerActivator builds the production activator client from the
// gateway's own TLS identity, pinned to the controller Service DNS. Wake
// dials re-read the certificate and trust pool per connection so leaf and CA
// rotation apply without a gateway restart (#149).
func NewControllerActivator(operatorNamespace, certFile, keyFile, caFile string) (*ControllerActivator, error) {
	loader := &tlsutil.CertLoader{CertFile: certFile, KeyFile: keyFile, CAFile: caFile}
	if _, err := loader.Certificate(); err != nil {
		return nil, err
	}
	if _, err := loader.CAPool(); err != nil {
		return nil, err
	}
	host := fmt.Sprintf("kaalm-controller.%s.svc.cluster.local", operatorNamespace)
	return &ControllerActivator{
		BaseURL: fmt.Sprintf("https://%s:9443", host),
		Client: &http.Client{
			Timeout:   10 * time.Second,
			Transport: &http.Transport{DialTLSContext: loader.DialTLSContext(host)},
		},
	}, nil
}
