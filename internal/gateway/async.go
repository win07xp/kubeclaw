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
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	kaalmv1beta1 "github.com/win07xp/kaalm/api/v1beta1"
)

// asyncTTL bounds each polling record's lifetime, fixed in v1.
const asyncTTL = time.Hour

// payloadKey is the ConfigMap data key holding the JSON envelope.
const payloadKey = "payload"

// AsyncRecord is one polling record's state.
type AsyncRecord struct {
	CreatedAt        time.Time
	ChannelNamespace string
	ChannelName      string
	Payload          []byte // nil until patched
}

// AsyncRecords persists async webhook response records
// (kaalm-async-{requestId} ConfigMaps in kaalm-system).
type AsyncRecords interface {
	Create(ctx context.Context, requestID string, channel *kaalmv1beta1.AgentChannel, expires time.Time) error
	Patch(ctx context.Context, requestID string, payload []byte) error
	Get(ctx context.Context, requestID string) (*AsyncRecord, bool, error)
	CountPending(ctx context.Context, channelNamespace, channelName string) (int, error)
}

// KubeAsyncRecords is the production AsyncRecords over the clientset.
type KubeAsyncRecords struct {
	Client            kubernetes.Interface
	OperatorNamespace string
}

func asyncCMName(requestID string) string { return "kaalm-async-" + requestID }

// Create writes the empty placeholder with channel labels and the expiry
// annotation, synchronously before the 202 is returned.
func (k *KubeAsyncRecords) Create(
	ctx context.Context, requestID string, channel *kaalmv1beta1.AgentChannel, expires time.Time,
) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      asyncCMName(requestID),
			Namespace: k.OperatorNamespace,
			Labels: map[string]string{
				kaalmv1beta1.LabelChannelNamespace: channel.Namespace,
				kaalmv1beta1.LabelChannelName:      channel.Name,
			},
			Annotations: map[string]string{
				kaalmv1beta1.AnnotationExpiresAt: expires.UTC().Format(time.RFC3339),
			},
		},
		Data: map[string]string{},
	}
	_, err := k.Client.CoreV1().ConfigMaps(k.OperatorNamespace).Create(ctx, cm, metav1.CreateOptions{})
	return err
}

// Patch adds the payload without resetting the expiry annotation.
func (k *KubeAsyncRecords) Patch(ctx context.Context, requestID string, payload []byte) error {
	patch, err := json.Marshal(map[string]any{"data": map[string]string{payloadKey: string(payload)}})
	if err != nil {
		return err
	}
	_, err = k.Client.CoreV1().ConfigMaps(k.OperatorNamespace).Patch(
		ctx, asyncCMName(requestID), types.MergePatchType, patch, metav1.PatchOptions{})
	return err
}

// Get reads one record.
func (k *KubeAsyncRecords) Get(ctx context.Context, requestID string) (*AsyncRecord, bool, error) {
	cm, err := k.Client.CoreV1().ConfigMaps(k.OperatorNamespace).Get(ctx, asyncCMName(requestID), metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	rec := &AsyncRecord{
		CreatedAt:        cm.CreationTimestamp.Time,
		ChannelNamespace: cm.Labels[kaalmv1beta1.LabelChannelNamespace],
		ChannelName:      cm.Labels[kaalmv1beta1.LabelChannelName],
	}
	if payload, ok := cm.Data[payloadKey]; ok && payload != "" {
		rec.Payload = []byte(payload)
	}
	return rec, true, nil
}

// CountPending counts a channel's live records for maxPendingAsyncResponses.
func (k *KubeAsyncRecords) CountPending(ctx context.Context, channelNamespace, channelName string) (int, error) {
	selector := fmt.Sprintf("%s=%s,%s=%s",
		kaalmv1beta1.LabelChannelNamespace, channelNamespace,
		kaalmv1beta1.LabelChannelName, channelName)
	list, err := k.Client.CoreV1().ConfigMaps(k.OperatorNamespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return 0, err
	}
	return len(list.Items), nil
}

// KubeCompletionWriter patches per-task mailboxes through the clientset (in
// production the per-task Role scopes this to update/patch on the named CM).
type KubeCompletionWriter struct {
	Client kubernetes.Interface
}

// PatchMailbox merges the completion payload into {taskName}-completion.
func (k *KubeCompletionWriter) PatchMailbox(ctx context.Context, namespace, taskName string, data map[string]string) error {
	patch, err := json.Marshal(map[string]any{"data": data})
	if err != nil {
		return err
	}
	_, err = k.Client.CoreV1().ConfigMaps(namespace).Patch(
		ctx, CompletionMailboxName(taskName), types.MergePatchType, patch, metav1.PatchOptions{})
	return err
}

// asyncAcceptResponse is the 202 body.
type asyncAcceptResponse struct {
	RequestID   string `json:"requestId"`
	ChannelPath string `json:"channelPath"`
	Status      string `json:"status"`
	Message     string `json:"message,omitempty"`
}

// handleAsyncAccept implements the 202 contract: pending-cap check,
// placeholder Create (503 on failure: a returned 202 always implies a
// queryable polling record), 202, then the background pipeline.
func (s *Server) handleAsyncAccept(
	w http.ResponseWriter, r *http.Request,
	channel *kaalmv1beta1.AgentChannel, agent *kaalmv1beta1.Agent, env MessageEnvelope,
) {
	maxPending := channel.Spec.Webhook.MaxPendingAsyncResponses
	if maxPending == 0 {
		maxPending = 100
	}
	if count, err := s.Async.CountPending(r.Context(), channel.Namespace, channel.Name); err == nil && count >= int(maxPending) {
		writeError(w, http.StatusServiceUnavailable, errorBody{
			Type: errInternalUnavailable, Retryable: true,
			Message: fmt.Sprintf("channel has %d pending async responses (cap %d)", count, maxPending)}, 5)
		return
	}

	requestID := uuid.NewString()
	if err := s.Async.Create(r.Context(), requestID, channel, time.Now().Add(asyncTTL)); err != nil {
		writeError(w, http.StatusServiceUnavailable, errorBody{
			Type: errInternalUnavailable, Retryable: true,
			Message: "failed to create the polling record"}, 5)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(asyncAcceptResponse{
		RequestID: requestID, ChannelPath: channel.Spec.Path(),
		Status: "accepted", Message: "Message accepted for processing",
	})

	// The caller is now gone; the pipeline continues in the background.
	// v1 limitation: this state is replica-local (no work claim/takeover).
	go s.runAsyncPipeline(s.Tracing.Detach(r.Context()), requestID, channel.DeepCopy(), agent.DeepCopy(), env)
}

// Callback delivery outcomes, the status vocabulary of
// kaalm_channel_callback_total (docs/src/gateways/user/operations.md).
const (
	callbackDelivered = "delivered"
	callbackExhausted = "exhausted"
	callbackRejected  = "rejected"
	callbackInvalid   = "invalid"
)

// runAsyncPipeline executes wake, delivery, and response dispatch after the
// 202. The full retry budget runs without a wall-clock deadline.
func (s *Server) runAsyncPipeline(
	traceCtx context.Context, requestID string,
	channel *kaalmv1beta1.AgentChannel, agent *kaalmv1beta1.Agent, env MessageEnvelope,
) {
	// traceCtx carries only the accept span's identity (Tracing.Detach), so
	// the delivery spans stay connected without the caller's cancellation.
	ctx, cancel := context.WithTimeout(traceCtx, 10*time.Minute)
	defer cancel()

	respBody, errType, err := s.wakeAndDeliver(ctx, channel.Spec.Path(), agent, env)
	var payload []byte
	if err != nil {
		if errType == errResponseTooLarge {
			s.Metrics.ResponseTooLarge(agent.Namespace, "async")
		}
		message := err.Error()
		retryable := errType == errControllerDown
		payload, _ = json.Marshal(map[string]any{
			"requestId": requestID, "channelPath": channel.Spec.Path(),
			"error":    map[string]any{"type": errType, "message": message, "retryable": retryable},
			"failedAt": time.Now().UTC().Format(time.RFC3339),
		})
	} else {
		var response json.RawMessage = respBody
		payload, _ = json.Marshal(map[string]any{
			"requestId": requestID, "channelPath": channel.Spec.Path(),
			"response": response, "completedAt": time.Now().UTC().Format(time.RFC3339),
		})
	}

	if channel.Spec.Webhook.CallbackURL != nil {
		callbackStart := time.Now()
		outcome := s.sendCallback(ctx, channel, requestID, payload)
		s.Metrics.ChannelCallback(channel.Namespace, outcome)
		s.Metrics.ChannelCallbackDuration(channel.Namespace, time.Since(callbackStart).Seconds())
		if outcome == callbackDelivered {
			return // delivered via callback; nothing to store
		}
	}
	s.patchWithRetry(ctx, requestID, channel.Namespace, payload)
}

// patchWithRetry patches the polling record on the shared bounded schedule.
// Exhaustion drops the payload (v1 limitation) with an error log and the
// kaalm_channel_async_patch_failed_total signal.
func (s *Server) patchWithRetry(ctx context.Context, requestID, namespace string, payload []byte) {
	backoff := append([]time.Duration{0}, s.Config.CallbackBackoff...)
	for _, delay := range backoff {
		if delay > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
		}
		if err := s.Async.Patch(ctx, requestID, payload); err == nil {
			return
		}
	}
	s.Metrics.AsyncPatchFailed(namespace)
	slog.Error("async response patch failed; payload dropped", "requestId", requestID)
}

// The pre-dial target check lives in internal/callbackpolicy, shared with the
// AgentChannelReconciler's reconcile-time half of rule 22 so the two cannot
// drift apart.

// sendCallback delivers the payload to callbackUrl with the pre-dial
// deny-range re-check (pinned-IP dial defeats DNS rebinding), per-attempt
// signing with a fresh timestamp, and the retried/terminal/bypassed buckets.
// Returns the callback outcome (delivered | rejected | invalid | exhausted).
func (s *Server) sendCallback(
	ctx context.Context, channel *kaalmv1beta1.AgentChannel, requestID string, payload []byte,
) string {
	cbURL := *channel.Spec.Webhook.CallbackURL
	parsed, err := url.Parse(cbURL)
	if err != nil || parsed.Scheme != "https" {
		s.ChannelHealth.RecordFailure(channel.Spec.Path(), healthReasonCallbackInvalid, "callbackUrl is not https")
		return callbackInvalid
	}
	secret := ""
	if channel.Spec.Webhook.CallbackAuth != nil {
		secret, err = s.channelSecret(ctx, channel.Namespace, channel.Spec.Webhook.CallbackAuth)
		if err != nil {
			s.ChannelHealth.RecordFailure(channel.Spec.Path(), healthReasonCallbackInvalid,
				"callbackAuth secret unavailable: "+err.Error())
			return callbackInvalid
		}
	}

	backoff := append([]time.Duration{0}, s.Config.CallbackBackoff...)
	for _, delay := range backoff {
		if delay > 0 {
			select {
			case <-ctx.Done():
				return callbackExhausted
			case <-time.After(delay):
			}
		}

		// Re-resolve and range-check immediately before every dial; the dial
		// is pinned to the checked IP so an independent re-resolution cannot
		// be rebound to a blocked address.
		host := parsed.Hostname()
		ips, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
		if err != nil || len(ips) == 0 {
			continue // resolution failure: retried bucket
		}
		ip := ips[0]
		if !s.Config.CallbackPolicy.Allowed(host, ip) {
			// Bypassed: no dial, no retry, no callback_invalid envelope; the
			// payload still reaches polling via the caller.
			s.ChannelHealth.RecordFailure(channel.Spec.Path(), healthReasonCallbackInvalid,
				fmt.Sprintf("callbackUrl host resolves to blocked address %s", ip))
			return callbackInvalid
		}

		status, err := s.dialCallbackOnce(ctx, parsed, ip, channel, secret, requestID, payload)
		if err == nil && status >= 200 && status <= 299 {
			// The resolved, pinned address is the one dial-time input no
			// versioned artifact can reproduce; denials already record theirs
			// in the health observation, so successes log it here (#139).
			slog.Info("callback delivered", "requestId", requestID,
				"channel", channel.Spec.Path(), "host", host, "resolvedIP", ip.String())
			return callbackDelivered
		}
		switch status {
		case 401, 403, 404, 405, 410, 415:
			// Terminal: the receiver permanently rejects this POST.
			s.ChannelHealth.RecordFailure(channel.Spec.Path(), healthReasonCallbackRejected,
				fmt.Sprintf("callback receiver returned %d", status))
			return callbackRejected
		}
		// Everything else (connect/TLS errors, timeouts, 408/429/422/5xx)
		// is the retried bucket.
	}
	return callbackExhausted
}

// dialCallbackOnce signs and POSTs one callback attempt against the pinned
// IP, preserving the hostname for Host and TLS SNI.
func (s *Server) dialCallbackOnce(
	ctx context.Context, parsed *url.URL, ip net.IP,
	channel *kaalmv1beta1.AgentChannel, secret, requestID string, payload []byte,
) (int, error) {
	port := parsed.Port()
	if port == "" {
		port = "443"
	}
	pinned := net.JoinHostPort(ip.String(), port)
	// Re-read the trust bundle per attempt so a rotated CA applies without a
	// gateway restart; the transport is already rebuilt for every dial.
	callbackCAs, err := s.callbackCAPool()
	if err != nil {
		return 0, err
	}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, pinned)
		},
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, ServerName: parsed.Hostname(), RootCAs: callbackCAs},
	}
	client := &http.Client{
		Transport: transport, Timeout: s.Config.AgentReadTimeout,
		// The pinned transport already keeps a redirect from leaving the
		// checked host; refusing them outright removes the ambiguity (#153).
		CheckRedirect: func(*http.Request, []*http.Request) error { return errNoRedirects },
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, parsed.String(), bytes.NewReader(payload))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if channel.Spec.Webhook.CallbackAuth != nil {
		signCallback(req, channel.Spec.Webhook.CallbackAuth, secret, requestID, payload, time.Now())
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode, nil
}

// handlePoll serves GET /v1/channels/responses/{requestId}?channelPath=...:
// channel auth, the channel-match assertion before ANY response, read-side
// TTL, and the Retry-After ladder on 202.
func (s *Server) handlePoll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		unauthorized(w, "unknown path")
		return
	}
	requestID := strings.TrimPrefix(r.URL.Path, "/v1/channels/responses/")
	if requestID == "" || strings.Contains(requestID, "/") {
		writeError(w, http.StatusNotFound, errorBody{Type: errInvalidRequest, Message: "not found"}, 0)
		return
	}
	channelPath := r.URL.Query().Get("channelPath")
	if channelPath == "" {
		badRequest(w, "channelPath query parameter is required")
		return
	}

	// An unknown channelPath is an auth failure, not a 404: the endpoint
	// must not reveal which webhook paths (and tenant namespaces) exist.
	channel, ok := s.Store.ChannelByPath(r.Context(), channelPath)
	if !ok {
		unauthorized(w, "auth failed")
		return
	}
	if !s.authenticatePoll(r.Context(), channel, r, requestID) {
		unauthorized(w, "auth failed")
		return
	}

	record, found, err := s.Async.Get(r.Context(), requestID)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable,
			errorBody{Type: errInternalUnavailable, Message: "reading the polling record failed", Retryable: true}, 1)
		return
	}
	if !found {
		http.Error(w, "", http.StatusNotFound)
		return
	}
	// Channel-match assertion: fires on every ConfigMap-present branch, so
	// cross-channel requestId existence is never wire-observable.
	if record.ChannelNamespace != channel.Namespace || record.ChannelName != channel.Name {
		slog.Info("poll channel mismatch", "reason", "ChannelMismatch", "requestId", requestID)
		http.Error(w, "", http.StatusNotFound)
		return
	}
	// Read-side TTL enforcement, independent of reconciler pruning.
	elapsed := time.Since(record.CreatedAt)
	if elapsed > asyncTTL {
		http.Error(w, "", http.StatusNotFound)
		return
	}
	if record.Payload == nil {
		w.Header().Set("Retry-After", fmt.Sprintf("%d", pollRetryAfter(elapsed)))
		w.WriteHeader(http.StatusAccepted)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(record.Payload)
}

// pollRetryAfter is the backoff-aware cadence ladder, computed from elapsed
// time since placeholder creation so it is replica-agnostic.
func pollRetryAfter(elapsed time.Duration) int {
	switch {
	case elapsed < 2*time.Second:
		return 2
	case elapsed < 6*time.Second:
		return 4
	case elapsed < 14*time.Second:
		return 8
	case elapsed < 30*time.Second:
		return 16
	default:
		return 30
	}
}
