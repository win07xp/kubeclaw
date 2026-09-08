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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	kaalmv1beta1 "github.com/win07xp/kaalm/api/v1beta1"
)

// The platform adapters (docs/src/gateways/user/platform-adapters.md, The
// platform adapters). A platform adapter turns one platform's HTTP delivery
// into envelopes and turns the agent's reply back into that platform's API
// calls. Both halves are async by construction: the platform's request ends
// at the acknowledgement, and the reply goes out later through the platform
// API at the operator-set base URL.

// platformAdapter is implemented once per platform type.
type platformAdapter interface {
	// Type is the spec.type value and the envelope's channelType.
	Type() string
	// Handle runs the inbound half: handshake, signature check, scope, and
	// the acknowledgement. It writes the platform's response itself and
	// returns the messages to dispatch (none for a handshake, a refusal, or
	// an auth failure).
	Handle(ctx context.Context, w http.ResponseWriter, r *http.Request,
		channel *kaalmv1beta1.AgentChannel, body []byte) inboundResult
	// SendReply delivers text (the agent's content, or an error rendered as
	// text) for one message through the platform API. Returns the callback
	// outcome vocabulary (delivered | rejected | exhausted).
	SendReply(ctx context.Context, channel *kaalmv1beta1.AgentChannel, msg platformMessage, text string) string
}

// platformMessage is one envelope plus the adapter-private context its reply
// needs (an interaction token, a recipient). The context never reaches the
// agent: the envelope carries identifiers only.
type platformMessage struct {
	env   MessageEnvelope
	reply any
}

// inboundResult is what Handle reports back to the shared pipeline.
type inboundResult struct {
	messages []platformMessage
	// authFailed, when non-empty, is recorded on channel health as
	// WebhookAuthFailed with this detail.
	authFailed string
	// rejected counts scope refusals and dropped events for the metric; they
	// are not health observations.
	rejected int
}

// platformPipelineTimeout bounds one message's wake, delivery, and reply,
// the same budget the async webhook pipeline runs under.
const platformPipelineTimeout = 10 * time.Minute

// platformAdapterFor returns the adapter for a platform channel, or false for
// a webhook channel or a type with no adapter yet.
func (s *Server) platformAdapterFor(channel *kaalmv1beta1.AgentChannel) (platformAdapter, bool) {
	switch channel.Spec.Type {
	case kaalmv1beta1.ChannelTypeDiscord:
		if channel.Spec.Discord == nil {
			return nil, false
		}
		return &discordAdapter{s: s}, true
	case kaalmv1beta1.ChannelTypeWhatsApp:
		if channel.Spec.WhatsApp == nil {
			return nil, false
		}
		return &whatsAppAdapter{s: s}, true
	}
	return nil, false
}

// handlePlatform is the platform half of the channel route: the adapter
// answers the platform, then each message runs its own pipeline in the
// background with the platform already gone.
func (s *Server) handlePlatform(
	w http.ResponseWriter, r *http.Request, channel *kaalmv1beta1.AgentChannel, body []byte, adapter platformAdapter,
) {
	res := adapter.Handle(r.Context(), w, r, channel, body)
	path := channel.Spec.Path()
	if res.authFailed != "" {
		s.ChannelHealth.RecordFailure(path, healthReasonAuthFailed, res.authFailed)
	}
	for i := 0; i < res.rejected; i++ {
		s.Metrics.ChannelMessage(adapter.Type(), channel.Namespace, "rejected")
	}
	if len(res.messages) == 0 {
		return
	}
	agent, ok := s.Store.AgentByName(r.Context(), channel.Namespace, channel.Spec.AgentRef.Name)
	if !ok {
		s.ChannelHealth.RecordFailure(path, healthReasonAgentNotReady, "referenced Agent not found")
		agent = nil
	}
	for _, m := range res.messages {
		ctx, endSpan := s.Tracing.Start(s.Tracing.Extract(r.Context(), r.Header), "channel.receive",
			trace.SpanKindServer,
			attribute.String("kaalm.channel_type", m.env.ChannelType),
			attribute.String("kaalm.namespace", channel.Namespace),
			attribute.String("kaalm.agent", channel.Spec.AgentRef.Name),
			attribute.String("kaalm.message_id", m.env.MessageID))
		var agentCopy *kaalmv1beta1.Agent
		if agent != nil {
			agentCopy = agent.DeepCopy()
		}
		go s.runPlatformPipeline(s.Tracing.Detach(ctx), channel.DeepCopy(), agentCopy, adapter, m)
		endSpan(nil)
	}
}

// runPlatformPipeline wakes, delivers, and replies for one message. An error
// anywhere in the pipeline travels back as reply text, because the platform
// has already been answered and the person is waiting in the chat.
func (s *Server) runPlatformPipeline(
	traceCtx context.Context, channel *kaalmv1beta1.AgentChannel, agent *kaalmv1beta1.Agent,
	adapter platformAdapter, m platformMessage,
) {
	ctx, cancel := context.WithTimeout(traceCtx, platformPipelineTimeout)
	defer cancel()

	var text string
	switch agent {
	case nil:
		text = errDeliveryFailed + ": referenced Agent not found"
	default:
		respBody, errType, err := s.wakeAndDeliver(ctx, channel.Spec.Path(), agent, m.env)
		if err != nil {
			if errType == errResponseTooLarge {
				s.Metrics.ResponseTooLarge(agent.Namespace, "async")
			}
			text = errType + ": " + err.Error()
			break
		}
		var reply ResponseEnvelope
		if jsonErr := json.Unmarshal(respBody, &reply); jsonErr == nil && reply.Content != nil {
			text = *reply.Content
		}
	}

	start := time.Now()
	outcome := adapter.SendReply(ctx, channel, m, text)
	s.Metrics.ChannelCallback(channel.Namespace, outcome)
	s.Metrics.ChannelCallbackDuration(channel.Namespace, time.Since(start).Seconds())
}

// ---- reply delivery (shared by every platform adapter) ----

// replyBucket is the three-way classification of one platform API answer.
type replyBucket int

const (
	bucketDelivered replyBucket = iota
	bucketTerminal
	bucketRetried
)

// classifyReplyStatus is the Reply delivery table: 2xx delivered; 400, 401,
// 403, 404, 405, 410, and 415 terminal (the platform validated the request
// and will validate it the same way again); everything else retried.
func classifyReplyStatus(status int) replyBucket {
	switch {
	case status >= 200 && status < 300:
		return bucketDelivered
	case status == http.StatusBadRequest, status == http.StatusUnauthorized, status == http.StatusForbidden,
		status == http.StatusNotFound, status == http.StatusMethodNotAllowed, status == http.StatusGone,
		status == http.StatusUnsupportedMediaType:
		return bucketTerminal
	}
	return bucketRetried
}

// platformClient is the HTTP client platform replies use: the callback trust
// pool (system roots plus whatever the operator added) and the per-attempt
// read timeout. The base URL is operator-set, so no deny-range check applies.
func (s *Server) platformClient() (*http.Client, error) {
	pool, err := s.callbackCAPool()
	if err != nil {
		return nil, err
	}
	return &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}},
		Timeout:   s.Config.AgentReadTimeout,
		// Refused like every outbound leg (#153); the reply schedule treats
		// it as a transport failure and retries, then reports exhaustion.
		CheckRedirect: func(*http.Request, []*http.Request) error { return errNoRedirects },
	}, nil
}

// replyResult is the last answer of one reply request's schedule.
type replyResult struct {
	bucket replyBucket
	status int    // 0 when no attempt was answered
	body   []byte // the last answer's body, capped, for the error message
	err    error  // the last transport error, when nothing was answered
}

// sendPlatformRequest runs one reply request through the bounded schedule
// (gateway.callbackRetryBackoff): a retried answer waits and tries again, a
// delivered or terminal answer returns at once, and exhaustion returns as
// retried with the last answer attached. build is called per attempt so each
// carries a fresh body reader.
func (s *Server) sendPlatformRequest(
	ctx context.Context, build func(ctx context.Context) (*http.Request, error),
	classify func(status int, body []byte) replyBucket,
) replyResult {
	last := replyResult{bucket: bucketRetried}
	backoff := append([]time.Duration{0}, s.Config.CallbackBackoff...)
	for _, delay := range backoff {
		if delay > 0 {
			select {
			case <-ctx.Done():
				last.err = ctx.Err()
				return last
			case <-time.After(delay):
			}
		}
		client, err := s.platformClient()
		if err != nil {
			last.err = err
			continue
		}
		req, err := build(ctx)
		if err != nil {
			return replyResult{bucket: bucketTerminal, err: err}
		}
		resp, err := client.Do(req)
		if err != nil {
			last.err = err
			continue
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		_ = resp.Body.Close()
		last = replyResult{bucket: classify(resp.StatusCode, body), status: resp.StatusCode, body: body}
		if last.bucket != bucketRetried {
			return last
		}
	}
	return last
}

// replyRefused records a terminal or exhausted reply on channel health as
// CallbackRejected, emits the Warning event, and maps it to the callback
// outcome vocabulary.
func (s *Server) replyRefused(channel *kaalmv1beta1.AgentChannel, platform string, res replyResult) string {
	detail := fmt.Sprintf("%s reply refused: HTTP %d %s", platform, res.status, strings.TrimSpace(string(res.body)))
	outcome := callbackRejected
	if res.bucket == bucketRetried {
		outcome = callbackExhausted
		detail = fmt.Sprintf("%s reply exhausted its retries", platform)
		if res.err != nil {
			detail += ": " + res.err.Error()
		} else if res.status != 0 {
			detail += fmt.Sprintf(": last HTTP %d", res.status)
		}
	}
	s.ChannelHealth.RecordFailure(channel.Spec.Path(), healthReasonCallbackRejected, detail)
	s.recordEvent(channel, "CallbackRejected", "%s", detail)
	return outcome
}

// splitChunks splits text into pieces of at most limit characters (runes,
// which is how the platforms count), breaking at the last newline in the
// window when there is one so a long reply reads as continued paragraphs.
func splitChunks(text string, limit int) []string {
	if utf8.RuneCountInString(text) <= limit {
		return []string{text}
	}
	var out []string
	runes := []rune(text)
	for len(runes) > limit {
		cut := limit
		for i := limit - 1; i >= limit/2; i-- {
			if runes[i] == '\n' {
				cut = i + 1
				break
			}
		}
		out = append(out, string(runes[:cut]))
		runes = runes[cut:]
	}
	if len(runes) > 0 {
		out = append(out, string(runes))
	}
	return out
}
