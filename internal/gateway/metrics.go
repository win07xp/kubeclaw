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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	kaalmv1beta1 "github.com/win07xp/kaalm/api/v1beta1"
)

// Shared Prometheus label names.
const (
	labelModel     = "model"
	labelProvider  = "provider"
	labelStatus    = "status"
	labelNamespace = "namespace"
	labelTool      = "tool"
	labelPeriod    = "period"
)

// Metrics is the gateway's Prometheus catalog (docs/src/operations/observability.md).
// No metric carries per-Agent or per-AgentTask identity: that resolution lives
// in logs and Events to keep cardinality bounded at 1000+ agents. A nil
// *Metrics no-ops every method, so tests need no registry.
type Metrics struct {
	llmRequests     *prometheus.CounterVec
	llmDuration     *prometheus.HistogramVec
	llmTokens       *prometheus.CounterVec
	llmSpend        *prometheus.CounterVec
	llmFallback     *prometheus.CounterVec
	budgetThreshld  *prometheus.CounterVec
	budgetBoundary  *prometheus.CounterVec
	llmServerTools  *prometheus.CounterVec
	toolCalls       *prometheus.CounterVec
	toolDuration    *prometheus.HistogramVec
	channelMsgs     *prometheus.CounterVec
	channelMsgDur   *prometheus.HistogramVec
	channelWake     *prometheus.CounterVec
	channelWakeDur  *prometheus.HistogramVec
	channelCB       *prometheus.CounterVec
	channelDelivery *prometheus.CounterVec
	channelCBDur    *prometheus.HistogramVec
	tooLarge        *prometheus.CounterVec
	patchFailed     *prometheus.CounterVec
}

// NewMetrics registers the gateway catalog with the given registerer.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)
	return &Metrics{
		llmRequests: f.NewCounterVec(prometheus.CounterOpts{
			Name: "kaalm_llm_requests_total", Help: "LLM proxy requests by outcome.",
		}, []string{labelProvider, labelModel, labelNamespace, labelStatus}),
		llmDuration: f.NewHistogramVec(prometheus.HistogramOpts{
			Name: "kaalm_llm_request_duration_seconds", Help: "LLM proxy request duration.",
		}, []string{labelProvider, labelModel}),
		llmTokens: f.NewCounterVec(prometheus.CounterOpts{
			Name: "kaalm_llm_tokens_total", Help: "Token usage by direction.",
		}, []string{labelProvider, labelModel, labelNamespace, "direction"}),
		llmSpend: f.NewCounterVec(prometheus.CounterOpts{
			Name: "kaalm_llm_spend_usd_total", Help: "Accumulated LLM spend in USD.",
		}, []string{labelProvider, labelNamespace}),
		llmFallback: f.NewCounterVec(prometheus.CounterOpts{
			Name: "kaalm_llm_fallback_total", Help: "Fallback attempts by reason.",
		}, []string{"from_provider", "to_provider", "reason"}),
		budgetThreshld: f.NewCounterVec(prometheus.CounterOpts{
			Name: "kaalm_budget_threshold_events_total", Help: "Budget threshold actions fired.",
		}, []string{labelProvider, labelNamespace, "action"}),
		budgetBoundary: f.NewCounterVec(prometheus.CounterOpts{
			Name: "kaalm_llm_budget_boundary_events_total", Help: "Hard-enforcement boundary events.",
		}, []string{labelProvider, labelNamespace, "event"}),
		llmServerTools: f.NewCounterVec(prometheus.CounterOpts{
			Name: "kaalm_llm_server_tool_use_total", Help: "Provider-side tool calls extracted from response usage.",
		}, []string{labelProvider, labelNamespace, labelTool}),
		toolCalls: f.NewCounterVec(prometheus.CounterOpts{
			Name: "kaalm_tool_calls_total", Help: "Brokered MCP calls by outcome.",
		}, []string{labelProvider, labelNamespace, labelTool, labelStatus}),
		toolDuration: f.NewHistogramVec(prometheus.HistogramOpts{
			Name: "kaalm_tool_call_duration_seconds", Help: "Brokered MCP call duration, forwarded calls only.",
		}, []string{labelProvider, labelTool}),
		channelMsgs: f.NewCounterVec(prometheus.CounterOpts{
			Name: "kaalm_channel_messages_total", Help: "Channel messages by outcome.",
		}, []string{"channel_type", labelNamespace, labelStatus}),
		channelMsgDur: f.NewHistogramVec(prometheus.HistogramOpts{
			Name: "kaalm_channel_message_duration_seconds", Help: "Channel message delivery duration, wake included.",
		}, []string{"channel_type"}),
		channelWake: f.NewCounterVec(prometheus.CounterOpts{
			Name: "kaalm_channel_wake_total", Help: "Wake-on-demand triggers.",
		}, []string{labelNamespace}),
		channelWakeDur: f.NewHistogramVec(prometheus.HistogramOpts{
			Name: "kaalm_channel_wake_duration_seconds", Help: "Wake duration from activation to ready or failure.",
		}, []string{labelNamespace, "result"}),
		channelCB: f.NewCounterVec(prometheus.CounterOpts{
			Name: "kaalm_channel_callback_total", Help: "Async callback attempts by outcome.",
		}, []string{labelNamespace, labelStatus}),
		channelDelivery: f.NewCounterVec(prometheus.CounterOpts{
			Name: "kaalm_channel_delivery_attempts_total", Help: "Agent delivery attempts by outcome.",
		}, []string{labelNamespace, "outcome"}),
		channelCBDur: f.NewHistogramVec(prometheus.HistogramOpts{
			Name: "kaalm_channel_callback_duration_seconds", Help: "Async callback delivery effort duration.",
		}, []string{labelNamespace}),
		tooLarge: f.NewCounterVec(prometheus.CounterOpts{
			Name: "kaalm_channel_response_too_large_total", Help: "Oversized agent responses.",
		}, []string{labelNamespace, "mode"}),
		patchFailed: f.NewCounterVec(prometheus.CounterOpts{
			Name: "kaalm_channel_async_patch_failed_total", Help: "Async response patch exhaustions (v1 silent-loss).",
		}, []string{labelNamespace}),
	}
}

// LLMRequest counts one proxied request by outcome (ok | error | rate_limited).
func (m *Metrics) LLMRequest(provider, model, namespace, status string) {
	if m == nil {
		return
	}
	m.llmRequests.WithLabelValues(provider, model, namespace, status).Inc()
}

// Duration records an LLM request's wall-clock.
func (m *Metrics) Duration(provider, model string, seconds float64) {
	if m == nil {
		return
	}
	m.llmDuration.WithLabelValues(provider, model).Observe(seconds)
}

// Tokens counts input and output tokens.
func (m *Metrics) Tokens(provider, model, namespace string, usage Usage) {
	if m == nil {
		return
	}
	m.llmTokens.WithLabelValues(provider, model, namespace, "input").Add(float64(usage.InputTokens))
	m.llmTokens.WithLabelValues(provider, model, namespace, "output").Add(float64(usage.OutputTokens))
}

// Spend accumulates USD spend.
func (m *Metrics) Spend(provider, namespace string, usd float64) {
	if m == nil || usd == 0 {
		return
	}
	m.llmSpend.WithLabelValues(provider, namespace).Add(usd)
}

// ServerToolUse counts provider-side tool calls extracted from a response.
func (m *Metrics) ServerToolUse(provider, namespace, tool string, count int64) {
	if m == nil || count <= 0 {
		return
	}
	m.llmServerTools.WithLabelValues(provider, namespace, tool).Add(float64(count))
}

// ToolCall counts one brokered MCP call. status is "ok", a wire error type,
// or "upstream_error"; tool is already bounded by the declared catalog
// (see docs/src/gateways/tool-plane.md, Audit and Metering).
func (m *Metrics) ToolCall(provider, namespace, tool, status string) {
	if m == nil {
		return
	}
	m.toolCalls.WithLabelValues(provider, namespace, tool, status).Inc()
}

// ToolCallDuration records a forwarded brokered call's wall-clock.
func (m *Metrics) ToolCallDuration(provider, tool string, seconds float64) {
	if m == nil {
		return
	}
	m.toolDuration.WithLabelValues(provider, tool).Observe(seconds)
}

// Fallback counts one fallback attempt.
func (m *Metrics) Fallback(from, to, reason string) {
	if m == nil {
		return
	}
	m.llmFallback.WithLabelValues(from, to, reason).Inc()
}

// BudgetThreshold counts one budget policy action.
func (m *Metrics) BudgetThreshold(provider, namespace, action string) {
	if m == nil {
		return
	}
	m.budgetThreshld.WithLabelValues(provider, namespace, action).Inc()
}

// BudgetBoundary counts one hard-enforcement boundary event
// (engaged | throttled | fail_closed | margin_raised).
func (m *Metrics) BudgetBoundary(provider, namespace, event string) {
	if m == nil {
		return
	}
	m.budgetBoundary.WithLabelValues(provider, namespace, event).Inc()
}

// ChannelMessage counts one message delivery by outcome: "delivered" or the
// sync error type. channelType is the envelope's channelType ("webhook" for
// channel deliveries, "console" for test-chat).
func (m *Metrics) ChannelMessage(channelType, namespace, status string) {
	if m == nil {
		return
	}
	m.channelMsgs.WithLabelValues(channelType, namespace, status).Inc()
}

// ChannelMessageDuration records one delivery pipeline's wall-clock,
// including any wake.
func (m *Metrics) ChannelMessageDuration(channelType string, seconds float64) {
	if m == nil {
		return
	}
	m.channelMsgDur.WithLabelValues(channelType).Observe(seconds)
}

// ChannelWake counts one wake trigger.
func (m *Metrics) ChannelWake(namespace string) {
	if m == nil {
		return
	}
	m.channelWake.WithLabelValues(namespace).Inc()
}

// ChannelWakeDuration records one wake's wall-clock by result
// (ready | controller_unavailable | wake_timeout).
func (m *Metrics) ChannelWakeDuration(namespace, result string, seconds float64) {
	if m == nil {
		return
	}
	m.channelWakeDur.WithLabelValues(namespace, result).Observe(seconds)
}

// ChannelDeliveryAttempt counts one POST /v1/message attempt by outcome:
// "ok" or the failure class from deliveryOutcome.
func (m *Metrics) ChannelDeliveryAttempt(namespace, outcome string) {
	if m == nil {
		return
	}
	m.channelDelivery.WithLabelValues(namespace, outcome).Inc()
}

// ChannelCallback counts one callback delivery effort by outcome.
func (m *Metrics) ChannelCallback(namespace, status string) {
	if m == nil {
		return
	}
	m.channelCB.WithLabelValues(namespace, status).Inc()
}

// ChannelCallbackDuration records one callback delivery effort's wall-clock
// across its whole retry schedule.
func (m *Metrics) ChannelCallbackDuration(namespace string, seconds float64) {
	if m == nil {
		return
	}
	m.channelCBDur.WithLabelValues(namespace).Observe(seconds)
}

// ResponseTooLarge counts one oversized reply.
func (m *Metrics) ResponseTooLarge(namespace, mode string) {
	if m == nil {
		return
	}
	m.tooLarge.WithLabelValues(namespace, mode).Inc()
}

// AsyncPatchFailed counts one dropped async payload.
func (m *Metrics) AsyncPatchFailed(namespace string) {
	if m == nil {
		return
	}
	m.patchFailed.WithLabelValues(namespace).Inc()
}

var _ = kaalmv1beta1.GroupVersion
