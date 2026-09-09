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
	"encoding/json"
	"net/http"
	"strings"
)

// Usage is the token spend extracted from a provider response, plus any
// provider-side tool calls its usage surface reported.
type Usage struct {
	InputTokens  int64
	OutputTokens int64
	// ServerTools counts provider-side tool calls by normalized tool id
	// (for example "web_search"). Nil when the response reported none.
	ServerTools map[string]int64
}

// isZero reports whether nothing was extracted. Usage carries a map, so
// struct equality is unavailable; "did we see usage" checks go through here.
func (u Usage) isZero() bool {
	return u.InputTokens == 0 && u.OutputTokens == 0 && len(u.ServerTools) == 0
}

// providerAdapter carries the per-provider knowledge: request-format paths,
// credential header shape, usage extraction (buffered and streamed), and
// streaming request fixups. Anthropic and OpenAI/OpenAI-compatible are the
// served types; google-vertex is reserved (#147) and keeps only its
// outbound adapter pieces.
type providerAdapter interface {
	// formatName identifies the request format for logs and metrics.
	formatName() string
	// injectCredential sets the provider's auth header.
	injectCredential(h http.Header, credential string)
	// extractUsage reads token counts from a buffered JSON response body.
	extractUsage(body []byte) (Usage, bool)
	// accumulateStreamUsage inspects one SSE data payload and folds any usage
	// it carries into u.
	accumulateStreamUsage(data []byte, u *Usage)
	// fixupRequestBody may rewrite the (already model-rewritten) request body
	// map before forwarding, for example injecting stream_options.
	fixupRequestBody(body map[string]any)
	// upstreamPath rewrites the inbound request path for the upstream. Most
	// adapters pass it through; Vertex embeds the model in the path and
	// injects ?alt=sse.
	upstreamPath(inboundPath, modelID string) string
}

// adapterForPath maps a request path to the adapter that registered it.
// Unrecognized paths on the cluster listener are rejected with 400.
func adapterForPath(urlPath string) (providerAdapter, bool) {
	path, _, _ := strings.Cut(urlPath, "?") // strip any query string for matching
	switch path {
	case "/v1/messages":
		return anthropicAdapter{}, true
	case "/v1/chat/completions", "/v1/completions":
		return openaiAdapter{}, true
	}
	// No Vertex-format path is routed: google-vertex is a reserved type in
	// this release (request-handling.md, The google-vertex type is reserved;
	// #147). The mux never routes anything else here either; this guard is
	// for handler-level callers.
	return nil, false
}

// Provider type enum values (ModelProvider.spec.type).
const (
	providerTypeAnthropic        = "anthropic"
	providerTypeOpenAI           = "openai"
	providerTypeOpenAICompatible = "openai-compatible"
	providerTypeVertex           = "google-vertex"
)

// adapterForProviderType returns the adapter that speaks a ModelProvider's
// wire protocol, used for credential injection.
func adapterForProviderType(providerType string) (providerAdapter, bool) {
	switch providerType {
	case providerTypeAnthropic:
		return anthropicAdapter{}, true
	case providerTypeOpenAI, providerTypeOpenAICompatible:
		return openaiAdapter{}, true
	case providerTypeVertex:
		return vertexAdapter{}, true
	}
	return nil, false
}

// ---- Anthropic ----

type anthropicAdapter struct{}

func (anthropicAdapter) formatName() string { return providerTypeAnthropic }

func (anthropicAdapter) injectCredential(h http.Header, credential string) {
	h.Set("x-api-key", credential)
}

func (anthropicAdapter) extractUsage(body []byte) (Usage, bool) {
	var resp struct {
		Usage struct {
			InputTokens   int64           `json:"input_tokens"`
			OutputTokens  int64           `json:"output_tokens"`
			ServerToolUse json.RawMessage `json:"server_tool_use"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return Usage{}, false
	}
	u := Usage{
		InputTokens:  resp.Usage.InputTokens,
		OutputTokens: resp.Usage.OutputTokens,
		ServerTools:  serverToolCounts(resp.Usage.ServerToolUse),
	}
	if u.isZero() {
		return Usage{}, false
	}
	return u, true
}

// serverToolCounts decodes an Anthropic usage.server_tool_use object
// ({"web_search_requests": 1}) into normalized per-tool counts
// ("web_search" -> 1). Decoded separately from the token fields so an
// unexpected value shape loses only the tool counts, never the tokens.
func serverToolCounts(raw json.RawMessage) map[string]int64 {
	var counts map[string]int64
	if len(raw) == 0 || json.Unmarshal(raw, &counts) != nil {
		return nil
	}
	var tools map[string]int64
	for key, n := range counts {
		if n <= 0 {
			continue
		}
		if tools == nil {
			tools = make(map[string]int64, len(counts))
		}
		tools[strings.TrimSuffix(key, "_requests")] = n
	}
	return tools
}

// accumulateStreamUsage: input_tokens arrive on message_start, cumulative
// output_tokens (and cumulative server_tool_use counts) on message_delta.
// message_stop carries no usage.
func (anthropicAdapter) accumulateStreamUsage(data []byte, u *Usage) {
	var evt struct {
		Type    string `json:"type"`
		Message struct {
			Usage struct {
				InputTokens int64 `json:"input_tokens"`
			} `json:"usage"`
		} `json:"message"`
		Usage struct {
			OutputTokens  int64           `json:"output_tokens"`
			ServerToolUse json.RawMessage `json:"server_tool_use"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(data, &evt); err != nil {
		return
	}
	switch evt.Type {
	case "message_start":
		u.InputTokens = evt.Message.Usage.InputTokens
	case "message_delta":
		if evt.Usage.OutputTokens > 0 {
			u.OutputTokens = evt.Usage.OutputTokens
		}
		// Counts are cumulative, so the latest snapshot replaces the last.
		if tools := serverToolCounts(evt.Usage.ServerToolUse); tools != nil {
			u.ServerTools = tools
		}
	}
}

func (anthropicAdapter) fixupRequestBody(map[string]any) {}

func (anthropicAdapter) upstreamPath(inboundPath, _ string) string { return inboundPath }

// ---- OpenAI and OpenAI-compatible ----

type openaiAdapter struct{}

func (openaiAdapter) formatName() string { return "openai" }

func (openaiAdapter) injectCredential(h http.Header, credential string) {
	h.Set("Authorization", "Bearer "+credential)
}

func (openaiAdapter) extractUsage(body []byte) (Usage, bool) {
	var resp struct {
		Usage struct {
			PromptTokens     int64 `json:"prompt_tokens"`
			CompletionTokens int64 `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return Usage{}, false
	}
	if resp.Usage.PromptTokens == 0 && resp.Usage.CompletionTokens == 0 {
		return Usage{}, false
	}
	return Usage{InputTokens: resp.Usage.PromptTokens, OutputTokens: resp.Usage.CompletionTokens}, true
}

// accumulateStreamUsage: a usage object appears in the final chunk preceding
// [DONE], present only when stream_options.include_usage was set (which
// fixupRequestBody guarantees).
func (openaiAdapter) accumulateStreamUsage(data []byte, u *Usage) {
	if bytes.Equal(bytes.TrimSpace(data), []byte("[DONE]")) {
		return
	}
	var chunk struct {
		Usage *struct {
			PromptTokens     int64 `json:"prompt_tokens"`
			CompletionTokens int64 `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(data, &chunk); err != nil || chunk.Usage == nil {
		return
	}
	u.InputTokens = chunk.Usage.PromptTokens
	u.OutputTokens = chunk.Usage.CompletionTokens
}

// fixupRequestBody injects stream_options: {include_usage: true} into
// streaming requests when absent; without it OpenAI-format streams emit no
// usage at all. The extra terminal usage chunk is backward-compatible.
func (openaiAdapter) fixupRequestBody(body map[string]any) {
	stream, _ := body["stream"].(bool)
	if !stream {
		return
	}
	if _, present := body["stream_options"]; !present {
		body["stream_options"] = map[string]any{"include_usage": true}
	}
}

func (openaiAdapter) upstreamPath(inboundPath, _ string) string { return inboundPath }
