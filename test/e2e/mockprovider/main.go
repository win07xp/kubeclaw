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

// Command mockprovider is an e2e test double: a stand-in LLM provider the
// gateway can forward to, plus an async-webhook callback receiver. It is NOT
// part of the product; the chart and release workflow never reference it.
//
// It speaks enough of the OpenAI wire format for the gateway's openai-compatible
// adapter. Behavior is keyed by the request path PREFIX, so several
// ModelProviders can point at one Service by giving each a distinct
// spec.endpoint prefix:
//
//	/ok        (default) -> 200 chat completion with non-zero usage
//	/fail                -> 503 (a fallbackable status, drives fallback tests)
//	/bigusage            -> 200 with large usage (drives budget-exhaustion tests)
//	/slow<ms>            -> 200 after <ms> milliseconds (the load harness's
//	                        realistic-latency provider)
//
// GET .../v1/models returns 200 for probe compatibility. POST /callback records
// async-webhook deliveries; GET /introspect/callbacks returns them for
// assertions. Every chat call is counted by its endpoint prefix and exposed
// at GET /introspect/requests, so specs can assert that a request never
// reached the upstream (S17's hard-budget proof).
package main

import (
	"encoding/json"
	"flag"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// recordedCallback is one async-webhook delivery the mock received.
type recordedCallback struct {
	Path    string              `json:"path"`
	Headers map[string][]string `json:"headers"`
	Body    string              `json:"body"`
}

type mock struct {
	mu        sync.Mutex
	callbacks []recordedCallback
	// requests counts chat calls by endpoint prefix (the path with the
	// /v1/chat/completions suffix stripped; "/" for a bare path).
	requests map[string]int
}

// chatPrefix reduces a chat path to its counting key.
func chatPrefix(path string) string {
	prefix := strings.TrimSuffix(strings.TrimSuffix(path, "/v1/chat/completions"), "/v1/messages")
	if prefix == "" {
		return "/"
	}
	return prefix
}

const mockReplyText = "ok from mock"

// anthropicMessage is the /v1/messages answer: the Anthropic shape with the
// same usage the chat completion carries (S24 proves the crossing both
// ways against one mock).
func anthropicMessage(model string, in, out int64) []byte {
	body, _ := json.Marshal(map[string]any{
		"id": "msg_mock", "type": "message", "role": "assistant", "model": model,
		"content":     []any{map[string]any{"type": "text", "text": mockReplyText}},
		"stop_reason": "end_turn", "stop_sequence": nil,
		"usage": map[string]any{"input_tokens": in, "output_tokens": out},
	})
	return body
}

// streamChat writes a chat-completions SSE stream: two content chunks, the
// finish chunk, a usage chunk when stream_options asked for one, [DONE].
func streamChat(w http.ResponseWriter, model string, in, out int64, includeUsage bool) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	f, _ := w.(http.Flusher)
	chunk := func(v map[string]any) {
		raw, _ := json.Marshal(v)
		_, _ = w.Write(append(append([]byte("data: "), raw...), '\n', '\n'))
		if f != nil {
			f.Flush()
		}
	}
	base := func(delta map[string]any, finish any) map[string]any {
		return map[string]any{"id": "chatcmpl-mock", "object": "chat.completion.chunk", "model": model,
			"choices": []any{map[string]any{"index": 0, "delta": delta, "finish_reason": finish}}}
	}
	chunk(base(map[string]any{"role": "assistant", "content": "ok from"}, nil))
	chunk(base(map[string]any{"content": " mock"}, nil))
	chunk(base(map[string]any{}, "stop"))
	if includeUsage {
		chunk(map[string]any{"id": "chatcmpl-mock", "object": "chat.completion.chunk", "model": model, "choices": []any{},
			"usage": map[string]any{"prompt_tokens": in, "completion_tokens": out, "total_tokens": in + out}})
	}
	_, _ = w.Write([]byte("data: [DONE]\n\n"))
	if f != nil {
		f.Flush()
	}
}

// streamMessages writes an Anthropic messages SSE stream.
func streamMessages(w http.ResponseWriter, model string, in, out int64) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	f, _ := w.(http.Flusher)
	event := func(name string, v map[string]any) {
		raw, _ := json.Marshal(v)
		_, _ = w.Write([]byte("event: " + name + "\ndata: " + string(raw) + "\n\n"))
		if f != nil {
			f.Flush()
		}
	}
	event("message_start", map[string]any{"type": "message_start", "message": map[string]any{
		"id": "msg_mock", "type": "message", "role": "assistant", "model": model, "content": []any{},
		"stop_reason": nil, "usage": map[string]any{"input_tokens": in, "output_tokens": 0}}})
	event("content_block_start", map[string]any{"type": "content_block_start", "index": 0,
		"content_block": map[string]any{"type": "text", "text": ""}})
	event("content_block_delta", map[string]any{"type": "content_block_delta", "index": 0,
		"delta": map[string]any{"type": "text_delta", "text": mockReplyText}})
	event("content_block_stop", map[string]any{"type": "content_block_stop", "index": 0})
	event("message_delta", map[string]any{"type": "message_delta",
		"delta": map[string]any{"stop_reason": "end_turn", "stop_sequence": nil},
		"usage": map[string]any{"output_tokens": out}})
	event("message_stop", map[string]any{"type": "message_stop"})
}

// chatCompletion is the minimal OpenAI-shaped success body. The gateway reads
// only usage.prompt_tokens / usage.completion_tokens; the rest is relayed to the
// caller verbatim.
func chatCompletion(model string, in, out int64) []byte {
	choice := map[string]any{
		"index":         0,
		"finish_reason": "stop",
		"message":       map[string]any{"role": "assistant", "content": "ok from mock"},
	}
	body, _ := json.Marshal(map[string]any{
		"id":      "chatcmpl-mock",
		"object":  "chat.completion",
		"model":   model,
		"choices": []any{choice},
		"usage":   map[string]any{"prompt_tokens": in, "completion_tokens": out, "total_tokens": in + out},
	})
	return body
}

// behaviorFor maps a request path to (status, inputTokens, outputTokens). The
// gateway forwards to {endpoint-prefix}{inbound path}, so the prefix selects
// behavior while the inbound path stays /v1/chat/completions.
func behaviorFor(path string) (status int, in, out int64) {
	switch {
	case strings.HasPrefix(path, "/fail"):
		return http.StatusServiceUnavailable, 0, 0
	case strings.HasPrefix(path, "/bigusage"):
		return http.StatusOK, 5_000_000, 5_000_000
	default: // "/ok", "/slow<ms>", and anything else
		return http.StatusOK, 11, 22
	}
}

// delayFor reads a simulated upstream latency off a "/slow<ms>" prefix, so the
// load harness can measure the gateway against a provider that takes realistic
// time to answer. Any other prefix answers immediately.
func delayFor(path string) time.Duration {
	const prefix = "/slow"
	if !strings.HasPrefix(path, prefix) {
		return 0
	}
	digits := strings.TrimPrefix(path, prefix)
	if i := strings.IndexByte(digits, '/'); i >= 0 {
		digits = digits[:i]
	}
	ms, err := strconv.Atoi(digits)
	if err != nil || ms <= 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

// chat serves both LLM paths: /v1/chat/completions in the OpenAI shape and
// /v1/messages in the Anthropic shape, streaming when the request asks.
func (m *mock) chat(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	if m.requests == nil {
		m.requests = map[string]int{}
	}
	m.requests[chatPrefix(r.URL.Path)]++
	m.mu.Unlock()

	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	var parsed map[string]any
	_ = json.Unmarshal(body, &parsed)
	model, _ := parsed["model"].(string)
	if model == "" {
		model = "mock-model"
	}
	stream, _ := parsed["stream"].(bool)
	anthropic := strings.HasSuffix(r.URL.Path, "/v1/messages")

	if d := delayFor(r.URL.Path); d > 0 {
		time.Sleep(d)
	}
	status, in, out := behaviorFor(r.URL.Path)
	if status != http.StatusOK {
		if anthropic {
			http.Error(w, `{"type":"error","error":{"type":"api_error","message":"mock failure"}}`, status)
			return
		}
		http.Error(w, `{"error":{"message":"mock failure"}}`, status)
		return
	}
	switch {
	case anthropic && stream:
		streamMessages(w, model, in, out)
	case anthropic:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(anthropicMessage(model, in, out))
	case stream:
		so, _ := parsed["stream_options"].(map[string]any)
		include, _ := so["include_usage"].(bool)
		streamChat(w, model, in, out, include)
	default:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(chatCompletion(model, in, out))
	}
}

// models answers the health probe (GET .../v1/models).
func (m *mock) models(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"mock-model","object":"model"}]}`))
}

// callback records an async-webhook delivery (S15) and acks it.
func (m *mock) callback(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	m.mu.Lock()
	m.callbacks = append(m.callbacks, recordedCallback{Path: r.URL.Path, Headers: r.Header, Body: string(body)})
	m.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

// introspect returns the recorded callbacks for test assertions.
func (m *mock) introspect(w http.ResponseWriter, _ *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(m.callbacks)
}

// introspectRequests returns the per-prefix chat call counts.
func (m *mock) introspectRequests(w http.ResponseWriter, _ *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if m.requests == nil {
		_, _ = w.Write([]byte("{}"))
		return
	}
	_ = json.NewEncoder(w).Encode(m.requests)
}

func (m *mock) handler() http.Handler {
	mux := http.NewServeMux()
	// Chat completions and models match on any prefix (the provider endpoint
	// prefix precedes the inbound path).
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case (strings.HasSuffix(r.URL.Path, "/v1/chat/completions") || strings.HasSuffix(r.URL.Path, "/v1/messages")) &&
			r.Method == http.MethodPost:
			m.chat(w, r)
		case strings.HasSuffix(r.URL.Path, "/v1/models") && r.Method == http.MethodGet:
			m.models(w, r)
		default:
			http.NotFound(w, r)
		}
	})
	mux.HandleFunc("/callback", m.callback)
	mux.HandleFunc("/introspect/callbacks", m.introspect)
	mux.HandleFunc("/introspect/requests", m.introspectRequests)
	return mux
}

func main() {
	var (
		addr     = flag.String("addr", ":8443", "HTTPS listen address")
		certFile = flag.String("tls-cert", "/var/run/tls/tls.crt", "server certificate")
		keyFile  = flag.String("tls-key", "/var/run/tls/tls.key", "server key")
	)
	flag.Parse()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	m := &mock{}
	srv := &http.Server{Addr: *addr, Handler: m.handler(), ReadHeaderTimeout: 10 * time.Second}
	logger.Info("mock provider listening", "addr", *addr)
	if err := srv.ListenAndServeTLS(*certFile, *keyFile); err != nil {
		logger.Error("server exited", "error", err)
		os.Exit(1)
	}
}
