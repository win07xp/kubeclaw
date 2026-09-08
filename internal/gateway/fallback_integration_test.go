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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	kaalmv1beta1 "github.com/win07xp/kaalm/api/v1beta1"
)

func metav1ObjectMeta(name string) metav1.ObjectMeta { return metav1.ObjectMeta{Name: name} }

func onceReset() sync.Once { return sync.Once{} }

// recordingRecorder is a no-op EventRecorder for tests that only need the
// gateway to have one wired.
type recordingRecorder struct{}

func (*recordingRecorder) Eventf(runtime.Object, string, string, string, ...any) {}

// addBackupProvider adds a fallback provider named "backup" (openai by
// default) backed by its own upstream, extending the newHarness setup;
// callers wire it onto the primary.
func (h *harness) addBackupProvider(t *testing.T, upstreamFn http.HandlerFunc) {
	t.Helper()
	const name = "backup"
	backendSrv := httptest.NewTLSServer(upstreamFn)
	t.Cleanup(backendSrv.Close)

	// Extend the upstream trust pool with the backup's cert.
	pool := h.server.Config.UpstreamCAs
	if pool == nil {
		pool = x509.NewCertPool()
	}
	pool.AddCert(backendSrv.Certificate())
	h.server.Config.UpstreamCAs = pool
	// Reset the memoized upstream client so it rebuilds with the new pool.
	h.server.upstreamOnce = onceReset()

	h.store.providers[name] = &kaalmv1beta1.ModelProvider{
		ObjectMeta: metav1ObjectMeta(name),
		Spec: kaalmv1beta1.ModelProviderSpec{
			Type:              "openai",
			Endpoint:          backendSrv.URL,
			AllowedNamespaces: []string{"team-*"},
			Models:            []kaalmv1beta1.ModelProviderModel{{ID: "m1"}},
		},
	}
	h.store.creds[name] = "sk-" + name
}

func TestIntegration_FallbackChainWalksToBackup(t *testing.T) {
	// S4: the primary returns 503; the gateway walks to the backup, which
	// succeeds, and the agent sees a 200.
	h := newHarness(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	h.seedRoute()
	h.server.Recorder = &recordingRecorder{}
	h.addBackupProvider(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"from-backup","usage":{"prompt_tokens":3,"completion_tokens":1}}`))
	})
	// Wire the fallback and allow the backup in the workload + class.
	h.store.providers["prov"].Spec.Fallback = []kaalmv1beta1.FallbackReference{{Name: "backup"}}
	h.store.agents["team-a/sup"].Spec.Providers = append(h.store.agents["team-a/sup"].Spec.Providers,
		kaalmv1beta1.AgentProviderReference{ProviderRef: kaalmv1beta1.LocalObjectReference{Name: "backup"}})
	h.store.classes["std"].Spec.AllowedProviders = append(h.store.classes["std"].Spec.AllowedProviders,
		kaalmv1beta1.LocalObjectReference{Name: "backup"})

	cert := agentCert(t, h.ca)
	resp := postJSON(t, h.client(&cert), h.url("/v1/chat/completions"),
		map[string]any{"model": "prov/m1", "messages": []any{}}, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("fallback chain should yield 200 from the backup, got %d", resp.StatusCode)
	}
	// The backup's spend was recorded, not the primary's.
	if u := h.spend.Total("team-a", "backup", "m1"); u.InputTokens != 3 {
		t.Errorf("backup usage not recorded: %+v", u)
	}
}

func TestIntegration_FallbackExhaustionMaps503(t *testing.T) {
	// Both providers fail at the connect layer -> 503 provider_unavailable.
	h := newHarness(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	h.seedRoute()
	// Point both providers at a dead endpoint.
	h.store.providers["prov"].Spec.Endpoint = "https://127.0.0.1:1"
	h.store.providers["prov"].Spec.Fallback = []kaalmv1beta1.FallbackReference{{Name: "backup"}}
	h.store.providers["backup"] = &kaalmv1beta1.ModelProvider{
		ObjectMeta: metav1ObjectMeta("backup"),
		Spec: kaalmv1beta1.ModelProviderSpec{
			Type: "openai", Endpoint: "https://127.0.0.1:1",
			AllowedNamespaces: []string{"team-*"},
			Models:            []kaalmv1beta1.ModelProviderModel{{ID: "m1"}},
		},
	}
	h.store.creds["backup"] = "sk-backup"
	h.store.agents["team-a/sup"].Spec.Providers = append(h.store.agents["team-a/sup"].Spec.Providers,
		kaalmv1beta1.AgentProviderReference{ProviderRef: kaalmv1beta1.LocalObjectReference{Name: "backup"}})
	h.store.classes["std"].Spec.AllowedProviders = append(h.store.classes["std"].Spec.AllowedProviders,
		kaalmv1beta1.LocalObjectReference{Name: "backup"})

	cert := agentCert(t, h.ca)
	resp := postJSON(t, h.client(&cert), h.url("/v1/chat/completions"), map[string]any{"model": "prov/m1"}, nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("all-connect-error exhaustion should be 503, got %d", resp.StatusCode)
	}
	if got := errType(t, resp); got != errProviderUnavailable {
		t.Errorf("error type %q", got)
	}
}

// TestIntegration_UpstreamRedirectIsRefused: the provider client never
// follows a redirect (#153): Go strips Authorization cross-host but not
// x-api-key, so following would re-send an Anthropic-format credential to
// the redirect target. One request reaches the upstream; the refusal
// classifies as a connect failure and exhausts as provider_unavailable.
func TestIntegration_UpstreamRedirectIsRefused(t *testing.T) {
	h := newHarness(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "https://internal.example/steal")
		w.WriteHeader(http.StatusFound)
	})
	h.seedRoute()
	cert := agentCert(t, h.ca)
	resp := postJSON(t, h.client(&cert), h.url("/v1/chat/completions"), map[string]any{"model": "prov/m1"}, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("redirect refusal should exhaust as 503, got %d", resp.StatusCode)
	}
	if got := errType(t, resp); got != errProviderUnavailable {
		t.Errorf("error type %q", got)
	}
	<-h.upreqs // the 302 answer itself
	select {
	case r := <-h.upreqs:
		t.Errorf("redirect was followed: a second upstream request arrived: %+v", r.header)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestIntegration_RateLimit429(t *testing.T) {
	h := newHarness(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	})
	h.seedRoute()
	h.server.RateLimiter = NewRateLimiter(func() int { return 1 })
	h.store.providers["prov"].Spec.RateLimits = kaalmv1beta1.ModelProviderRateLimits{RequestsPerMinute: 2}
	cert := agentCert(t, h.ca)
	call := func() int {
		resp := postJSON(t, h.client(&cert), h.url("/v1/chat/completions"), map[string]any{"model": "prov/m1"}, nil)
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	// The ceiling of 2 lets two through, then 429.
	if first := call(); first != 200 {
		t.Fatalf("first call = %d, want 200", first)
	}
	if second := call(); second != 200 {
		t.Fatalf("second call = %d, want 200", second)
	}
	resp := postJSON(t, h.client(&cert), h.url("/v1/chat/completions"), map[string]any{"model": "prov/m1"}, nil)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("third call should be 429, got %d", resp.StatusCode)
	}
	if got := errType(t, resp); got != "rate_limited" {
		t.Errorf("error type %q", got)
	}
}

// ---- crossing formats (since v0.7.0) ----

// crossingHarness: an anthropic primary at the dead default upstream and an
// openai backup at its own upstream, joined by an edge with a model map.
// The caller speaks Anthropic; the backup speaks chat completions.
func crossingHarness(t *testing.T, backupFn http.HandlerFunc) (*harness, *tls.Certificate) {
	t.Helper()
	h := newHarness(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	h.seedRoute()
	h.server.Recorder = &recordingRecorder{}
	h.store.providers["prov"].Spec.Type = "anthropic"
	h.addBackupProvider(t, backupFn)
	h.store.providers["backup"].Spec.Models = []kaalmv1beta1.ModelProviderModel{{ID: "gpt-5-mini"}}
	h.store.providers["prov"].Spec.Fallback = []kaalmv1beta1.FallbackReference{{
		Name: "backup", ModelMap: map[string]string{"m1": "gpt-5-mini"}}}
	h.store.agents["team-a/sup"].Spec.Providers = append(h.store.agents["team-a/sup"].Spec.Providers,
		kaalmv1beta1.AgentProviderReference{ProviderRef: kaalmv1beta1.LocalObjectReference{Name: "backup"}})
	h.store.classes["std"].Spec.AllowedProviders = append(h.store.classes["std"].Spec.AllowedProviders,
		kaalmv1beta1.LocalObjectReference{Name: "backup"})
	cert := agentCert(t, h.ca)
	return h, &cert
}

func TestIntegration_CrossFormatFallback_AnthropicCallerOpenAIBackup(t *testing.T) {
	var got struct {
		path string
		body map[string]any
	}
	h, cert := crossingHarness(t, func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		got.path = r.URL.Path
		_ = json.Unmarshal(raw, &got.body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"chatcmpl-9","object":"chat.completion","model":"gpt-5-mini",
		  "choices":[{"index":0,"message":{"role":"assistant","content":"Cold."},"finish_reason":"stop"}],
		  "usage":{"prompt_tokens":7,"completion_tokens":2,"total_tokens":9}}`))
	})
	resp := postJSON(t, h.client(cert), h.url("/v1/messages"), map[string]any{
		"model": "prov/m1", "max_tokens": 64,
		"system":   "Be brief.",
		"messages": []any{map[string]any{"role": "user", "content": "Is it cold?"}},
	}, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("crossing fallback should yield 200, got %d", resp.StatusCode)
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	// The caller sees an Anthropic message naming the model that served.
	if out["type"] != "message" || out["model"] != "gpt-5-mini" || out["stop_reason"] != "end_turn" {
		t.Errorf("response not Anthropic-shaped: %v", out)
	}
	if content, _ := out["content"].([]any); len(content) != 1 || content[0].(map[string]any)["text"] != "Cold." {
		t.Errorf("content wrong: %v", out["content"])
	}
	// The backup received a chat completion on its own path, for the mapped
	// model, with the system prompt lifted and the stream_options fixup.
	if got.path != "/v1/chat/completions" || got.body["model"] != "gpt-5-mini" {
		t.Errorf("backup got %s %v", got.path, got.body["model"])
	}
	msgs, _ := got.body["messages"].([]any)
	if len(msgs) != 2 || msgs[0].(map[string]any)["role"] != "system" || msgs[1].(map[string]any)["content"] != "Is it cold?" {
		t.Errorf("backup messages wrong: %v", msgs)
	}
	// Spend landed on the backup for the mapped model, read with its adapter.
	if u := h.spend.Total("team-a", "backup", "gpt-5-mini"); u.InputTokens != 7 || u.OutputTokens != 2 {
		t.Errorf("backup usage not recorded for the mapped model: %+v", u)
	}
	if u := h.spend.Total("team-a", "prov", "m1"); !u.isZero() {
		t.Errorf("primary must record nothing: %+v", u)
	}
}

func TestIntegration_CrossFormatFallback_Streaming(t *testing.T) {
	h, cert := crossingHarness(t, func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		if body["stream"] != true {
			t.Errorf("backup did not receive stream: true: %v", body)
		}
		if so, _ := body["stream_options"].(map[string]any); so["include_usage"] != true {
			t.Errorf("the openai fixup must apply to the translated body: %v", body["stream_options"])
		}
		w.Header().Set("Content-Type", "text/event-stream")
		f, _ := w.(http.Flusher)
		for _, line := range []string{
			`data: {"id":"c","object":"chat.completion.chunk","model":"gpt-5-mini","choices":[{"index":0,"delta":{"role":"assistant","content":"Co"},"finish_reason":null}]}`,
			`data: {"choices":[{"index":0,"delta":{"content":"ld."},"finish_reason":null}]}`,
			`data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			`data: {"choices":[],"usage":{"prompt_tokens":5,"completion_tokens":3}}`,
			`data: [DONE]`,
		} {
			_, _ = w.Write([]byte(line + "\n\n"))
			if f != nil {
				f.Flush()
			}
		}
	})
	resp := postJSON(t, h.client(cert), h.url("/v1/messages"), map[string]any{
		"model": "prov/m1", "max_tokens": 64, "stream": true,
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
	}, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("status %d content-type %s", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	raw, _ := io.ReadAll(resp.Body)
	text := string(raw)
	for _, want := range []string{"event: message_start", `"text":"Co"`, `"text":"ld."`,
		"event: message_delta", `"stop_reason":"end_turn"`, `"output_tokens":3`, "event: message_stop"} {
		if !strings.Contains(text, want) {
			t.Errorf("translated stream lacks %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "[DONE]") || strings.Contains(text, "chat.completion.chunk") {
		t.Errorf("OpenAI chunk shapes leaked to an Anthropic caller:\n%s", text)
	}
	// Usage was read from the upstream's own final chunk.
	waitForSpend(t, h, "backup", "gpt-5-mini", 5)
}

func waitForSpend(t *testing.T, h *harness, provider, model string, input int64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if u := h.spend.Total("team-a", provider, model); u.InputTokens == input {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("spend for %s/%s never reached %d input tokens: %+v", provider, model, input, h.spend.Total("team-a", provider, model))
}

func TestIntegration_CrossFormatFallback_OpenAICallerAnthropicBackup(t *testing.T) {
	var gotBody map[string]any
	h := newHarness(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	h.seedRoute() // prov is openai; the caller speaks chat completions
	h.server.Recorder = &recordingRecorder{}
	h.addBackupProvider(t, func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotBody)
		if r.URL.Path != "/v1/messages" {
			t.Errorf("anthropic backup got path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"msg_9","type":"message","role":"assistant","model":"claude-sonnet-4-6",
		  "content":[{"type":"text","text":"Warm."}],"stop_reason":"end_turn","usage":{"input_tokens":4,"output_tokens":1}}`))
	})
	max := int64(4096)
	h.store.providers["backup"].Spec.Type = "anthropic"
	h.store.providers["backup"].Spec.Models = []kaalmv1beta1.ModelProviderModel{{ID: "claude-sonnet-4-6", MaxOutputTokens: &max}}
	h.store.providers["prov"].Spec.Fallback = []kaalmv1beta1.FallbackReference{{
		Name: "backup", ModelMap: map[string]string{"m1": "claude-sonnet-4-6"}}}
	h.store.agents["team-a/sup"].Spec.Providers = append(h.store.agents["team-a/sup"].Spec.Providers,
		kaalmv1beta1.AgentProviderReference{ProviderRef: kaalmv1beta1.LocalObjectReference{Name: "backup"}})
	h.store.classes["std"].Spec.AllowedProviders = append(h.store.classes["std"].Spec.AllowedProviders,
		kaalmv1beta1.LocalObjectReference{Name: "backup"})
	cert := agentCert(t, h.ca)

	// No max_tokens in the request: the catalog's ceiling is supplied.
	resp := postJSON(t, h.client(&cert), h.url("/v1/chat/completions"), map[string]any{
		"model": "prov/m1", "messages": []any{map[string]any{"role": "user", "content": "hi"}},
	}, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if out["object"] != "chat.completion" || out["model"] != "claude-sonnet-4-6" {
		t.Errorf("response not a chat completion: %v", out)
	}
	choice := out["choices"].([]any)[0].(map[string]any)
	if choice["message"].(map[string]any)["content"] != "Warm." || choice["finish_reason"] != "stop" {
		t.Errorf("choice wrong: %v", choice)
	}
	if gotBody["max_tokens"] != float64(4096) || gotBody["model"] != "claude-sonnet-4-6" {
		t.Errorf("backup request wrong: %v", gotBody)
	}
	if u := h.spend.Total("team-a", "backup", "claude-sonnet-4-6"); u.InputTokens != 4 {
		t.Errorf("spend wrong: %+v", u)
	}
}

func TestIntegration_CrossFormatFallback_UntranslatableSkipsTheCandidate(t *testing.T) {
	backupHit := false
	h, cert := crossingHarness(t, func(w http.ResponseWriter, _ *http.Request) {
		backupHit = true
		w.WriteHeader(200)
	})
	resp := postJSON(t, h.client(cert), h.url("/v1/messages"), map[string]any{
		"model": "prov/m1", "max_tokens": 64,
		"thinking": map[string]any{"type": "enabled", "budget_tokens": 1024},
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
	}, nil)
	defer func() { _ = resp.Body.Close() }()
	// The primary's 503 was the only attempt: the crossing candidate was
	// ineligible for this request, so the walk exhausted.
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("status %d, want 502 provider_error", resp.StatusCode)
	}
	if backupHit {
		t.Error("an untranslatable request must never reach the crossing candidate")
	}
}

func TestIntegration_CrossFormatFallback_ErrorEnvelopeIsTheCallers(t *testing.T) {
	h, cert := crossingHarness(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"message":"context too long","type":"invalid_request_error","code":null}}`))
	})
	resp := postJSON(t, h.client(cert), h.url("/v1/messages"), map[string]any{
		"model": "prov/m1", "max_tokens": 64, "messages": []any{map[string]any{"role": "user", "content": "hi"}},
	}, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 400 {
		t.Fatalf("status %d, want the backup's 400 relayed", resp.StatusCode)
	}
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if out["type"] != "error" || out["error"].(map[string]any)["message"] != "context too long" {
		t.Errorf("error not in the Anthropic envelope: %v", out)
	}
}

func TestIntegration_LegacyCompletionsNeverCross(t *testing.T) {
	backupHit := false
	h := newHarness(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	h.seedRoute()
	h.server.Recorder = &recordingRecorder{}
	h.addBackupProvider(t, func(w http.ResponseWriter, _ *http.Request) {
		backupHit = true
		w.WriteHeader(200)
	})
	h.store.providers["backup"].Spec.Type = "anthropic"
	h.store.providers["prov"].Spec.Fallback = []kaalmv1beta1.FallbackReference{{Name: "backup"}}
	h.store.agents["team-a/sup"].Spec.Providers = append(h.store.agents["team-a/sup"].Spec.Providers,
		kaalmv1beta1.AgentProviderReference{ProviderRef: kaalmv1beta1.LocalObjectReference{Name: "backup"}})
	h.store.classes["std"].Spec.AllowedProviders = append(h.store.classes["std"].Spec.AllowedProviders,
		kaalmv1beta1.LocalObjectReference{Name: "backup"})
	cert := agentCert(t, h.ca)
	resp := postJSON(t, h.client(&cert), h.url("/v1/completions"), map[string]any{"model": "prov/m1", "prompt": "hi"}, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadGateway || backupHit {
		t.Errorf("legacy completions must not cross: status %d backupHit %v", resp.StatusCode, backupHit)
	}
}
