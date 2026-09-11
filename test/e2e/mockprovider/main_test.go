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

package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestBehaviorFor(t *testing.T) {
	cases := []struct {
		path       string
		wantStatus int
		wantUsage  bool // expect non-zero token counts
	}{
		{"/ok/v1/chat/completions", http.StatusOK, true},
		{"/v1/chat/completions", http.StatusOK, true}, // default is ok
		{"/fail/v1/chat/completions", http.StatusServiceUnavailable, false},
		{"/bigusage/v1/chat/completions", http.StatusOK, true},
	}
	for _, c := range cases {
		status, in, out := behaviorFor(c.path)
		if status != c.wantStatus {
			t.Errorf("%s: status=%d want %d", c.path, status, c.wantStatus)
		}
		if got := in > 0 && out > 0; got != c.wantUsage {
			t.Errorf("%s: nonzero usage=%v want %v (in=%d out=%d)", c.path, got, c.wantUsage, in, out)
		}
	}
	// bigusage must dwarf ok so budget tests cross the ceiling in few calls.
	_, bigIn, _ := behaviorFor("/bigusage/x")
	_, okIn, _ := behaviorFor("/ok/x")
	if bigIn <= okIn {
		t.Errorf("bigusage input tokens %d not greater than ok %d", bigIn, okIn)
	}
}

func TestDelayForSlowPrefix(t *testing.T) {
	cases := []struct {
		path string
		want time.Duration
	}{
		{"/slow50/v1/chat/completions", 50 * time.Millisecond},
		{"/slow250/leg/v1/messages", 250 * time.Millisecond},
		{"/slow/v1/chat/completions", 0},   // no number
		{"/slow-5/v1/chat/completions", 0}, // not a positive integer
		{"/ok/v1/chat/completions", 0},
		{"/v1/chat/completions", 0},
	}
	for _, c := range cases {
		if got := delayFor(c.path); got != c.want {
			t.Errorf("delayFor(%q) = %v, want %v", c.path, got, c.want)
		}
	}
	// A slow prefix still answers as /ok does.
	if status, in, out := behaviorFor("/slow50/v1/chat/completions"); status != http.StatusOK || in == 0 || out == 0 {
		t.Errorf("slow prefix behavior = (%d, %d, %d), want 200 with usage", status, in, out)
	}
}

func TestChatSuccessCarriesUsage(t *testing.T) {
	m := &mock{}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/ok/v1/chat/completions",
		strings.NewReader(`{"model":"mock-model","messages":[]}`))
	m.handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200", rec.Code)
	}
	var body struct {
		Usage struct {
			PromptTokens     int64 `json:"prompt_tokens"`
			CompletionTokens int64 `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if body.Usage.PromptTokens == 0 || body.Usage.CompletionTokens == 0 {
		t.Errorf("usage fields must be non-zero, got %+v", body.Usage)
	}
}

func TestChatFailReturns503(t *testing.T) {
	m := &mock{}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/fail/v1/chat/completions", strings.NewReader(`{}`))
	m.handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503", rec.Code)
	}
}

func TestCallbackRecordedAndIntrospected(t *testing.T) {
	m := &mock{}
	post := httptest.NewRecorder()
	m.handler().ServeHTTP(post, httptest.NewRequest(http.MethodPost, "/callback",
		strings.NewReader(`{"requestId":"abc"}`)))
	if post.Code != http.StatusOK {
		t.Fatalf("callback status=%d want 200", post.Code)
	}

	get := httptest.NewRecorder()
	m.handler().ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/introspect/callbacks", nil))
	var recorded []recordedCallback
	if err := json.Unmarshal(get.Body.Bytes(), &recorded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(recorded) != 1 || !strings.Contains(recorded[0].Body, "abc") {
		t.Errorf("callback not recorded: %+v", recorded)
	}
}

func TestRequestCounterAndIntrospection(t *testing.T) {
	m := &mock{}
	for i := 0; i < 3; i++ {
		req := httptest.NewRequest(http.MethodPost, "/s17hard/v1/chat/completions", strings.NewReader(`{}`))
		m.chat(httptest.NewRecorder(), req)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{}`))
	m.chat(httptest.NewRecorder(), req)

	rec := httptest.NewRecorder()
	m.introspectRequests(rec, httptest.NewRequest(http.MethodGet, "/introspect/requests", nil))
	var counts map[string]int
	if err := json.Unmarshal(rec.Body.Bytes(), &counts); err != nil {
		t.Fatalf("decoding counts: %v", err)
	}
	if counts["/s17hard"] != 3 || counts["/"] != 1 {
		t.Fatalf("counts = %v, want /s17hard:3 and /:1", counts)
	}
}

func TestMessagesPathSpeaksAnthropic(t *testing.T) {
	m := &mock{}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/ok/v1/messages", "application/json",
		strings.NewReader(`{"model":"claude-x","max_tokens":10,"messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out["type"] != "message" || out["model"] != "claude-x" || out["stop_reason"] != "end_turn" {
		t.Errorf("not an Anthropic message: %v", out)
	}
	usage, _ := out["usage"].(map[string]any)
	if usage["input_tokens"] != float64(11) || usage["output_tokens"] != float64(22) {
		t.Errorf("usage wrong: %v", usage)
	}
	// The counter keys both paths by prefix.
	if m.requests["/ok"] != 1 {
		t.Errorf("request counter = %v", m.requests)
	}
	// /fail answers the Anthropic error envelope.
	resp2, _ := http.Post(srv.URL+"/fail/v1/messages", "application/json", strings.NewReader(`{}`))
	defer func() { _ = resp2.Body.Close() }()
	raw, _ := io.ReadAll(resp2.Body)
	if resp2.StatusCode != 503 || !strings.Contains(string(raw), `"type":"error"`) {
		t.Errorf("fail = %d %s", resp2.StatusCode, raw)
	}
}

func TestStreamingBothShapes(t *testing.T) {
	m := &mock{}
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/ok/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"g","stream":true,"stream_options":{"include_usage":true},"messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	text := string(raw)
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") ||
		!strings.Contains(text, `"content":"ok from"`) || !strings.Contains(text, `"finish_reason":"stop"`) ||
		!strings.Contains(text, `"prompt_tokens":11`) || !strings.HasSuffix(strings.TrimSpace(text), "data: [DONE]") {
		t.Errorf("chat stream wrong:\n%s", text)
	}

	resp, err = http.Post(srv.URL+"/ok/v1/messages", "application/json",
		strings.NewReader(`{"model":"c","stream":true,"max_tokens":5,"messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	text = string(raw)
	for _, want := range []string{"event: message_start", `"input_tokens":11`, "event: content_block_delta",
		`"text":"ok from mock"`, "event: message_delta", `"output_tokens":22`, "event: message_stop"} {
		if !strings.Contains(text, want) {
			t.Errorf("messages stream lacks %q:\n%s", want, text)
		}
	}
}
