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
	"crypto/x509"
	"errors"
	"net"
	"net/url"
	"os"
	"sync/atomic"
	"syscall"

	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kaalmv1beta1 "github.com/win07xp/kaalm/api/v1beta1"
)

// fakeAsync is an in-memory AsyncRecords.
type fakeAsync struct {
	mu      sync.Mutex
	records map[string]*AsyncRecord
}

func newFakeAsync() *fakeAsync { return &fakeAsync{records: map[string]*AsyncRecord{}} }

func (f *fakeAsync) Create(_ context.Context, id string, ch *kaalmv1beta1.AgentChannel, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records[id] = &AsyncRecord{CreatedAt: time.Now(), ChannelNamespace: ch.Namespace, ChannelName: ch.Name}
	return nil
}
func (f *fakeAsync) Patch(_ context.Context, id string, payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.records[id]
	if !ok {
		return fmt.Errorf("record %s missing", id)
	}
	rec.Payload = payload
	return nil
}
func (f *fakeAsync) Get(_ context.Context, id string) (*AsyncRecord, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.records[id]
	if !ok {
		return nil, false, nil
	}
	cp := *rec
	return &cp, true, nil
}
func (f *fakeAsync) CountPending(_ context.Context, ns, name string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, r := range f.records {
		if r.ChannelNamespace == ns && r.ChannelName == name {
			n++
		}
	}
	return n, nil
}

// userHarness serves the user listener over plain httptest TLS plus a fake
// agent backend reached via the host/port overrides.
type userHarness struct {
	server    *Server
	store     *fakeStore
	async     *fakeAsync
	userSrv   *httptest.Server
	agentSrv  *httptest.Server
	agentHits chan MessageEnvelope
}

func newUserHarness(t *testing.T, agentFn http.HandlerFunc) *userHarness {
	t.Helper()
	h := &userHarness{store: newFakeStore(), async: newFakeAsync(), agentHits: make(chan MessageEnvelope, 8)}

	h.agentSrv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var env MessageEnvelope
		_ = json.NewDecoder(r.Body).Decode(&env)
		h.agentHits <- env
		agentFn(w, r)
	}))
	t.Cleanup(h.agentSrv.Close)

	agentURL := strings.TrimPrefix(h.agentSrv.URL, "https://")
	host, portStr, _ := strings.Cut(agentURL, ":")
	port, _ := strconv.Atoi(portStr)

	cfg := Config{
		OperatorNamespace:        "kaalm-system",
		DeliveryBackoff:          []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond},
		CallbackBackoff:          []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond},
		AgentServiceHostOverride: host,
		AgentServicePortOverride: int32(port),
		InsecureSkipAgentVerify:  true,
		SyncDeliveryDeadline:     5 * time.Second,
		AgentReadTimeout:         2 * time.Second,
	}
	h.server = NewServer(cfg, h.store, NewTokenAuthenticator(&fakeReviewer{}), NewMemorySpend())
	h.server.Async = h.async

	h.userSrv = httptest.NewTLSServer(h.server.UserHandler())
	t.Cleanup(h.userSrv.Close)
	return h
}

// seedChannel installs a Ready bearer-auth channel and its agent.
func (h *userHarness) seedChannel(mode string) *kaalmv1beta1.AgentChannel {
	ch := &kaalmv1beta1.AgentChannel{
		ObjectMeta: metav1.ObjectMeta{Name: "support", Namespace: "team-a"},
		Spec: kaalmv1beta1.AgentChannelSpec{
			AgentRef: kaalmv1beta1.LocalObjectReference{Name: "sup"},
			Webhook: &kaalmv1beta1.AgentChannelWebhook{
				Path: "/channels/team-a/support",
				Auth: kaalmv1beta1.ChannelAuth{
					Type:      authTypeBearer,
					SecretRef: &kaalmv1beta1.SecretKeyReference{Name: "hook-secret", Key: "token"},
				},
				ResponseMode: mode,
			},
			Session: kaalmv1beta1.AgentChannelSession{Enabled: true},
		},
		Status: kaalmv1beta1.AgentChannelStatus{Phase: kaalmv1beta1.ChannelActive},
	}
	h.store.channels[ch.Spec.Webhook.Path] = ch
	h.store.secrets["team-a/hook-secret/token"] = "hook-token"
	h.store.agents["team-a/sup"] = &kaalmv1beta1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "sup", Namespace: "team-a"},
		Status:     kaalmv1beta1.AgentStatus{Phase: kaalmv1beta1.AgentRunning},
	}
	return ch
}

func (h *userHarness) post(t *testing.T, path, token string, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, h.userSrv.URL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := h.userSrv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestWebhook_SyncRoundTrip(t *testing.T) {
	h := newUserHarness(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"content":"hello back","attachments":[],"metadata":{}}`))
	})
	h.seedChannel("sync")

	resp := h.post(t, "/channels/team-a/support", "hook-token", []byte(`{"text":"hi"}`))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	var reply ResponseEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil || *reply.Content != "hello back" {
		t.Fatalf("reply wrong: %+v err=%v", reply, err)
	}

	env := <-h.agentHits
	if env.MessageID == "" || env.ChannelID != "/channels/team-a/support" {
		t.Errorf("envelope wrong: %+v", env)
	}
	// Raw-body fallback content: the body JSON-encoded as a string.
	if env.Content != `"{\"text\":\"hi\"}"` {
		t.Errorf("raw-body content wrong: %q", env.Content)
	}
	// session.enabled: deterministic UUIDv5.
	if env.SessionID != SessionID(env.ChannelID, env.UserID) {
		t.Errorf("sessionId not deterministic: %q", env.SessionID)
	}
}

func TestWebhook_AuthAndPathPosture(t *testing.T) {
	h := newUserHarness(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	h.seedChannel("sync")

	// Wrong token: 401.
	resp := h.post(t, "/channels/team-a/support", "wrong", []byte(`{}`))
	if resp.StatusCode != 401 {
		t.Errorf("wrong token = %d", resp.StatusCode)
	}
	// Unregistered path: same 401, no existence leak.
	resp = h.post(t, "/channels/team-a/nope", "hook-token", []byte(`{}`))
	if resp.StatusCode != 401 {
		t.Errorf("unregistered path = %d", resp.StatusCode)
	}
	// Oversized body to an UNREGISTERED path: 413 fires before path
	// resolution, preserving the 413-vs-401 threat model.
	h.server.Config.MaxMessageBodyBytes = 64
	big := bytes.Repeat([]byte("x"), 256)
	resp = h.post(t, "/channels/team-a/unknown", "hook-token", big)
	if resp.StatusCode != 413 {
		t.Errorf("oversized to unknown path = %d, want 413", resp.StatusCode)
	}
}

func TestWebhook_HMACAuth(t *testing.T) {
	h := newUserHarness(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"content":"ok"}`))
	})
	ch := h.seedChannel("sync")
	prefix := "sha256="
	ch.Spec.Webhook.Auth = kaalmv1beta1.ChannelAuth{
		Type: authTypeHMAC,
		HMAC: &kaalmv1beta1.ChannelHMAC{
			Header:          "X-Hub-Signature-256",
			Algorithm:       "sha256",
			SecretRef:       kaalmv1beta1.SecretKeyReference{Name: "hook-secret", Key: "token"},
			SignaturePrefix: &prefix,
		},
	}

	body := []byte(`{"event":"push"}`)
	mac := hmac.New(sha256.New, []byte("hook-token"))
	mac.Write(body)
	sig := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	req, _ := http.NewRequest(http.MethodPost, h.userSrv.URL+"/channels/team-a/support", bytes.NewReader(body))
	req.Header.Set("X-Hub-Signature-256", sig)
	resp, err := h.userSrv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("valid HMAC = %d", resp.StatusCode)
	}

	// Tampered body fails.
	req, _ = http.NewRequest(http.MethodPost, h.userSrv.URL+"/channels/team-a/support", strings.NewReader(`{"event":"tampered"}`))
	req.Header.Set("X-Hub-Signature-256", sig)
	resp, err = h.userSrv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Errorf("tampered HMAC = %d, want 401", resp.StatusCode)
	}
}

func TestWebhook_ExtractorsAndBadJSON(t *testing.T) {
	h := newUserHarness(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"content":"ok"}`))
	})
	ch := h.seedChannel("sync")
	userPath := "user.id"
	contentPath := "message.text"
	fallback := "anonymous"
	ch.Spec.Webhook.UserID = kaalmv1beta1.ChannelExtractor{FromBody: &userPath, Fallback: &fallback}
	ch.Spec.Webhook.Content = kaalmv1beta1.ChannelExtractor{FromBody: &contentPath}

	resp := h.post(t, "/channels/team-a/support", "hook-token",
		[]byte(`{"user":{"id":"u-42"},"message":{"text":"help me"}}`))
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	env := <-h.agentHits
	if env.UserID != "u-42" || env.Content != "help me" {
		t.Errorf("extraction wrong: userId=%q content=%q", env.UserID, env.Content)
	}

	// Non-JSON body with fromBody configured: 400.
	resp = h.post(t, "/channels/team-a/support", "hook-token", []byte(`not-json`))
	if resp.StatusCode != 400 {
		t.Errorf("non-JSON with fromBody = %d, want 400", resp.StatusCode)
	}
	_ = resp.Body.Close()
}

func TestWebhook_DeliveryRetryAndFailure(t *testing.T) {
	attempts := 0
	h := newUserHarness(t, func(w http.ResponseWriter, _ *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"content":"third time lucky"}`))
	})
	h.seedChannel("sync")

	// Two failures then success: the retry pipeline recovers, and the same
	// messageId is reused across attempts.
	resp := h.post(t, "/channels/team-a/support", "hook-token", []byte(`{}`))
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("retried delivery = %d", resp.StatusCode)
	}
	first := <-h.agentHits
	second := <-h.agentHits
	third := <-h.agentHits
	if first.MessageID != second.MessageID || second.MessageID != third.MessageID {
		t.Error("messageId must be reused across delivery retries")
	}

	// Persistent malformed envelope: delivery_failed after the budget.
	h2 := newUserHarness(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"no_content":true}`))
	})
	h2.seedChannel("sync")
	resp = h2.post(t, "/channels/team-a/support", "hook-token", []byte(`{}`))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 502 {
		t.Fatalf("malformed envelope = %d, want 502", resp.StatusCode)
	}
	if got := errType(t, resp); got != "delivery_failed" {
		t.Errorf("error type %q", got)
	}
}

func TestWebhook_AsyncAcceptAndPoll(t *testing.T) {
	h := newUserHarness(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"content":"async reply"}`))
	})
	h.seedChannel("async")

	resp := h.post(t, "/channels/team-a/support", "hook-token", []byte(`{"q":"1"}`))
	if resp.StatusCode != 202 {
		t.Fatalf("async accept = %d", resp.StatusCode)
	}
	var accept asyncAcceptResponse
	if err := json.NewDecoder(resp.Body).Decode(&accept); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if accept.RequestID == "" || accept.ChannelPath != "/channels/team-a/support" || accept.Status != "accepted" {
		t.Fatalf("202 body wrong: %+v", accept)
	}

	// The placeholder exists immediately (202 implies a queryable record).
	pollURL := h.userSrv.URL + "/v1/channels/responses/" + accept.RequestID +
		"?channelPath=" + strings.ReplaceAll(accept.ChannelPath, "/", "%2F")
	poll := func() *http.Response {
		req, _ := http.NewRequest(http.MethodGet, pollURL, nil)
		req.Header.Set("Authorization", "Bearer hook-token")
		r, err := h.userSrv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}

	// Eventually the background pipeline patches the payload: poll to 200.
	deadline := time.Now().Add(5 * time.Second)
	var final *http.Response
	for time.Now().Before(deadline) {
		final = poll()
		if final.StatusCode == 200 {
			break
		}
		if final.StatusCode == 202 && final.Header.Get("Retry-After") == "" {
			t.Fatal("202 poll must carry Retry-After")
		}
		_ = final.Body.Close()
		time.Sleep(50 * time.Millisecond)
	}
	defer func() { _ = final.Body.Close() }()
	if final.StatusCode != 200 {
		t.Fatalf("poll never reached 200, last=%d", final.StatusCode)
	}
	var payload struct {
		RequestID string `json:"requestId"`
		Response  struct {
			Content string `json:"content"`
		} `json:"response"`
	}
	if err := json.NewDecoder(final.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if payload.RequestID != accept.RequestID || payload.Response.Content != "async reply" {
		t.Errorf("stored payload wrong: %+v", payload)
	}

	// Wrong-channel probing: a second channel's credentials see 404.
	other := &kaalmv1beta1.AgentChannel{
		ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "team-b"},
		Spec: kaalmv1beta1.AgentChannelSpec{
			AgentRef: kaalmv1beta1.LocalObjectReference{Name: "x"},
			Webhook: &kaalmv1beta1.AgentChannelWebhook{
				Path: "/channels/team-b/other",
				Auth: kaalmv1beta1.ChannelAuth{
					Type:      authTypeBearer,
					SecretRef: &kaalmv1beta1.SecretKeyReference{Name: "other-secret", Key: "token"},
				},
			},
		},
	}
	h.store.channels[other.Spec.Webhook.Path] = other
	h.store.secrets["team-b/other-secret/token"] = "other-token"
	req, _ := http.NewRequest(http.MethodGet, h.userSrv.URL+"/v1/channels/responses/"+accept.RequestID+
		"?channelPath=%2Fchannels%2Fteam-b%2Fother", nil)
	req.Header.Set("Authorization", "Bearer other-token")
	crossResp, err := h.userSrv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = crossResp.Body.Close()
	if crossResp.StatusCode != 404 {
		t.Errorf("cross-channel probe = %d, want 404", crossResp.StatusCode)
	}
}

func TestWebhook_AsyncCallbackDelivery(t *testing.T) {
	callbackHits := make(chan *http.Request, 1)
	callbackBodies := make(chan []byte, 1)
	callback := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := json.Marshal(r.Header)
		_ = raw
		body := new(bytes.Buffer)
		_, _ = body.ReadFrom(r.Body)
		callbackBodies <- body.Bytes()
		callbackHits <- r.Clone(context.Background())
		w.WriteHeader(200)
	}))
	defer callback.Close()

	h := newUserHarness(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"content":"cb reply"}`))
	})
	ch := h.seedChannel("async")
	cbURL := callback.URL // https://127.0.0.1:port; loopback is blocked, so allow via test hook
	ch.Spec.Webhook.CallbackURL = &cbURL
	ch.Spec.Webhook.CallbackAuth = &kaalmv1beta1.ChannelAuth{
		Type:      authTypeBearer,
		SecretRef: &kaalmv1beta1.SecretKeyReference{Name: "hook-secret", Key: "token"},
	}
	// The callback target is the loopback test server: it would be blocked
	// by the deny ranges. Verify the block first, then use polling instead.
	resp := h.post(t, "/channels/team-a/support", "hook-token", []byte(`{}`))
	var accept asyncAcceptResponse
	_ = json.NewDecoder(resp.Body).Decode(&accept)
	_ = resp.Body.Close()

	// The payload must land at the polling endpoint (bypassed callback).
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		rec, ok, _ := h.async.Get(context.Background(), accept.RequestID)
		if ok && rec.Payload != nil {
			if !strings.Contains(string(rec.Payload), "cb reply") {
				t.Fatalf("stored payload wrong: %s", rec.Payload)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("payload never stored after bypassed callback")
}

func TestChannelHealth_ReflectsTraffic(t *testing.T) {
	h := newUserHarness(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"content":"ok"}`))
	})
	h.seedChannel("sync")

	// A successful delivery records success.
	resp := h.post(t, "/channels/team-a/support", "hook-token", []byte(`{}`))
	_ = resp.Body.Close()
	snap := h.server.ChannelHealth.Snapshot("team-a")
	entry := snap.Channels["/channels/team-a/support"]
	if entry.State != "success" || entry.Reason == nil || *entry.Reason != healthReasonWebhookReady {
		t.Errorf("health after success wrong: %+v", entry)
	}

	// An auth failure records failure (visible in a fresh window as the most
	// recent failure alongside the earlier success -> still success state).
	resp = h.post(t, "/channels/team-a/support", "bad-token", []byte(`{}`))
	_ = resp.Body.Close()
	snap = h.server.ChannelHealth.Snapshot("team-a")
	entry = snap.Channels["/channels/team-a/support"]
	if entry.State != "success" {
		t.Errorf("success within window must win: %+v", entry)
	}
	if snap.WindowSeconds != 300 {
		t.Errorf("window seconds = %d", snap.WindowSeconds)
	}
}

func TestWakeAndDeliver_WakeTimeout(t *testing.T) {
	h := newUserHarness(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	h.seedChannel("sync")
	agent := h.store.agents["team-a/sup"]
	agent.Status.Phase = kaalmv1beta1.AgentHibernated
	agent.Spec.Lifecycle.WakeTimeout = metav1.Duration{Duration: 80 * time.Millisecond}

	// Wake succeeds but the agent never becomes reachable: point delivery at a
	// closed port so waitAgentReachable exhausts its (short) wakeTimeout.
	h.server.Activator = &fakeActivator{}
	h.server.Config.AgentServicePortOverride = 1 // nothing listens on :1
	h.server.Config.AgentConnectTimeout = 20 * time.Millisecond

	resp := h.post(t, "/channels/team-a/support", "hook-token", []byte(`{}`))
	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Errorf("wake timeout = %d, want 504", resp.StatusCode)
	}
	if got := errType(t, resp); got != errWakeTimeout {
		t.Errorf("error type = %q, want %q", got, errWakeTimeout)
	}
}

func TestAgentHTTPClient_WithCertFiles(t *testing.T) {
	ca := newTestCA(t)
	certFile, keyFile, caFile := certFiles(t, ca, "gw")
	s := &Server{Config: Config{CertFile: certFile, KeyFile: keyFile, CAFile: caFile}}

	client, err := s.agentHTTPClient()
	if err != nil || client == nil {
		t.Fatalf("agentHTTPClient = %v err=%v", client, err)
	}
	// One client for every delivery: a second call returns the same one.
	if again, err := s.agentHTTPClient(); err != nil || again != client {
		t.Errorf("second agentHTTPClient = %p err=%v, want the shared %p", again, err, client)
	}
	tr, ok := client.Transport.(*http.Transport)
	if !ok || tr.DialTLSContext == nil || tr.MaxIdleConns != agentMaxIdleConns {
		t.Errorf("delivery transport = %#v, want a pooled transport with the mTLS dialer", client.Transport)
	}
}

func TestAgentHTTPClient_BadCertFiles(t *testing.T) {
	s := &Server{Config: Config{CertFile: "/nonexistent/tls.crt", KeyFile: "/nonexistent/tls.key", CAFile: "/nonexistent/ca.crt"}}
	if _, err := s.agentHTTPClient(); err == nil {
		t.Error("missing cert files must error")
	}
}

func TestWebhook_TerminatingAndAgentMissing(t *testing.T) {
	h := newUserHarness(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"content":"ok"}`))
	})
	ch := h.seedChannel("sync")

	// A Terminating channel accepts no new work: 401 (no existence leak).
	ch.Status.Phase = kaalmv1beta1.ChannelTerminating
	resp := h.post(t, "/channels/team-a/support", "hook-token", []byte(`{}`))
	if resp.StatusCode != 401 {
		t.Errorf("terminating channel = %d, want 401", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// Restore the channel but drop its Agent: referenced Agent not found -> 502.
	ch.Status.Phase = kaalmv1beta1.ChannelActive
	delete(h.store.agents, "team-a/sup")
	resp = h.post(t, "/channels/team-a/support", "hook-token", []byte(`{}`))
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("missing agent = %d, want 502", resp.StatusCode)
	}
	_ = resp.Body.Close()
}

func TestWebhook_WrongMethod(t *testing.T) {
	h := newUserHarness(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	h.seedChannel("sync")
	req, _ := http.NewRequest(http.MethodGet, h.userSrv.URL+"/channels/team-a/support", nil)
	resp, err := h.userSrv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Errorf("GET webhook = %d, want 401", resp.StatusCode)
	}
	// Unknown path on the user listener: 401.
	req, _ = http.NewRequest(http.MethodGet, h.userSrv.URL+"/nonsense", nil)
	resp, _ = h.userSrv.Client().Do(req)
	_ = resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Errorf("unknown user path = %d, want 401", resp.StatusCode)
	}
}

func TestWebhook_RawBodyInvalidUTF8(t *testing.T) {
	h := newUserHarness(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	h.seedChannel("sync") // no content extractor -> raw-body fallback
	// A body with an invalid UTF-8 byte fails raw-body normalization with 400.
	resp := h.post(t, "/channels/team-a/support", "hook-token", []byte{0xff, 0xfe})
	if resp.StatusCode != 400 || !bodyContains(t, resp, "UTF-8") {
		t.Errorf("invalid UTF-8 raw body = %d", resp.StatusCode)
	}
}

// A successful channel delivery must record gatewayTraffic activity: the
// channel half of the GET /v1/activity contract. Before v0.5.0 only the LLM
// proxy recorded traffic, so a chat-only agent hibernated mid-conversation.
func TestWebhook_DeliveryRecordsActivity(t *testing.T) {
	h := newUserHarness(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"content":"hi"}`))
	})
	h.seedChannel("sync")

	resp := h.post(t, "/channels/team-a/support", "hook-token", []byte(`{}`))
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("delivery = %d, want 200", resp.StatusCode)
	}
	snap := h.server.Activity.Snapshot("team-a")
	src, ok := snap.Agents["sup"]
	if !ok || src.GatewayTraffic == nil {
		t.Error("channel delivery must record gatewayTraffic for the agent")
	}

	// A failed delivery records nothing: activity means the agent answered.
	h2 := newUserHarness(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(500) })
	h2.seedChannel("sync")
	resp = h2.post(t, "/channels/team-a/support", "hook-token", []byte(`{}`))
	_ = resp.Body.Close()
	if _, ok := h2.server.Activity.Snapshot("team-a").Agents["sup"]; ok {
		t.Error("a failed delivery must not count as activity")
	}
}

func TestDeliveryOutcomeClassifiesTheFailingLayer(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"dns", &url.Error{Op: "Post", Err: &net.OpError{Op: "dial", Err: &net.DNSError{Name: "a.b.svc", IsTimeout: true}}}, deliveryOutcomeDNS},
		{"deadline", fmt.Errorf("Post: %w", context.DeadlineExceeded), deliveryOutcomeTimeout},
		{"canceled", fmt.Errorf("Post: %w", context.Canceled), deliveryOutcomeCanceled},
		{"io timeout", errors.New("read tcp 10.0.0.1:1->10.0.0.2:8443: i/o timeout"), deliveryOutcomeTimeout},
		{"refused", &url.Error{Op: "Post", Err: &net.OpError{Op: "dial", Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}}}, deliveryOutcomeConnect},
		{"connect stage", &url.Error{Op: "Post", Err: &dialStageError{stage: dialStageConnect, err: context.DeadlineExceeded}}, deliveryOutcomeConnect},
		{"handshake stage", &url.Error{Op: "Post", Err: &dialStageError{stage: dialStageHandshake, err: context.DeadlineExceeded}}, deliveryOutcomeTLS},
		{"reset", errors.New("read: connection reset by peer"), deliveryOutcomeConnect},
		{"tls", &url.Error{Op: "Post", Err: x509.UnknownAuthorityError{}}, deliveryOutcomeTLS},
		{"status", errors.New("agent returned 503"), deliveryOutcomeStatus},
		{"malformed", errors.New("agent returned 200 with a malformed response envelope"), deliveryOutcomeMalformed},
		{"too large", errors.New("agent response body exceeded 921600 bytes; externalize large outputs and reference by URL"), deliveryOutcomeTooLarge},
		{"other", errors.New("boom"), deliveryOutcomeOther},
	}
	for _, c := range cases {
		if got := deliveryOutcome(c.err); got != c.want {
			t.Errorf("%s: deliveryOutcome(%v) = %q, want %q", c.name, c.err, got, c.want)
		}
	}
}

// deliveryServer returns a Server whose agent deliveries land on fn, with a
// fast retry schedule and a registry of its own for the attempt counter.
func deliveryServer(t *testing.T, fn http.HandlerFunc) *Server {
	t.Helper()
	srv := httptest.NewTLSServer(fn)
	t.Cleanup(srv.Close)
	host, portStr, _ := strings.Cut(strings.TrimPrefix(srv.URL, "https://"), ":")
	port, _ := strconv.Atoi(portStr)
	s := NewServer(Config{
		OperatorNamespace:        "kaalm-system",
		DeliveryBackoff:          []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond},
		AgentServiceHostOverride: host,
		AgentServicePortOverride: int32(port),
		InsecureSkipAgentVerify:  true,
		AgentReadTimeout:         2 * time.Second,
		MaxResponseBodyBytes:     1 << 20,
	}, &fakeStore{}, NewTokenAuthenticator(&fakeReviewer{}), NewMemorySpend())
	s.Metrics = NewMetrics(prometheus.NewRegistry())
	return s
}

func TestDeliverToAgentCountsEveryAttemptByOutcome(t *testing.T) {
	agent := &kaalmv1beta1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "sup", Namespace: "team-a"}}
	env := MessageEnvelope{MessageID: "m1"}

	// Every attempt answers 503: four attempts, all "status", none "ok".
	s := deliveryServer(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) })
	if _, err := s.deliverToAgent(context.Background(), agent, env); err == nil {
		t.Fatal("delivery against a 503 agent must fail")
	}
	if got := testutil.ToFloat64(s.Metrics.channelDelivery.WithLabelValues("team-a", deliveryOutcomeStatus)); got != 4 {
		t.Errorf("status attempts = %v, want 4", got)
	}
	if got := testutil.ToFloat64(s.Metrics.channelDelivery.WithLabelValues("team-a", deliveryOutcomeOK)); got != 0 {
		t.Errorf("ok attempts = %v, want 0", got)
	}

	// One 503 then success: one "status", one "ok".
	var calls int32
	s = deliveryServer(t, func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"content":"hi"}`))
	})
	if _, err := s.deliverToAgent(context.Background(), agent, env); err != nil {
		t.Fatalf("delivery must succeed on the second attempt: %v", err)
	}
	if got := testutil.ToFloat64(s.Metrics.channelDelivery.WithLabelValues("team-a", deliveryOutcomeStatus)); got != 1 {
		t.Errorf("status attempts = %v, want 1", got)
	}
	if got := testutil.ToFloat64(s.Metrics.channelDelivery.WithLabelValues("team-a", deliveryOutcomeOK)); got != 1 {
		t.Errorf("ok attempts = %v, want 1", got)
	}
}

func TestDeliverToAgentReusesTheConnection(t *testing.T) {
	var opened int32
	agent := &kaalmv1beta1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "sup", Namespace: "team-a"}}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"content":"hi"}`))
	}))
	srv.Config.ConnState = func(_ net.Conn, st http.ConnState) {
		if st == http.StateNew {
			atomic.AddInt32(&opened, 1)
		}
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	host, portStr, _ := strings.Cut(strings.TrimPrefix(srv.URL, "https://"), ":")
	port, _ := strconv.Atoi(portStr)
	s := NewServer(Config{
		OperatorNamespace: "kaalm-system", AgentServiceHostOverride: host, AgentServicePortOverride: int32(port),
		InsecureSkipAgentVerify: true, AgentReadTimeout: 2 * time.Second, AgentConnectTimeout: time.Second,
		MaxResponseBodyBytes: 1 << 20,
	}, &fakeStore{}, NewTokenAuthenticator(&fakeReviewer{}), NewMemorySpend())
	s.Metrics = NewMetrics(prometheus.NewRegistry())

	for i := 0; i < 5; i++ {
		if _, err := s.deliverToAgent(context.Background(), agent, MessageEnvelope{MessageID: fmt.Sprintf("m%d", i)}); err != nil {
			t.Fatalf("delivery %d: %v", i, err)
		}
	}
	if got := atomic.LoadInt32(&opened); got != 1 {
		t.Errorf("agent saw %d connections for 5 deliveries, want 1 (the pooled connection)", got)
	}
}

func TestDeliverToAgentBoundsTheConnect(t *testing.T) {
	// A blackhole address: the SYN is never answered, so only the connect
	// bound ends the attempt, and the outcome names the connect stage.
	agent := &kaalmv1beta1.Agent{ObjectMeta: metav1.ObjectMeta{Name: "sup", Namespace: "team-a"}}
	s := NewServer(Config{
		OperatorNamespace: "kaalm-system", AgentServiceHostOverride: "192.0.2.1", AgentServicePortOverride: 8443,
		InsecureSkipAgentVerify: true, AgentReadTimeout: 5 * time.Second, AgentConnectTimeout: 50 * time.Millisecond,
		DeliveryBackoff: []time.Duration{time.Millisecond},
	}, &fakeStore{}, NewTokenAuthenticator(&fakeReviewer{}), NewMemorySpend())
	s.Metrics = NewMetrics(prometheus.NewRegistry())

	start := time.Now()
	_, err := s.deliverToAgent(context.Background(), agent, MessageEnvelope{MessageID: "m1"})
	if err == nil {
		t.Fatal("delivery to a blackhole must fail")
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("two attempts took %v; the connect bound did not apply", took)
	}
	if got := testutil.ToFloat64(s.Metrics.channelDelivery.WithLabelValues("team-a", deliveryOutcomeConnect)); got != 2 {
		t.Errorf("connect outcomes = %v, want 2", got)
	}
}
