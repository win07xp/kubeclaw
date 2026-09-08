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
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kaalmv1beta1 "github.com/win07xp/kaalm/api/v1beta1"
)

// discordFake stands in for the Discord API: it records every reply request
// and answers with a per-path status the test chooses.
type discordFake struct {
	srv      *httptest.Server
	mu       sync.Mutex
	requests []capturedDiscordRequest
	status   map[string]int // method+path prefix -> status; default 200
	got      chan capturedDiscordRequest
}

type capturedDiscordRequest struct {
	Method, Path, Authorization string
	Body                        map[string]any
}

func newDiscordFake(t *testing.T) *discordFake {
	t.Helper()
	f := &discordFake{status: map[string]int{}, got: make(chan capturedDiscordRequest, 16)}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		req := capturedDiscordRequest{Method: r.Method, Path: r.URL.Path, Authorization: r.Header.Get("Authorization"), Body: body}
		f.mu.Lock()
		f.requests = append(f.requests, req)
		status := 200
		for k, v := range f.status {
			if strings.HasPrefix(r.Method+" "+r.URL.Path, k) {
				status = v
			}
		}
		f.mu.Unlock()
		f.got <- req
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"message":"fake"}`))
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *discordFake) next(t *testing.T) capturedDiscordRequest {
	t.Helper()
	select {
	case r := <-f.got:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("no reply request reached the Discord fake")
		return capturedDiscordRequest{}
	}
}

// discordHarness is the user harness plus a Discord channel, a keypair, and
// the fake API.
type discordHarness struct {
	*userHarness
	fake    *discordFake
	pub     ed25519.PublicKey
	priv    ed25519.PrivateKey
	channel *kaalmv1beta1.AgentChannel
}

func newDiscordHarness(t *testing.T, agentFn http.HandlerFunc) *discordHarness {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	h := &discordHarness{userHarness: newUserHarness(t, agentFn), fake: newDiscordFake(t), pub: pub, priv: priv}
	h.server.Config.DiscordAPIBaseURL = h.fake.srv.URL
	guild := "123456789012345678"
	h.channel = &kaalmv1beta1.AgentChannel{
		ObjectMeta: metav1.ObjectMeta{Name: "disc", Namespace: "team-a"},
		Spec: kaalmv1beta1.AgentChannelSpec{
			AgentRef: kaalmv1beta1.LocalObjectReference{Name: "sup"},
			Type:     kaalmv1beta1.ChannelTypeDiscord,
			Discord: &kaalmv1beta1.AgentChannelDiscord{
				Path:           "/channels/team-a/disc",
				CredentialsRef: kaalmv1beta1.LocalObjectReference{Name: "disc-creds"},
				GuildID:        &guild,
				ContentOption:  "message",
			},
			Session: kaalmv1beta1.AgentChannelSession{Enabled: true},
		},
		Status: kaalmv1beta1.AgentChannelStatus{Phase: kaalmv1beta1.ChannelActive},
	}
	h.store.channels[h.channel.Spec.Discord.Path] = h.channel
	h.store.secrets["team-a/disc-creds/publicKey"] = hex.EncodeToString(pub)
	h.store.agents["team-a/sup"] = &kaalmv1beta1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: "sup", Namespace: "team-a"},
		Status:     kaalmv1beta1.AgentStatus{Phase: kaalmv1beta1.AgentRunning},
	}
	return h
}

// send signs body with the harness key (or a caller-supplied key and
// timestamp) and POSTs it to the channel path.
func (h *discordHarness) send(t *testing.T, body []byte, priv ed25519.PrivateKey, ts string) *http.Response {
	t.Helper()
	if priv == nil {
		priv = h.priv
	}
	if ts == "" {
		ts = strconv.FormatInt(time.Now().Unix(), 10)
	}
	sig := ed25519.Sign(priv, append([]byte(ts), body...))
	req, err := http.NewRequest(http.MethodPost, h.userSrv.URL+h.channel.Spec.Discord.Path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Signature-Ed25519", hex.EncodeToString(sig))
	req.Header.Set("X-Signature-Timestamp", ts)
	resp, err := h.userSrv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func discordCommand(guild, channel, user, message string) []byte {
	raw, _ := json.Marshal(map[string]any{
		"id": "1290000000000000001", "application_id": "1230000000000000000", "type": discordInteractionCommand,
		"token": "tok-abc", "guild_id": guild, "channel_id": channel,
		"member": map[string]any{"user": map[string]any{"id": user, "username": "dev"}},
		"data": map[string]any{
			"name": "ask",
			"options": []map[string]any{
				{"name": "message", "type": 3, "value": message},
				{"name": "file", "type": discordOptionAttachment, "value": "1300000000000000000"},
			},
			"resolved": map[string]any{"attachments": map[string]any{
				"1300000000000000000": map[string]any{
					"id": "1300000000000000000", "url": "https://cdn.discordapp.com/x.png",
					"filename": "x.png", "content_type": "image/png", "size": 42},
			}},
		},
		"locale": "en-US",
	})
	return raw
}

func decodeDiscordResponse(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return out
}

func healthReason(h *userHarness, path string) (string, bool) {
	h.server.ChannelHealth.mu.Lock()
	defer h.server.ChannelHealth.mu.Unlock()
	obs := h.server.ChannelHealth.observations[path]
	if len(obs) == 0 {
		return "", false
	}
	last := obs[len(obs)-1]
	return last.reason, last.success
}

func TestDiscord_PingPong(t *testing.T) {
	h := newDiscordHarness(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(500) })
	resp := h.send(t, []byte(`{"id":"1","application_id":"2","type":1,"token":"t"}`), nil, "")
	if resp.StatusCode != 200 {
		t.Fatalf("PING status %d", resp.StatusCode)
	}
	if out := decodeDiscordResponse(t, resp); out["type"] != float64(discordResponsePong) {
		t.Errorf("PING answered %v, want PONG", out)
	}
	select {
	case env := <-h.agentHits:
		t.Errorf("PING reached the agent: %+v", env)
	case <-time.After(100 * time.Millisecond):
	}
	if _, ok := healthReason(h.userHarness, h.channel.Spec.Discord.Path); ok {
		t.Error("a handshake must not be a health observation")
	}
}

func TestDiscord_SignatureAndTimestampPosture(t *testing.T) {
	h := newDiscordHarness(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	body := []byte(`{"type":1}`)

	_, other, _ := ed25519.GenerateKey(nil)
	if resp := h.send(t, body, other, ""); resp.StatusCode != 401 {
		t.Errorf("wrong key = %d, want 401", resp.StatusCode)
	}
	if reason, _ := healthReason(h.userHarness, h.channel.Spec.Discord.Path); reason != healthReasonAuthFailed {
		t.Errorf("health reason after bad signature = %q", reason)
	}
	stale := strconv.FormatInt(time.Now().Add(-10*time.Minute).Unix(), 10)
	if resp := h.send(t, body, nil, stale); resp.StatusCode != 401 {
		t.Errorf("stale timestamp = %d, want 401", resp.StatusCode)
	}
	// Missing headers entirely.
	req, _ := http.NewRequest(http.MethodPost, h.userSrv.URL+h.channel.Spec.Discord.Path, bytes.NewReader(body))
	if resp, _ := h.userSrv.Client().Do(req); resp.StatusCode != 401 {
		t.Errorf("unsigned = %d, want 401", resp.StatusCode)
	}
	// GET on a Discord path is the generic 401, not a handshake.
	req, _ = http.NewRequest(http.MethodGet, h.userSrv.URL+h.channel.Spec.Discord.Path, nil)
	if resp, _ := h.userSrv.Client().Do(req); resp.StatusCode != 401 {
		t.Errorf("GET = %d, want 401", resp.StatusCode)
	}
	// An unusable public key is an auth failure with its own detail.
	h.store.secrets["team-a/disc-creds/publicKey"] = "zz"
	if resp := h.send(t, body, nil, ""); resp.StatusCode != 401 {
		t.Errorf("bad key material = %d, want 401", resp.StatusCode)
	}
}

func TestDiscord_CommandIsDeferredThenAnsweredThroughTheFollowup(t *testing.T) {
	h := newDiscordHarness(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"content":"Your order ships tomorrow.","attachments":[],"metadata":{}}`))
	})
	resp := h.send(t, discordCommand("123456789012345678", "987654321098765432", "555555555555555555", "Where is my order?"), nil, "")
	if resp.StatusCode != 200 {
		t.Fatalf("command status %d", resp.StatusCode)
	}
	if out := decodeDiscordResponse(t, resp); out["type"] != float64(discordResponseDeferredMessage) {
		t.Errorf("command answered %v, want deferred (5)", out)
	}

	env := <-h.agentHits
	if env.ChannelType != "discord" || env.ChannelID != "/channels/team-a/disc" || env.UserID != "555555555555555555" {
		t.Errorf("envelope identity wrong: %+v", env)
	}
	if env.Content != "Where is my order?" {
		t.Errorf("content = %q", env.Content)
	}
	if env.SessionID != SessionID(env.ChannelID, env.UserID) {
		t.Errorf("sessionId not the shared derivation: %q", env.SessionID)
	}
	if env.Metadata["command"] != "ask" || env.Metadata["guildId"] != "123456789012345678" || env.Metadata["interactionId"] != "1290000000000000001" {
		t.Errorf("metadata wrong: %+v", env.Metadata)
	}
	if _, leaked := env.Metadata["token"]; leaked {
		t.Error("the interaction token must not reach the agent")
	}
	if len(env.Attachments) != 1 || !strings.Contains(string(env.Attachments[0]), `"discord.attachment"`) ||
		!strings.Contains(string(env.Attachments[0]), `cdn.discordapp.com`) {
		t.Errorf("attachments wrong: %s", env.Attachments)
	}

	got := h.fake.next(t)
	if got.Method != http.MethodPatch || got.Path != "/webhooks/1230000000000000000/tok-abc/messages/@original" {
		t.Errorf("first reply request = %s %s", got.Method, got.Path)
	}
	if got.Body["content"] != "Your order ships tomorrow." || got.Authorization != "" {
		t.Errorf("reply body/auth wrong: %+v auth=%q", got.Body, got.Authorization)
	}
	if reason, ok := healthReason(h.userHarness, h.channel.Spec.Discord.Path); !ok || reason != healthReasonWebhookReady {
		t.Errorf("health after a delivered command = %q ok=%v", reason, ok)
	}
}

func TestDiscord_LongReplyChunksIntoFollowups(t *testing.T) {
	long := strings.Repeat("a", 1990) + "\n" + strings.Repeat("b", 2000) + strings.Repeat("c", 10)
	reply, _ := json.Marshal(map[string]any{"content": long})
	h := newDiscordHarness(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(reply)
	})
	h.send(t, discordCommand("123456789012345678", "9", "5", "hi"), nil, "")
	<-h.agentHits
	first := h.fake.next(t)
	second := h.fake.next(t)
	third := h.fake.next(t)
	if first.Method != http.MethodPatch || second.Method != http.MethodPost || third.Method != http.MethodPost {
		t.Errorf("methods = %s %s %s", first.Method, second.Method, third.Method)
	}
	if second.Path != "/webhooks/1230000000000000000/tok-abc" {
		t.Errorf("follow-up path = %s", second.Path)
	}
	// The split lands at the newline, not mid-word.
	if c, _ := first.Body["content"].(string); !strings.HasSuffix(c, "\n") || len(c) != 1991 {
		t.Errorf("first chunk len=%d ends %q", len(c), c[len(c)-1:])
	}
}

func TestDiscord_OutOfScopeIsAnEphemeralRefusal(t *testing.T) {
	h := newDiscordHarness(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	resp := h.send(t, discordCommand("999999999999999999", "9", "5", "hi"), nil, "")
	out := decodeDiscordResponse(t, resp)
	data, _ := out["data"].(map[string]any)
	if out["type"] != float64(discordResponseMessage) || data["flags"] != float64(discordFlagEphemeral) || data["content"] != discordRefusalText {
		t.Errorf("refusal = %v", out)
	}
	select {
	case env := <-h.agentHits:
		t.Errorf("out-of-scope interaction reached the agent: %+v", env)
	case <-time.After(100 * time.Millisecond):
	}
	if _, ok := healthReason(h.userHarness, h.channel.Spec.Discord.Path); ok {
		t.Error("a refusal must not be a health observation")
	}

	// Channel scoping on top of the guild.
	h.channel.Spec.Discord.AllowedChannelIDs = []string{"111111111111111111"}
	resp = h.send(t, discordCommand("123456789012345678", "987654321098765432", "5", "hi"), nil, "")
	if out := decodeDiscordResponse(t, resp); out["type"] != float64(discordResponseMessage) {
		t.Errorf("channel scope refusal = %v", out)
	}
}

func TestDiscord_OtherInteractionTypes(t *testing.T) {
	h := newDiscordHarness(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	cases := []struct {
		typ  int
		want float64
	}{
		{discordInteractionComponent, discordResponseDeferredUpdate},
		{discordInteractionAutocomplete, discordResponseAutocomplete},
		{discordInteractionModal, discordResponseMessage},
	}
	for _, c := range cases {
		body, _ := json.Marshal(map[string]any{"id": "1", "application_id": "2", "type": c.typ, "token": "t",
			"guild_id": "123456789012345678"})
		resp := h.send(t, body, nil, "")
		if out := decodeDiscordResponse(t, resp); out["type"] != c.want {
			t.Errorf("type %d answered %v, want %v", c.typ, out["type"], c.want)
		}
	}
	// An unknown type is a 400; a non-JSON body too.
	if resp := h.send(t, []byte(`{"type":99}`), nil, ""); resp.StatusCode != 400 {
		t.Errorf("unknown type = %d", resp.StatusCode)
	}
	if resp := h.send(t, []byte(`not json`), nil, ""); resp.StatusCode != 400 {
		t.Errorf("non-JSON = %d", resp.StatusCode)
	}
}

func TestDiscord_FollowupNotFoundFallsBackToBotToken(t *testing.T) {
	h := newDiscordHarness(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"content":"late answer"}`))
	})
	h.fake.status["PATCH /webhooks/"] = 404
	h.store.secrets["team-a/disc-creds/botToken"] = "bot-secret"
	h.send(t, discordCommand("123456789012345678", "987654321098765432", "555555555555555555", "hi"), nil, "")
	<-h.agentHits
	first := h.fake.next(t)
	if first.Method != http.MethodPatch {
		t.Fatalf("first = %s", first.Method)
	}
	second := h.fake.next(t)
	if second.Path != "/channels/987654321098765432/messages" || second.Authorization != "Bot bot-secret" {
		t.Errorf("fallback request = %s %s auth=%q", second.Method, second.Path, second.Authorization)
	}
	if c, _ := second.Body["content"].(string); !strings.HasPrefix(c, "<@555555555555555555> late answer") {
		t.Errorf("fallback content = %q", c)
	}
	if reason, ok := healthReason(h.userHarness, h.channel.Spec.Discord.Path); !ok || reason != healthReasonWebhookReady {
		t.Errorf("health after bot fallback = %q ok=%v", reason, ok)
	}
}

func TestDiscord_RefusedReplyRecordsCallbackRejected(t *testing.T) {
	h := newDiscordHarness(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"content":"x"}`))
	})
	// 404 without a bot token: terminal.
	h.fake.status["PATCH /webhooks/"] = 404
	h.send(t, discordCommand("123456789012345678", "9", "5", "hi"), nil, "")
	<-h.agentHits
	h.fake.next(t)
	waitFor(t, func() bool {
		reason, ok := healthReason(h.userHarness, h.channel.Spec.Discord.Path)
		return !ok && reason == healthReasonCallbackRejected
	})

	// 403: terminal at once, no retries.
	h.fake.status["PATCH /webhooks/"] = 403
	before := len(h.fake.requests)
	h.send(t, discordCommand("123456789012345678", "9", "5", "hi"), nil, "")
	<-h.agentHits
	h.fake.next(t)
	time.Sleep(150 * time.Millisecond) // longer than the harness backoff schedule
	h.fake.mu.Lock()
	n := len(h.fake.requests) - before
	h.fake.mu.Unlock()
	if n != 1 {
		t.Errorf("terminal 403 was retried: %d requests", n)
	}

	// 503 until exhaustion: every attempt in the schedule, then rejected.
	h.fake.status["PATCH /webhooks/"] = 503
	before = len(h.fake.requests)
	h.send(t, discordCommand("123456789012345678", "9", "5", "hi"), nil, "")
	<-h.agentHits
	for i := 0; i < 4; i++ {
		h.fake.next(t)
	}
	time.Sleep(100 * time.Millisecond)
	h.fake.mu.Lock()
	n = len(h.fake.requests) - before
	h.fake.mu.Unlock()
	if n != 4 {
		t.Errorf("retried bucket ran %d attempts, want 4", n)
	}
}

// discordCommandWith is discordCommand with caller-chosen reply-context
// fields, for the crafted-segment tests (#150).
func discordCommandWith(appID, token, guild, channel, user, message string) []byte {
	raw, _ := json.Marshal(map[string]any{
		"id": "1290000000000000001", "application_id": appID, "type": discordInteractionCommand,
		"token": token, "guild_id": guild, "channel_id": channel,
		"member": map[string]any{"user": map[string]any{"id": user, "username": "dev"}},
		"data": map[string]any{"name": "ask", "options": []map[string]any{
			{"name": "message", "type": 3, "value": message}}},
	})
	return raw
}

func TestDiscordSnowflakeAndTokenShape(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want bool
	}{
		{"1230000000000000000", true}, {"9", true},
		{"", false}, {"123456789012345678901", false}, {"12a", false},
		{"12/34", false}, {"..", false},
	} {
		if got := discordSnowflake(tc.in); got != tc.want {
			t.Errorf("discordSnowflake(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
	for _, tc := range []struct {
		in   string
		want bool
	}{
		{"tok-abc", true}, {"aW50ZXJhY3Rpb24.token_1-2", true},
		{"", false}, {"tok/../../evil", false}, {"tok?x=1", false},
		{"tok#frag", false}, {"tok%2e%2e", false},
		{strings.Repeat("a", 513), false},
	} {
		if got := discordTokenShape(tc.in); got != tc.want {
			t.Errorf("discordTokenShape(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// TestDiscord_CraftedReplySegmentsAreRefusedBeforeAnyDial: a channel owner
// controls the verifying key and can sign interactions with arbitrary
// application ids and tokens; a crafted value must terminate as a refused
// reply without a single request to the platform API (#150).
func TestDiscord_CraftedReplySegmentsAreRefusedBeforeAnyDial(t *testing.T) {
	h := newDiscordHarness(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"content":"x"}`))
	})
	h.send(t, discordCommandWith("1230000000000000000", "tok/../../evil",
		"123456789012345678", "9", "5", "hi"), nil, "")
	<-h.agentHits
	waitFor(t, func() bool {
		reason, ok := healthReason(h.userHarness, h.channel.Spec.Discord.Path)
		return !ok && reason == healthReasonCallbackRejected
	})
	h.fake.mu.Lock()
	n := len(h.fake.requests)
	h.fake.mu.Unlock()
	if n != 0 {
		t.Errorf("crafted token still produced %d platform requests", n)
	}
}

// TestDiscord_BotFallbackRefusesCraftedChannelID: the bot-token fallback
// appends the interaction's channel id to the API base; a non-snowflake
// value is refused before the fallback dials (#150).
func TestDiscord_BotFallbackRefusesCraftedChannelID(t *testing.T) {
	h := newDiscordHarness(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"content":"late"}`))
	})
	h.fake.status["PATCH /webhooks/"] = 404
	h.store.secrets["team-a/disc-creds/botToken"] = "bot-secret"
	h.send(t, discordCommandWith("1230000000000000000", "tok-abc",
		"123456789012345678", "987654321098765432/../../evil", "5", "hi"), nil, "")
	<-h.agentHits
	first := h.fake.next(t) // the follow-up PATCH that answers 404
	if first.Method != http.MethodPatch {
		t.Fatalf("first = %s", first.Method)
	}
	waitFor(t, func() bool {
		reason, ok := healthReason(h.userHarness, h.channel.Spec.Discord.Path)
		return !ok && reason == healthReasonCallbackRejected
	})
	h.fake.mu.Lock()
	n := len(h.fake.requests)
	h.fake.mu.Unlock()
	if n != 1 {
		t.Errorf("crafted channel id still produced %d platform requests, want the single 404 PATCH", n)
	}
}

func TestDiscord_PipelineErrorTravelsAsReplyText(t *testing.T) {
	h := newDiscordHarness(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(500) })
	h.send(t, discordCommand("123456789012345678", "9", "5", "hi"), nil, "")
	for i := 0; i < 4; i++ { // the delivery schedule
		<-h.agentHits
	}
	got := h.fake.next(t)
	if c, _ := got.Body["content"].(string); !strings.HasPrefix(c, errDeliveryFailed+": ") {
		t.Errorf("error reply = %q", c)
	}
	if reason, ok := healthReason(h.userHarness, h.channel.Spec.Discord.Path); ok || reason != healthReasonDispatchFailed {
		t.Errorf("health after a failed delivery = %q ok=%v", reason, ok)
	}
}

func TestDiscord_MissingAgentIsReportedInTheChat(t *testing.T) {
	h := newDiscordHarness(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	delete(h.store.agents, "team-a/sup")
	h.send(t, discordCommand("123456789012345678", "9", "5", "hi"), nil, "")
	got := h.fake.next(t)
	if c, _ := got.Body["content"].(string); !strings.Contains(c, "referenced Agent not found") {
		t.Errorf("reply = %q", c)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met")
}

func TestSplitChunks(t *testing.T) {
	if got := splitChunks("short", 10); len(got) != 1 || got[0] != "short" {
		t.Errorf("short: %q", got)
	}
	got := splitChunks("aaaa\nbbbb\ncccc", 9)
	if len(got) != 2 || got[0] != "aaaa\n" || got[1] != "bbbb\ncccc" {
		t.Errorf("newline split: %q", got)
	}
	// No newline in the back half of the window: a hard split on runes.
	got = splitChunks(strings.Repeat("é", 25), 10)
	if len(got) != 3 || len([]rune(got[0])) != 10 || len([]rune(got[2])) != 5 {
		t.Errorf("rune split: %d chunks, lens %d/%d", len(got), len([]rune(got[0])), len([]rune(got[len(got)-1])))
	}
}

func TestClassifyReplyStatus(t *testing.T) {
	cases := map[int]replyBucket{
		200: bucketDelivered, 204: bucketDelivered,
		400: bucketTerminal, 401: bucketTerminal, 403: bucketTerminal, 404: bucketTerminal,
		405: bucketTerminal, 410: bucketTerminal, 415: bucketTerminal,
		408: bucketRetried, 429: bucketRetried, 500: bucketRetried, 502: bucketRetried,
	}
	for status, want := range cases {
		if got := classifyReplyStatus(status); got != want {
			t.Errorf("%d = %v, want %v", status, got, want)
		}
	}
}
