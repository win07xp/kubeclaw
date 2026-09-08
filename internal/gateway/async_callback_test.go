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
	"crypto/x509"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kaalmv1beta1 "github.com/win07xp/kaalm/api/v1beta1"
)

// callbackServer starts a TLS server signed by ca for the given DNS name,
// returning its listener address and a channel receiving each request's
// signature header.
func callbackServer(t *testing.T, ca *testCA, dnsName string, status int) (addr string, sigs chan string) {
	t.Helper()
	sigs = make(chan string, 4)
	cert := ca.issue(t, dnsName)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{
		ReadHeaderTimeout: time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			sigs <- r.Header.Get("X-Sig")
			w.WriteHeader(status)
		}),
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return ln.Addr().String(), sigs
}

func callbackClientServer(t *testing.T, ca *testCA) *Server {
	t.Helper()
	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)
	return &Server{Config: Config{CallbackCAs: pool, AgentReadTimeout: 5 * time.Second}}
}

func TestDialCallbackOnce_SuccessAndSigning(t *testing.T) {
	ca := newTestCA(t)
	addr, sigs := callbackServer(t, ca, "callback.test", http.StatusOK)
	_, port, _ := net.SplitHostPort(addr)

	s := callbackClientServer(t, ca)
	parsed, _ := url.Parse("https://callback.test:" + port + "/cb")
	ch := &kaalmv1beta1.AgentChannel{
		ObjectMeta: metav1.ObjectMeta{Name: "ch", Namespace: "team-a"},
		Spec: kaalmv1beta1.AgentChannelSpec{Webhook: &kaalmv1beta1.AgentChannelWebhook{
			CallbackAuth: &kaalmv1beta1.ChannelAuth{
				Type: authTypeHMAC,
				HMAC: &kaalmv1beta1.ChannelHMAC{Header: "X-Sig", Algorithm: "sha256"},
			},
		}},
	}
	status, err := s.dialCallbackOnce(context.Background(), parsed, net.ParseIP("127.0.0.1"),
		ch, "shh", "req-1", []byte(`{"x":1}`))
	if err != nil || status != http.StatusOK {
		t.Fatalf("dialCallbackOnce = %d err=%v", status, err)
	}
	select {
	case sig := <-sigs:
		if sig == "" {
			t.Error("callback must carry the HMAC signature header")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("callback server never received the request")
	}

	// Terminal status is still returned to the caller (no error).
	addr2, _ := callbackServer(t, ca, "callback.test", http.StatusForbidden)
	_, port2, _ := net.SplitHostPort(addr2)
	parsed2, _ := url.Parse("https://callback.test:" + port2 + "/cb")
	status, err = s.dialCallbackOnce(context.Background(), parsed2, net.ParseIP("127.0.0.1"),
		ch, "shh", "req-2", []byte(`{}`))
	if err != nil || status != http.StatusForbidden {
		t.Errorf("terminal dial = %d err=%v", status, err)
	}

	// A default 443 port with nothing listening yields a transport error.
	parsedNoPort, _ := url.Parse("https://callback.test/cb")
	if _, err := s.dialCallbackOnce(context.Background(), parsedNoPort, net.ParseIP("127.0.0.1"),
		ch, "shh", "req-3", []byte(`{}`)); err == nil {
		t.Error("dial to a closed default port must error")
	}
}

// TestDialCallbackOnce_RedirectRefused: the callback dialer never follows a
// redirect (#153). The pinned transport already keeps one on the checked
// host; refusing makes the posture explicit and the attempt fails as a
// transport error (the retried bucket).
func TestDialCallbackOnce_RedirectRefused(t *testing.T) {
	ca := newTestCA(t)
	cert := ca.issue(t, "callback.test")
	hits := make(chan string, 4)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert},
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{
		ReadHeaderTimeout: time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits <- r.URL.Path
			w.Header().Set("Location", "https://callback.test/elsewhere")
			w.WriteHeader(http.StatusFound)
		}),
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	_, port, _ := net.SplitHostPort(ln.Addr().String())

	s := callbackClientServer(t, ca)
	parsed, _ := url.Parse("https://callback.test:" + port + "/cb")
	ch := &kaalmv1beta1.AgentChannel{
		ObjectMeta: metav1.ObjectMeta{Name: "ch", Namespace: "team-a"},
		Spec:       kaalmv1beta1.AgentChannelSpec{Webhook: &kaalmv1beta1.AgentChannelWebhook{}},
	}
	_, err = s.dialCallbackOnce(context.Background(), parsed, net.ParseIP("127.0.0.1"),
		ch, "", "req-r", []byte(`{}`))
	if err == nil {
		t.Fatal("a redirect answer must surface as an error, not be followed")
	}
	if got := <-hits; got != "/cb" {
		t.Errorf("first request path = %q", got)
	}
	select {
	case p := <-hits:
		t.Errorf("redirect was followed to %q", p)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestSendCallback_Rejections(t *testing.T) {
	s := &Server{ChannelHealth: NewChannelHealthStore(0)}
	base := &kaalmv1beta1.AgentChannel{
		ObjectMeta: metav1.ObjectMeta{Name: "ch", Namespace: "team-a"},
		Spec:       kaalmv1beta1.AgentChannelSpec{Webhook: &kaalmv1beta1.AgentChannelWebhook{Path: "/channels/team-a/hook"}},
	}

	// Non-HTTPS callback URL is invalid.
	httpURL := "http://insecure.example.com/cb"
	ch := base.DeepCopy()
	ch.Spec.Webhook.CallbackURL = &httpURL
	if got := s.sendCallback(context.Background(), ch, "r1", []byte(`{}`)); got != callbackInvalid {
		t.Errorf("non-https callback outcome = %q, want %q", got, callbackInvalid)
	}

	// Unparseable URL.
	badURL := "://nope"
	ch2 := base.DeepCopy()
	ch2.Spec.Webhook.CallbackURL = &badURL
	if got := s.sendCallback(context.Background(), ch2, "r2", []byte(`{}`)); got != callbackInvalid {
		t.Errorf("unparseable callback URL outcome = %q, want %q", got, callbackInvalid)
	}

	// HTTPS with callbackAuth whose secret cannot be resolved.
	httpsURL := "https://8.8.8.8/cb"
	ch3 := base.DeepCopy()
	ch3.Spec.Webhook.CallbackURL = &httpsURL
	ch3.Spec.Webhook.CallbackAuth = &kaalmv1beta1.ChannelAuth{Type: authTypeBearer} // no secretRef
	s.Store = newFakeStore()
	if got := s.sendCallback(context.Background(), ch3, "r3", []byte(`{}`)); got != callbackInvalid {
		t.Errorf("unresolvable callbackAuth secret outcome = %q, want %q", got, callbackInvalid)
	}
}

func TestPollRetryAfter(t *testing.T) {
	cases := []struct {
		elapsed time.Duration
		want    int
	}{
		{time.Second, 2},
		{4 * time.Second, 4},
		{10 * time.Second, 8},
		{20 * time.Second, 16},
		{time.Minute, 30},
	}
	for _, c := range cases {
		if got := pollRetryAfter(c.elapsed); got != c.want {
			t.Errorf("pollRetryAfter(%s) = %d, want %d", c.elapsed, got, c.want)
		}
	}
}

func TestHandlePoll_InputValidation(t *testing.T) {
	h := newUserHarness(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	h.seedChannel("async")

	// Wrong method: 401 (posture: no path-existence leak).
	req, _ := http.NewRequest(http.MethodPost, h.userSrv.URL+"/v1/channels/responses/req-1?channelPath=x", nil)
	resp, err := h.userSrv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Errorf("POST to poll = %d, want 401", resp.StatusCode)
	}

	// Missing channelPath: 400.
	req, _ = http.NewRequest(http.MethodGet, h.userSrv.URL+"/v1/channels/responses/req-1", nil)
	resp, _ = h.userSrv.Client().Do(req)
	_ = resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Errorf("missing channelPath = %d, want 400", resp.StatusCode)
	}

	// Unknown channelPath: 401 (does not reveal which paths exist).
	req, _ = http.NewRequest(http.MethodGet, h.userSrv.URL+"/v1/channels/responses/req-1?channelPath=%2Fchannels%2Fteam-a%2Fghost", nil)
	req.Header.Set("Authorization", "Bearer hook-token")
	resp, _ = h.userSrv.Client().Do(req)
	_ = resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Errorf("unknown channelPath = %d, want 401", resp.StatusCode)
	}

	// Known channel, wrong token: auth failure 401.
	req, _ = http.NewRequest(http.MethodGet, h.userSrv.URL+"/v1/channels/responses/req-1?channelPath=%2Fchannels%2Fteam-a%2Fsupport", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	resp, _ = h.userSrv.Client().Do(req)
	_ = resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Errorf("bad poll auth = %d, want 401", resp.StatusCode)
	}

	// Authenticated but unknown requestId: 404.
	req, _ = http.NewRequest(http.MethodGet, h.userSrv.URL+"/v1/channels/responses/ghost-id?channelPath=%2Fchannels%2Fteam-a%2Fsupport", nil)
	req.Header.Set("Authorization", "Bearer hook-token")
	resp, _ = h.userSrv.Client().Do(req)
	_ = resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Errorf("unknown requestId = %d, want 404", resp.StatusCode)
	}
}

func TestHandlePoll_TTLExpired(t *testing.T) {
	h := newUserHarness(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	h.seedChannel("async")
	// Seed a record older than the TTL directly in the fake store.
	h.async.records["old"] = &AsyncRecord{
		CreatedAt: time.Now().Add(-2 * asyncTTL), ChannelNamespace: "team-a", ChannelName: "support",
	}
	req, _ := http.NewRequest(http.MethodGet, h.userSrv.URL+"/v1/channels/responses/old?channelPath=%2Fchannels%2Fteam-a%2Fsupport", nil)
	req.Header.Set("Authorization", "Bearer hook-token")
	resp, err := h.userSrv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Errorf("expired record = %d, want 404", resp.StatusCode)
	}
}
