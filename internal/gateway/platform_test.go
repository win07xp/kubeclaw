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
	"fmt"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	kaalmv1beta1 "github.com/win07xp/kaalm/api/v1beta1"
)

func TestTruncatedReplyBody(t *testing.T) {
	if got := truncatedReplyBody([]byte("  {\"code\": 50027}  ")); got != `{"code": 50027}` {
		t.Errorf("short body = %q", got)
	}
	long := strings.Repeat("x", 64<<10)
	got := truncatedReplyBody([]byte(long))
	if len(got) > maxReplyRefusalDetail+len("... (truncated)") {
		t.Errorf("truncated body still %d bytes", len(got))
	}
	if !strings.HasSuffix(got, "... (truncated)") {
		t.Errorf("truncation marker missing: %q", got[len(got)-32:])
	}
	// Arbitrary bytes must reduce to valid UTF-8 for etcd.
	if got := truncatedReplyBody([]byte{0xff, 0xfe, 'o', 'k'}); !utf8.ValidString(got) {
		t.Errorf("invalid UTF-8 survived: %q", got)
	}
	// A truncation cut mid-rune must not leave a partial encoding behind.
	runes := strings.Repeat("\u20ac", maxReplyRefusalDetail) // 3 bytes each; the cut lands mid-rune
	if got := truncatedReplyBody([]byte(runes)); !utf8.ValidString(got) {
		t.Errorf("mid-rune cut produced invalid UTF-8")
	}
}

// eventCapture records Eventf messages for assertions.
type eventCapture struct {
	mu       sync.Mutex
	messages []string
}

func (c *eventCapture) Eventf(_ runtime.Object, _, _, messageFmt string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.messages = append(c.messages, fmt.Sprintf(messageFmt, args...))
}

// TestReplyRefused_BoundsEventDetail: a 64 KiB platform error body must not
// travel into the Warning event or the health observation (#151).
func TestReplyRefused_BoundsEventDetail(t *testing.T) {
	capture := &eventCapture{}
	s := &Server{ChannelHealth: NewChannelHealthStore(0), Recorder: capture}
	channel := &kaalmv1beta1.AgentChannel{
		ObjectMeta: metav1.ObjectMeta{Name: "disc", Namespace: "team-a"},
		Spec: kaalmv1beta1.AgentChannelSpec{
			Type:    kaalmv1beta1.ChannelTypeDiscord,
			Discord: &kaalmv1beta1.AgentChannelDiscord{Path: "/channels/team-a/disc"},
		},
	}
	huge := []byte(strings.Repeat("z", 64<<10))
	outcome := s.replyRefused(channel, "discord", replyResult{bucket: bucketTerminal, status: 400, body: huge})
	if outcome != callbackRejected {
		t.Fatalf("outcome = %q", outcome)
	}

	capture.mu.Lock()
	defer capture.mu.Unlock()
	if len(capture.messages) != 1 {
		t.Fatalf("events = %d, want 1", len(capture.messages))
	}
	if len(capture.messages[0]) > 1024 {
		t.Errorf("event detail is %d bytes; the platform body was not truncated", len(capture.messages[0]))
	}

	s.ChannelHealth.mu.Lock()
	obs := s.ChannelHealth.observations[channel.Spec.Path()]
	s.ChannelHealth.mu.Unlock()
	if len(obs) != 1 {
		t.Fatalf("observations = %d, want 1", len(obs))
	}
	if len(obs[0].lastError) > 1024 {
		t.Errorf("health detail is %d bytes; the platform body was not truncated", len(obs[0].lastError))
	}
}
