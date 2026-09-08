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
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ktypes "k8s.io/apimachinery/pkg/types"

	kaalmv1beta1 "github.com/win07xp/kaalm/api/v1beta1"
)

type fakeCompletions struct {
	mu      sync.Mutex
	patched map[string]map[string]string // ns/task -> data
	fail    bool
}

func newFakeCompletions() *fakeCompletions {
	return &fakeCompletions{patched: map[string]map[string]string{}}
}

func (f *fakeCompletions) PatchMailbox(_ context.Context, ns, task string, data map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return context.DeadlineExceeded
	}
	f.patched[ns+"/"+task] = data
	return nil
}

// seedTask installs an agentReported task whose Pod UID matches the harness
// source IP (127.0.0.1).
func seedTask(h *harness, name string, mutate func(*kaalmv1beta1.AgentTask)) {
	task := &kaalmv1beta1.AgentTask{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "team-a"},
		Spec: kaalmv1beta1.AgentTaskSpec{
			AgentClassRef: kaalmv1beta1.LocalObjectReference{Name: "std"},
			Artifacts:     []kaalmv1beta1.AgentTaskArtifact{{Name: "pr-url"}},
		},
		Status: kaalmv1beta1.AgentTaskStatus{
			Phase:         kaalmv1beta1.TaskRunning,
			CurrentPodUID: "uid-live",
		},
	}
	if mutate != nil {
		mutate(task)
	}
	h.store.tasks["team-a/"+name] = task
	h.store.podsByIP["127.0.0.1"] = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name + "-pod", Namespace: "team-a", UID: ktypes.UID("uid-live")},
	}
}

// bodyContains drains the response and reports whether it contains needle.
func bodyContains(t *testing.T, resp *http.Response, needle string) bool {
	t.Helper()
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	return strings.Contains(string(raw), needle)
}

func TestTaskComplete_HappyPath(t *testing.T) {
	h := newHarness(t, func(w http.ResponseWriter, _ *http.Request) {})
	completions := newFakeCompletions()
	h.server.Completions = completions
	seedTask(h, "fix-42", nil)
	cert := h.ca.issue(t, "fix-42.team-a.task.kaalm.io")

	resp := postJSON(t, h.client(&cert), h.url("/v1/task/complete"), map[string]any{
		"status": "success", "message": "PR opened",
		"artifacts": map[string]string{"pr-url": "https://example.com/pr/1"},
	}, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	data := completions.patched["team-a/fix-42"]
	if data[CompletionKeyStatus] != CompletionStatusSuccess || data["artifact.pr-url"] == "" || data[CompletionKeyMessage] != "PR opened" {
		t.Errorf("mailbox data wrong: %v", data)
	}
}

// TestTaskComplete_InformerLagLiveFallback covers the new-Pod window (#148):
// the calling Pod is not in the informer cache yet, but the live
// namespace-narrowed lookup finds it, so the middleware cross-check and the
// UID gate both pass and the completion lands.
func TestTaskComplete_InformerLagLiveFallback(t *testing.T) {
	h := newHarness(t, func(w http.ResponseWriter, _ *http.Request) {})
	completions := newFakeCompletions()
	h.server.Completions = completions
	seedTask(h, "lag-task", nil)
	// Move the Pod out of the informer view: only the live lookup sees it.
	h.store.livePodsByIP["127.0.0.1"] = h.store.podsByIP["127.0.0.1"]
	delete(h.store.podsByIP, "127.0.0.1")
	cert := h.ca.issue(t, "lag-task.team-a.task.kaalm.io")

	resp := postJSON(t, h.client(&cert), h.url("/v1/task/complete"), map[string]any{
		"status": "success", "artifacts": map[string]string{"pr-url": "https://example.com/pr/2"},
	}, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d, want 200 via the live fallback", resp.StatusCode)
	}
	if completions.patched["team-a/lag-task"] == nil {
		t.Error("mailbox was not patched")
	}

	// A live Pod in another namespace does not answer for this caller: the
	// narrowed query misses and the cross-check stays a 401.
	seedTask(h, "lag-task-2", nil)
	other := h.store.podsByIP["127.0.0.1"]
	other.Namespace = "team-b"
	h.store.livePodsByIP["127.0.0.1"] = other
	delete(h.store.podsByIP, "127.0.0.1")
	cert2 := h.ca.issue(t, "lag-task-2.team-a.task.kaalm.io")
	resp2 := postJSON(t, h.client(&cert2), h.url("/v1/task/complete"), map[string]any{
		"status": "success", "artifacts": map[string]string{"pr-url": "https://example.com/pr/3"},
	}, nil)
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != 401 {
		t.Fatalf("status %d, want 401 for a cross-namespace live Pod", resp2.StatusCode)
	}
}

func TestTaskComplete_Gates(t *testing.T) {
	h := newHarness(t, func(w http.ResponseWriter, _ *http.Request) {})
	completions := newFakeCompletions()
	h.server.Completions = completions

	// exitCode task: TaskNotAgentReported.
	seedTask(h, "exit-task", func(task *kaalmv1beta1.AgentTask) {
		task.Spec.Completion.Condition = "exitCode"
	})
	cert := h.ca.issue(t, "exit-task.team-a.task.kaalm.io")
	resp := postJSON(t, h.client(&cert), h.url("/v1/task/complete"), map[string]any{"status": "success"}, nil)
	if resp.StatusCode != 403 || !bodyContains(t, resp, "TaskNotAgentReported") {
		t.Errorf("exitCode task = %d", resp.StatusCode)
	}

	// Terminal phase: TaskAlreadyCompleted.
	seedTask(h, "done-task", func(task *kaalmv1beta1.AgentTask) {
		task.Status.Phase = kaalmv1beta1.TaskSucceeded
		task.Spec.Artifacts = nil
	})
	cert = h.ca.issue(t, "done-task.team-a.task.kaalm.io")
	resp = postJSON(t, h.client(&cert), h.url("/v1/task/complete"), map[string]any{"status": "success"}, nil)
	if resp.StatusCode != 403 || !bodyContains(t, resp, "TaskAlreadyCompleted") {
		t.Errorf("terminal task = %d", resp.StatusCode)
	}

	// Stale UID: StalePodCompletion, retryable.
	seedTask(h, "stale-task", func(task *kaalmv1beta1.AgentTask) {
		task.Status.CurrentPodUID = "uid-other"
		task.Spec.Artifacts = nil
	})
	cert = h.ca.issue(t, "stale-task.team-a.task.kaalm.io")
	resp = postJSON(t, h.client(&cert), h.url("/v1/task/complete"), map[string]any{"status": "success"}, nil)
	if resp.StatusCode != 403 || !bodyContains(t, resp, "StalePodCompletion") {
		t.Errorf("stale pod = %d", resp.StatusCode)
	}

	// Artifact validation: success missing a declared artifact is 400.
	seedTask(h, "arts-task", nil)
	cert = h.ca.issue(t, "arts-task.team-a.task.kaalm.io")
	resp = postJSON(t, h.client(&cert), h.url("/v1/task/complete"), map[string]any{"status": "success"}, nil)
	if resp.StatusCode != 400 || !bodyContains(t, resp, "missing declared artifact") {
		t.Errorf("missing artifact = %d", resp.StatusCode)
	}
	// Failure with a subset passes.
	resp = postJSON(t, h.client(&cert), h.url("/v1/task/complete"),
		map[string]any{"status": "failure", "message": "boom"}, nil)
	if resp.StatusCode != 200 {
		t.Errorf("failure subset = %d, want 200", resp.StatusCode)
	}

	// Oversized artifact: 413.
	seedTask(h, "big-task", nil)
	cert = h.ca.issue(t, "big-task.team-a.task.kaalm.io")
	resp = postJSON(t, h.client(&cert), h.url("/v1/task/complete"), map[string]any{
		"status": "success", "artifacts": map[string]string{"pr-url": strings.Repeat("x", 5<<10)},
	}, nil)
	if resp.StatusCode != 413 {
		t.Errorf("oversized artifact = %d, want 413", resp.StatusCode)
	}
	_ = resp.Body.Close()
}

func TestValidateCompletionArtifacts(t *testing.T) {
	declared := []kaalmv1beta1.AgentTaskArtifact{{Name: "a"}, {Name: "b"}}
	if msg := ValidateCompletionArtifacts(CompletionStatusSuccess, map[string]string{"a": "1", "b": "2"}, declared); msg != "" {
		t.Errorf("complete success set must pass: %s", msg)
	}
	if msg := ValidateCompletionArtifacts(CompletionStatusSuccess, map[string]string{"a": "1"}, declared); msg == "" {
		t.Error("missing declared must fail on success")
	}
	if msg := ValidateCompletionArtifacts("failure", map[string]string{"a": "1"}, declared); msg != "" {
		t.Errorf("subset must pass on failure: %s", msg)
	}
	if msg := ValidateCompletionArtifacts("failure", map[string]string{"rogue": "1"}, declared); msg == "" {
		t.Error("undeclared must fail in both branches")
	}
}

func TestParseCompletionData(t *testing.T) {
	data := map[string]string{
		CompletionKeyStatus:                 CompletionStatusSuccess,
		CompletionKeyMessage:                "done",
		CompletionArtifactPrefix + "pr-url": "https://x/1",
		CompletionArtifactPrefix + "log":    "tail",
		"unrelated":                         "ignored",
	}
	status, message, artifacts := ParseCompletionData(data)
	if status != CompletionStatusSuccess || message != "done" {
		t.Errorf("status/message wrong: %q %q", status, message)
	}
	if artifacts["pr-url"] != "https://x/1" || artifacts["log"] != "tail" {
		t.Errorf("artifacts wrong: %v", artifacts)
	}
	if _, ok := artifacts["unrelated"]; ok {
		t.Error("non-artifact keys must be dropped")
	}
	if _, ok := artifacts["status"]; ok {
		t.Error("status key must not appear as artifact")
	}
}

func TestCompletionMailboxName(t *testing.T) {
	if got := CompletionMailboxName("fix-42"); got != "fix-42-completion" {
		t.Errorf("CompletionMailboxName = %q", got)
	}
}

func TestTaskComplete_BadRequestsAndPatchFailure(t *testing.T) {
	h := newHarness(t, func(w http.ResponseWriter, _ *http.Request) {})
	completions := newFakeCompletions()
	h.server.Completions = completions
	seedTask(h, "fix-42", func(task *kaalmv1beta1.AgentTask) { task.Spec.Artifacts = nil })
	cert := h.ca.issue(t, "fix-42.team-a.task.kaalm.io")
	client := h.client(&cert)

	// Non-JSON body: 400.
	req, _ := http.NewRequest(http.MethodPost, h.url("/v1/task/complete"), strings.NewReader("not-json"))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 400 {
		t.Errorf("non-JSON body = %d, want 400", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// Invalid status value: 400.
	resp = postJSON(t, client, h.url("/v1/task/complete"), map[string]any{"status": "weird"}, nil)
	if resp.StatusCode != 400 || !bodyContains(t, resp, "failure") {
		t.Errorf("invalid status = %d", resp.StatusCode)
	}

	// Total-size cap: many small artifacts that are individually fine but
	// together exceed 32 KiB. Declare them so validation passes.
	big := map[string]string{}
	declared := []kaalmv1beta1.AgentTaskArtifact{}
	for i := 0; i < 10; i++ {
		name := string(rune('a'+i)) + "-art"
		big[name] = strings.Repeat("y", 4<<10)
		declared = append(declared, kaalmv1beta1.AgentTaskArtifact{Name: name})
	}
	seedTask(h, "big-total", func(task *kaalmv1beta1.AgentTask) { task.Spec.Artifacts = declared })
	certBig := h.ca.issue(t, "big-total.team-a.task.kaalm.io")
	resp = postJSON(t, h.client(&certBig), h.url("/v1/task/complete"),
		map[string]any{"status": CompletionStatusSuccess, "artifacts": big}, nil)
	if resp.StatusCode != 413 || !bodyContains(t, resp, "combined") {
		t.Errorf("total-size cap = %d, want 413", resp.StatusCode)
	}

	// Mailbox patch failure: 503.
	completions.fail = true
	resp = postJSON(t, client, h.url("/v1/task/complete"), map[string]any{"status": CompletionStatusSuccess}, nil)
	if resp.StatusCode != 503 {
		t.Errorf("patch failure = %d, want 503", resp.StatusCode)
	}
	_ = resp.Body.Close()
}

func TestTaskComplete_NoTaskBacksCaller(t *testing.T) {
	h := newHarness(t, func(w http.ResponseWriter, _ *http.Request) {})
	h.server.Completions = newFakeCompletions()
	// No task seeded for this SAN.
	cert := h.ca.issue(t, "ghost.team-a.task.kaalm.io")
	h.store.podsByIP["127.0.0.1"] = &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a"}}
	resp := postJSON(t, h.client(&cert), h.url("/v1/task/complete"), map[string]any{"status": CompletionStatusSuccess}, nil)
	if resp.StatusCode != 403 || !bodyContains(t, resp, "no AgentTask") {
		t.Errorf("no backing task = %d", resp.StatusCode)
	}
}
