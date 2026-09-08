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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	kaalmv1beta1 "github.com/win07xp/kaalm/api/v1beta1"
)

// Completion mailbox wire format: the data keys the gateway writes into the
// {taskName}-completion ConfigMap and the AgentTaskReconciler reads. An empty
// mailbox has no status key.
const (
	CompletionKeyStatus      = "status"  // "success" | "failure"
	CompletionKeyMessage     = "message" // optional human-readable text
	CompletionArtifactPrefix = "artifact."

	CompletionStatusSuccess = "success"
	CompletionStatusFailure = "failure"

	// Artifact size caps: per-value and total, measured in UTF-8 bytes of
	// the message and artifact VALUES (keys are bounded by ConfigMap rules).
	maxArtifactBytes      = 4 << 10
	maxArtifactTotalBytes = 32 << 10
)

// CompletionMailboxName returns the per-task mailbox ConfigMap name.
func CompletionMailboxName(taskName string) string { return taskName + "-completion" }

// ValidateCompletionArtifacts applies the per-status rule: on success every
// declared name must be present and none undeclared; on failure only the
// no-undeclared rule holds (a crashing task may report a subset). Returns ""
// when valid, else a message naming the offender.
func ValidateCompletionArtifacts(status string, artifacts map[string]string, declared []kaalmv1beta1.AgentTaskArtifact) string {
	names := map[string]bool{}
	for _, a := range declared {
		names[a.Name] = true
	}
	for k := range artifacts {
		if !names[k] {
			return "undeclared artifact in payload: " + k
		}
	}
	if status == CompletionStatusSuccess {
		for _, a := range declared {
			if _, ok := artifacts[a.Name]; !ok {
				return "missing declared artifact: " + a.Name
			}
		}
	}
	return ""
}

// CompletionWriter patches the per-task completion mailbox. The production
// implementation goes through the name-scoped per-task Role.
type CompletionWriter interface {
	PatchMailbox(ctx context.Context, namespace, taskName string, data map[string]string) error
}

// taskCompleteRequest is the POST /v1/task/complete body.
type taskCompleteRequest struct {
	Status    string            `json:"status"`
	Message   string            `json:"message,omitempty"`
	Artifacts map[string]string `json:"artifacts,omitempty"`
}

// handleTaskComplete implements POST /v1/task/complete. The middleware has
// already enforced mTLS and the AgentTask kind; this handler runs the mode,
// terminal-phase, and identity gates in order, then validates and patches.
// See docs/src/gateways/api/task-complete.md.
func (s *Server) handleTaskComplete(w http.ResponseWriter, r *http.Request) {
	c := callerFrom(r.Context())
	task, ok := s.Store.TaskByName(r.Context(), c.Namespace, c.Workload.Name)
	if !ok {
		forbidden(w, errAccessDenied, "no AgentTask backs this caller")
		return
	}
	// (b) exitCode tasks have no completion mailbox.
	if task.Spec.Completion.Condition == "exitCode" {
		writeError(w, http.StatusForbidden, errorBody{
			Type: errAccessDenied, Message: "TaskNotAgentReported: this task completes via container exit"}, 0)
		return
	}
	// (d) terminal phases reject further writes.
	switch task.Status.Phase {
	case kaalmv1beta1.TaskSucceeded, kaalmv1beta1.TaskFailed, kaalmv1beta1.TaskTimedOut:
		writeError(w, http.StatusForbidden, errorBody{
			Type: errAccessDenied, Message: "TaskAlreadyCompleted: the task has reached a terminal phase"}, 0)
		return
	}
	// (c) the identity gate: the calling Pod's UID must match
	// status.currentPodUID. Resolved from the source IP, with the live
	// namespace-narrowed fallback on an informer miss (task-complete.md,
	// cross-check step 2; #148); retryable because the benign informer-lag
	// race on currentPodUID itself shares this rejection.
	if !s.Config.DisableSourceIPCheck {
		ip := sourceIP(r)
		pod, found := s.Store.PodByIP(r.Context(), ip)
		if !found {
			pod, found = s.Store.PodByIPLive(r.Context(), c.Namespace, ip)
		}
		if !found || task.Status.CurrentPodUID == "" || string(pod.UID) != task.Status.CurrentPodUID {
			writeError(w, http.StatusForbidden, errorBody{
				Type: errAccessDenied, Retryable: true,
				Message: "StalePodCompletion: the calling Pod is not the task's current Pod"}, 0)
			return
		}
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		badRequest(w, "reading request body: "+err.Error())
		return
	}
	var req taskCompleteRequest
	if err := json.Unmarshal(body, &req); err != nil {
		badRequest(w, "request body is not valid JSON")
		return
	}
	if req.Status != CompletionStatusSuccess && req.Status != CompletionStatusFailure {
		badRequest(w, `status must be "success" or "failure"`)
		return
	}
	if msg := ValidateCompletionArtifacts(req.Status, req.Artifacts, task.Spec.Artifacts); msg != "" {
		badRequest(w, msg)
		return
	}
	// Size caps: 4 KiB per artifact value, 32 KiB total (message included).
	total := len(req.Message)
	for k, v := range req.Artifacts {
		if len(v) > maxArtifactBytes {
			writeError(w, http.StatusRequestEntityTooLarge, errorBody{
				Type:    errRequestTooLarge,
				Message: fmt.Sprintf("artifact %q exceeds %d bytes; externalize and reference by URL", k, maxArtifactBytes)}, 0)
			return
		}
		total += len(v)
	}
	if total > maxArtifactTotalBytes {
		writeError(w, http.StatusRequestEntityTooLarge, errorBody{
			Type:    errRequestTooLarge,
			Message: fmt.Sprintf("combined artifact payload exceeds %d bytes", maxArtifactTotalBytes)}, 0)
		return
	}

	data := map[string]string{CompletionKeyStatus: req.Status}
	if req.Message != "" {
		data[CompletionKeyMessage] = req.Message
	}
	for k, v := range req.Artifacts {
		data[CompletionArtifactPrefix+k] = v
	}
	if err := s.Completions.PatchMailbox(r.Context(), c.Namespace, c.Workload.Name, data); err != nil {
		writeError(w, http.StatusServiceUnavailable, errorBody{
			Type: errInternalUnavailable, Retryable: true,
			Message: "patching the completion ConfigMap failed"}, 1)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// parseCompletionData is the read-side decode of the mailbox layout, shared
// with the AgentTaskReconciler.
func ParseCompletionData(data map[string]string) (status, message string, artifacts map[string]string) {
	artifacts = map[string]string{}
	for k, v := range data {
		switch k {
		case CompletionKeyStatus:
			status = v
		case CompletionKeyMessage:
			message = v
		default:
			if name, ok := strings.CutPrefix(k, CompletionArtifactPrefix); ok {
				artifacts[name] = v
			}
		}
	}
	return status, message, artifacts
}
