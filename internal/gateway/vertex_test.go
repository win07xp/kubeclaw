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
	"net/http"
	"testing"
)

// TestVertexPathsNotServed locks the reserved status (#147): google-vertex
// has no inbound surface, so Vertex-shaped paths resolve no adapter, exactly
// like any unrecognized path.
func TestVertexPathsNotServed(t *testing.T) {
	base := "/v1/projects/p/locations/us/publishers/google/models/gw-vertex%2Fgemini-2:generateContent"
	if _, ok := adapterForPath(base); ok {
		t.Fatal("generateContent path must not resolve: google-vertex is reserved")
	}
	stream := "/v1/projects/p/locations/us/publishers/google/models/gw-vertex%2Fgemini-2:streamGenerateContent"
	if _, ok := adapterForPath(stream); ok {
		t.Fatal("streamGenerateContent path must not resolve: google-vertex is reserved")
	}
	if _, ok := adapterForPath("/v1/nonsense"); ok {
		t.Error("an unrecognized path must not resolve")
	}
	// The outbound half stays: the type still has its adapter.
	if _, ok := adapterForProviderType(providerTypeVertex); !ok {
		t.Error("the reserved type keeps its outbound adapter")
	}
}

func TestVertexUpstreamPath_ModelRewriteAndAltSSE(t *testing.T) {
	v := vertexAdapter{}
	// The qualified {providerRef}/{modelId} in the URL is rewritten to the raw
	// model ID (URL-encoded).
	in := "/v1/projects/p/locations/us/publishers/google/models/prov%2Fgemini-2:generateContent"
	got := v.upstreamPath(in, "gemini-2")
	if got != "/v1/projects/p/locations/us/publishers/google/models/gemini-2:generateContent" {
		t.Errorf("model not rewritten: %q", got)
	}
	// Streaming gets ?alt=sse appended when absent.
	streamIn := "/v1/projects/p/locations/us/publishers/google/models/prov%2Fgemini-2:streamGenerateContent"
	gotStream := v.upstreamPath(streamIn, "gemini-2")
	if gotStream != "/v1/projects/p/locations/us/publishers/google/models/gemini-2:streamGenerateContent?alt=sse" {
		t.Errorf("alt=sse not injected: %q", gotStream)
	}
	// An existing alt= is preserved (not doubled).
	withAlt := streamIn + "?alt=sse"
	if got := v.upstreamPath(withAlt, "gemini-2"); got != gotStream {
		t.Errorf("alt=sse should not be doubled: %q", got)
	}
}

func TestVertexUsageExtraction(t *testing.T) {
	v := vertexAdapter{}
	u, ok := v.extractUsage([]byte(`{"usageMetadata":{"promptTokenCount":88,"candidatesTokenCount":12}}`))
	if !ok || u.InputTokens != 88 || u.OutputTokens != 12 {
		t.Errorf("buffered usage wrong: %+v ok=%v", u, ok)
	}
	var acc Usage
	v.accumulateStreamUsage([]byte(`{"candidates":[{"content":{}}]}`), &acc)
	v.accumulateStreamUsage([]byte(`{"usageMetadata":{"promptTokenCount":5,"candidatesTokenCount":3}}`), &acc)
	if acc.InputTokens != 5 || acc.OutputTokens != 3 {
		t.Errorf("stream usage wrong: %+v", acc)
	}
}

// Grounding with Google Search reports executed queries in
// groundingMetadata.webSearchQueries; the adapter counts them as the
// "google_search" server tool.
func TestVertexUsageExtraction_Grounding(t *testing.T) {
	v := vertexAdapter{}
	u, ok := v.extractUsage([]byte(`{"usageMetadata":{"promptTokenCount":88,"candidatesTokenCount":12},` +
		`"candidates":[{"groundingMetadata":{"webSearchQueries":["euro 2024 winner","who won euro 2024"]}}]}`))
	if !ok || u.ServerTools["google_search"] != 2 {
		t.Errorf("grounded usage = %+v ok=%v, want google_search:2", u, ok)
	}

	// Streaming: groundingMetadata arrives on its own chunk before the final
	// usageMetadata chunk; both must survive.
	var acc Usage
	v.accumulateStreamUsage([]byte(`{"candidates":[{"groundingMetadata":{"webSearchQueries":["q1"]}}]}`), &acc)
	v.accumulateStreamUsage([]byte(`{"usageMetadata":{"promptTokenCount":5,"candidatesTokenCount":3}}`), &acc)
	if acc.InputTokens != 5 || acc.OutputTokens != 3 || acc.ServerTools["google_search"] != 1 {
		t.Errorf("grounded stream usage = %+v", acc)
	}

	// Ungrounded responses report no server tools.
	u, _ = v.extractUsage([]byte(`{"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1},"candidates":[{}]}`))
	if u.ServerTools != nil {
		t.Errorf("ungrounded response must yield nil ServerTools: %v", u.ServerTools)
	}
}

func TestVertexCredentialHeader(t *testing.T) {
	h := http.Header{}
	vertexAdapter{}.injectCredential(h, "ya29.minted-token")
	if h.Get("Authorization") != "Bearer ya29.minted-token" {
		t.Errorf("vertex credential must be a bearer token, got %q", h.Get("Authorization"))
	}
}
