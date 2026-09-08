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

package controller

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	webhookconversion "sigs.k8s.io/controller-runtime/pkg/webhook/conversion"

	kaalmv1alpha1 "github.com/win07xp/kaalm/api/v1alpha1"
	kaalmv1beta1 "github.com/win07xp/kaalm/api/v1beta1"
)

func TestMaxBytesHandler(t *testing.T) {
	var read []byte
	var readErr error
	inner := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		read, readErr = io.ReadAll(r.Body)
	})
	h := MaxBytesHandler(inner, 16)

	// Under the limit: the body arrives intact.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/convert", strings.NewReader("small body")))
	if readErr != nil || string(read) != "small body" {
		t.Fatalf("under-limit read = %q err=%v", read, readErr)
	}

	// Over the limit: the read errors instead of consuming the stream.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/convert",
		bytes.NewReader(bytes.Repeat([]byte("x"), 64))))
	if readErr == nil {
		t.Fatal("over-limit body must surface a read error")
	}
}

// TestConversionHandlerBounded proves the wrapper composes with the real
// conversion handler: an oversized POST answers an error response rather
// than streaming into memory (#152).
func TestConversionHandlerBounded(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := kaalmv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := kaalmv1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	h := MaxBytesHandler(webhookconversion.NewWebhookHandler(scheme), 1024)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/convert",
		bytes.NewReader(bytes.Repeat([]byte("a"), 4096)))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)
	if rec.Code < 400 || rec.Code >= 500 {
		t.Fatalf("oversized ConversionReview answered %d, want a 4xx refusal", rec.Code)
	}
}
