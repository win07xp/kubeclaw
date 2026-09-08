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

import "net/http"

// MaxConversionBodyBytes caps a ConversionReview request body. The
// conversion listener requires no client authentication by design (the
// apiserver presents no certificate), and controller-runtime's conversion
// handler decodes the body with no limit of its own, so without a cap any
// in-cluster caller can stream an unbounded body into controller memory
// (#152). 64 MiB is far above any real review: the apiserver batches the
// objects of one API request, and Kaalm resources are kilobytes each.
const MaxConversionBodyBytes = 64 << 20

// MaxBytesHandler bounds the request body before next reads it. An
// oversized body surfaces to next as a read error, which the conversion
// handler answers as a malformed review.
func MaxBytesHandler(next http.Handler, limit int64) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, limit)
		next.ServeHTTP(w, r)
	})
}
