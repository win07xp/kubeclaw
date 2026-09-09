// Command gateway is the Kaalm Gateway: the cluster listener on :8443 (the
// LLM proxy, the MCP tool broker, and the internal endpoints, with per-path
// client authentication), the Ingress-fronted user listener on :8080, and a
// dedicated health port. See docs/src/gateways/.
package main

import (
	"reflect"
	"testing"
	"time"
)

func TestParseBackoff(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want []time.Duration
	}{
		{
			name: "empty string returns nil (Config default)",
			raw:  "",
			want: nil,
		},
		{
			name: "single duration",
			raw:  "1s",
			want: []time.Duration{time.Second},
		},
		{
			name: "typical backoff schedule",
			raw:  "1s,5s,25s",
			want: []time.Duration{time.Second, 5 * time.Second, 25 * time.Second},
		},
		{
			name: "entries with surrounding whitespace are trimmed",
			raw:  " 1s , 5s ,25s ",
			want: []time.Duration{time.Second, 5 * time.Second, 25 * time.Second},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseBackoff(tt.raw)
			if err != nil {
				t.Fatalf("parseBackoff(%q) errored: %v", tt.raw, err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("parseBackoff(%q) = %#v, want %#v", tt.raw, got, tt.want)
			}
		})
	}

	// A malformed entry is a startup error, never a silently shortened
	// schedule (#155).
	for _, raw := range []string{"1s,not-a-duration,25s", "garbage"} {
		if _, err := parseBackoff(raw); err == nil {
			t.Errorf("parseBackoff(%q) must error", raw)
		}
	}
}

func TestValidatePlatformBaseURL(t *testing.T) {
	for _, ok := range []string{
		"https://discord.com/api/v10",
		"https://graph.facebook.com/v23.0",
		"http://mockdiscord.kaalm-e2e.svc:8080",
	} {
		if err := validatePlatformBaseURL(ok); err != nil {
			t.Errorf("validatePlatformBaseURL(%q) = %v, want nil", ok, err)
		}
	}
	for _, bad := range []string{
		"",
		"discord.com/api",        // no scheme
		"ftp://discord.com",      // wrong scheme
		"https://",               // no host
		"https://h.example?x=1",  // query
		"https://h.example#frag", // fragment
		"https://h .example/api", // unparseable
	} {
		if err := validatePlatformBaseURL(bad); err == nil {
			t.Errorf("validatePlatformBaseURL(%q) must error", bad)
		}
	}
}
