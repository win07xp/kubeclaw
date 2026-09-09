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

package callbackpolicy

import (
	"net"
	"testing"
)

func TestPolicy_DefaultDeniesInternalAllowsPublic(t *testing.T) {
	p := Policy{} // zero value: no allowlist
	cases := []struct {
		ip   string
		want bool
	}{
		{"8.8.8.8", true},
		{"1.1.1.1", true},
		{"2606:4700:4700::1111", true},
		{"127.0.0.1", false},
		{"::1", false},
		{"10.1.2.3", false},
		{"172.16.5.5", false},
		{"192.168.0.1", false},
		{"fc00::1", false},
		{"169.254.169.254", false},
		{"fe80::1", false},
		{"0.0.0.0", false},
		// Internal-in-practice space outside net.IP.IsPrivate (rule 22, #154):
		// RFC 6598 shared address space and RFC 2544 benchmarking space,
		// including the IPv4-in-IPv6 form a dual-stack resolver can return.
		{"100.64.0.1", false},
		{"100.127.255.254", false},
		{"::ffff:100.64.0.1", false},
		{"198.18.0.5", false},
		{"198.19.255.254", false},
		// The neighbors just outside both ranges stay public.
		{"100.128.0.1", true},
		{"198.20.0.1", true},
	}
	for _, c := range cases {
		if got := p.Allowed("receiver.example.com", net.ParseIP(c.ip)); got != c.want {
			t.Errorf("Allowed(%s) = %v, want %v", c.ip, got, c.want)
		}
	}
}

func TestPolicy_SuffixEntryOpensPrivateTarget(t *testing.T) {
	p := New([]string{".svc.cluster.local"})

	// A private in-cluster address is permitted when its host matches.
	if !p.Allowed("mock-provider.e2e.svc.cluster.local", net.ParseIP("10.43.0.9")) {
		t.Error("an allowlisted DNS suffix must permit a private in-cluster target")
	}
	// The same private address is refused for a host outside the allowlist.
	if p.Allowed("evil.example.com", net.ParseIP("10.43.0.9")) {
		t.Error("a non-matching host must not inherit the allowlist")
	}
	// A leading dot is optional and the bare name matches too.
	if !New([]string{"svc.cluster.local"}).Allowed("svc.cluster.local", net.ParseIP("10.43.0.9")) {
		t.Error("an entry must match the name itself, not only its subdomains")
	}
	// Suffix matching is on label boundaries, not raw string suffix.
	if New([]string{"example.com"}).Allowed("notexample.com", net.ParseIP("10.0.0.1")) {
		t.Error("suffix matching must respect label boundaries")
	}
}

func TestPolicy_CIDREntryOpensPrivateTarget(t *testing.T) {
	p := New([]string{"10.43.0.0/16"})

	if !p.Allowed("anything.internal", net.ParseIP("10.43.0.9")) {
		t.Error("an allowlisted CIDR must permit the private target inside it")
	}
	if p.Allowed("anything.internal", net.ParseIP("10.44.0.9")) {
		t.Error("an address outside the allowlisted CIDR stays denied")
	}
}

// The floor is the security-relevant property: an operator can open internal
// network space, but cannot allowlist their way back to loopback or the cloud
// metadata endpoint.
// TestPolicy_AllowlistOpensSharedAddressSpace: the new ranges sit in the
// deniable tier, so an operator who runs Pods in 100.64.0.0/10 can open the
// space deliberately, unlike the loopback and link-local floor.
func TestPolicy_AllowlistOpensSharedAddressSpace(t *testing.T) {
	p := New([]string{"100.64.0.0/10"})
	if !p.Allowed("receiver.internal", net.ParseIP("100.64.7.7")) {
		t.Error("an allowlisted shared-space CIDR must open it")
	}
	if p.Allowed("receiver.internal", net.ParseIP("198.18.0.5")) {
		t.Error("benchmarking space stays denied without its own entry")
	}
}

func TestPolicy_FloorHoldsAgainstExplicitAllowlist(t *testing.T) {
	p := New([]string{"169.254.169.254/32", "127.0.0.0/8", "::1/128", "0.0.0.0/0", "metadata.internal"})

	floored := []string{"169.254.169.254", "127.0.0.1", "::1", "fe80::1", "0.0.0.0"}
	for _, ip := range floored {
		if p.Allowed("metadata.internal", net.ParseIP(ip)) {
			t.Errorf("%s must stay denied even when explicitly allowlisted", ip)
		}
	}
	// The same wide-open allowlist still permits ordinary private space, so the
	// entries above were genuinely applied and only the floor rejected them.
	if !p.Allowed("metadata.internal", net.ParseIP("10.1.2.3")) {
		t.Error("the allowlist should still open non-floor private space")
	}
}

func TestNewFromCSVStrict(t *testing.T) {
	p, err := NewFromCSVStrict(" .svc.cluster.local , 10.43.0.0/16 ,, ")
	if err != nil {
		t.Fatalf("valid CSV rejected: %v", err)
	}
	if !p.Allowed("a.svc.cluster.local", net.ParseIP("10.9.9.9")) {
		t.Error("CSV suffix entry not applied")
	}
	if !p.Allowed("other.internal", net.ParseIP("10.43.1.1")) {
		t.Error("CSV CIDR entry not applied")
	}
	if p.Allowed("other.internal", net.ParseIP("192.168.1.1")) {
		t.Error("blank CSV entries must not widen the policy")
	}

	// A bare IP entry becomes a single-address CIDR: it matches by resolved
	// address, whatever hostname the callbackUrl used.
	p, err = NewFromCSVStrict("192.168.7.7, fd12::7")
	if err != nil {
		t.Fatalf("bare IP entries rejected: %v", err)
	}
	if !p.Allowed("receiver.internal", net.ParseIP("192.168.7.7")) {
		t.Error("bare IPv4 entry must match by address")
	}
	if !p.Allowed("receiver.internal", net.ParseIP("fd12::7")) {
		t.Error("bare IPv6 entry must match by address")
	}
	if p.Allowed("receiver.internal", net.ParseIP("192.168.7.8")) {
		t.Error("a single-address entry must not widen to neighbors")
	}

	// Malformed entries fail instead of half-applying (#155).
	for _, bad := range []string{
		"10.0.0/8",          // typo'd CIDR: contains "/" but does not parse
		"192.168.0.0/33",    // impossible mask
		"svc cluster local", // spaces are neither CIDR nor DNS
		"bad_host",          // underscore is not a DNS label character
		"-leading.example",  // label starts with a hyphen
	} {
		if _, err := NewFromCSVStrict(bad); err == nil {
			t.Errorf("entry %q must be rejected", bad)
		}
	}
}
