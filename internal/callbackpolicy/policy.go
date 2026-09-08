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

// Package callbackpolicy decides which AgentChannel callbackUrl targets the
// operator permits. It is the single source of truth for validation rule 22,
// shared by the two places that enforce it: the AgentChannelReconciler at
// reconcile time, and the gateway's pre-dial re-check on every async delivery
// attempt (which defeats DNS rebinding between the two).
package callbackpolicy

import (
	"net"
	"strings"
)

// Policy is an immutable, comparable-by-use allowlist. The zero value is the
// default deny-internal policy: public targets only.
type Policy struct {
	suffixes []string
	cidrs    []*net.IPNet
}

// New builds a Policy from allowlist entries. An entry that parses as a CIDR is
// matched against the resolved address; anything else is treated as a DNS-name
// suffix matched against the callbackUrl host. A leading dot is optional, so
// ".svc.cluster.local" and "svc.cluster.local" behave identically, and an entry
// matches both the name itself and any subdomain of it.
func New(entries []string) Policy {
	var p Policy
	for _, raw := range entries {
		entry := strings.TrimSpace(raw)
		if entry == "" {
			continue
		}
		if _, network, err := net.ParseCIDR(entry); err == nil {
			p.cidrs = append(p.cidrs, network)
			continue
		}
		p.suffixes = append(p.suffixes, strings.ToLower(strings.TrimPrefix(entry, ".")))
	}
	return p
}

// NewFromCSV builds a Policy from a comma-separated flag value.
func NewFromCSV(csv string) Policy { return New(strings.Split(csv, ",")) }

// internalInPractice holds address space that is internal in real clusters
// but outside net.IP.IsPrivate: RFC 6598 shared address space (100.64.0.0/10,
// the Pod or Service CIDR in several managed clusters and CNIs) and RFC 2544
// benchmarking space (198.18.0.0/15). Denied by default at the same tier as
// private space, so the allowlist can open them deliberately (rule 22, #154).
var internalInPractice = mustCIDRs("100.64.0.0/10", "198.18.0.0/15")

func mustCIDRs(entries ...string) []*net.IPNet {
	nets := make([]*net.IPNet, 0, len(entries))
	for _, entry := range entries {
		_, network, err := net.ParseCIDR(entry)
		if err != nil {
			panic(err)
		}
		nets = append(nets, network)
	}
	return nets
}

// Allowed reports whether a callbackUrl resolving to ip (with the URL's host)
// may receive async responses.
//
// Loopback, link-local (which covers the 169.254.169.254 cloud-metadata
// endpoint), and the unspecified address are refused unconditionally: an
// allowlist can open internal network space, but it cannot re-expose the
// targets that make SSRF trivially exploitable. Beyond that floor, an
// allowlist match wins; otherwise private space (RFC1918, unique-local IPv6,
// shared address space, and benchmarking space) is denied and public
// addresses are allowed.
func (p Policy) Allowed(host string, ip net.IP) bool {
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
		return false
	}
	if p.matches(host, ip) {
		return true
	}
	// net.IP.IsPrivate covers RFC1918 and RFC4193 unique-local IPv6.
	if ip.IsPrivate() {
		return false
	}
	for _, network := range internalInPractice {
		if network.Contains(ip) {
			return false
		}
	}
	return true
}

func (p Policy) matches(host string, ip net.IP) bool {
	name := strings.ToLower(strings.TrimSuffix(host, "."))
	for _, suffix := range p.suffixes {
		if name == suffix || strings.HasSuffix(name, "."+suffix) {
			return true
		}
	}
	for _, network := range p.cidrs {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}
