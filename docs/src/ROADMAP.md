# Implementation Roadmap

The rest of this book is the design. This page is where the implementation stands
against it, and what comes next.

## Where the project stands

**v0.7.0 shipped on 2026-08-31**
([release](https://github.com/win07xp/kaalm/releases/tag/v0.7.0)). It installs
with a single `helm install` from the published OCI chart, upgrades in place
from the previous release with two documented commands, and every acceptance
scenario (S1 to S24) is proven on a real cluster.

The operator is feature-complete against the v1 design: all six CRDs, the
reconciling controller (lifecycle, hibernation and wake, budgets, health probes,
finalizers), the two-listener gateway (LLM proxy with credential isolation,
budgets, rate limits, and fallback trees that cross API formats; the MCP tool
broker; user gateway with webhook, Discord, and WhatsApp channels), the
optional operator console, the Helm chart with cert-manager TLS wiring, the
runtime contract with published base images and starter templates, and this
book.

What v0.7.0 added on top of v0.6.0 (the "Reach" milestone):

- **Platform channel adapters.** AgentChannel grows two types beside the
  generic webhook: `discord` (the Interactions endpoint: Ed25519-signed
  slash commands in, replies through the interaction's follow-up webhook,
  with an optional bot-token fallback past the token window) and `whatsapp`
  (the Cloud API webhook: the verification handshake, HMAC-signed events in,
  replies through the Graph API as the business number). Both are inbound
  HTTP; nothing holds a persistent connection, which is what lets any
  gateway replica take any event. The design is the AgentChannel chapter's
  [Platform types](resources/agentchannel.md#platform-types) and
  [Platform Adapters](gateways/user/platform-adapters.md); S22 and S23
  prove them against mock platforms.
- **Cross-format fallback.** A fallback edge may cross API formats:
  `anthropic` against `openai` or `openai-compatible`, in either direction,
  with a `modelMap` on the edge naming the model the fallback serves. The
  gateway rewrites the request before the first byte and the response,
  streaming or not, back into the caller's format; a request carrying a
  feature the other format cannot express skips that candidate with an
  event naming the feature. The design is
  [Crossing formats](gateways/llm/fallback.md#crossing-formats); S24 proves
  both directions against the mock provider.
- **Two gaps the new scenarios exposed, fixed.** The channel health poller
  existed in the design and the reducer but was never wired into the
  manager, so `PlatformConnected` had never been written on a real cluster;
  and the gateway's ClusterRole never granted `events`, so its runtime
  warnings (`FallbackIneligible`, `CredentialsInvalid`, `CallbackRejected`)
  were silently rejected. Both work now, and both are the kind of finding
  the acceptance scenarios exist to force.

Quality bar at release: 87% project test coverage enforced by an 85% CI gate,
envtest suites against a real apiserver, a k3d end-to-end suite (76 specs)
that is green both locally and in GitHub Actions, and the 6-spec upgrade
suite that gates every release tag.

Two honest caveats:

- `v1alpha1` is deprecated. Everything that says it keeps working, with a
  warning per request, at least through v1.0.0; the
  [deprecation policy](operations/api-versioning.md#deprecation-policy) is
  the contract, and moving a manifest is one `apiVersion` line because the
  schema is identical.
- The `v0.1.0` tag predates the release machinery and installs only from
  source.

## Next

The next milestone is **v1.0.0, "The complete release"** (tracking issue
[#51](https://github.com/win07xp/kaalm/issues/51)): the security pass, the
scale proof, the Agent Sandbox decision, and the docs audit.

The Agent Sandbox decision is made
([#141](https://github.com/win07xp/kaalm/issues/141)): v1 stays on raw Pods
and documents the RuntimeClass alternative, and the **v1.1.0** milestone
opens with the isolation theme. It carries the `agentSandbox` runtime
backend ([#167](https://github.com/win07xp/kaalm/issues/167)), gated on
upstream shipping identity association and its first stable series earning
a track record, and the verified microVM path, Kata first
([#168](https://github.com/win07xp/kaalm/issues/168)).

Beyond those two milestones, items below remain the unscheduled backlog.

## Beyond

These are the deferrals the design itself names (see
[Scope for v1](concepts/vision-and-scope.md#scope-for-v1)), roughly in the order
they are likely to matter:

- **The Discord Gateway WebSocket adapter.** Free-text message bots need a
  persistent connection per bot (identify, heartbeat, resume, sharding) and
  one replica to own it; the v0.7.0 Discord adapter covers slash commands
  over HTTP instead. Tracked as
  [#124](https://github.com/win07xp/kaalm/issues/124).
- **Serving the `google-vertex` provider type.** The enum is reserved and
  the adapter's outbound pieces exist, but no inbound path is routed and
  the OAuth2 token minting the platform requires is not implemented
  ([#147](https://github.com/win07xp/kaalm/issues/147) records the
  alignment). Google now brands the platform the Gemini Enterprise Agent
  Platform; the wire API is unchanged.
- **Larger horizons.** Agent-to-agent orchestration, multi-cluster
  federation, and agent-aware scheduling (GPU awareness, priority, preemption).

## How this page is maintained

Items move here from the release notes when a version ships, and out of here into
the design book when they get designed. History lives in git and in the
[releases](https://github.com/win07xp/kaalm/releases); this page only ever
describes the present and the future.
