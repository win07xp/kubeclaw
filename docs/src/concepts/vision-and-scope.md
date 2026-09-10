# Vision and Scope

> **Note:** "Kaalm" is a working codename. Replace throughout once a final name is selected.

## What is Kaalm?

Kaalm is a Kubernetes-native platform that makes AI agents a first-class workload type. It provides a set of custom resources and a controller that manage the full lifecycle of agents, from deployment and hibernation through resumption and teardown, alongside two managed gateway types: an **LLM Gateway** (TLS-secured) for controlled access to AI model providers, and a **User Gateway** for connecting agents to user-facing channels: a generic webhook, and since v0.7.0 Discord and WhatsApp adapters.

Kaalm is **not** an agent framework, an agent marketplace, or an IDE. It does not define how an agent thinks, which tools it uses, or how users talk to it at the application layer; since the v0.3.0 design it does govern how tool access is granted, brokered, and audited (the [tool plane](../gateways/tool-plane.md), implemented in v0.4.0), without ever shipping a tool itself. It defines how an agent is **run**: what image, under what isolation policy, against which LLM providers, with what lifecycle, within what cost guardrails, and over what user-facing channels.

## The Problem

Deploying an AI agent to Kubernetes today is manual glue work. A team that wants to run an agent must assemble a Deployment or StatefulSet, a Service, a Secret for LLM API keys, a PVC for persistent memory, a custom proxy for token counting, and some mechanism for connecting the agent to the outside world (Slack, a webhook, a web client). They must decide independently how to handle idle agents (keep paying for them, or build their own hibernation), how to get visibility into LLM spend (usually they don't, and get surprised by the bill), and how to isolate agents that execute untrusted code.

This problem is most acute in **shared clusters with many agents**. Consider a platform team offering a self-service "personal AI assistant" capability to an engineering organization: hundreds of developers, each with their own persistent agent, each using a shared LLM provider. Without a platform-level abstraction, the platform team has no way to enforce usage policies, limit per-user spend, or provide a consistent channel integration. Every agent is a bespoke Helm chart with custom secrets management.

Platform teams face a structural tension: they want to offer agents as a self-service capability to developers, but they need to enforce security and cost guardrails centrally. Today there is no Kubernetes abstraction that captures "agent" as a workload with these concerns built in.

## What Kaalm Provides

Kaalm introduces six custom resources:

- **AgentClass** (cluster-scoped): a policy resource, analogous to StorageClass, that defines the runtime configuration, isolation level, resource limits, and allowed providers for a category of agents. Platform teams own these. See [AgentClass](../resources/agentclass.md).
- **ModelProvider** (cluster-scoped): a managed abstraction over an LLM provider that holds API keys, provides visibility into token usage and spend, handles fallback, and can be shared across namespaces under policy control. Platform teams own these. See [ModelProvider](../resources/modelprovider.md).
- **ToolProvider** (cluster-scoped, since v0.4.0): the same managed abstraction over an external MCP tool server: the gateway holds its credential and brokers tool calls under policy control. Platform teams own these. See [ToolProvider](../resources/toolprovider.md).
- **Agent** (namespace-scoped): a developer-facing workload resource that describes a single agent: its image, its persistence needs, which AgentClass it belongs to, which ModelProviders it uses (if any), and its lifecycle mode (persistent or task). Developers own these. See [Agent](../resources/agent.md).
- **AgentTask** (namespace-scoped): a Job-like resource for ephemeral, goal-driven agents with a defined completion condition and artifact collection. Developers own these. See [AgentTask](../resources/agenttask.md).
- **AgentChannel** (namespace-scoped): a resource that connects a running Agent to a user-facing communication channel (Discord, WhatsApp, iMessage, a generic webhook, etc.). Developers own these. See [AgentChannel](../resources/agentchannel.md).

The controller reconciles these resources into standard Kubernetes primitives (Pods, PVCs, Services, ConfigMaps) while layering in agent-aware lifecycle logic (idle detection, hibernation, wake-on-demand, task completion semantics) and managing two shared gateway components:

- The **LLM Gateway**: a replicated proxy Deployment in `kaalm-system` that mediates all agent-to-provider traffic. It provides spend visibility, budget guardrails (soft by default, with an opt-in hard cap), rate limiting, fallback routing, and credential isolation. Using it is optional per agent. It serves two tiers of caller: Kaalm-managed Pods, and existing workloads that have no Agent resource at all (the gateway-only tier). How each tier authenticates, and which access-control policies apply to it, is detailed in [Namespace Identification](../gateways/llm/workload-identity.md). One caveat worth knowing at this altitude: the default-deny NetworkPolicy that forces traffic through the gateway covers Kaalm-managed Pods only, so gateway-only workloads route through it voluntarily unless the platform team adds its own egress policy; see [Network Policy](../security/model.md#network-policy).
- The **User Gateway**: a listener on the same gateway Deployment that receives inbound webhook messages, normalizes them into a standard envelope, and delivers them to the agent's HTTP endpoint; the Discord and WhatsApp adapters (since v0.7.0) do the same for their platforms' HTTP delivery. See [User Gateway request flow](../gateways/user/overview.md#request-flow).

## Budget Visibility and Guardrails

Kaalm tracks LLM token usage and spend per namespace through the LLM Gateway. At each API call, the gateway checks the current budget state and enforces policies: degrading to a cheaper model as the budget ceiling is approached, and blocking requests when it is exceeded.

Budget enforcement in Kaalm is **intentionally approximate by default**. The gateway maintains an in-process counter and updates it synchronously on each request, but in a multi-replica gateway deployment, counters are reconciled periodically rather than on every request. As a result, spend can exceed configured limits by a bounded amount under high concurrency near a budget threshold. This is the right tradeoff for most teams: continuous cross-replica synchronization adds latency and coordination cost that is rarely worth it. The mechanics live in [Budget State Management](../gateways/llm/budgets-and-rate-limits.md#budget-state-management).

The default mode is therefore best understood as **spend visibility and soft guardrails**. Providers whose invoice must not exceed the manifest opt in to [hard enforcement](../gateways/llm/budgets-and-rate-limits.md#hard-enforcement) per ModelProvider, which trades serialized admission near the ceiling for a stated spend guarantee. Provider-level account limits (Anthropic and OpenAI support account-level spend limits; on Vertex, GCP budgets natively provide alerts only, so a hard stop requires additional automation) remain sound defense in depth under either mode.

## Landscape Positioning

Several projects overlap with parts of Kaalm's scope. Kaalm is designed to be additive to the ecosystem rather than replacing existing primitives. This section describes other people's software, so it ages faster than the rest of the book; it reflects the field as of August 2026, and the Agent Sandbox entry as of September 2026.

**Agent Sandbox (kubernetes-sigs, SIG Apps)** is a lower-level primitive: a `Sandbox` CRD (with `SandboxTemplate`, `SandboxClaim`, and `SandboxWarmPool`) for a single, stateful, isolated pod, with suspend and resume, warm pools, snapshot-based restore, and gVisor or Kata isolation. Its API reached 1.0 in August 2026, serving v1beta1 exclusively, and GKE ships a productized version. The Kubernetes project has stated that model access and budget controls are out of its scope, which is exactly the ground Kaalm's ModelProvider and ToolProvider occupy. Agent Sandbox is the natural *runtime backend* for Kaalm agents that need strong isolation: the planned `agentSandbox` backend creates Sandbox resources instead of raw Pods. The v1 decision is to stay on raw Pods and document the RuntimeClass alternative, which already covers gVisor, Kata, and Kata-fronted microVMs; the backend is scheduled for v1.1, once upstream ships identity association and its first stable series has a track record. Two of its roadmap items, identity association and per-claim NetworkPolicy attachment, would overlap Kaalm's identity and egress mechanics at the pod layer; the policy semantics above them (which provider, whose budget, which tools) are not on its roadmap.

**kagent (CNCF Sandbox)** is a more opinionated agent framework: agents are primarily declared as prompts plus tool references in an `Agent` CRD and executed by a built-in ADK-based engine, with per-agent `ModelConfig` resources and MCP server lifecycle tooling. It began as a DevOps-agent platform and is drifting general-purpose. Kaalm differs on both axes that matter here: any container satisfying a minimal [runtime contract](../runtime/contract.md) can be an Agent (no engine, no prompt schema), and model and tool access are cluster-scoped, budget-governed shared resources rather than per-agent configuration. kagent has no spend tracking, budget enforcement, hibernation, or task-completion semantics.

**KARS (Microsoft, released July 2026)** is the closest statement of Kaalm's thesis from a large vendor: an MIT-licensed "agent reference stack" for Kubernetes in which model, tool, memory, and MCP access are declared as policy CRDs and enforced by a router deployed alongside each agent pod, across several supported agent frameworks. The differences are shape and center of gravity: KARS is a reference stack assembled from components, enforces per-pod, and is oriented toward the Azure and Foundry ecosystem; Kaalm is a single small operator with a shared gateway, a two-tier resource model, and dollar-denominated budget enforcement, which KARS does not have.

**The gateway plane** projects govern traffic rather than workloads. agentgateway (Linux Foundation) is a data plane for LLM, MCP, and A2A traffic with credential injection and per-tool authorization on JWT claims; Envoy AI Gateway (CNCF ecosystem) provides CRD-native LLM routing with token-denominated quotas and MCP route filtering; LiteLLM is the widely deployed standalone proxy with dollar budgets in its own key-and-team tenancy model. All of these overlap Kaalm's gateway features, and none is an operator that owns the agent workload. Their budgets are token-denominated or keyed to their own tenancy rather than to Kubernetes namespaces, their grant subjects are keys or JWT claims rather than the workload's ServiceAccount or mTLS identity, and none can guarantee that the workload actually routes through them. For the workloads it manages, Kaalm can, because the same operator that runs the agent also writes its default-deny NetworkPolicy.

One boundary is worth stating explicitly. Kaalm models an agent as a long-lived Kubernetes workload: a pod with an identity, storage, and a lifecycle. Patterns that fan out very large numbers of sub-second agent invocations outgrow pod-per-agent economics, and purpose-built invocation fabrics (such as Google's Agent Substrate) exist for that regime. Kaalm's primary scenario, hundreds of long-lived agents that are idle most of the time, is the regime where pod-per-agent plus hibernation is the right shape; swarm-scale invocation is out of scope.

Kaalm's differentiator is the combination none of its neighbors has: **dollar-denominated budget enforcement per namespace, tool grants whose subject is the workload's Kubernetes identity, and egress policy owned by the same operator that runs the workload**, under a clean two-tier platform/developer model with native channel integration.

## Design Principles

1. **General-purpose over framework-specific.** Any container that satisfies the [runtime contract](../runtime/contract.md) can be an Agent. No assumption about language, framework, or agent architecture.
2. **Two-tier platform/developer model.** Cluster-scoped resources (AgentClass, ModelProvider) let platform teams set guardrails. Namespace-scoped resources (Agent, AgentTask, AgentChannel) let developers self-serve within those guardrails.
3. **Composable with the ecosystem.** Agent Sandbox can be used as a runtime backend. MCP is the tool protocol, brokered by the gateway from v0.4.0 (the [tool plane](../gateways/tool-plane.md)) rather than reimplemented. No reinvention of primitives that already exist.
4. **Opinionated defaults, BYO escape hatches.** A minimal runtime contract makes the simple case simple. v1 ships starter templates (one Go, one Python) under `examples/` that implement the full contract end-to-end: adopters copy the template and replace the agent logic. Full-featured reference base images (published as container images that wrap the contract) are planned for a future release. Custom images are a first-class path.
5. **Policy at the boundary, not in the workload.** Budget guardrails, isolation policy, and provider access control live in cluster-scoped resources, not in individual Agent manifests.
6. **Kubernetes-native semantics.** Lifecycle mirrors familiar primitives: AgentClass is to Agent as StorageClass is to PVC; AgentTask is to Agent as Job is to Deployment.
7. **Honest about tradeoffs.** Where the system makes a tradeoff (soft budget limits by default, serialized admission near the ceiling under hard enforcement), each mode's exact bound is documented explicitly rather than obscured.

## Scope for v1

**In scope:**

- All six CRDs and the reconciling controller (ToolProvider since v0.4.0)
- Persistent and task-mode agent lifecycle (including idle detection, hibernation, wake-on-demand, timeout, artifact collection); see [Controller Lifecycle](../controller/agent-lifecycle.md)
- LLM Gateway: TLS-secured cluster-level proxy with spend tracking, budget guardrails (soft by default, hard opt-in since v0.3.0), rate limiting, fallback chains (crossing Anthropic and OpenAI formats since v0.7.0), and provider credential isolation. Two authentication modes: mTLS for Kaalm-managed Pods and `TokenReview`-validated ServiceAccount tokens for existing workloads. See [Namespace Identification](../gateways/llm/workload-identity.md).
- User Gateway: channel integration via AgentChannel (generic webhook with sync and async response modes; Discord and WhatsApp adapters since v0.7.0)
- RBAC, namespace scoping, and a documented [security model](../security/model.md)
- cert-manager-based TLS certificate lifecycle for the gateway and per-agent serving certs; see [Certificate Lifecycle](../operations/deployment.md#certificate-lifecycle)
- Starter templates (one Go, one Python) under `examples/` that implement the runtime contract; see [Starter Templates](../runtime/starter-templates.md)
- Helm chart with [tiered on-ramp](../operations/deployment.md#tiered-on-ramp) (gateway-only → full agent lifecycle with channels)
- An optional operator console (since v0.5.0, default off): read-only fleet visibility plus one governed test-chat action; see [Console Overview](../console/overview.md)

**Out of scope for v1** (may land in later versions):

- Agent-to-agent communication and multi-agent orchestration
- Observability stack (audit export; the Grafana dashboards and OpenTelemetry tracing ship since v0.5.0: [Dashboards](../operations/observability.md#dashboards), [Tracing](../operations/observability.md#tracing))
- Multi-cluster federation
- Advanced scheduling (GPU-awareness, priority classes, preemption policies specific to agents)
- Hard budget enforcement (synchronous per-request aggregation)
- Cross-format fallback involving Google Vertex (Anthropic and OpenAI cross since v0.7.0)
- Agent Sandbox integration (`agentSandbox` runtime backend) and the verified microVM isolation path, Kata first: v1.1
- The Discord Gateway WebSocket adapter for free-text message bots (a persistent connection per bot); the HTTP adapters ship since v0.7.0
- Full-featured reference base images (published container images wrapping the runtime contract): v1.1 (v1 ships starter templates instead)

The v1 scope is deliberately narrow: get the workload abstraction, provider management, and channel integration right first. Everything else is an additive layer.

## Scoping Summary

Every concern in the system has exactly one owner. This table is the quick reference for where each concern lives; the rest of the book expands on each row.

| Concern | Where it lives |
|---|---|
| Policy (who can use what, at what cost) | AgentClass, ModelProvider (cluster-scoped) |
| Workload definition | Agent, AgentTask (namespace-scoped) |
| Channel integration | AgentChannel (namespace-scoped) |
| Lifecycle orchestration | Kaalm Controller (cluster-level) |
| Runtime isolation | RuntimeClass via AgentClass, or Sandbox backend |
| LLM traffic / spend tracking | LLM Gateway in kaalm-system (shared) |
| Channel message routing | User Gateway in kaalm-system (shared) |
| Tool access | MCP; direct and ungoverned in v1, gateway-brokered from v0.4.0 (the [tool plane](../gateways/tool-plane.md)) |
| External exposure | Kubernetes Ingress / Gateway API (user-managed, not Kaalm) |
| Observability | Controller + Gateway Prometheus metrics; see [Observability](../operations/observability.md) |
| In-cluster TLS issuance | cert-manager + trust-manager (prerequisite); see [Deployment](../operations/deployment.md) |
| Network policy enforcement | NP-capable CNI (prerequisite); see [Network Policy](../security/model.md#network-policy) |
