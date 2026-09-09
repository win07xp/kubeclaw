# Security Model and Isolation

This part of the book defines Kaalm's security posture: the RBAC model, credential scoping, isolation guarantees, and the trust boundaries between platform engineers, developers, agent containers, and the cluster itself. It is written to be the answer sheet when a security team asks "what can go wrong here?"

This page covers the foundations: who trusts whom, how agent Pods are isolated at the runtime, Pod-security, network, and resource layers, what data crosses which boundary, and how to deploy Kaalm safely. The remaining pages in this part drill into each mechanism: [RBAC and authentication](rbac.md) covers the ServiceAccounts, roles, and how agents authenticate to the gateway; [credential handling](credentials.md#lifecycle-of-an-llm-api-key) traces the lifecycle of every secret Kaalm touches; [TLS and certificates](tls.md#in-cluster-tls) is the canonical reference for the in-cluster trust chain; and the [threat model](threat-model.md) enumerates concrete attacks and their mitigations.

## Trust Model

Kaalm assumes four trust tiers:

1. **Cluster administrator**: trusted to install Kaalm, manage CRDs, and deploy the operator.
2. **Platform engineer**: trusted to create AgentClasses and ModelProviders, and to manage LLM credentials. This role should be distinct from agent developers.
3. **Agent developer**: trusted to deploy workloads in their namespace within the guardrails set by the platform team. Not trusted with LLM credentials or cross-namespace access.
4. **Agent container**: **not trusted**. Even developer-authored agents may execute LLM-generated code. Agent containers should be treated as potentially adversarial.

Kaalm's security design flows from the assumption that agent containers are untrusted. Whenever a later page explains why a control exists, that assumption is usually the reason.

## Isolation

Isolation is layered. The runtime boundary (RuntimeClass) decides how strongly the kernel is separated from the container; Pod Security Standards constrain what the container may ask the kernel for; NetworkPolicy constrains what it can reach; and resource limits constrain what it can consume.

### RuntimeClass

AgentClass may specify a `runtimeClassName` naming a Kubernetes `RuntimeClass` that must already exist on the cluster. Pod admission fails with "RuntimeClass not found" otherwise, and stock clusters (kubeadm, EKS, GKE, AKS) define none. In particular there is no built-in RuntimeClass named `runc`, so naming one is a common and avoidable way to break admission.

Platform teams use this field to require stronger isolation for risky agents:

| `runtimeClassName` | Isolation | Use when |
|---|---|---|
| unset (default) | Cluster's default container runtime, runc in practice. Standard container isolation. | The agent only calls APIs and is trusted. |
| `gvisor` / `runsc` | Userspace kernel, syscall filtering. | The agent executes untrusted code. |
| `kata` | VM-level isolation from a lightweight hypervisor. | Strong multi-tenancy is required. |
| `firecracker` (through Kata or Agent Sandbox) | microVM isolation. Highest isolation, comparable cost to Kata. | The strongest available boundary is required. |

The default is *unset*, not `runc`: leaving the field empty is what selects the cluster's default runtime.

Platform teams create separate AgentClasses for each isolation tier (for example, `standard` leaves `runtimeClassName` unset and `sandboxed` requires gVisor) and developers choose based on their needs.

### Pod Security Standards

Every Kaalm-created Pod complies with the `restricted` Pod Security Standard by default:

- `runAsNonRoot: true`
- `allowPrivilegeEscalation: false`
- `readOnlyRootFilesystem: true`. Writable storage comes from the PVC when persistence is enabled, and the controller additionally mounts an `emptyDir` at `/tmp` in every Agent and AgentTask Pod so images that write temp files work under a read-only root even without a PVC
- All Linux capabilities dropped
- `seccompProfile: RuntimeDefault`

AgentClass can override these defaults, but the operator emits warnings for any deviation and the AgentClass reconciler sets a condition for any deviation from the restricted baseline. Deviating is possible, but never silent.

### Network Policy

AgentClass includes network policy fields that the controller translates into `NetworkPolicy` resources.

**Egress**: by default, deny all egress except to the Kaalm gateway in `kaalm-system` and DNS. Because the gateway is a separate Pod (not a sidecar), this is enforceable with standard Kubernetes NetworkPolicy without requiring a service mesh. The platform team adds explicit allowlist entries for external APIs and other direct egress with two fields (from v0.4.0, MCP tool servers are preferentially reached through the gateway's [tool plane](../gateways/tool-plane.md), which needs no per-agent egress at all; the fields remain the escape hatch):

- **`spec.network.egress.allowedCIDRs`**: array of CIDR blocks. Maps directly to `NetworkPolicy.egress.to.ipBlock.cidr` and works on every CNI that implements Kubernetes NetworkPolicy. This is the portable primitive and should be preferred.
- **`spec.network.egress.allowedHosts`**: array of DNS names. Only enforceable on CNIs that support FQDN egress policies (Cilium with `CiliumNetworkPolicy.toFQDNs`, Calico Enterprise). Standard `NetworkPolicy` has no equivalent. On unsupported CNIs the AgentClassReconciler emits a `Warning` event and ignores the field; `allowedCIDRs` alone governs egress. See [AgentClassReconciler](../controller/reconcilers.md#agentclassreconciler).

**Ingress**: by default, deny all ingress except from the Kaalm gateway (which delivers channel messages through `POST /v1/message`). The Service makes the agent reachable within the cluster by the gateway; no other inbound traffic is allowed by default. NetworkPolicy and the agent-side mTLS check on `POST /v1/message` (see [The Runtime Contract](../runtime/contract.md) bullet 4) are layered controls: a misconfigured per-Agent NetworkPolicy no longer opens delivery to arbitrary in-cluster callers.

**Inter-agent**: disabled by default. To allow same-namespace agent-to-agent traffic, platform teams set `spec.network.allowSameNamespaceIngress: true` on the AgentClass. The controller translates this into a NetworkPolicy `ingress.from.podSelector` rule scoped to Pods in the same namespace bearing the Kaalm agent label. This is opt-in: the default deny-all-ingress posture reflects the assumption that agent containers are untrusted.

**The "standard Kubernetes NetworkPolicy is sufficient" claim is scoped to agent → gateway enforcement and to IP-/CIDR-level egress governance.** Hostname-based egress (`allowedHosts`) is *not* enforceable on standard NetworkPolicy; clusters that require FQDN-level egress must use Cilium or Calico Enterprise.

### Resource Isolation

Every Agent/AgentTask has resource limits enforced through Pod `resources.limits`. AgentClass.maxLimits sets the cap. This prevents a runaway agent from exhausting node resources.

## Data Flow and Audit

### What Flows Where

Eleven traffic classes cross a Kaalm boundary; the last three exist only when the matching feature is in use. Knowing which is which is the fastest way to reason about what an attacker on any given wire would see.

- **Agent → LLM Gateway**: prompts and completions. In-cluster HTTPS (TLS terminated at the gateway; the agent trusts the Kaalm CA through the projected trust bundle). See [In-cluster TLS](tls.md#in-cluster-tls).
- **LLM Gateway → LLM Provider**: prompts and completions over egress. Always HTTPS. Custom CA bundles supported for enterprise environments, see [Upstream TLS Configuration](../gateways/llm/provider-routing.md#upstream-tls-configuration).
- **Channel Platform → User Gateway**: inbound webhook messages. HTTPS inbound to the gateway's public endpoint, through Ingress.
- **User Gateway → Agent**: normalized message envelope through `POST /v1/message` to the agent's ClusterIP Service over **bidirectional mTLS** (the gateway verifies the agent's cert-manager-issued TLS certificate against `kaalm-ca`; the agent verifies the gateway's client cert with SAN-match against `kaalm-ca` per [The Runtime Contract](../runtime/contract.md) bullet 4). See [In-cluster TLS](tls.md#in-cluster-tls).
- **User Gateway → `callbackUrl`**: async response and error payloads POSTed to the AgentChannel's configured callback receiver, the gateway's third outbound traffic class alongside provider egress and agent delivery. Always HTTPS; every POST is signed per `spec.webhook.callbackAuth` ([rule 25](../resources/validation-and-defaulting.md#cross-resource-validation)); targets are constrained by the deny-internal ranges / `gateway.callbackUrl.allowlist` with pre-dial host re-resolution and the checked IP pinned into the dial (see the SSRF row in [§ Threat Model](threat-model.md)). Payloads contain agent replies, which may carry PII: receivers are outside Kaalm's trust boundary and are chosen by the channel owner.
- **Controller → Gateway**: activity timestamp queries through `GET /v1/activity` (mTLS with SAN-based authorization, internal ClusterIP Service). See [Internal Endpoint Authentication](rbac.md#internal-endpoint-authentication).
- **Gateway → API Server**: task completion data written to per-task ConfigMaps in user namespaces; async response payloads and per-replica budget partials written to ConfigMaps in `kaalm-system`.
- **Controller ↔ API server**: CRD updates, Pod creation, events. Standard kubelet/apiserver channels.
- **User Gateway → Platform reply API** (since v0.7.0): agent replies to Discord and WhatsApp, POSTed to the operator-configured `gateway.platforms.<type>.apiBaseUrl` carrying that channel's platform credential. This is the gateway's third outbound edge that leaves the boundary; unlike `callbackUrl` its target is operator-set, not developer-set (see the matching [threat model note](threat-model.md#channels-and-webhooks)). See [The platform adapters](../gateways/user/platform-adapters.md).
- **Operator's browser ↔ Console** (optional, since v0.5.0): fleet metadata, spend figures, and test-chat over TLS with an in-memory session. The console reads the API server under its own ServiceAccount and dials the gateway's `/v1/test-chat` and `/v1/spend` over mTLS with SAN authorization. See [the console](../console/overview.md) and [Internal Endpoint Authentication](rbac.md#internal-endpoint-authentication).
- **Gateway → OTLP collector** (optional, since v0.5.0): trace spans, carrying request metadata only, never prompt or reply content. The endpoint is operator-set; an `https` endpoint is verified against the gateway's upstream trust pool. See [Tracing](../operations/observability.md#tracing).

![A component diagram of the traffic classes framed by Kaalm's trust boundary. Inside the boundary sit a kaalm-system frame holding the gateway and controller, a user namespaces frame holding an Agent Pod, and the Kubernetes API server. Outside the cluster sit the channel platform, the LLM provider, and the callbackUrl receiver, which is marked as untrusted. Classes 1, 4, 6, 7 and 8 stay inside the boundary. Class 3, the inbound webhook, crosses in but never out. Three edges leave the boundary, all drawn in red from the gateway: class 2 to the LLM provider, class 5 to the callbackUrl receiver, which terminates at a receiver Kaalm does not trust, and class 9, the platform reply, back to the channel platform. The optional console and OTLP classes 10 and 11 are named in the legend rather than drawn.](../diagrams/trust-boundaries.svg)

**Reading the diagram.** Count the red edges. Three of the eleven classes leave the trust boundary, all outbound from the gateway, and one of them (class 5) ends at a receiver Kaalm does not choose and does not trust. That asymmetry is why class 5 carries the most machinery: signing, allowlisting, and pre-dial re-resolution with the checked IP pinned into the connection. Classes 2 and 9 leave too, but toward hosts the operator configured, which is why neither needs class 5's defenses. The figure deliberately says nothing about component responsibilities or wiring; [System Architecture](../concepts/system-architecture.md) owns that.

### Audit Trail

The operator emits Kubernetes Events for:

- Every phase transition on Agent/AgentTask.
- Every provider access decision (grant/deny).
- Every budget threshold crossing.
- Every credential rotation.

Since v0.7.0 the gateway also emits `Warning` events on the resources it governs at runtime: `FallbackIneligible` and `CredentialsInvalid` on a ModelProvider during a fallback walk, and `CallbackRejected` on an AgentChannel when a callback receiver or a platform refuses a reply. Event messages carry reasons and status codes, never credential material, and a `CallbackRejected` detail quotes at most a short bounded prefix of the platform's response body. See [Gateway ServiceAccount permissions](rbac.md#gateway-serviceaccount-permissions) for the grant that carries them.

Events persist in etcd per the cluster's Event retention. For long-term audit, platform teams should ship events to an external audit log (standard k8s audit logging, Falco, etc.).

Kaalm does **not** log prompts or completions. LLM payloads are sensitive (they may contain PII or proprietary data) and Kaalm takes no responsibility for their persistence. If prompt auditing is required, it should be implemented as a separate concern (for example, an auditing provider adapter that duplicates traffic to a log sink).

## Recommendations for Deployment

1. **Install Kaalm in a dedicated, locked-down namespace** (`kaalm-system`). Restrict who can `exec` into or modify resources in this namespace.
2. **Expose the User Gateway through a dedicated Ingress or LoadBalancer** with TLS termination. The gateway's public endpoint receives inbound platform events.
3. **Enable k8s audit logging** at the `Metadata` level minimum, `RequestResponse` for Secret access if feasible.
4. **Standard Kubernetes NetworkPolicy is sufficient** for the agent → gateway egress rule and for CIDR-scoped external egress (`allowedCIDRs`), no service mesh required. The cluster-level gateway architecture makes agent→gateway egress cross-Pod and fully enforceable. If you need FQDN-based egress (`allowedHosts`), install Cilium or Calico Enterprise; standard NetworkPolicy cannot express hostname rules. **This guarantee is automatic only for Kaalm-managed Pods (Agents and AgentTasks).** Gateway-only-tier workloads do not receive a Kaalm-synthesized NetworkPolicy: platform teams adopting that tier must apply their own default-deny egress posture on those namespaces if they want to prevent direct provider calls. See the matching rows in the [threat model](threat-model.md).
5. **Separate LLM credential management from platform engineering access** if possible (for example, only a secrets-admin role can read/write credential Secrets in `kaalm-system`). This requires cluster RBAC beyond Kaalm's scope.
6. **Require an appropriate RuntimeClass for any AgentClass that allows LLM code execution.** Platform admins own RuntimeClass installation and compatibility validation.
