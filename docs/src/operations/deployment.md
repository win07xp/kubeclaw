# Deployment

This page covers how Kaalm is packaged and installed: the Helm chart contents, the cert-manager and trust-manager certificate inventory, the NetworkPolicy CNI prerequisite, the two-tier on-ramp (gateway-only vs full agent lifecycle), and the upgrade path. For the system topology and component responsibilities, see [System Architecture](../concepts/system-architecture.md).

Kaalm has three hard prerequisites, none of which the chart installs for you:

1. **cert-manager**, for TLS lifecycle management. It must run with `--enable-certificate-owner-ref=true`; see [In-cluster TLS](../security/tls.md#in-cluster-tls) for why.
2. **trust-manager**, to project the Kaalm CA into user namespaces.
3. **A NetworkPolicy-enforcing CNI** (see [Network Policy Prerequisite](#network-policy-prerequisite)).

## Helm Chart Contents

The chart targets the `kaalm-system` namespace. Install with `--namespace kaalm-system --create-namespace`.

![A deployment inventory of the Helm chart, organised by where each object lands. A red frame at the top holds the three hard prerequisites the chart does not install: the cert-manager controller, the trust-manager controller, and a NetworkPolicy-enforcing CNI. A cluster-scoped frame holds the six CRDs from the chart's crds/ directory, which helm install applies and helm upgrade never touches, the two ClusterIssuers, the ClusterRoles and ClusterRoleBindings, and the sample standard AgentClass. An kaalm-system frame holds the controller and gateway Deployments with their replica floors, PDBs and controller-only anti-affinity, the two ClusterIP Services with their ports, the ServiceAccounts and namespaced Roles, the leader-election Lease, and the gateway and controller leaf Certificates. A cert-manager frame holds the kaalm-ca Certificate and Secret and the trust-manager Bundle. A user namespace frame holds the projected kaalm-ca ConfigMap.](../diagrams/helm-install-inventory.svg)

**Reading the diagram.** It answers "what lands where", not "how the pieces relate". The three dashed grey boxes in the red frame are prerequisites: everything else is chart-installed. The trust chain those cert-manager objects form is drawn once, on [In-cluster TLS](../security/tls.md#trust-chain).

### CRDs

The six CRDs (AgentClass, ModelProvider, ToolProvider, Agent, AgentTask, AgentChannel) ship in the chart's `crds/` directory. Helm applies that directory on `helm install` and **never touches it on `helm upgrade`**, so CRD schema changes need an explicit step. See [Upgrade and Migration](#upgrade-and-migration). Since v0.6.0 each CRD serves `v1beta1` (the storage version) and the deprecated `v1alpha1`, and carries the conversion stanza that points the apiserver at the controller; see [API Versioning and Deprecation](api-versioning.md).

### The two Deployments

The chart installs the operator Deployment (with RBAC, ServiceAccount, and leader election) and the Kaalm Gateway Deployment (with its own RBAC and ServiceAccount). Both share the same availability settings, except that pod anti-affinity is specified for the operator only:

| Setting | Value |
|---|---|
| Default replicas | `2` (`controller.replicas`, `gateway.replicas`) |
| PodDisruptionBudget | `minAvailable: 1` |
| Rolling update | `maxUnavailable: 1` |
| Anti-affinity (controller only) | `topologyKey: kubernetes.io/hostname`, so the two replicas land on different nodes |

Both Deployments have a **hard floor of 2 replicas**, enforced by a Helm `fail` template guard that aborts rendering when the value drops below 2. An operator cannot accidentally go under it.

The floor is operational, not correctness-driven, on both components:

- **Controller.** Leader election picks one active replica; the second is a warm standby for failover. Critically, the second replica also serves the activator endpoint, because the activator handler runs on **every** replica (see [Control Plane](../concepts/system-architecture.md#control-plane)), so two replicas keep the activator reachable across voluntary disruptions. Leader election and the activator both work with one replica, but a single-replica controller breaks wake-on-demand availability during drains and single-replica involuntary failures, which contradicts the "hard control-plane dependency" framing in [The Kaalm Gateway](../gateways/overview.md). The PDB matters for the same reason: without it, a multi-node drain or an autoscaler downscale could evict both replicas simultaneously, surfacing as `controller_unavailable` 504s for any in-flight webhook to a hibernated Agent (see [Activator](../gateways/user/activation-and-activity.md#the-activator) step 5).
- **Gateway.** At one replica, `minAvailable: 1` blocks all voluntary eviction (node drains stall) and `maxUnavailable: 1` rolling updates have no headroom, so chart upgrades would briefly take the gateway offline for both LLM proxy and inbound webhook traffic. The multi-replica state model in [The Kaalm Gateway](../gateways/overview.md) (spend ConfigMap exchange, divide-by-replicas rate buckets, controller activity fan-out) degrades gracefully to one replica, so correctness is not the reason for the floor.

With `console.enabled`, the chart adds a third, optional Deployment, `kaalm-console`, deliberately outside everything above: one replica, no PodDisruptionBudget, no floor. See [Console Overview](../console/overview.md) and the `console.enabled` note below.

### Configuration reference

This table is the canonical list of Kaalm's Helm values. Every tunable named elsewhere in this book resolves to a row here.

| Value | Default | What it does |
|---|---|---|
| `controller.replicas` | `2` | Operator replica count. Rendering fails below `2`. |
| `gateway.replicas` | `2` | Gateway replica count. Rendering fails below `2`. |
| `gateway.maxFallbackDepth` | `3` | Maximum fallback chain depth for LLM provider routing. Sets `KAALM_MAX_FALLBACK_DEPTH` on the gateway Deployment. See [Fallback Logic](../gateways/llm/fallback.md). |
| `gateway.trustClusterCAForUpstream` | `false` | Also trust the cluster CA (`kaalm-ca`, already mounted) for upstream provider TLS, added to the system roots. Enables in-cluster or self-hosted providers whose HTTPS endpoint is served with a `kaalm-ca-issuer` certificate. Distinct from the operator-supplied `kaalm-upstream-ca` bundle; see [Upstream TLS Configuration](../gateways/llm/provider-routing.md#upstream-tls-configuration). |
| `gateway.upstreamCA.configMap` | `""` | Name of an operator-supplied ConfigMap of additional CA certificates to trust for upstream provider TLS (the `kaalm-upstream-ca` bundle). The chart mounts it and points the gateway at it. Composes with `trustClusterCAForUpstream`: enabling both merges the cluster CA and this bundle into one trust pool. |
| `gateway.upstreamCA.key` | `ca.crt` | Key within that ConfigMap holding the PEM bundle. |
| `gateway.callbackUrl.allowlist` | unset | List of DNS-name suffixes or CIDR blocks whose `AgentChannel.spec.webhook.callbackUrl` targets are permitted despite the deny-internal default. Loopback, link-local and the cloud-metadata IPs stay refused even when listed. |
| `controller.networkPolicy.dnsSelector` | `{ namespaceLabels: { "kubernetes.io/metadata.name": "kube-system" }, podLabels: { "k8s-app": "kube-dns" } }` | Selectors for the DNS egress rule on every synthesized per-agent NetworkPolicy. |
| `controller.trustClusterCAForProbes` | `false` | Also trust the cluster CA (`kaalm-ca`, already mounted) for ModelProvider and ToolProvider health probes, added to the system roots. The probe-side mirror of `gateway.trustClusterCAForUpstream`: enable both so an in-cluster provider under a `kaalm-ca-issuer` certificate is both forwarded to and probed `Healthy`. |
| `controller.probeCA.configMap` | `""` | Name of an operator-supplied ConfigMap of additional CA certificates to trust for health probes, mirroring `gateway.upstreamCA`. Composes with `trustClusterCAForProbes` into one additive pool; the controller re-reads it when it rotates. |
| `controller.probeCA.key` | `ca.crt` | Key within that ConfigMap holding the PEM bundle. |
| `controller.maxConcurrentReconciles` | `4` | Reconciles the Agent, AgentChannel, and AgentTask controllers may each run at once. controller-runtime serializes per object at any setting; this lets different objects reconcile in parallel. |
| `controller.pprofPort` | `0` | Port for a `net/http/pprof` listener on the controller, for profiling under load. `0` keeps it off. Unauthenticated and never behind a Service; reach it with a port-forward. See [Profiling](observability.md#profiling). |
| `gateway.externalHostnames` | unset | Additional DNS names appended to the `kaalm-gateway-tls` Certificate's SAN list. |
| `gateway.channelHealthWindow` | `5m` | Rolling window over which the gateway evaluates `AgentChannel.status.conditions[type=PlatformConnected]`. |
| `gateway.platforms.discord.apiBaseUrl` | `https://discord.com/api/v10` | Base URL the Discord adapter replies through (since v0.7.0). Operator-set and trusted like a ModelProvider endpoint: not subject to the `callbackUrl` deny ranges, `http://` accepted, so an e2e mock can stand in. |
| `gateway.platforms.whatsapp.apiBaseUrl` | a Graph API version URL pinned in the chart | Base URL the WhatsApp adapter replies through (since v0.7.0), including the Graph API version segment. Same trust as the Discord value. |
| `gateway.agentDeliveryConnectTimeout` | `1s` | Bounds the TCP connect attempt when delivering `POST /v1/message` to an Agent Service. |
| `gateway.syncDeliveryDeadline` | `30s` | Bounds total sync-mode wall-clock (wake plus delivery retries plus agent processing). Exceeded gives `504 sync_deadline_exceeded`. See [Request Flow step 6a](../gateways/user/overview.md#request-flow). |
| `gateway.providerFirstByteTimeout` | `120s` | Bounds each upstream LLM-provider attempt from connection start through the first response byte, and the idle gap between SSE chunks. |
| `gateway.agentReadTimeout` | `10s` | Bounds the per-attempt read of an agent's `/v1/message` response. |
| `gateway.callbackReadTimeout` | `10s` | Bounds the per-attempt read of a callback receiver's response. |
| `gateway.agentDeliveryRetryBackoff` | `1s,5s,25s` | Backoff schedule for the agent-delivery pipeline (4 attempts total). |
| `gateway.callbackRetryBackoff` | `1s,5s,25s` | Backoff schedule for the callback-delivery pipeline (4 attempts total). Reused by the async response-`Patch` pipeline. |
| `gateway.maxResponseBodyBytes` | `900Ki` | Caps an agent's webhook response, uniformly in sync and async modes. Over-cap gives `response_too_large`. |
| `gateway.maxMessageBodyBytes` | `1Mi` | Caps inbound webhook bodies on `:8080`. Over-cap POSTs get `413` at the listener level, before path resolution and auth. See [Request Flow step 2](../gateways/user/overview.md#request-flow). |
| `gateway.maxLLMRequestBodyBytes` | `4Mi` | Caps inbound LLM-proxy request bodies on `:8443`. Over-cap gives `413 request_too_large` before namespace identification. See [LLM Proxy Endpoints](../gateways/api/overview.md#llm-proxy-endpoints). |
| `gateway.healthPort` | `8081` | Port for the gateway's internal kubelet-probe listener (`/healthz`, `/readyz`; TLS, no client auth). See [Gateway Readiness](../gateways/llm/operations.md#gateway-readiness). |
| `gateway.tracing.otlpEndpoint` | `""` | OTLP/HTTP base URL the gateway exports trace spans to (for example `http://collector.monitoring.svc:4318`). Empty means tracing is off entirely: no tracer installed, no trace context created or forwarded. An `https` endpoint is verified against the gateway's upstream trust pool. See [Tracing](observability.md#tracing). |
| `gateway.tracing.sampleRatio` | `1.0` | Parent-based head sampling ratio for traces the gateway starts; propagated sampling decisions are honored either way. |
| `gateway.pprofPort` | `0` | Port for a `net/http/pprof` listener on the gateway, for profiling under load. `0` keeps it off. Unauthenticated and never behind a Service; reach it with a port-forward. See [Profiling](observability.md#profiling). |
| `console.enabled` | `false` | Installs the optional [operator console](../console/overview.md): the `kaalm-console` Deployment, Service, RBAC, and certificate. Off renders none of them. |
| `console.image.repository` / `.tag` / `.pullPolicy` | `ghcr.io/win07xp/kaalm-console`, appVersion, `IfNotPresent` | The console image. |
| `console.healthPort` | `8081` | Port for the console's kubelet-probe listener (`/healthz`, `/readyz`; TLS, no client auth). |
| `console.resources` | unset | Resource requests and limits for the console container, passed verbatim. |
| `certManager.clusterResourceNamespace` | `"cert-manager"` | Namespace holding the CA `Certificate` and `kaalm-ca` Secret. Must match your cert-manager and trust-manager deployment. See [Certificate Lifecycle](#certificate-lifecycle). |
| `trustManager.bundleSelector` | unset | Object with `matchLabels` / `matchExpressions`, passed verbatim into the `kaalm-ca` `Bundle`'s `target.namespaceSelector`. |

The values that need more than a sentence of explanation follow.

**`gateway.callbackUrl.allowlist`.** Leaving it unset preserves the default: `https://` only, with loopback, link-local, RFC1918, unique-local IPv6, and cloud-metadata IPs denied. When you set it, the [AgentChannelReconciler](../controller/reconcilers.md#agentchannelreconciler) and the gateway's delivery-time re-check admit only hosts matching one of the configured entries. Entries parse strictly at startup: an entry containing `/` must be a valid CIDR, a bare IP address means that one address, and anything else must be shaped like a DNS-name suffix. A malformed entry fails startup for both binaries instead of leaving the allowlist half-applied. See [Cross-Resource Validation rule 22](../resources/validation-and-defaulting.md#cross-resource-validation).

**`console.enabled`.** The console Deployment is fixed at one replica with no PodDisruptionBudget and no replica floor: login sessions are held in memory, so a second replica would break logins rather than add availability, and a read surface carries no wake-on-demand style dependency. The chart deliberately ships no `console.replicas` knob. Exposure is also deliberate: no Ingress and no LoadBalancer are templated; operators reach it by port-forward or front it themselves. See [Console Overview](../console/overview.md).

**`controller.networkPolicy.dnsSelector`.** The object has the shape `{ namespaceLabels: {...}, podLabels: {...} }` and supplies the `namespaceSelector` and `podSelector` for the DNS egress rule. The default matches kubeadm, EKS, GKE, AKS, and the upstream CoreDNS chart. Override it for clusters that run DNS in a non-standard namespace or with custom labels. See [Protecting agent containers from LLM provider access](../security/credentials.md#protecting-agent-containers-from-llm-provider-access).

**`gateway.externalHostnames`.** Required when the User Gateway is exposed via TLS pass-through Ingress, so that external clients see a cert whose SAN matches the public hostname they dialed. Backend re-encrypt Ingress works without it, because the Ingress controller dials the in-cluster Service DNS, which is already in the default SAN set. See [TLS and Ingress](../gateways/user/overview.md#tls-and-ingress).

**`gateway.platforms.*.apiBaseUrl`.** The two platform adapters reply through these URLs and nowhere else. They are chart values rather than AgentChannel fields on purpose: a tenant's channel Secret holds that tenant's platform credentials, and the gateway presents them only to the host the operator configured, so no channel can redirect a bearer token. Change the WhatsApp value to move to a newer Graph API version; the Discord value rarely changes. Both values must parse as `http(s)://host[/path]` with no query or fragment; a malformed value fails gateway startup instead of surfacing as delivery failures. See [Reply delivery](../gateways/user/platform-adapters.md#reply-delivery).

**`gateway.channelHealthWindow`.** Within the window, a channel reports `True` if any inbound request succeeded, `False` if the window holds only failures, and `Unknown` (`reason=NoRecentTraffic`) if no observations exist on a replica that has been up the full window. Tune it larger for low-traffic webhooks to avoid spurious `Unknown` flapping, and smaller for high-traffic channels where staler "last success" data is undesirable. See [Channel Health Tracking](../gateways/user/platform-adapters.md#channel-health-tracking).

**`gateway.agentDeliveryConnectTimeout`.** On iptables-mode kube-proxy, hibernated Agents fail-fast via RST and this timeout is essentially unused. On IPVS, Cilium kube-proxy replacement, and eBPF data paths that drop packets to empty-endpoint ClusterIPs, it is the per-wake latency cap before the gateway falls through to the activator wake. See [Per-Agent and Per-Task Child Resources](../runtime/child-resources.md) and [Activator](../gateways/user/activation-and-activity.md#the-activator).

**`gateway.providerFirstByteTimeout`.** This is the per-attempt timeout behind the fallback table's "timeout before any response bytes" trigger and the `504 provider_timeout` exhaustion mapping. The same value also applies mid-stream as an idle-bytes bound between SSE chunks, so an upstream that stalls without closing cannot hold gateway and agent connections open indefinitely. The default is deliberately generous, because LLM providers can legitimately take a minute-plus to first byte on long prompts, and it bounds the worst-case pre-error wait at `maxFallbackDepth × providerFirstByteTimeout`. See [Fallback Logic](../gateways/llm/fallback.md).

**The read timeouts and retry backoffs.** `gateway.agentDeliveryRetryBackoff` and `gateway.callbackRetryBackoff` are independently tunable; they merely share a default. Together with the read timeouts they define the retry-budget wall-clock derived in [Async Webhook Response](../gateways/api/async-responses.md).

**`gateway.maxResponseBodyBytes`.** The `900Ki` default sits well below the Kubernetes ~1 MiB ConfigMap object cap, so async responses fit in the per-request `kaalm-async-{requestId}` ConfigMap with envelope headroom. It also bounds the gateway's per-request memory footprint while it buffers the response in either mode. The payload is stored as text in the ConfigMap's `data` field (a JSON envelope), never in `binaryData`: base64 encoding would inflate 900Ki past the object cap and silently break the headroom math. Over-cap responses return `response_too_large` to the caller (sync) or to `callbackUrl` / the polling endpoint (async). See [The Kaalm Gateway](../gateways/overview.md) and [Request Flow](../gateways/user/overview.md#request-flow).

### Services

The chart installs two ClusterIP `Service`s (three with `console.enabled`). The whole SAN and endpoint design hangs on their stable DNS names:

| Service | Ports |
|---|---|
| `kaalm-gateway` | `:8080` user listener, `:8443` LLM/internal mTLS listener, `:9090` metrics |
| `kaalm-controller` | `:9443` activator, `:9444` conversion webhook (since v0.6.0), `:8080` metrics |
| `kaalm-console` (optional) | `:8443` pages and read API |

Every certificate SAN, every `$KAALM_GATEWAY_ENDPOINT` value, and every internal RPC in the other docs assumes these names in `kaalm-system`.

The optional `kaalm-upstream-ca` ConfigMap ([Upstream TLS Configuration](../gateways/llm/provider-routing.md#upstream-tls-configuration)) holds operator-supplied CA certificates: you create it, and the chart wires it in when you set `gateway.upstreamCA.configMap` to its name. The chart never templates its contents. It is projected next to the cluster CA under a distinct filename, because a projected volume cannot concatenate two ConfigMap keys into one file; the gateway merges every configured path into a single trust pool and re-reads them when they rotate.

### Metrics

Prometheus metrics are served on dedicated ports: controller `:8080/metrics` and gateway `:9090/metrics`, both unauthenticated in-cluster. The chart does **not** ship `ServiceMonitor` or `PodMonitor` manifests; scrape integration (and a NetworkPolicy admitting only the Prometheus ServiceAccount, if desired) is left to the platform team. See [Metrics](observability.md#metrics). Three Grafana dashboards ship as JSON in `config/grafana/`; see [Dashboards](observability.md#dashboards).

### Sample resources

- A single default AgentClass (`standard`) that platform teams can customize or delete. It leaves `runtimeClassName` unset, so Pods run under the cluster's default container runtime (runc in practice). Stock clusters define no `RuntimeClass` objects at all, so pinning a named one (even `runc`) would fail Pod admission with "RuntimeClass not found" everywhere it isn't explicitly created. Its fields are written in the API's canonical serialization (durations as `30m0s`, not `30m`), and that is a rule for any custom resource the chart authors: the controller's reconcile-time defaulting re-serializes what it reads, a field whose value changes in the process moves to the controller's field manager, and a server-side-apply Helm would then conflict with the controller on upgrade. A value that round-trips byte-identical keeps shared ownership and upgrades cleanly.
- A `sandboxed` example manifest (gVisor [`RuntimeClass`](../security/model.md#runtimeclass)) in the chart's `examples/` directory. Operators apply it after confirming the matching `RuntimeClass` is installed on the cluster. Shipping it as a live default would put any Agent that selected it into [`Degraded`](../controller/agent-lifecycle.md) on clusters without gVisor.
- Optionally, a sample ModelProvider manifest stub (keys not included) as a starting template.

## Certificate Lifecycle

**cert-manager and trust-manager are required dependencies.** The chart does not install the cert-manager or trust-manager controllers themselves, so teams with an existing cert-manager deployment reuse them. It ships the `ClusterIssuer`, `Certificate`, and `Bundle` resources Kaalm needs. The trust chain those resources form, and the mTLS topology built on it, are described in [In-cluster TLS](../security/tls.md#in-cluster-tls); this page covers only the resource inventory and the operational constraints. The [chart inventory figure](#helm-chart-contents) above shows where each of the resources below lands.

Admission webhooks are not used. The cert-manager dependency covers TLS lifecycle management and, since v0.6.0, the CRD conversion webhook's `caBundle`, which cert-manager's cainjector keeps current from the controller's certificate ([API Versioning and Deprecation](api-versioning.md#where-the-conversion-webhook-runs)).

### Resource inventory

| Resource | Name | Purpose |
|---|---|---|
| `ClusterIssuer` (self-signed) | `kaalm-selfsigned` | Creates the `Certificate` for the Kaalm CA. |
| `ClusterIssuer` (CA) | `kaalm-ca-issuer` | Sources the `kaalm-ca` Secret and signs all Kaalm-issued leaf certs. |
| `Certificate` | `kaalm-gateway-tls` | Gateway serving cert, used by both listeners. |
| `Certificate` | `kaalm-controller-tls` | The controller's activator endpoint and, since v0.6.0, its conversion webhook listener; the CRDs' `caBundle` is injected from this certificate's CA. |
| `Certificate` (optional) | `kaalm-console-tls` | The console's serving cert, and its client cert for the gateway's test-chat endpoint. Created only with `console.enabled`. |
| `Certificate` (one per Agent) | per-Agent | Created by the [AgentReconciler](../controller/reconcilers.md#agentreconciler) at provisioning time, owned by the Agent via ownerRef. See [Lifecycle of an agent TLS serving certificate](../security/tls.md#lifecycle-of-an-agent-tls-serving-certificate). |
| `Certificate` (one per AgentTask) | per-AgentTask | Created by the [AgentTaskReconciler](../controller/reconcilers.md#agenttaskreconciler) at provisioning time, owned by the AgentTask via ownerRef. See [Lifecycle of an AgentTask TLS client certificate](../security/tls.md#lifecycle-of-an-agenttask-tls-client-certificate). |
| `Bundle` (trust-manager) | `kaalm-ca` | Projects the Kaalm CA as a ConfigMap into every non-system user namespace. |

A `ClusterIssuer` (rather than a namespaced `Issuer`) is used for `kaalm-ca-issuer` so that per-namespace `Certificate` resources can reference the same signing key.

### The cluster resource namespace constraint

The CA `Certificate` and the `kaalm-ca` Secret it writes live in cert-manager's **cluster resource namespace** (Helm value `certManager.clusterResourceNamespace`, default `"cert-manager"`), **not** in `kaalm-system`. This is not a stylistic choice:

- A CA `ClusterIssuer` resolves `spec.ca.secretName` only in that namespace (cert-manager's `--cluster-resource-namespace` flag).
- trust-manager likewise reads `Bundle` sources only from its trust namespace (`--trust-namespace`, default `cert-manager`).

Operators who run either controller with a non-default namespace must set the value to match, or issuance fails cluster-wide with `SecretNotFound`.

### The gateway serving cert

`kaalm-gateway-tls` serves both gateway listeners: the cluster listener on port 8443 and the User listener on port 8080, from the same cert. **Despite the conventional HTTP association of port 8080, the User listener is TLS-only.** An Ingress fronting it must use HTTPS as its backend protocol. External webhook traffic arrives via Ingress configured for backend re-encrypt (or TLS pass-through); there is no plaintext listener on the gateway. See [TLS and Ingress](../gateways/user/overview.md#tls-and-ingress).

### CA projection into user namespaces

The `kaalm-ca` `Bundle` projects the CA into every non-system user namespace, including future ones added after install. Agent and AgentTask Pods mount the resulting ConfigMap at `/var/run/kaalm/ca.crt` to verify the gateway's TLS cert. Platform teams that need a tighter projection override `trustManager.bundleSelector`.

## Network Policy Prerequisite

**An NP-enforcing CNI is a required prerequisite** alongside cert-manager and trust-manager. See the NetworkPolicy bullet under [Per-Agent and Per-Task Child Resources](../runtime/child-resources.md) and [Network Policy](../security/model.md#network-policy).

## Tiered On-Ramp

The Helm chart supports a tiered on-ramp, so a platform team can get value from the gateway before adopting the agent lifecycle. For teams arriving with existing framework agents (LangGraph, LangChain), the user guide's Running Framework Agents page maps both tiers onto that starting point, with worked examples under `examples/langgraph-*/`.

### Tier 1: gateway only

Install the chart, configure a [ModelProvider](../resources/modelprovider.md), and point existing workloads at the gateway for LLM traffic and spend tracking. No AgentClass, Agent, AgentTask, or AgentChannel resources need to be created.

Existing workloads authenticate to the gateway using their own projected ServiceAccount tokens. No client certificate is required in this tier (see [Agent→Gateway Authentication](../security/rbac.md#agent-to-gateway-authentication) Mode 2 and [Namespace Identification](../gateways/llm/workload-identity.md)). Three things are on the workload owner in this tier:

1. **Mint the token for the right audience.** The token **must** be minted for audience `kaalm-gateway` via a `serviceAccountToken` projected volume (`audience: kaalm-gateway`). The gateway's `TokenReview` names that audience, so a Pod's default `kubernetes.default.svc`-audience token is rejected. A workload that skips this step gets `401` on every call.
2. **Supply the gateway URL.** Because Kaalm does not mutate non-managed Pods in this tier, the workload manifest must hard-code or template the URL itself. `https://kaalm-gateway.kaalm-system.svc:8443` is the in-cluster Service DNS. The controller injects `$KAALM_GATEWAY_ENDPOINT` only into full-lifecycle-tier Pods.
3. **Trust the CA.** Existing workloads must mount the `kaalm-ca` ConfigMap (projected by trust-manager into every non-system namespace) and configure their HTTP client to trust it. Otherwise calls to the gateway fail TLS verification.

Access control in this tier is governed by `ModelProvider.spec.allowedNamespaces` plus `spec.models` only. AgentClass `allowedProviders` does not apply, because there is no Agent resource to reconcile against. Platform teams who need class-scoped provider policy must use the full lifecycle tier.

### Tier 2: full agent lifecycle

Configure [AgentClasses](../resources/agentclass.md), deploy Agents and AgentTasks with hibernation and wake-on-demand, and connect them to user-facing channels via AgentChannels (webhook in v1). Channel integration is included in this tier because wake-on-demand requires a channel to be fully testable.

Kaalm-managed Pods authenticate via mTLS using per-agent certificates issued by cert-manager. The LLM Gateway enforces the full routing chain (Agent → AgentClass `allowedProviders` → ModelProvider `allowedNamespaces`/`models`) for this tier. See [Provider Routing](../gateways/llm/provider-routing.md).

## Upgrade and Migration

Since v0.6.0 the API is graduated: `v1beta1` is served and stored, `v1alpha1` is served, deprecated, and converted by the controller, and the machinery this section once deferred (the conversion webhook, storage-version migration, and the deprecation window) is specified on [API Versioning and Deprecation](api-versioning.md). This section keeps the operational order of an upgrade and the chart-level notes; the version mechanics, what happens in the window between the two steps, and the policy live there.

### Rolling upgrade order

**Apply the CRDs first.** CRDs live in the chart's `crds/` directory, which `helm install` applies but `helm upgrade` **never touches**. CRD schema changes must be applied explicitly:

```bash
kubectl apply --server-side -f crds/
```

That command is the documented first step of every chart upgrade, before `helm upgrade`. Since v0.6.0 it is also the step that adds a new API version and its conversion stanza, and the window between it and the `helm upgrade` has a precise, bounded cost, stated on [API Versioning and Deprecation](api-versioning.md#upgrading-in-place). Keeping CRDs in `crds/` rather than `templates/` also means `helm uninstall` never deletes them, which matters because deleting a CRD cascade-deletes every CR of that kind cluster-wide.

**Apply order is not a rollout barrier.** Helm's kind-sorted apply does not help here: the controller and gateway Deployments are applied together and roll **concurrently**, so there is no ordering guarantee between the controller observing new CRD fields and the gateway consuming what the controller writes (status fields, budget `_canonical`, per-task RBAC). Version-skew tolerance, not apply order, is what makes the rollout safe. Both Deployments roll with `maxUnavailable: 1` under their PDBs, so one replica of each stays serving throughout: wake-on-demand and the LLM proxy remain available across the upgrade.

**Version-skew tolerance is one chart version, and only for the duration of an in-progress rollout.** The internal contracts (activity and channel-health response bodies, the budget ConfigMap shape, the activator wire contract, the per-request async ConfigMap labels) evolve additively within a minor version, so a mixed old/new controller↔gateway pair works mid-rollout. Running mixed versions as a steady state is unsupported.

### CRD schema evolution

Within a served version, changes are additive only: new optional fields with reconcile-time defaulting (per [Defaulting](../resources/validation-and-defaulting.md#defaulting)), new condition reasons, new enum values. Anything breaking ships as a new API version with conversion in both directions, never in place. The full rule set, the `v1alpha1` window, and the conversion mechanics are on [API Versioning and Deprecation](api-versioning.md#deprecation-policy).

### Helm chart upgrades

Values whose change has workload-visible effects are the ones to review before an upgrade:

| Value | Effect on change |
|---|---|
| `gateway.replicas` | Rate-limit buckets re-divide on the next refill cycle. See [Rate Limiting](../gateways/llm/budgets-and-rate-limits.md#rate-limiting). |
| `controller.networkPolicy.dnsSelector` | Regenerates every synthesized per-agent NetworkPolicy. |
| `gateway.channelHealthWindow` | Changes `PlatformConnected` flapping behavior. |
| The body-size caps | In-flight requests sized between the old and new caps change fate. |

The chart's `fail` template guards (the `replicas ≥ 2` floors) abort rendering on known-bad values.

A gateway rollout also resets in-memory activity state: expect idle and hibernation transitions to defer for `idleTimeout` afterwards, per [Activity Detection](../controller/hibernation-and-wake.md#activity-detection). Schedule chart upgrades accordingly on clusters with multi-hour idle timeouts.

### ModelProvider credential rotation, end-to-end

No Pod restarts are needed at any step, in either tier.

1. The platform engineer updates the credential Secret in `kaalm-system`.
2. The gateway's Secret watch refreshes in-memory credentials without a restart (see [Lifecycle of an LLM API key](../security/credentials.md#lifecycle-of-an-llm-api-key)). In-flight requests complete on the old key.
3. The next ModelProviderReconciler health probe validates the new key. A bad rotation surfaces as `Ready=False, reason=CredentialsInvalid` within one probe interval (default 60s), plus a `Warning` event from any live traffic that hits upstream `401`/`403` and falls back (see [Fallback triggers](../gateways/llm/fallback.md#fallback-triggers)).
4. Audit. The Secret update lands in the Kubernetes audit log (enable `RequestResponse` level for `kaalm-system` Secrets per [Recommendations](../security/model.md#recommendations-for-deployment)); confirm cut-over on the provider's own key-usage dashboard.

### Breaking spec changes

Since v0.6.0 a breaking spec change ships as a new API version with conversion, so an upgrade never replaces existing objects ([API Versioning and Deprecation](api-versioning.md#deprecation-policy)). The replace-not-migrate procedure (let in-flight AgentTasks run to completion or delete them, hibernate or delete Agents, apply the new CRDs, then re-apply updated manifests) remains the path for a fresh install or a rollback across the graduation.

Agent state survives a replace through the PVC path: `pvcRetention: Retain` on delete, or snapshot-and-remount via [`persistence.existingClaim`](../resources/agent.md).

Channel receivers see a bounded gap. Webhook paths return `401` while the AgentChannel is absent (indistinguishable from an unregistered path, per the [401 contract](../gateways/api/channel-webhook.md)), and stored async responses survive independently in `kaalm-system` until their 1-hour TTL, unless the channel is deleted, in which case the finalizer sweeps them (see [Finalizers](../controller/finalizers.md)).
