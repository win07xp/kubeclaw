# Threat Model

This page enumerates the threats Kaalm defends against, the threats it deliberately does **not** defend against, and the reasoning behind each boundary. It is organized into ten themes. Every threat has exactly one row. Where a mitigation needs more than a sentence, the row states the decision and links to a note that carries the full argument.

Read [Trust Model](model.md#trust-model) first: several rows resolve to "out of scope" purely because of where the trust boundary sits. A threat being out of scope is a design decision, not an oversight.

## Credential Containment and Egress

The central invariant is that LLM provider credentials never leave `kaalm-system`. Agent Pods reach providers only by proxying through the gateway, and a synthesized NetworkPolicy is what makes that the only reachable path.

| Threat | Mitigation |
|---|---|
| Malicious agent container (Kaalm-managed Pod) tries to call LLM providers directly | Credentials never leave `kaalm-system`; the AgentReconciler synthesizes a per-Agent NetworkPolicy whose default-deny egress (standard k8s, no service mesh required) blocks direct egress from the agent Pod to provider IPs. Same for AgentTask Pods. |
| Gateway-only-tier workload calls LLM providers directly with its own keys | **Not mitigated by Kaalm.** See [Gateway-only-tier workload calls providers directly](#gateway-only-tier-workload-calls-providers-directly). |
| Developer authors a permissive NetworkPolicy in their own namespace that broadens Agent Pod egress | **Out of scope by trust model.** See [Developer authors a permissive NetworkPolicy](#developer-authors-a-permissive-networkpolicy). |
| Developer bypasses gateway by embedding credentials in image | No mitigation at the platform level: process/review concern; mitigate with image scanning and registry controls. |

### Notes

#### Gateway-only-tier workload calls providers directly

Gateway-only-tier workloads are existing Deployments that the platform team has granted gateway access to through `TokenReview`; they have no Agent CR and therefore no Kaalm-synthesized NetworkPolicy. Routing through the gateway is voluntary in this tier: a workload that holds its own provider credentials can bypass the gateway entirely. Platform teams adopting the gateway-only tier must apply their own default-deny egress NetworkPolicy (or use a service mesh) on those namespaces if they want to enforce gateway routing. The full Agent lifecycle tier remains the only path with automatic NetworkPolicy enforcement.

#### Developer authors a permissive NetworkPolicy

[§ Trust Model](model.md#trust-model) places the developer in the trusted tier; opting out of guardrails is the developer's choice, not a defended-against threat. NetworkPolicy is additive, so an additional permissive policy unions with Kaalm's synthesized one: Kaalm cannot prevent this through synthesis alone. Platform teams who treat developers as untrusted should restrict `networkpolicies` create/patch in user namespaces with cluster RBAC. The agent-container threat (the actual untrusted actor) cannot author NetworkPolicies: its ServiceAccount has no such permissions by default. See [§ Network Policy](model.md#network-policy) and [§ Protecting agent containers from LLM provider access](credentials.md#protecting-agent-containers-from-llm-provider-access).

## Workload Isolation

Agent containers execute LLM-generated code, so they are treated as the untrusted actor inside an otherwise trusted namespace.

| Threat | Mitigation |
|---|---|
| Developer deploys agent with resource bomb | AgentClass.maxLimits enforced; image allowlist prevents arbitrary images |
| Agent container executes LLM-generated code that attempts container escape | RuntimeClass (gVisor/Kata) provides kernel-level isolation; Pod Security Standards prevent privilege escalation |
| Namespace member with ConfigMap write injects unreviewed code into an image-review-approved base image through a handler mount ([`Agent.spec.handler`](../resources/agent.md)) | Off by default: [rule 30](../resources/validation-and-defaulting.md#cross-resource-validation) requires `AgentClass.spec.image.allowHandlerMounts: true`, so image review stays authoritative unless a class explicitly opts out. See [What granting `allowHandlerMounts` means](#what-granting-allowhandlermounts-means). |

See [§ RuntimeClass](model.md#runtimeclass) for the enforcement details.

### Notes

#### What granting `allowHandlerMounts` means

**Granting the gate makes ConfigMap write access in a namespace equivalent to code execution as that namespace's handler-mounting Agents**, with their ServiceAccount, certificate identity, and gateway access, effective at the next Pod recreation. Grant it on dedicated classes whose namespaces treat every ConfigMap author as a code author.

Injected code runs under the same containment as reviewed code: the class SecurityContext, a RoleBinding-less ServiceAccount, the synthesized NetworkPolicy, and the optional RuntimeClass. A cross-namespace reference is unrepresentable, because `configMapRef` is a local reference. See [Reference Base Images](../runtime/base-images.md) and rules 30 and 31.

## Credential Storage and Rotation

| Threat | Mitigation |
|---|---|
| Platform credentials leak through an etcd backup | Standard k8s concern; encrypt etcd at rest |
| Stale credentials after rotation cause silent failures | Gateway watches Secrets for changes; ModelProviderReconciler verifies credential validity on each health check |
| Channel credential leaked from agent namespace | Channel credentials are stored in the agent's namespace; blast radius is limited to that namespace's channels. The platform team (not the developer) is responsible for rotation. |

Lifecycle detail lives in [§ Lifecycle of an LLM API key](credentials.md#lifecycle-of-an-llm-api-key).

## Component Compromise

These rows ask what an attacker gains by taking over Kaalm's own control-plane components. The honest answer for credential reads is: everything in `kaalm-system`. The scoping that exists is a least-privilege default and an integrity control against drift, not a hard boundary.

| Threat | Mitigation |
|---|---|
| Compromised operator reads credential Secrets | No cluster-wide Secret access; standing read scoped to `kaalm-system`, which does contain every provider key. See [Compromised operator reads credential Secrets](#compromised-operator-reads-credential-secrets). |
| Compromised gateway reads all LLM credentials | Gateway Secret access scoped to `kaalm-system`; gateway image should be signed and verified; restrict who can update gateway Deployment |
| Compromised gateway writes malicious ConfigMaps to user namespaces | Gateway has **no `create` verb** on user-namespace ConfigMaps. See [Compromised gateway writes malicious ConfigMaps](#compromised-gateway-writes-malicious-configmaps). |
| Untrusted workload sends a hostile or oversized request body to exploit the gateway's parsers (JSON decoding; since v0.7.0 the cross-format translator) | Every listener bounds its reads before parsing and fails closed on a body it cannot decode; the translator adds no parsing trust. See [Hostile bodies and the parsers](#hostile-bodies-and-the-parsers). |

### Notes

#### Compromised operator reads credential Secrets

The operator has no cluster-wide Secret access: its standing Secret read is scoped to `kaalm-system`, which **does** contain every LLM provider key, so a compromised operator reads them all, the same blast radius as a compromised gateway (mitigate identically: image signing, restricted Deployment update rights, audit logging on `kaalm-system` Secret access). In user namespaces the only reach is through dynamic per-AgentChannel Roles, each `resourceNames`-scoped to that channel's auth Secret(s); a compromised operator cannot *directly* enumerate or read arbitrary user-namespace Secrets.

The `escalate`/`bind` grants on `roles`/`rolebindings` (see [Operator ServiceAccount](rbac.md#operator-serviceaccount)) mean a fully compromised operator could widen those Roles and bind them to principals of its choosing. The per-channel scoping is an integrity control against drift and a least-privilege default, not a hard boundary against operator compromise.

#### Compromised gateway writes malicious ConfigMaps

The `{taskName}-completion` ConfigMap is pre-created by the AgentTaskReconciler with the AgentTask as `ownerRef`; the gateway's per-task Role grants only `update, patch` on that exact name (`resourceNames`-scoped, with `get` omitted since the write is a blind merge patch). The gateway has **no `create` verb** on user-namespace ConfigMaps, so a compromised gateway cannot introduce new ConfigMaps in user namespaces: it can mutate only the per-task and per-channel resources it has explicit name-scoped access to. See [§ Gateway ServiceAccount permissions](rbac.md#gateway-serviceaccount-permissions).

#### Hostile bodies and the parsers

Every listener bounds its reads before parsing: the LLM proxy rejects oversized bodies with `413` at its configured cap, and the message, MCP, and platform paths carry their own caps. Bodies decode with the standard library into typed structures; an unparseable body fails closed with `400` before any provider or agent is contacted.

The translator adds no parsing trust: it rewrites between two shapes the proxy has already decoded, and a request it cannot express in the target format makes that candidate ineligible rather than producing a partial translation. Prompt bodies are never logged in the default build ([PII safety](../operations/observability.md#pii-safety)).

## Tenant Isolation and Budgets

Budgets are guardrails by default, not hard caps; providers that need a cap opt in to [hard enforcement](../gateways/llm/budgets-and-rate-limits.md#hard-enforcement), which states its guarantee and its bounds. Two rows here restate the default's consequence so it is not mistaken for a bug.

| Threat | Mitigation |
|---|---|
| One tenant exhausts another tenant's budget through a shared provider | Per-namespace spend accounting; `allowedNamespaces` restricts access; budgets are soft limits with bounded overspend, or hard-capped when the provider opts in |
| Agent makes requests to unauthorized provider | Gateway validates model against ModelProvider.models and namespace against allowedNamespaces. Every fallback candidate is re-validated the same way during the walk, including a cross-format candidate's mapped model against that candidate's own catalog ([rules 12 and 41](../resources/validation-and-defaulting.md#cross-resource-validation)) |
| Budget guardrails exceeded under high concurrency | Soft mode documents its bounded overspend; hard enforcement bounds the crossing to the stated in-flight guarantee; provider account limits remain defense in depth |
| Gateway-only tenant uses a provider their AgentClass would have denied in the mTLS tier | **Expected behavior, not a vulnerability.** See [Gateway-only tenant uses a provider AgentClass would have denied](#gateway-only-tenant-uses-a-provider-agentclass-would-have-denied). |
| Denied caller probes which providers and models exist | Authorization ordering protects the model catalog; provider existence is deliberately distinguishable. See [What a denied caller learns](#what-a-denied-caller-learns). |
| Fallback routes a request to a provider outside the workload's AgentClass `allowedProviders` | **Expected behavior: the edge is the platform team's routing decision.** See [Fallback edges and allowedProviders](#fallback-edges-and-allowedproviders). |
| Upstream provider error text leaks platform detail to callers | Only non-fallbackable 4xx answers relay verbatim; every other failure reduces to a generic classified envelope. See [What flows back from a failed provider call](#what-flows-back-from-a-failed-provider-call). |

### Notes

#### Gateway-only tenant uses a provider AgentClass would have denied

The gateway-only tier is deliberately not gated by AgentClass: those workloads have no Agent resource and therefore no `allowedProviders` to consult. Access control reduces to `ModelProvider.spec.allowedNamespaces` plus `spec.models`. Platform teams who need class-scoped provider policy must onboard workloads through the full Agent lifecycle tier. See [Provider Routing § Gateway-only tier](../gateways/llm/provider-routing.md).

#### What a denied caller learns

Authorization ordering keeps the model catalog behind the namespace gate: `allowedNamespaces` is checked before model existence, so a namespace that is not allowed to use a provider cannot learn which models it hosts. Provider existence is the accepted remainder: an unknown provider answers `400 invalid_request` and a denied one answers `403 access_denied`, so a caller can tell whether a provider name exists. Provider names are cluster-scoped identifiers on the same footing as namespace names, and both planes, the LLM proxy and the MCP broker, share this posture.

#### Fallback edges and allowedProviders

A fallback candidate is re-validated against its own `allowedNamespaces` and catalog ([rules 12 and 41](../resources/validation-and-defaulting.md#cross-resource-validation)), but not against the workload's AgentClass `allowedProviders`. The workload gates govern what a caller may request; a fallback edge is the platform team's routing decision, declared on the provider they own ([Fallback Logic](../gateways/llm/fallback.md) lists the static checks). A platform team that does not want traffic reaching a provider through fallback does not declare the edge.

#### What flows back from a failed provider call

Only a non-fallbackable provider answer (`400`, `422`, and the other 4xx statuses except `401`, `403`, and `429`) relays to the caller verbatim, translated into the caller's format when the serving candidate crossed. The statuses that could echo credential material (`401`, `403`) and the retryable classes (`429`, 5xx, transport errors) never relay: the fallback walk continues past them, and exhaustion answers with a generic classified envelope. Transport error text is reduced to a failure class before it can carry an upstream URL or address, so provider-side detail cannot travel back through error text.

## Channels and Webhooks

Inbound webhooks come from third parties that follow their own signing conventions, and outbound callbacks leave the gateway, which has stronger egress than any user namespace. Both directions get explicit treatment.

| Threat | Mitigation |
|---|---|
| Malicious message from channel platform | Webhook adapter authenticates inbound events (bearer token, HMAC signature) before processing |
| Captured inbound webhook is replayed against the gateway | Not preventable at the gateway: inbound HMAC is body-only with no timestamp. Cost bounded by budgets; side-effect dedup is the agent's job. See [Captured inbound webhook is replayed](#captured-inbound-webhook-is-replayed). |
| Forged Discord interaction or WhatsApp event | The platform adapters verify the platform's own signature before anything else: Ed25519 over timestamp and body for Discord, HMAC-SHA256 over the body with the app secret for WhatsApp. A verification handshake is authenticated too. See [Replay bounds on the platform surfaces](#replay-bounds-on-the-platform-surfaces). |
| Tenant points a platform channel's reply at a host of their choosing, carrying the channel's bearer token | Not expressible: the platform API base URLs are gateway-level Helm values (`gateway.platforms.<type>.apiBaseUrl`), not channel fields. A tenant's Secret holds only that tenant's platform credentials, and the gateway only ever presents them to the operator-configured host. See [Why the platform reply path has no deny-range machinery](#why-the-platform-reply-path-has-no-deny-range-machinery). |
| Caller with channel A's credentials fetches channel B's async response by supplying channel B's `requestId` to the poll endpoint | Poll endpoint asserts the stored response's channel labels match the authenticated AgentChannel; mismatch returns `404 Not Found`. See [Cross-channel async response fetch](#cross-channel-async-response-fetch). |
| Developer uses `AgentChannel.spec.webhook.callbackUrl` as SSRF against internal cluster services (such as `kubernetes-dashboard.kube-system` or the cloud metadata IP 169.254.169.254) | `https://` plus internal-IP-range denial enforced at admission, re-checked and IP-pinned on every delivery. See [SSRF through callbackUrl](#ssrf-through-callbackurl). |
| Third party forges a callback POST to a developer's `callbackUrl` | The gateway signs every callback POST using `AgentChannel.spec.webhook.callbackAuth`, which CRD CEL makes mandatory whenever `callbackUrl` is set. See [Forged callback POST](#forged-callback-post). |

### Notes

#### Captured inbound webhook is replayed

Inbound HMAC is body-only with no timestamp (see [Inbound webhook auth](../gateways/api/channel-webhook.md)): the deliberate cost of supporting arbitrary third-party senders that follow their own signing conventions (GitHub-style body-only HMAC, etc.). The gateway therefore cannot reject replays of a captured `(body, HMAC)` pair until the secret is rotated.

Cost replay is bounded by per-namespace LLM budgets (see [Multi-tenancy](../concepts/tenancy-and-tiers.md#multi-tenancy)). Side-effect replay is the agent's responsibility: agents performing non-idempotent inbound actions must dedup on a caller-supplied idempotency key or content hash. The gateway-side `messageId` dedup ([The Runtime Contract item 7](../runtime/contract.md)) addresses gateway-retry duplicates only, not external replay.

#### Replay bounds on the platform surfaces

Discord's signed timestamp is checked against a 300s skew window, which bounds replay on that surface. WhatsApp's signature is body-only, the same replay posture as the generic webhook: see [Captured inbound webhook is replayed](#captured-inbound-webhook-is-replayed). The adapters and their wire contracts are documented in [The platform adapters](../gateways/user/platform-adapters.md#inbound).

#### Why the platform reply path has no deny-range machinery

The SSRF defenses on `callbackUrl` (deny ranges, pre-dial re-resolution, IP pinning) exist because the URL is developer-supplied. A platform base URL is operator-supplied, set at install time in the same trust tier as a ModelProvider `baseUrl` or an MCP upstream endpoint; defending the gateway against its own operator is out of scope by [the trust model](model.md#trust-model). The value also accepts `http://` for the same reason: the e2e mock platforms stand in at install time, and the operator owns the consequence.

#### Cross-channel async response fetch

The poll endpoint authenticates the caller against the AgentChannel named by the `channelPath` query parameter, then asserts that the stored response's `kaalm.io/channel-namespace` / `kaalm.io/channel-name` labels match that same AgentChannel before returning the payload. `requestId` values are UUIDs but not secrets; the label check prevents cross-channel data leakage. Mismatches return `404 Not Found`, indistinguishable on the wire from "unknown `requestId`", to avoid confirming the existence of a cross-channel response (the gateway logs the mismatch with `reason=ChannelMismatch` for operator debugging). See [Async Webhook Response poll semantics](../gateways/api/async-responses.md).

#### SSRF through callbackUrl

The gateway has stronger egress than any user namespace, so an unrestricted `callbackUrl` would let the developer turn the gateway into a confused deputy. The AgentChannelReconciler enforces at admission/reconcile time that `callbackUrl` uses `https://` and that its host does not resolve to loopback, link-local, RFC1918, unique-local IPv6, shared address space (100.64.0.0/10), benchmarking space (198.18.0.0/15), or cloud-metadata IPs (see [Cross-Resource Validation rule 22](../resources/validation-and-defaulting.md#cross-resource-validation)).

On every delivery attempt the gateway re-resolves the host, re-applies the check, and **dials the exact IP that passed**: a custom dialer resolves once, range-checks the result, and connects to that pinned IP:port while preserving the Host header and SNI (see [Request Flow step 8](../gateways/user/overview.md#request-flow)). Handing the hostname back to the HTTP transport would let it re-resolve independently, re-opening the DNS-rebinding window the check exists to close. Platform teams may replace the deny-internal default with an explicit allowlist through the Helm value `gateway.callbackUrl.allowlist`.

#### Forged callback POST

The gateway signs every callback POST (success and error payloads alike) using `AgentChannel.spec.webhook.callbackAuth`: bearer (`Authorization: Bearer …`) or HMAC over the canonical string `"{requestId}\n{timestamp}\n{sha256(body)}"` with the timestamp in `X-Kaalm-Timestamp`. `callbackAuth` is required by [cross-resource validation rule 25](../resources/validation-and-defaulting.md#cross-resource-validation) whenever `callbackUrl` is set: CRD CEL rejects AgentChannels that try to configure an unsigned callback, so unsigned callbacks cannot be deployed by accident. Receivers verify the signature using the same Secret material; replay is bounded by a 300s timestamp skew window (mirroring the polling-endpoint contract). See [Callback authentication](../gateways/api/async-responses.md).

## Identity and Namespace Spoofing

Every authorization decision the gateway makes keys off a namespace, so forging a namespace is the highest-value attack. Both auth modes attest identity cryptographically and then cross-check it topologically against the source Pod; both must agree. The full request-time flow, the SAN shapes, and the label counts live in [Namespace Identification](../gateways/llm/workload-identity.md).

| Threat | Mitigation |
|---|---|
| Agent spoofs namespace to bypass budget/access controls (mTLS tier) | Namespace comes from the `kaalm-ca`-signed cert SAN; the CA key is unreachable from agent Pods. Two extra defenses close the dotted-name label-shift bypass. See [Agent spoofs namespace (mTLS tier)](#agent-spoofs-namespace-mtls-tier). |
| Agent spoofs namespace (gateway-only-tier, token auth) | Namespace is extracted from the token's `status.user.username` returned by `TokenReview`, which the apiserver signs. An agent cannot forge a token for a different namespace: the token's signature is checked by the apiserver, not the gateway. Source-IP to Pod cross-check validates the Pod's actual namespace matches. Both cryptographic (apiserver signature) and topological (source IP) attestation must agree. |
| Gateway-only-tier tenant uses a ServiceAccount token from another namespace to read another tenant's budget | Impossible: `TokenReview`'s returned `status.user.username` names the token's actual namespace of origin. The gateway uses *that* namespace for all authorization decisions, not any namespace the caller claims in the request body. A valid token from namespace A can only be used to act as namespace A. |
| Stolen `kubernetes.default.svc`-audience token reused against the gateway | Gateway's `TokenReview` request specifies audience `kaalm-gateway`. Tokens minted for a different audience fail validation. Workloads must explicitly project a gateway-audience token. |
| Kaalm-managed Agent/AgentTask Pod tries to downgrade auth by using its ServiceAccount token instead of mTLS | Rejected by the gateway's **Pod-ownership precheck** on the bearer-token path, before any `TokenReview` call. See [Auth downgrade to ServiceAccount token](#auth-downgrade-to-serviceaccount-token). |
| Agent created in `kaalm-system` acquires a certificate whose SAN collides with an internal Service identity | The reconcilers refuse to provision Agent, AgentTask, or AgentChannel resources in `kaalm-system` (`Ready=False, reason=SystemNamespaceForbidden`, [rule 28](../resources/validation-and-defaulting.md#cross-resource-validation)). See [SAN collision in kaalm-system](#san-collision-in-kaalm-system). |

### Notes

#### Agent spoofs namespace (mTLS tier)

Namespace is extracted from the cert SAN, signed by `kaalm-ca`. Agents use `{name}.{namespace}.svc.cluster.local`; AgentTasks use `{name}.{namespace}.task.kaalm.io`. An agent cannot forge a cert for a different namespace (the CA key is not reachable from any agent Pod).

Two additional defenses specifically prevent a dotted-name label-shift bypass, where a name containing dots would shift the SAN's labels and make the parser read the wrong position as the namespace:

1. Agent and AgentTask `metadata.name` are restricted to DNS-1123 **label** form (no dots) by CRD CEL. See [Cross-Resource Validation rule 21](../resources/validation-and-defaulting.md#cross-resource-validation).
2. The gateway's SAN parser requires the exact label count for each shape (5 for Service-DNS, 4 for `.task.kaalm.io`) and rejects any cert whose SAN has extra labels. See [Namespace Identification § Mode 1](../gateways/llm/workload-identity.md#mode-1-mtls-client-certificate).

Source-IP to Pod cross-check validates the cert identity against the actual source Pod; all checks must agree.

#### Auth downgrade to ServiceAccount token

Before any `TokenReview` call, the gateway resolves the request's source IP to a Pod through its informer cache and returns `401 Unauthorized` if that Pod has an `ownerRef` to an `Agent` or `AgentTask` resource or carries the Kaalm-managed label set. The check runs on every request (it is not cached) and runs before `TokenReview`, so it is unaffected by token-cache hits or apiserver latency. The gateway also attempts mTLS first when a client cert is presented; if both auth materials are present the bearer header is ignored.

mTLS remains the only accepted auth mode for that tier, keeping its credential surface to one bounded-lifetime, namespace-pinned artifact. Rotating a leaf cert contains exposure but does not revoke the old one, which is why a single credential surface matters here (see [§ In-cluster TLS](tls.md#in-cluster-tls) for the containment-versus-revocation distinction and the CA re-key runbook). See [Namespace Identification, Mode 2](../gateways/llm/workload-identity.md#mode-2-serviceaccount-bearer-token).

#### SAN collision in kaalm-system

An Agent named `kaalm-gateway` would be issued SAN `kaalm-gateway.kaalm-system.svc.cluster.local`, exactly the identity that activator, activity, channel-health, and agent-side `/v1/message` authorization trust. The per-Agent SAN shape is distinguishable from the internal-endpoint SANs only by its namespace label, so the guard makes the collision unreachable even for admins; locking down `kaalm-system` ([Recommendation 1](model.md#recommendations-for-deployment)) is the outer layer.

## Internal Endpoint Abuse

Certificates signed by `kaalm-ca` are not interchangeable. Authorization on internal endpoints is by SAN, not by mere possession of a CA-signed cert, which is what keeps a compromised agent from reaching them with its own valid cert.

| Threat | Mitigation |
|---|---|
| Unauthorized agent wake-up through the activator endpoint | The activator endpoint requires an mTLS client cert whose SAN matches the gateway Service DNS: any other SAN, even one signed by `kaalm-ca`, is rejected with `403 Forbidden`. The per-Agent SAN shape cannot match the gateway SAN, so a compromised agent cannot use its own cert to trigger wake-ups. See [§ Internal Endpoint Authentication](rbac.md#internal-endpoint-authentication). |
| Compromised in-cluster Pod with network reach to an agent's Service forges channel messages | The agent's `POST /v1/message` listener requires a client cert with the gateway SAN. NetworkPolicy is the first layer; the agent-side per-path mTLS check is the second. See [Forged channel messages from a compromised Pod](#forged-channel-messages-from-a-compromised-pod). |
| Non-apiserver in-cluster caller POSTs ConversionReview payloads to the cert-less conversion listener | **Accepted with bounds**: conversion is a pure function with nothing to extract, and request bodies are capped. See [The conversion listener's posture](#the-conversion-listeners-posture). |

### Notes

#### Forged channel messages from a compromised Pod

The agent's `POST /v1/message` listener requires a client certificate whose SAN matches the gateway Service DNS (`kaalm-gateway.kaalm-system.svc.cluster.local` / `.svc`). A compromised non-gateway Pod cannot present such a cert (the `kaalm-ca` private key is not reachable from any non-gateway Pod), so even if it bypasses or piggybacks on a misconfigured per-Agent NetworkPolicy, the request is rejected at the handler: the agent's listener accepts the handshake without a client cert (`VerifyClientCertIfGiven`, so kubelet probes on the shared port keep working) and the `/v1/message` handler then returns `401` for a missing cert or `403` for a non-gateway SAN. NetworkPolicy is the first layer; the agent-side per-path mTLS check is the second. See [The Runtime Contract](../runtime/contract.md) bullet 4 and [§ In-cluster TLS](tls.md#in-cluster-tls).

#### The conversion listener's posture

The CRD conversion listener (`:9444`) requires no client authentication because its caller is the apiserver, which presents no certificate. What an unauthorized in-cluster caller gains is deliberately nothing: the handler is a pure function that decodes a ConversionReview, converts between `kaalm.io` versions, and echoes the result, reading no cluster state and holding no credentials. The remaining lever was resource exhaustion, and request bodies are capped far above any real review. The chart ships no NetworkPolicy for `kaalm-system` components, so restricting who can reach `:9444` and the unauthenticated metrics port `:9090` is the operator's network policy to write; [Deployment](../operations/deployment.md) covers the install-time posture.

## The Console

The [console](../console/overview.md) is optional and off by default. When enabled it is a listener with human callers, so its threats are session-shaped rather than workload-shaped.

| Threat | Mitigation |
|---|---|
| Stolen or replayed console session cookie | The cookie is `Secure`, `HttpOnly`, `SameSite=Strict`; sessions live in console memory and expire with the pasted token or after 24 hours, whichever comes first. A console restart invalidates every session. |
| Caller uses the console to view namespaces their token does not grant | The console fixes the caller's identity at login with `TokenReview` and gates every namespace-scoped read with a `SubjectAccessReview` for that identity. An unauthorized token sees an empty namespace list, `403` on direct access, and `401` when invalid ([S19](../appendix/scenarios.md#s19-see-the-fleet-without-kubectl)). |
| Compromised console | The console's ServiceAccount reads CRD status and creates `TokenReview`/`SubjectAccessReview`, and holds nothing else: no Secret access anywhere, so no credential can leak through it. The capability gained is the console SAN's test-chat and spend access. See [What a compromised console gains](#what-a-compromised-console-gains). |
| Cross-site request forgery against test-chat, the console's one state-changing route | The `SameSite=Strict` session cookie is the control; there is no separate CSRF token. See [Why SameSite is the CSRF control](#why-samesite-is-the-csrf-control). |

### Notes

#### What a compromised console gains

The gateway authorizes the console SAN on `POST /v1/test-chat` and `GET /v1/spend`, so a compromised console can wake agents, deliver messages to them (spending their namespaces' budgets), and read spend figures. Test-chat deliveries carry a `/console/{namespace}/{agentName}` channel origin, so they are distinguishable in the gateway's delivery log and in session identity. The component is stateless; the mitigation posture matches the other `kaalm-system` components: restrict and audit access to the namespace, and leave the console disabled where it is not used.

#### Why SameSite is the CSRF control

Browser sessions ride a `SameSite=Strict` cookie, which browsers do not attach to any cross-site request, so a hostile page cannot ride an operator's session into test-chat. API callers authenticate per request with a bearer header, which a cross-site page cannot set. The login POST carries no session yet, and a forged login would bind the attacker's own token, gaining the attacker nothing. The console therefore carries no separate CSRF token.

## Transport and PKI Dependencies

cert-manager and trust-manager are cluster-critical dependencies. Both fail fast at install; both degrade gracefully at runtime, in the sense that running workloads keep working while new provisioning stalls.

| Threat | Mitigation |
|---|---|
| In-cluster traffic sniffed on shared nodes | All agent/gateway and gateway/controller traffic is TLS-encrypted. Certificates are cert-manager-managed and rooted at `kaalm-ca`. See [§ In-cluster TLS](tls.md#in-cluster-tls). |
| cert-manager not installed or unhealthy | Chart install fails fast if `kaalm-ca-issuer` cannot be created. Runtime degradation delays new provisioning and blocks rotation; running agents continue. See [cert-manager unhealthy](#cert-manager-unhealthy). |
| trust-manager not installed or unhealthy | Chart install fails fast if the `Bundle` resource cannot be created. Runtime degradation blocks CA distribution to new namespaces. See [trust-manager unhealthy](#trust-manager-unhealthy). |

### Notes

#### cert-manager unhealthy

Chart install fails fast if `kaalm-ca-issuer` cannot be created; a mismatched `certManager.clusterResourceNamespace` (the ClusterIssuer resolves the CA Secret only there) surfaces as the issuer stuck `Ready=False, reason=SecretNotFound`. Runtime degradation of cert-manager delays new Agent/AgentTask provisioning (the `Certificate` Secret is not populated) and blocks cert rotation, but running agents continue until their current certs approach expiry. Operators should monitor cert-manager health as a cluster-critical dependency.

#### trust-manager unhealthy

Chart install fails fast if the `Bundle` resource cannot be created. Runtime degradation prevents the Kaalm CA ConfigMap from appearing in new namespaces, so Pods scheduled into those namespaces fail to mount `/var/run/kaalm/ca.crt` and cannot verify the gateway's TLS cert. Existing namespaces with the ConfigMap already projected are unaffected until the next CA rotation. Monitor trust-manager alongside cert-manager.

---

Continue to [Recommendations for Deployment](model.md#recommendations-for-deployment) for the operational posture that backs several of the preceding rows.
