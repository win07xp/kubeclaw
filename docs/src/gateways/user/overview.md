# User Gateway

The User Gateway is the listener responsible for delivering channel messages from user-facing platforms to agent containers. Alongside message delivery it owns two subsystems: the **activator**, which wakes hibernated Agents on demand, and **activity tracking**, which tells the controller when an Agent last did any work. This chapter starts with the end-to-end request flow and the listener's TLS and Ingress story, then splits into three sub-pages: [Platform Adapters and Channel Health](platform-adapters.md) covers the adapter plugin interface and how per-channel delivery health becomes an AgentChannel condition; [Activation and Activity Tracking](activation-and-activity.md#the-activator) covers wake-on-demand and the activity API the controller polls; and [User Gateway Operations](operations.md#observability) covers metrics and failure modes.

For the LLM Gateway (provider routing, budget, fallback) and the shared gateway architecture rationale, see [LLM Gateway](../llm/overview.md#why-a-shared-gateway). For the HTTP endpoint contracts agents implement, see [HTTP API](../api/overview.md).

---

## Request Flow

A webhook call travels through eight steps: size check, authentication, normalization, channel lookup, activator check, delivery, and response. Sync mode returns the agent's reply inline; async mode returns `202` early and delivers the reply out of band.

![Sequence diagram of a webhook call through the User Gateway. The caller POSTs to /channels/{ns}/{path}. The gateway runs the payload size check at the listener level on the raw frame, returning 413 for bodies over maxMessageBodyBytes; a note records that this runs before path resolution and before the path-not-registered 401, so the 413-versus-401 distinction never leaks which paths exist. Then per-AgentChannel bearer or HMAC auth (401 on failure), normalization into the Kaalm envelope (400 invalid_request when fromBody is configured and the body is not JSON), and the AgentChannel lookup, which routes only to channels with Ready=True. In async mode the gateway creates the kaalm-async-{requestId} placeholder ConfigMap via the apiserver, returning 503 if the Create fails and otherwise 202 Accepted with requestId and channelPath; a highlighted note marks that the caller is now gone while steps 5 to 7 still execute. Step 5 calls the controller's activator over mTLS if the Agent is Hibernated, which patches kaalm.io/wake=true, and the gateway polls readiness bounded by wakeTimeout. Step 6 delivers POST /v1/message over bidirectional mTLS with three retries at 1s, 5s and 25s. The response leaves by one of three exits: an inline 200 in sync mode bounded by syncDeliveryDeadline, a signed POST to the callbackUrl receiver, or a ConfigMap Patch served at the polling endpoint.](../../diagrams/user-webhook-flow.svg)

Reading the diagram: the shape worth noticing is the async branch. The `202` is returned from inside the alt fragment, and everything below it (activation, delivery, retry, response) executes afterwards against a caller that has already been answered and disconnected. Sync mode is the same pipeline with the caller still attached, which is why only sync carries a wall-clock deadline.

### 1. Webhook event arrives

An external system POSTs to the gateway's webhook endpoint, for example `/channels/team-support/support-assistant`.

### 2. Payload size check

The gateway rejects webhook payloads exceeding `maxMessageBodyBytes` (default: 1 MiB, configurable via Helm value `gateway.maxMessageBodyBytes`) with HTTP 413 before anything else.

The check runs at the listener level on the raw request frame, **before path resolution, before auth, and before the path-not-registered → `401` branch**. So `413` fires uniformly for any oversized POST to `:8080`, regardless of whether the path is a registered AgentChannel. This ordering does two things: it prevents oversized payloads from consuming gateway resources or being forwarded to agent containers, and it preserves the path-existence threat model documented in [Polling Fallback](../api/async-responses.md#polling-fallback) (the `413`-vs-`401` distinction must not leak which paths are hosted).

### 2a. The adapter authenticates

The gateway verifies the request using the channel type's scheme: for a webhook channel, bearer token validation or HMAC signature verification per the AgentChannel's webhook auth config (see [AgentChannel webhook auth types](../../resources/agentchannel.md)); for a Discord or WhatsApp channel, the platform's fixed signature scheme with the channel's credential Secret (see [The platform adapters](platform-adapters.md#inbound)).

### 3. Normalization

The adapter translates the webhook payload into the Kaalm message envelope:

```json
{
  "messageId": "uuid",
  "channelType": "webhook",
  "channelId": "/channels/team-support/support-assistant",
  "userId": "caller-id-extracted-per-config",
  "content": "Hello, I need help with my order",
  "attachments": [],
  "metadata": {}
}
```

Field resolution:

- **`userId`** is resolved using `AgentChannel.spec.webhook.userId` config (`fromHeader` or `fromBody`). If neither is configured, or the value is absent, the configured `fallback` is used (empty string if omitted).
- **`content`** is resolved using `AgentChannel.spec.webhook.content` config (same shape as `userId`). When neither `fromHeader` nor `fromBody` is set, the gateway uses the raw inbound body, JSON-encoded as a string, as `content`. This preserves the generic-webhook story for senders whose body shape Kaalm has no a-priori knowledge of.
- **`attachments`** and **`metadata`** are populated by adapter-specific code: empty `[]` and `{}` for the generic webhook adapter; the Discord and WhatsApp adapters fill them per platform ([Discord Channel](../api/channel-discord.md#normalization), [WhatsApp Channel](../api/channel-whatsapp.md#normalization)).

If `fromBody` is configured for either field and the body cannot be parsed as JSON, the gateway rejects the request with `400 Bad Request` (`error.type: invalid_request`) before delivering to the agent. See [AgentChannel](../../resources/agentchannel.md) for extraction configuration.

### 4. AgentChannel lookup

The gateway finds the AgentChannel resource matching the webhook path, which identifies the target Agent and namespace.

The gateway routes webhook traffic **only** to AgentChannels whose `status.conditions[type=Ready].status == True`. `Ready=False` channels (including a `PathConflict` loser that has not yet been deleted) receive no traffic regardless of their order in the gateway's watch event stream. As defense in depth, the gateway also refuses to register any path that does not begin with the channel's own `/channels/{namespace}/` prefix. That is the gateway-side half of [rule 15](../../resources/validation-and-defaulting.md#cross-resource-validation), independent of the reconciler's `Ready=False, reason=InvalidPath` status.

On a path collision, the channel with the earliest `creationTimestamp` is the one that reaches `Ready=True`: the [AgentChannelReconciler](../../controller/reconcilers.md#agentchannelreconciler) marks the newer collider as `Ready=False, reason=PathConflict`. The gateway's Ready filter therefore naturally selects the same winner the reconciler does. See [AgentChannel](../../resources/agentchannel.md) for session configuration and webhook path uniqueness constraints.

#### Session ID derivation

If the AgentChannel has `session.enabled: true`, the gateway generates a deterministic `sessionId` from the message's `channelId` and `userId`:

```
sessionId = UUIDv5(namespace: <fixed published namespace UUID>, name: channelId + ":" + userId)
```

The namespace is a fixed UUID published as part of the Kaalm API specification, identical across all installations and versions. See [POST /v1/message](../api/agent-endpoints.md#post-v1message) for the constant and its stability guarantee.

Because the derivation is a pure function of the envelope, the ID is stable across gateway replicas and restarts, so no gateway-side session state is required. Session expiry and rotation are the agent's responsibility using its PVC state.

### 5. Activator check

If the Agent is `Hibernated`, the gateway signals the controller to wake it and waits up to `wakeTimeout` for the Pod to become Ready. In sync mode, the webhook caller blocks during this wait. In async mode, the gateway has already returned `202` (see step 5a). See [Activator](activation-and-activity.md#the-activator).

### 5a. Async early return (async mode only)

If `AgentChannel.spec.webhook.responseMode` is `async`, the gateway returns HTTP 202 Accepted immediately after the AgentChannel lookup (step 4) with a body containing `requestId` (a UUID) and `channelPath` (the originating AgentChannel's webhook path). Both the mode decision and the `channelPath` in the body come from the resolved AgentChannel, so the lookup is the earliest point the `202` can be emitted. Steps 5 to 7 proceed asynchronously; the webhook caller does not block.

Before returning `202`, the gateway creates an empty placeholder `kaalm-async-{requestId}` ConfigMap in `kaalm-system` (standard channel-namespace/channel-name labels and a 1-hour expiry annotation, no payload field) so the polling record is immediately queryable. If the placeholder `Create` fails, the gateway returns `503 Service Unavailable` to the inbound webhook caller instead of `202`. The invariant is that a returned `202` always implies a queryable polling record exists.

The `requestId` is opaque; callers must not parse or construct it. The `channelPath` must be preserved by the caller and passed back as the `channelPath` query parameter on any poll request. See [Async Webhook Response](../api/async-responses.md) for the full schema.

#### Serving a poll

On poll (`GET /v1/channels/responses/{requestId}?channelPath=...`), any gateway replica reads the response from the `kaalm-async-{requestId}` ConfigMap in `kaalm-system`. The `channelPath` query parameter is used to look up AgentChannel auth config for authenticating the poll request.

After authentication, the gateway asserts that the ConfigMap's `kaalm.io/channel-namespace` / `kaalm.io/channel-name` labels match the authenticated channel **before serving any response**, including the empty-placeholder `202`. That way a `requestId` stored by one channel cannot be probed via another channel's credentials at any point in its lifecycle. Mismatches return `404 Not Found`, indistinguishable on the wire from "unknown `requestId`", to avoid confirming that a cross-channel response exists. See [Async Webhook Response](../api/async-responses.md) for the response schemas and the channel-match assertion.

### 6. Message delivery

The gateway posts the normalized envelope to `POST /v1/message` on the Agent's ClusterIP Service over HTTPS. The gateway verifies the agent's TLS certificate against the Kaalm CA (`kaalm-ca`, managed by cert-manager, see [TLS on the Cluster Listener](../listener-tls.md)).

### 6a. Delivery retry

If `POST /v1/message` fails (connection error, non-200 response), the gateway retries up to 3 times with exponential backoff (1s, 5s, 25s), 4 attempts total. Each attempt is itself bounded by the per-attempt agent read timeout (`gateway.agentReadTimeout`, default 10s). The full wall-clock arithmetic for this budget is the same framing as the callback-retry budget, documented in [Async Webhook Response](../api/async-responses.md).

The schedule is the same in both sync and async modes, and is recorded in AgentChannel status conditions in either case. On exhaustion:

- **Async mode**: the gateway delivers a `delivery_failed` error payload to `callbackUrl` (if configured, with its own retries) or stores it at the polling endpoint under the original `requestId`.
- **Sync mode**: the gateway returns `502 Bad Gateway` with a `delivery_failed` error envelope. Sync callers with shorter HTTP timeouts than the cumulative retry budget see only their own timeout; the 502 lands on a closed connection.

See [Async Webhook Response](../api/async-responses.md) for the error payload schema.

#### Sync-mode wall-clock budget (`gateway.syncDeliveryDeadline`)

In sync mode, the gateway tracks total wall-clock from inbound webhook acceptance through delivery-retry-pipeline completion, including activator wake time, agent processing, and any in-progress retry. When the elapsed time would exceed `gateway.syncDeliveryDeadline` (Helm value, default 30s), the gateway short-circuits the in-progress request with `504 Gateway Timeout` carrying `error.type: sync_deadline_exceeded`, `retryable: true`. The failure is timing-related, not structural, so a fresh attempt within the deadline may succeed.

Async mode does **not** apply this deadline: the agent-delivery retry budget runs to completion regardless of wall-clock, because callback and polling are receiver-driven and not bounded by the original webhook caller's HTTP timeout.

The deadline gives sync callers a deterministic upper SLA without changing the underlying retry pipeline. Tenants needing a tighter SLA should switch to `responseMode: async` with `callbackUrl` or polling. Sync mode is positioned for fast webhooks where the agent is `Running` and replies within seconds; channels with hibernation enabled or known-slow-startup agents should use `responseMode: async` (the deadline does not apply, and `wake_timeout` / `delivery_failed` are reachable in their async payload form, see [Async Webhook Response](../api/async-responses.md)). [Sync-Mode Reachability](../api/async-responses.md#sync-mode-reachability) draws the deadline, the delivery budget, and `wakeTimeout` on one time axis, showing why the latter two are unreachable here under defaults. See [sync-mode response table](../api/channel-webhook.md) and [§ User Gateway Error Responses](../api/errors.md#user-gateway-error-responses) for the wire contract.

### 7. Response (sync mode, default)

The agent returns a response envelope; the gateway returns it as the webhook HTTP response body.

### 8. Response (async mode)

The agent returns a response envelope; the gateway POSTs it to the configured `callbackUrl` (with retries) or stores it for polling retrieval via `GET /v1/channels/responses/{requestId}?channelPath={url-encoded-webhook-path}`. See [Async Webhook Response](../api/async-responses.md).

#### Async response persistence

The receiving replica creates an empty placeholder `kaalm-async-{requestId}` ConfigMap in `kaalm-system` at 202-acceptance time (see step 5a above) and later `Patch`es it with the response payload (or error envelope) when the agent reply is ready and the response will not be delivered via callback, either because no `callbackUrl` is configured, or because callback retries (3× with 1s/5s/25s backoff) were exhausted.

**Lifetime.** The expiry annotation is set on placeholder creation (1 hour from 202-acceptance) and is **not** reset by the payload `Patch`, so total per-`requestId` ConfigMap lifetime is bounded at 1 hour regardless of how long the agent takes to reply. Each ConfigMap is labeled with `kaalm.io/channel-namespace` and `kaalm.io/channel-name` to identify the originating AgentChannel, and annotated with that 1-hour expiry. The gateway already has full ConfigMap access in `kaalm-system`, so no additional RBAC is required.

**Size limit.** The gateway rejects agent responses exceeding `gateway.maxResponseBodyBytes` (default 900 KiB, leaving headroom under Kubernetes' ~1 MiB ConfigMap object cap, and bounding per-request gateway memory in sync mode where the response is buffered before forwarding) with a `response_too_large` error returned to the caller (sync) or delivered to `callbackUrl` / the polling endpoint (async). The payload is stored as a text value under the ConfigMap's `data` field (JSON envelope), never `binaryData`, whose base64 encoding would inflate a 900 KiB payload past the ~1 MiB object cap.

**Cleanup.** Ownership cannot be expressed with an ownerRef, so linkage is by the channel labels and the AgentChannelReconciler prunes expired ConfigMaps for live channels on its reconcile passes; see [Async Webhook Response](../api/async-responses.md) for the ownerRef and pruning rationale. When an AgentChannel is deleted, its finalizer sweeps all of that channel's `kaalm-async-*` ConfigMaps in one shot before removing the finalizer, see [Finalizers](../../controller/finalizers.md). Without this sweep they would be orphaned indefinitely, since the reconciler's expiry prune runs only for live channels.

**Scaling.** Any gateway replica can serve poll requests by reading from this ConfigMap, so no replica-affinity or in-memory routing is required. The store targets low-to-moderate async rates: live records accumulate at roughly the channel's async request rate × the 1-hour TTL, and [`maxPendingAsyncResponses`](../../resources/agentchannel.md) (default 100) is the per-channel guardrail that bounds this etcd pressure. High-QPS channels should use sync mode fronted by an external queue rather than raising the cap.

**Replica-failure limitation.** The delivery and callback pipelines themselves are replica-local in-memory state: a replica death between `202` and the ConfigMap `Patch` silently drops the in-flight request (no peer-replica takeover in v1). See [Async Webhook Response](../api/async-responses.md) "Replica-failure semantics".

#### `callbackUrl` re-validation on delivery

Before each POST attempt to `callbackUrl` (both the initial attempt and each retry), the gateway re-resolves the host and re-checks it against the blocked IP ranges from [AgentChannel validation rule 22](../../resources/validation-and-defaulting.md#cross-resource-validation). This defeats DNS rebinding between reconcile-time validation and delivery, but only if the range check and the dial operate on the same address.

The delivery transport therefore uses a custom dialer that resolves the host once, range-checks the resolved IP, and dials that pinned `IP:port` (the URL's hostname is preserved for the `Host` header and TLS SNI). The hostname is never handed back to the HTTP client to resolve on its own: an independent dial-time resolution could be rebound to a blocked address after the check passed, defeating the exact attack the re-check exists to stop. Connections to a callback host are pooled and reused across attempts and messages; a pooled connection was dialed the same way, and the re-resolution and re-check still run before every attempt, so a host that has come to resolve to a blocked range is never posted to, on a new connection or a pooled one.

If the host now resolves to a blocked range (loopback, link-local, RFC1918, unique-local IPv6, cloud metadata, or anything outside `gateway.callbackUrl.allowlist` when set), the delivery attempt is not dialed. The gateway instead stores a `callback_invalid` error payload at the polling endpoint (or drops the callback if polling is unavailable) and records the AgentChannel `Warning` event `reason=CallbackInvalid`. The error is delivered to the polling endpoint per async rules; retries are not attempted for a host that fails validation.

#### Callback signing

After the IP-allowlist re-check passes and before dialing, `SendReply` constructs the signed callback per [Callback authentication](../api/async-responses.md) using the channel's `spec.webhook.callbackAuth` (required by [rule 25](../../resources/validation-and-defaulting.md#cross-resource-validation) whenever `callbackUrl` is set).

Signing happens on every attempt, initial and each retry, using a fresh timestamp, so a delayed retry doesn't ship a stale `X-Kaalm-Timestamp`. The signing material is loaded from the Secret referenced by `callbackAuth.secretRef` via the same per-channel scoped Role used for inbound `auth` (see [Gateway ServiceAccount permissions](../../security/rbac.md#gateway-serviceaccount-permissions)).

---

## TLS and Ingress

The User Gateway listener on `:8080` serves TLS using the same `kaalm-gateway-tls` Certificate as the cluster listener: both listeners share a single cert whose SAN set covers the gateway's in-cluster Service DNS names. No plaintext path exists on the gateway; all webhook, activator, activity, and async-polling traffic is TLS end-to-end.

**Listener separation**: port 8080 serves only externally-reachable channel traffic, namely webhook intake under `/channels/*` and the async polling fallback under `/v1/channels/responses/*`. All internal mTLS-authenticated endpoints (`/v1/activity`, `/v1/channels/health`) live on the cluster listener on `:8443`. This split ensures that an Ingress fronting 8080 cannot route untrusted traffic to an endpoint whose authorization assumes a controller-SAN client cert. See [TLS on the Cluster Listener](../listener-tls.md) for the 8443 endpoint set.

**Recommended Ingress configuration**: external webhook traffic arrives at a cluster Ingress that terminates TLS with the cluster's public certificate and then connects to the gateway backend. Two Ingress modes are supported; operators pick one:

- **Backend re-encrypt (HTTPS-to-HTTPS)**: the Ingress controller speaks HTTPS to the gateway on port 8080, presenting the Kaalm CA as the backend CA bundle (or disabling verification if the controller trusts cluster-internal names). This is the recommended default because it works with off-the-shelf Ingress controllers (NGINX, Traefik, HAProxy, most cloud LB Ingress classes).
- **TLS pass-through**: the Ingress forwards raw TLS bytes to the gateway without terminating, so the external client speaks TLS directly with the gateway. This preserves end-to-end TLS with the gateway's cert but requires the Ingress controller to support pass-through SNI routing and requires clients to trust the gateway's cert chain. Operators using pass-through must set the Helm value `gateway.externalHostnames` to add the public hostname to the gateway cert's SAN list: the default SAN set covers only in-cluster Service DNS, which would fail verification for an external client dialing the public hostname. See [Helm Chart Contents](../../operations/deployment.md#helm-chart-contents).

Internal callers inside the cluster verify the gateway's cert against `kaalm-ca` (projected by `trust-manager` into every namespace) when dialing the gateway directly. Cluster-local webhook producers and async-polling callers dial the User listener on `:8080`; the controller's activity fan-out dials the cluster listener on `:8443` (see [§ Activity Tracking API](activation-and-activity.md#activity-tracking-api)).
