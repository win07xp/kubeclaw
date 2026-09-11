# Observability

Kaalm's v1 observability surface has three pillars:

1. **Prometheus metrics** scraped from the controller and the gateway.
2. **Structured JSON logs** from both.
3. **Kubernetes Events** on the Kaalm CRDs and their child resources.

Both the controller and the gateway expose Prometheus metrics on dedicated ports. Each component owns its own metric catalog and documents its own emit-points:

- [Controller metrics](../controller/operations.md#observability): reconcile counts/duration/queue depth, agent/task/channel phase counts, hibernation/wake events, canonical spend.
- [LLM Gateway metrics](../gateways/llm/operations.md#observability): request counts/duration, token usage, spend, fallback events, budget utilization.
- [User Gateway metrics](../gateways/user/operations.md#observability): channel message counts/duration, hibernation wakes triggered.

This page is the aggregator over those three. It rolls the per-component catalogs into one table, specifies log conventions and PII safety, and lists architecturally-significant alerts and dashboards. When you need to know *when* a metric increments or *what* a label value means, follow the link to the owning component; when you need to know *what exists*, read the table here.

An audit-export pipeline is out of scope for v1 (see [Scope for v1](../concepts/vision-and-scope.md#scope-for-v1)); the Grafana dashboards and OpenTelemetry tracing ship since v0.5.0 ([Dashboards](#dashboards), [Tracing](#tracing)).

## Scope

**In v1.**

- Prometheus metrics on dedicated ports (controller `:8080/metrics`, gateway `:9090/metrics`)
- Structured JSON logs from controller and gateway with a hard PII-safety rule
- Kubernetes Events on all six Kaalm CRDs
- A small recommended-alerts set tied to architectural failure modes
- Three Grafana dashboards (per-namespace, per-provider, cluster) as importable JSON, since v0.5.0
- OpenTelemetry tracing across the gateway to agent to provider hops (default off), since v0.5.0

**Deferred to v1.1+** (per [Scope for v1](../concepts/vision-and-scope.md#scope-for-v1)).

- Audit-log export pipeline beyond standard Kubernetes audit logging
- Cost analytics / chargeback reporting

## Metrics

### Endpoints

| Component | Port | Path | Auth | Notes |
|---|---|---|---|---|
| Controller | `:8080` | `/metrics` | None | Standard controller-runtime metrics port |
| Gateway | `:9090` | `/metrics` | None | Shared by LLM Gateway and User Gateway (single Deployment) |

Both endpoints are unauthenticated by design, which is the standard Prometheus scrape pattern. They are reachable inside the cluster only. Platform teams running an in-cluster Prometheus should restrict scrape RBAC and apply a NetworkPolicy admitting only the Prometheus ServiceAccount.

The chart documents the scrape ports but does not ship `ServiceMonitor` / `PodMonitor` manifests. Scrape integration is left to the platform team. See [Deployment](deployment.md).

### Aggregated catalog

Standard controller-runtime reconcile metrics (counts, duration, queue depth, work-queue saturation) are emitted automatically by the controller. See [Observability](../controller/operations.md#observability) for the per-component canonical list.

The Kaalm-specific metrics across all three components:

| Source | Metric | Type | Labels |
|---|---|---|---|
| Controller | `kaalm_agents` | gauge | `phase`, `namespace` |
| Controller | `kaalm_tasks` | gauge | `phase`, `namespace` |
| Controller | `kaalm_channels` | gauge | `namespace`, `phase`, `ready`, `platform_connected` |
| Controller | `kaalm_provider_budget_canonical_usd` | gauge | `provider`, `namespace`, `period` |
| Controller | `kaalm_hibernations_total` | counter | `namespace` |
| Controller | `kaalm_wakes_total` | counter | `namespace`, `trigger` |
| LLM Gateway | `kaalm_llm_requests_total` | counter | `provider`, `model`, `namespace`, `status` |
| LLM Gateway | `kaalm_llm_request_duration_seconds` | histogram | `provider`, `model` |
| LLM Gateway | `kaalm_llm_tokens_total` | counter | `provider`, `model`, `namespace`, `direction` |
| LLM Gateway | `kaalm_llm_spend_usd_total` | counter | `provider`, `namespace` |
| LLM Gateway | `kaalm_llm_fallback_total` | counter | `from_provider`, `to_provider`, `reason` |
| LLM Gateway | `kaalm_llm_budget_utilization` | gauge | `provider`, `namespace`, `period` |
| LLM Gateway | `kaalm_budget_threshold_events_total` | counter | `provider`, `namespace`, `action` |
| LLM Gateway | `kaalm_llm_budget_boundary_events_total` | counter | `provider`, `namespace`, `event` |
| LLM Gateway | `kaalm_llm_server_tool_use_total` | counter | `provider`, `namespace`, `tool` |
| Tool broker | `kaalm_tool_calls_total` | counter | `provider`, `namespace`, `tool`, `status` |
| Tool broker | `kaalm_tool_call_duration_seconds` | histogram | `provider`, `tool` |
| User Gateway | `kaalm_channel_messages_total` | counter | `channel_type`, `namespace`, `status` |
| User Gateway | `kaalm_channel_message_duration_seconds` | histogram | `channel_type` |
| User Gateway | `kaalm_channel_wake_total` | counter | `namespace` |
| User Gateway | `kaalm_channel_wake_duration_seconds` | histogram | `namespace`, `result` |
| User Gateway | `kaalm_channel_delivery_attempts_total` | counter | `namespace`, `outcome` |
| User Gateway | `kaalm_channel_callback_total` | counter | `namespace`, `status` |
| User Gateway | `kaalm_channel_callback_duration_seconds` | histogram | `namespace` |
| User Gateway | `kaalm_channel_response_too_large_total` | counter | `namespace`, `mode` |
| User Gateway | `kaalm_channel_async_patch_failed_total` | counter | `namespace` |

For full semantics (when each metric increments, what each label value means, retention and reset behavior) see the per-component canonical sections:

- [Observability](../controller/operations.md#observability)
- [Observability](../gateways/llm/operations.md#observability)
- [Observability](../gateways/user/operations.md#observability)
- [Audit and Metering](../gateways/tool-plane.md#audit-and-metering) (tool broker)

### Cardinality

The `namespace` label appears on most metrics and dominates cardinality in clusters with many active tenants. The `model` and `provider` labels are bounded by `ModelProvider.spec.models` and the count of declared providers. The `tool` label is bounded by declared catalogs: on the broker metrics it carries only ids from `ToolProvider.spec.tools` (everything else collapses to `uncataloged`; see [Audit and Metering](../gateways/tool-plane.md#audit-and-metering)), and on `kaalm_llm_server_tool_use_total` it carries the provider-side tool vocabulary, a handful of values per provider type. Enum labels (`status`, `result`, `mode`, `phase`, `trigger`, `action`, `direction`, `ready`, `platform_connected`) carry a handful of values each.

**No metric carries per-Agent or per-AgentTask identity as a label.** That resolution belongs in logs, Events, and (since v0.5.0) the console read API backed by the gateway's [per-workload spend ledger](../gateways/llm/budgets-and-rate-limits.md#per-workload-spend), not metrics, to keep cardinality bounded as the cluster scales to thousands of agents.

## Logs

Both the controller and the gateway emit **structured JSON logs to stdout** (klog / logr). The chart does not configure log shipping; platform teams ship via a standard cluster log pipeline (Fluent Bit, Vector, Loki, etc.).

- **Default level:** `info`. The runtime level can be raised to `debug` via a Helm value for development clusters. Even at `debug`, prompt and response bodies are not logged in the default build (see PII safety below).
- **Per-line fields:** timestamp, level, component (`controller` | `gateway`), reconciler/handler name, namespace, resource name, and a request-correlation field on gateway request paths.

### PII safety

**Hard rule: in the default build, prompt and response bodies are never logged at any level.** This holds at `info`, at `debug`, and on every code path. Specifically:

- The **LLM Gateway** logs request metadata only: namespace, workload identity, model, status, latency, and prompt/response token counts. Prompt content and provider responses are never serialized to logs.
- The **tool broker** logs the per-call audit record (caller identity, ToolProvider, tool, method, outcome, duration, sizes; see [Audit and Metering](../gateways/tool-plane.md#audit-and-metering)). Tool-call arguments and results are never logged.
- The **User Gateway** logs webhook envelope metadata: channel, request id, status, latency. Channel message bodies (inbound webhook payloads) and agent reply bodies are never logged.
- **Reconciler logs** cite resource names and condition reasons, never Secret content, channel auth tokens, or provider API keys.

This is a hard rule because logs are typically shipped to lower-trust aggregation pipelines, and prompt content can include credentials, customer data, or platform-team policy decisions surfaced through tool calls. The same posture is stated from the security side at [Audit trail](../security/model.md#audit-trail).

### Debug-build escape hatch

A separate **debug build**, gated by the Go build tag `kaalm_debug_logs` at compile time, can log prompt and response bodies on the LLM proxy paths, and tool-call request and response bodies on the MCP broker routes, for contract bring-up and integration debugging. The escape hatch exists at the build layer only:

- The official Helm chart only ships default builds. Debug-build images carry the `-debug` tag suffix and emit a startup banner, so an operator who accidentally pulls one notices.
- There is **no runtime Helm value, environment variable, feature flag, or admin endpoint** that flips body logging on in a default build. The gate is build-time only.

This keeps the production wire format provably PII-clean while leaving developers a way to inspect bodies during local work against the [runtime contract](../runtime/contract.md).

## Kubernetes Events

Events are the primary surface for status changes that platform teams discover via `kubectl describe`. The canonical Events list lives at [Event Emission](../controller/operations.md#event-emission); reconciler-specific reasons (`FQDNPolicyUnsupported`, `WakeIgnored`, `FallbackIneligible`, `DegradeTargetNotCheapest`) are documented at the relevant reconciler step. Note that `InvalidDegradeTarget` is a `Ready=False` condition reason, not an Event: see [ModelProviderReconciler](../controller/reconcilers.md#modelproviderreconciler) step 6. The gateway also emits a runtime `FallbackIneligible` Warning at request time: see [Fallback Logic](../gateways/llm/fallback.md).

Architecturally-significant Event groups, with the reasons attached:

- **Phase transitions** on Agent and AgentTask (`Normal`, `PhaseChanged`).
- **Hibernation / wake** on Agent (`Normal`, `Hibernated` / `Woken`; `Warning`, `WakeIgnored`). See [Hibernation mechanics](../controller/hibernation-and-wake.md#hibernation-mechanics) and [Wake trigger](../controller/hibernation-and-wake.md#wake-trigger).
- **Provider health and budget** on ModelProvider (`Warning`, `ProviderUnhealthy` / `BudgetExhausted` / `BoundaryMarginRaised`).
- **Validation failures** on any CRD (`Warning`, `InvalidReference` plus reconciler-specific reasons).
- **Fallback misconfiguration** on ModelProvider (`Warning`, `FallbackIneligible` from both reconcile-time and runtime paths; `DegradeTargetNotCheapest` advisory).
- **Callback failures** on AgentChannel (`Warning`, `CallbackInvalid` when the `callbackUrl` fails the pre-dial deny-range / allowlist re-check; `CallbackRejected` when the receiver terminally rejects the POST). These are the per-occurrence signal paired with the persistent `PlatformConnected=False` condition; see [Async Webhook Response](../gateways/api/async-responses.md).
- **AgentClass propagation cascade** on each affected Agent during recreate-and-clamp or `Degraded` transitions. See [Per-Agent and Per-Task Child Resources](../runtime/child-resources.md).

Events persist per the cluster's standard Event retention. For long-term audit, see [Audit trail](../security/model.md#audit-trail).

## Recommended Alerts

A v1 alert set tied to architectural failure modes already named in the doc set. Concrete PromQL and threshold tuning is implementation work and is not specified here.

| Alert | Severity | Architectural hook |
|---|---|---|
| Controller all replicas unready | Page | Wake-on-demand is a hard control-plane dependency. See [The Kaalm Gateway](../gateways/overview.md) |
| Gateway all replicas unready | Page | LLM and webhook traffic blocked cluster-wide |
| Reconcile error rate elevated | Warn | Reconciler is stuck. Surface before the work queue backs up |
| LLM error rate elevated for a provider | Warn | Provider degraded; consider promoting fallback |
| Sustained fallback rate to a backup provider | Warn | Primary provider effectively down |
| Budget threshold `degrade` or `block` triggered | Warn / Page | Tenant or provider crossed the configured spend ceiling |
| Hibernation / wake churn for a single Agent | Warn | Likely idle-timeout misconfig. See [Agent (persistent mode)](../controller/agent-lifecycle.md) |
| Per-namespace rate-limit saturation | Warn | Tenant hitting the per-(namespace, model) ceiling. See [Rate Limiting](../gateways/llm/budgets-and-rate-limits.md#rate-limiting) |
| Wake duration p95 elevated | Warn | [Activator](../gateways/user/activation-and-activity.md#the-activator) path slow. Watch `kaalm_channel_wake_duration_seconds` |
| Async-callback exhaustion rate elevated | Warn | Receivers' `callbackUrl` repeatedly unreachable; receivers should [poll](../gateways/api/async-responses.md) |
| `kaalm_channel_async_patch_failed_total` nonzero | Warn | The v1 async silent-loss limitation fired. See below |

The last alert deserves a note. A nonzero `kaalm_channel_async_patch_failed_total` means a response was dropped after `Patch` retry exhaustion: pollers see `202` followed by `404` with no stored envelope. See [Response-Patch failure semantics](../gateways/api/async-responses.md).

## Dashboards

Three Grafana dashboards ship as JSON in `config/grafana/` (since v0.5.0), one per topology level. Each file is self-contained: import it through the Grafana UI or provision it from the file, with no manual edits. The data source is chosen by a `datasource` template variable, the only import form that works unchanged on both paths (file provisioning never substitutes `__inputs`).

| File | Scope | Variables | Panels |
|---|---|---|---|
| `kaalm-namespace.json` | One tenant namespace | `datasource`, `namespace` | Agent, AgentTask, and AgentChannel counts by phase and condition; canonical spend and budget utilization per provider; budget policy actions; LLM request, rate-limited, and token rates; channel message rate and p95 duration; wake count and p95 duration; tool calls; async callbacks, oversized responses, patch failures |
| `kaalm-provider.json` | One ModelProvider | `datasource`, `provider` | Request rate, error ratio, latency p50 and p95 by model, token rates, fallback events by target and reason, budget utilization and canonical spend by namespace, spend rate, budget policy and hard-enforcement boundary events, provider-side tool use |
| `kaalm-cluster.json` | The whole cluster | `datasource`, `job` | Scrape targets up, controller leader, reconcile errors, duration, and queue depth; fleet totals by phase; hibernations and wakes; wake duration; channel messages; LLM and tool-broker traffic and latency; fallbacks; canonical spend by provider; async delivery state |

Conventions the panels follow:

- Every query is over the [aggregated catalog](#aggregated-catalog), and every catalog metric is on at least one panel; the test under `test/dashboards` pins both directions and the import shape. The only non-catalog series are on the cluster dashboard's control-plane row: the scrape `up` series, controller-runtime's reconcile and work-queue families, and its `leader_election_master_status` gauge.
- The phase-count and budget-utilization gauges are computed on every scrape by every replica, so the panels aggregate them with `max`, never `sum`.
- The per-namespace rate-limit panel is the `rate_limited` outcome of `kaalm_llm_requests_total`; there is no separate utilization gauge for the per-(namespace, model) ceiling.
- The cluster dashboard's `job` variable matches scrape jobs whose name contains `kaalm`; a ServiceMonitor on the chart's Services resolves to such names. Every other panel is independent of how the scrape is configured.

`make dashboards-verify` proves the files against a live cluster: it installs a throwaway Prometheus and Grafana, provisions the three files unchanged, and checks that Grafana serves each dashboard, that Grafana can query the data source, that every panel query is valid PromQL against the scraped series, and that the metric families the e2e suite exercises are present.

## Tracing

OpenTelemetry tracing ships since v0.5.0, default off. It connects one user message to the LLM and tool calls it caused, across the gateway to agent to provider hops.

**Propagation** is W3C `traceparent` and `tracestate`, and nothing else. Spans key on the correlation the logs already carry, as span attributes rather than metric labels (the [cardinality](#cardinality) doctrine binds metrics; spans are per-request by design): `kaalm.message_id`, `kaalm.namespace`, `kaalm.agent`, `kaalm.workload`, `kaalm.provider`, `kaalm.model`, `kaalm.channel_type`, `kaalm.method`, and `kaalm.tool`, each where it applies.

**Span inventory.** Every span is created by the gateway; the agent hop propagates:

| Span | Kind | Where | Notes |
|---|---|---|---|
| `channel.receive` | server | User Gateway | Webhook and test-chat receipt; root unless the caller sent trace context. Covers handling through the sync reply, or through the `202` in async mode; the background delivery stays connected through the span identity without inheriting the caller's cancellation |
| `agent.deliver` | client | User Gateway | The delivery to the agent, retries included; its context travels to the agent on the delivery request |
| `llm.request` | server | LLM proxy | Parented by whatever context the agent propagated. A denial past route authorization (budget, rate limit) closes it with an error status, so a blocked request is visible in its trace |
| `llm.forward` | client | LLM proxy | One per provider attempt, fallback candidates included, named for the candidate it tried |
| `tool.call` | server | Tool broker | The governed MCP call; broker denials carry an error status |
| `tool.forward` | client | Tool broker | The upstream half of a forwarded call |

The agent's own processing appears as the gap between `agent.deliver` and its child spans, deliberately: the base images carry no OpenTelemetry SDK, and the platform's promise is the connected trace, which propagation alone delivers. The runtime forwards the delivery's trace context on every gateway call ([contract item 8](../runtime/contract.md#8-trace-context-propagation)); a framework running its own SDK reads the same context (`kaalm.trace_context()` in Python, `agentruntime.TraceContext` in Go) and fills the gap with real agent spans.

**Exporter.** OTLP over HTTP, configured by two Helm values ([Deployment](deployment.md)): `gateway.tracing.otlpEndpoint` (default `""`) and `gateway.tracing.sampleRatio` (default `1.0`, parent-based head sampling for traces the gateway starts). With no endpoint, no tracer is installed: no spans, no propagation, and request handling behaves exactly as it did before tracing existed, which is the default install. An `https` endpoint is verified against the gateway's upstream trust pool.

The controller emits no spans in this version: the traced path is the message path, and reconcile visibility remains metrics, logs, and Events. Scenario [S20](../appendix/scenarios.md#s20-follow-one-message-across-the-hops) proves the connected trace live: one webhook message, one trace, its spans read back out of a Jaeger beside the e2e cluster.

## Profiling

Both components can serve Go's `net/http/pprof` profiles, off by default. Setting `controller.pprofPort` or `gateway.pprofPort` to a port number adds the flag (`--pprof-bind-address` on the controller, `--pprof-addr` on the gateway) and a named `pprof` container port; the listener then serves CPU, heap, goroutine, mutex, and block profiles under `/debug/pprof/`. Turning it on also enables mutex and block sampling, which Go leaves off because each costs a little on every contended lock and blocking call.

The listener is a debugging aid, not an operations surface. It is unauthenticated, and the chart never puts it behind a Service, so the way to reach it is a port-forward for the length of a profiling session:

```bash
kubectl -n kaalm-system port-forward deploy/kaalm-gateway 6060:6060
go tool pprof -http=:8000 http://127.0.0.1:6060/debug/pprof/profile?seconds=30
```

Leave both values at `0` in production. The [load harness](load-and-scale.md) turns them on for the load cluster so a profile can be taken during any phase.

## See also

- [Observability](../controller/operations.md#observability): controller metric catalog and emit-points
- [Event Emission](../controller/operations.md#event-emission): controller Events list
- [Observability](../gateways/llm/operations.md#observability): LLM Gateway metric catalog
- [Observability](../gateways/user/operations.md#observability): User Gateway metric catalog
- [Audit trail](../security/model.md#audit-trail): Kubernetes audit logging guidance
