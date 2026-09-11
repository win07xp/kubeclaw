# Budgets and Rate Limits

The LLM gateway enforces two per-namespace controls on LLM traffic: **budgets** (how much a namespace may spend in a period) and **rate limits** (how fast a namespace may call a model). Both are enforced in the gateway itself, because the gateway is the single choke point every LLM request already passes through.

Rate limits, and budgets in their default **soft** mode, are deliberately **approximate**: the design trades exactness for the absence of a distributed coordination layer, and every section below states the exact bound on the resulting error so you can decide whether that trade works for your deployment. Since v0.3.0, budgets also offer an opt-in **hard** mode ([Hard Enforcement](#hard-enforcement)) that pays for a stated spend guarantee with serialized admission near the ceiling. Soft remains the default.

## Budget State Management

Budget counters are maintained **in-process in the gateway**. Because the gateway is the single choke point for all LLM traffic, there is no need for a separate aggregator or distributed counter.

Each gateway replica maintains an in-memory spend counter per (provider, namespace, period) tuple. On startup, each replica reads the current period's spent value from the canonical ConfigMap managed by the [ModelProviderReconciler](../../controller/reconcilers.md#modelproviderreconciler). On each LLM call, the counter is updated synchronously.

### The budget counter exchange

Replicas do not talk to each other directly. They exchange spend through a ConfigMap, and the reconciler acts as the reducer over what they write.

![Sequence diagram of the budget counter exchange between two gateway replicas, the kaalm-budget-{provider} ConfigMap, and the ModelProviderReconciler, divided into four cadences. At startup a replica reads _canonical once to initialize its counter, and a note records that this is the only time a replica ever reads that key. Per request, the replica increments its in-memory counter synchronously per provider, namespace and period. Every 10 seconds each replica server-side-applies its partial to its own key, named for its Pod, using its own Pod name as the field manager, so simultaneous writes never conflict; the period tag lets the reducer drop stale entries during rollover. On every ConfigMap watch event, with the 10 second tick as backstop, each replica folds its peers' current-period partials plus the _retired accumulator into its enforcement view; a highlighted note marks this as load-bearing, since the enforced value is the replica's own live counter plus every peer's latest partial, stale by at most one peer publish interval: 10 seconds under soft enforcement, one immediate settle-publish under hard enforcement's boundary region, and without the fold a long-lived replica would never observe peer spend and drift would grow unbounded within a period. On each reconcile the reconciler reads all keys, filters out entries whose period is not current, prunes keys with no live gateway Pod name folding them into _retired first, sums the remaining partials plus _retired, writes _canonical, and updates status.budgetUsage. A closing note marks that _canonical is not on the enforcement path: it is the durable roll-up for status reporting and replica restarts, while per-request enforcement reads peers' partials straight off the watch.](../../diagrams/budget-exchange.svg)

Reading the diagram: the four cadences are the structure, and the thing to notice is which arrows the enforcement decision actually depends on. It depends on the per-request increment and on the watch-driven fold of peers' partials. It does not depend on `_canonical`, which is written by the reconciler and read exactly once per replica lifetime, at startup.

Each gateway replica periodically (every 10s) writes its partial spend counters to a ConfigMap in `kaalm-system` named `kaalm-budget-{providerName}`, keyed by the replica's Pod name. Replicas use **server-side apply** with per-replica field managers (field manager name = Pod name), so each replica owns only its own key. This eliminates optimistic concurrency conflicts between replicas writing simultaneously.

The ConfigMap data structure is:

```yaml
data:
  # Each key is a gateway Pod name; value is JSON with the budget period and per-namespace spend.
  # The "period" field is required so the reconciler can exclude stale entries from prior periods
  # during rollover. Replicas transition to the new period independently on their first request,
  # so mixed-period entries are expected in the rollover window.
  kaalm-gateway-0: '{"period": "2026-04", "team-support": "142.50", "team-ml": "87.30"}'
  kaalm-gateway-1: '{"period": "2026-04", "team-support": "138.20", "team-ml": "91.10"}'
  _retired: '{"period": "2026-04", "team-support": "12.10"}'
  _canonical: '{"team-support": "292.80", "team-ml": "178.40"}'
```

The three roles in this ConfigMap are worth naming precisely:

- **Per-replica keys** (`kaalm-gateway-0`, `kaalm-gateway-1`) are written by the replicas, each owning exactly one key via its own field manager. They are partials: one replica's view of its own spend. Inside a partial, underscore-prefixed fields (for example `_marginExceeded`, see [Hard Enforcement](#hard-enforcement)) are flags, never spend; parsers skip them, and flag values are never bare numbers so that an older parser cannot mistake one for a namespace total.
- **`_retired`** is written only by the reconciler: when it prunes a terminated replica's current-period key (see [Stale replica cleanup](#stale-replica-cleanup)), that key's totals fold into this period-tagged accumulator first, so spend a dead replica already published is never erased. Replicas fold `_retired` into their enforcement view like a peer partial.
- **`_canonical`** is written only by the reconciler. It is the durable roll-up, including `_retired`.

The ModelProviderReconciler reads this ConfigMap on each reconcile pass (event-driven plus the controller's 5-minute periodic requeue per [Reconcile Interval and Performance](../../controller/overview.md#reconcile-interval-and-performance)), **filters out any per-replica entries whose `period` does not match the current period**, sums the remaining partials plus `_retired`, writes the `_canonical` key with the total, and updates `status.budgetUsage` on the ModelProvider. Gateway replicas read the `_canonical` key on startup to initialize their local counters. This avoids a Prometheus dependency and works with existing ConfigMap RBAC.

### Cross-replica enforcement view

After startup, each replica folds the other replicas' current-period partials into its enforcement view: on every budget-ConfigMap watch event (a ConfigMap informer scoped to `kaalm-system`), with the 10-second exchange tick as a backstop when the watch goes quiet. Freshness is therefore bounded by how often peers publish, not by how often replicas fold. Under soft enforcement peers publish on the 10-second tick, so the enforcement view is at most one publish interval stale; under [hard enforcement](#hard-enforcement), settles inside the boundary region publish immediately, and peer spend lands in every enforcement view one watch propagation later. The spend value a replica enforces budget policies against is its own live in-memory counter plus every peer's most recently written partial, plus the `_retired` accumulator.

This is load-bearing: if replicas only read `_canonical` at startup, a long-lived replica would never observe peer spend and enforcement drift would grow unbounded within a budget period. The reconciler's `_canonical` write remains the durable roll-up for status reporting and replica (re)starts; per-request enforcement never waits on it.

### Period tag rationale

At period rollover (midnight UTC), gateway replicas detect the new period on their first incoming request and reset their local counter to zero. Because replicas transition independently, there is a window where some replicas have written new-period partials and others still hold old-period totals.

Without the `period` field, the reconciler would sum mixed values and produce an incorrect canonical total. By tagging each entry, the reconciler skips old-period entries until all replicas have transitioned, giving a correct (if slightly underestimated) total during the rollover window, which is acceptable for a soft guardrail.

### Budget state on crash

If all gateway replicas crash simultaneously, up to 10s of spend data (the partial-write interval) may be lost. This is acceptable under soft enforcement: the bounded loss is small relative to typical budget thresholds. Gateway replicas re-initialize from the `_canonical` ConfigMap value on restart and immediately fold live partials on top; [hard enforcement](#hard-enforcement) tightens both the loss bound and the restart posture.

### The overspend bound

This means soft budget enforcement is **approximate under high concurrency**. Replicas can collectively overspend within a partial-write window, since each replica sees peer spend at most ~10s stale (the partial-write interval). The overspend is bounded by:

```
number_of_replicas x max_calls_per_second_per_replica x cost_per_call x partial_write_interval_seconds
```

For typical deployments (2-3 replicas, 1 call/sec peak per replica, $0.01-0.10 per call, 10s partial-write interval), the maximum overspend per window is roughly $0.20-$3.00, which is acceptable for a soft guardrail.

**Streaming widens this window.** Streamed usage lands on the counters only after the stream completes (see [Streaming Responses](request-handling.md#streaming-responses)), so an in-flight stream's cost is invisible to every replica, including the one serving it, for the stream's full duration, often 30 to 120s rather than 10s. The bound therefore carries an additional term:

```
+ number_of_replicas x concurrent_streams_per_replica x cost_per_call
```

Streaming-heavy namespaces should size their soft-guardrail slack from that larger figure.

**Soft enforcement is spend visibility and guardrails, not a financial cap, and that is its design point**: zero coordination cost on the request path, with the overspend bounded and quantified above. Teams that need a cap opt in to [hard enforcement](#hard-enforcement), which states exactly what it guarantees and exactly what it costs. Provider-level account limits remain sound defense in depth under either mode.

### Budget period rollover

Budget periods roll over at midnight UTC. Each gateway replica detects the period change on its first request of the new period and resets its local counter, writing a new-period entry (with the updated `period` field) to its ConfigMap key. The reconciler, filtering by the current period, excludes old-period entries during the rollover window: the canonical total may be temporarily underestimated until all replicas have written new-period entries, which is acceptable for soft guardrails.

Once the new period is fully established, the controller:

1. Archives the previous period's totals to ModelProvider status for auditability.
2. Deletes all per-replica keys from the budget ConfigMap.
3. Writes a fresh `_canonical: {}`.

### Stale replica cleanup

When a gateway replica is scaled down or replaced, its entry in the budget ConfigMap persists. Server-side apply gives each replica ownership of its own key, but nothing reclaims that key when the replica goes away.

The ModelProviderReconciler cross-references ConfigMap keys against the current set of gateway Pod names and deletes stale entries before summing partials. This prevents inflated spend totals from terminated replicas.

Deleting a key must not delete the spend it recorded: before removing a current-period key, the reconciler folds its totals into the `_retired` accumulator. Under soft enforcement the distinction is cosmetic (without it, a rollout would produce a small bounded undercount); under [hard enforcement](#hard-enforcement) it is load-bearing, because a rolling restart that erased every replaced replica's published spend would void the ceiling once per rollout.

## Per-Workload Spend

Since v0.5.0 the ledger also answers "which agent spent it": beside the
per-namespace enforcement counters, each replica accumulates per-workload
spend keyed `{namespace}/{workload}`, where the workload is the attested
`agent/{name}` or `task/{name}` from the caller's certificate SAN, or the
visible `(unattributed)` bucket for gateway-only-tier callers, which
authenticate by token and carry no workload identity. Keeping that bucket
visible is what makes the per-workload rows always sum to the namespace
figure. Spend lands at the same single settle point the namespace counters
use, under the same mutex, and rolls over with the same period reset; the
admission math never reads the workload maps, so hard enforcement is
untouched by construction.

Persistence rides the same exchange in a second object,
`kaalm-agentspend-{provider}`: each replica publishes its workload partial
with the same server-side-apply one-key-per-replica pattern and the same
period tag, folds peers back on the tick and on the ConfigMap watch, and
seeds from `_canonical` at startup. A separate ConfigMap for two reasons.
First, safety: the budget fold sums every non-underscore key in the budget
ConfigMap as namespace spend, so workload keys inside it would silently
corrupt utilization. Second, capacity: both objects live under the ~1 MiB
cap, and at the design target of 1000+ agents the workload keys need the
room. The reducer half mirrors the budget reducer: a pruned replica's
current-period partial folds into `_retired` before its key deletes, stale
periods drop, and `_canonical` carries live plus retired.

Three deliberate boundaries. The breakdown keeps the **current period
only**: the namespace figures archive one prior generation in
`ModelProvider.status`, the breakdown does not, and it never enters
provider status at all (namespaces times workloads would grow the CR
against the same object cap). It never becomes a metric label: the
cardinality doctrine stands, and per-workload resolution lives in the
[console read API](../../console/overview.md) via the gateway's
[GET /v1/spend](../api/internal-endpoints.md#get-v1spend), which any single
replica answers from its folded union, current to within one publish
interval. And it is priced spend: calls to unpriced models cost zero and do
not appear, the same soft-guardrail posture the namespace figures have.

## Hard Enforcement

Setting `spec.budget.enforcement: hard` on a ModelProvider (default `soft`) turns that provider's `action: block` thresholds into a cap with a stated guarantee. Nothing else changes: `warn` and `degrade` policies remain advisory in both modes, both ceilings (`perNamespaceUSD` and `clusterUSD`) participate through the same worse-of-two utilization, and outside the boundary region described below the machinery is byte-for-byte the soft machinery. Three validation rules gate the mode: hard requires at least one `block` policy (rule 32), every catalog model priced (rule 33, because an unpriced call costs zero and a cap over zeros is a lie), and a coherent boundary margin (rule 34). See [Cross-Resource Validation](../../resources/validation-and-defaulting.md#cross-resource-validation).

### The boundary region

A hard cap cannot be enforced by after-the-fact counting alone: by the time a settled cost lands on the counters, the money is spent. Instead of estimating request costs up front (impossible in general, and dishonest for streams), hard mode changes behavior only inside a **boundary region** just below each `block` threshold, where the remaining headroom is small enough that uncoordinated concurrency could cross the ceiling.

The region starts `boundaryMarginPercent` percentage points below each `block` policy's `atPercent` (the knob under `budget.hard`, default 5). The configured value is a floor, not the whole truth: each replica also computes a margin from what it has actually observed this period, and the **effective margin** is the larger of the two:

```
marginUSD = replicas x maxObservedCostPerCall
          + (replicas - 1) x observedPeakSpendRatePerSec x stalenessWindowSeconds

effectiveMarginPercent = max(boundaryMarginPercent, 100 x marginUSD / ceilingUSD)
```

The first term covers one unsettled in-flight request per replica; the second covers spend a peer has settled but not yet propagated. The observed inputs are maintained per provider within the period: the running maximum settled cost of a single call, and the peak spend rate over 10-second buckets, monotone within the period and reset at rollover. An early burst therefore widens the margin for the rest of the period, which errs toward throttling earlier, the safe direction. The replica count includes not-yet-Ready gateway Pods; that too is the conservative direction, and is deliberate.

When the computed margin exceeds the configured knob, the replica raises the `_marginExceeded` flag in its published partial, and the ModelProviderReconciler surfaces it as a `BoundaryMarginRaised` condition and a Warning event on the ModelProvider: the operator learns the knob is undersized for the observed traffic without the guarantee ever having depended on it. The one residual this reactive scheme cannot close, stated plainly: a traffic burst without precedent in the current period can outrun the computed margin in the first staleness window it appears. Sizing the configured knob from the [soft overspend bound](#the-overspend-bound) formula with your own worst-case rates closes that hole with operator knowledge the gateway cannot have.

### Serialized admission

Inside the boundary region, each replica admits **at most one request at a time** per governed ceiling: a per-`(provider, namespace)` slot for the namespace ceiling, and a provider-wide slot when the cluster ceiling is the one in boundary, both acquired in the same atomic step. A request that finds the slot held is rejected immediately with `429 budget_throttled` and `Retry-After: 1` ([error schema](../api/errors.md#llm-gateway-error-responses)); there is no queue, deliberately, because a queued request holding one provider's slot while waiting on another's (reachable through a fallback chain) is a deadlock shape, and because near the ceiling a queue mostly drains into blocks anyway.

Settlement is the other half of the invariant: when the admitted request completes, its actual cost lands on the counter and the slot frees in one atomic step, so the next admitted request always sees the previous one's real cost. The settle also publishes the replica's partial immediately instead of waiting for the 10-second tick, which is what shrinks peer staleness to one watch propagation inside the region. A held slot stays authoritative even if a fold momentarily drops utilization below the boundary (peer prune during a rollout, for example): the slot releases only on settle, never on recomputation.

The block decision itself is unchanged from soft mode, with one addition: the `429 budget_exhausted` message names which ceiling fired, `namespace budget exhausted: <ns>` or `cluster budget exhausted`, so a block is attributable at a glance.

### The guarantee

For a hard-mode provider, spend within a period never exceeds a `block` ceiling by more than:

- the actual cost of at most **one in-flight request per replica, per governed ceiling**, plus
- each peer's settled-but-not-yet-propagated spend, bounded by **one settle-publish-to-watch propagation** per peer, typically well under a second.

Fine print, all of it load-bearing:

- The gateway can only cap what providers report. A response with no usage metadata settles at zero cost ([Streaming Responses](request-handling.md#streaming-responses)); a provider that omits usage evades any gateway-side cap. Rule 33 keeps unpriced *models* out of hard mode, but usage-less *responses* are a provider behavior no proxy can price.
- The margin machinery above bounds when serialization engages, not what a single admitted request may cost. A single request larger than the remaining headroom is admitted (there is exactly one of it per replica) and its overshoot is the first bullet's bound.
- Simultaneous crash of all replicas loses at most the in-flight requests' costs inside the boundary region (settles publish immediately there), rather than soft mode's 10 seconds of spend.

### Failure posture

Hard mode fails closed, but only where the guarantee is live. Below the boundary region, apiserver unavailability degrades hard enforcement to exactly the soft bound: counters keep counting, publishes retry on the tick, requests flow. Inside the boundary region, a replica rejects requests with `503 budget_state_unavailable` when either staleness signal trips:

- **Write path**: it holds settled spend older than the staleness window that it has been unable to publish, so peers may be admitting against a stale view of this replica.
- **Read path**: it has not successfully refreshed its peer view within the staleness window (a dead watch with quiet peers is indistinguishable from silence, so the exchange tick doubles as the liveness probe).

The staleness window is derived, not configurable: three publish intervals, 30 seconds. Recovery is automatic on the first successful publish or fold. A cap that fails open is not a cap; a cap that fails closed everywhere is an availability hazard; failing closed only inside the region where money is actually at stake is the trade this design picks.

### Restarts and rollover

A restarting replica seeds from `_canonical` and folds live partials on top. Until the reconciler's next prune-and-roll-up pass, the replica's own pre-restart key may still be present alongside `_canonical` totals that already include it, so the enforcement view can transiently overcount. Overcounting blocks early rather than late, the correct failure side for a cap, and it clears within one reconcile. Rolling restarts do not erase published spend at all: the reconciler folds pruned keys into `_retired` before deleting them (see [Stale replica cleanup](#stale-replica-cleanup)).

At period rollover, counters, slots, and the observed-traffic tracker all reset. A request admitted before midnight settles into the new period (the same attribution soft mode gives a midnight-spanning call), the boundary flag drops with the first new-period publish, and the margin collapses back to the configured knob until new observations accrue.

### Streaming under hard enforcement

A streaming request admitted in the boundary region holds its admission slot for the stream's full duration, since its cost settles only when the stream completes. The upstream client timeout (default 120 seconds) bounds the hold. Concretely: near the cap, streams serialize, and a long stream makes its neighbors wait out `budget_throttled` retries for its whole duration. That is the honest shape of a hard cap over pay-per-token streaming, and the reason the region is kept as narrow as the margin math allows.

### Interaction with fallback

A hard-mode primary that is blocked, throttled, or failed closed short-circuits exactly as a soft block does today: the fallback walk never starts, because a capped namespace must not drain a fallback provider's budget. Inside a walk, a hard fallback candidate is governed by the same admission rules, consumes an attempt slot, and falls through to its children. A walk exhausted entirely by budget outcomes returns `429 budget_exhausted` (with the largest observed `Retry-After`), or `503 budget_state_unavailable` when fail-closed candidates are why; it never masquerades as `502 provider_error`.

### What hard mode costs

Serialized admission near the ceiling, one ConfigMap write per settle inside the boundary region, and a ConfigMap watch per gateway replica. The first is the one to plan for: budget utilization is monotonic within a period, so a namespace that reaches its boundary region lives there until rollover, and month-end is precisely when its traffic serializes. Namespaces that need throughput at high utilization should widen their budget, not their margin.

## Rate Limiting

Rate limits are enforced at the gateway using token-bucket limiters keyed on (namespace, model). Limits come from `ModelProvider.spec.rateLimits` and represent **cluster-wide ceilings**. When a limit is hit, the gateway returns HTTP 429 with a `Retry-After` header.

### Dividing by live replica count

Each gateway replica divides the configured limit by the number of active gateway replicas (discovered from its Pod informer: count Pods matching the gateway label selector, refreshed at most every few seconds rather than on every request). When replicas scale up or down, each replica adjusts its local token bucket capacity within that refresh and the next refill cycle. This means the configured value directly represents the intended cluster-wide rate limit regardless of replica count.

**Note:** because each replica enforces its share independently, the effective cluster-wide limit is approximate. Transient bursts may slightly exceed the configured ceiling. The approximation is bounded by `configured_limit / number_of_replicas` per replica (one replica's full bucket) and is acceptable for v1.

### Worst-case deviation during scaling events

During scale-up, existing replicas immediately divide by N+1 when the new Pod appears in their informer, before the new replica begins serving traffic, momentarily reducing each existing replica's effective limit.

During rolling restarts (`maxUnavailable: 1`), different replicas can transiently hold different bucket sizes, causing the effective cluster-wide ceiling to deviate by up to one replica's share.

If tighter enforcement is required, replace per-replica division with a shared ConfigMap-backed token bucket (the `kaalm-budget-{providerName}` ConfigMap already provides the coordination primitive).

## Related

- [ModelProviderReconciler](../../controller/reconcilers.md#modelproviderreconciler): the reducer that sums partials, prunes stale keys, and writes `_canonical` and `status.budgetUsage`.
- [ModelProvider](../../resources/modelprovider.md): where `spec.rateLimits` and the budget configuration are declared.
- [Streaming Responses](request-handling.md#streaming-responses): why streamed spend is invisible until the stream completes.
- [Gateway Error Responses](../api/errors.md#llm-gateway-error-responses): the structured error schema and status code mapping, including budget exhaustion and 429 responses.
- [Gateway ServiceAccount permissions](../../security/rbac.md#gateway-serviceaccount-permissions): the ConfigMap RBAC the budget exchange relies on.
