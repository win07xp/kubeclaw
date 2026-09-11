# Load and Scale

This page is the scale proof for the v1.0.0 design: a repeatable load harness, the environment it runs on, and the baseline it produced. The numbers are a baseline, not a pass mark: later releases run the same harness on the same environment and compare against it, and the page states what a real cluster changes.

## What the harness measures

The harness (`test/load`, driven by `make load`) creates a dedicated k3d cluster, installs the chart the way the e2e suite does, and runs six phases in order. Each phase writes its own block of the summary JSON and cleans up its own objects.

1. **Gateway steady state.** An in-cluster load generator calls the LLM proxy at fixed concurrency in four legs: the ServiceAccount-token tier against a mock provider that answers immediately, then the mTLS path (the load generator presents the certificate of a real Agent, so the gateway sees a Kaalm-managed workload calling from its own namespace) against an immediate provider, a 50 ms provider, and an immediate provider under hard budget enforcement. Each leg records client-observed latency, the gateway's own request histogram, and the gateway's peak CPU and memory. The immediate legs isolate the gateway's per-request cost; the hard leg isolates what synchronous ledger admission adds.
2. **Max-active ramp.** Agents are created in waves of 50, with persistence and hibernation off, and none are retired. The ramp stops at the target or at the environment's first limit: agent Pods crash-looping on probe timeouts, host memory below a floor, a node reporting memory pressure, or the scheduler refusing a Pod. The wave that hits the limit is trimmed, so the later phases run on the largest fleet that came up clean. Each wave records time-to-Ready (creation to the Ready condition) and its breakdown (certificate issuance, Pod start, start to Ready), the controller's reconcile histogram and queue depth, the operator components' memory, and the host's available memory, which yields the memory cost per running agent.
3. **Hold and serve.** At the peak fleet, every active agent receives one message per interval through its own async webhook channel, with replies pushed to the mock's callback receiver. The phase records accepted messages, delivery outcomes, callback counts, message latency, and whether any agent lost readiness or restarted.
4. **Fleet teardown.** Deleting the whole ramp fleet and timing until its Pods are gone.
5. **Hibernation churn.** A persistence-enabled subset with short idle timers. The harness waits for every agent's first hibernation, then sends each agent one message per cycle, with the cycle longer than a hibernation cycle so every message is a cold wake. It records the wake latency distribution from the gateway's wake histogram, wakes and hibernations from the controller's counters, and delivery outcomes.
6. **Concurrent tasks.** Submitting the task batch at once (the runtime's autocomplete flag makes each task report success on startup) and recording provisioning latency, run time, makespan, throughput, and retries.

Timings that come from API objects (time-to-Ready, task completion) have one-second granularity, so distribution shape comes from the Prometheus histograms and the objects supply the coarse per-agent numbers.

## Run the harness

```bash
make load                       # fresh cluster, images, chart, the full run
make load-run LOAD_FLAGS='-phases gateway -gateway-duration 30s'   # inner loop on the existing cluster
make load-down                  # delete the cluster
```

`make load` takes about an hour on the baseline machine. Results land in `test/load/results/` as JSON; the published baseline lives in `test/load/baseline/`. The flags in `test/load/config.go` change the fleet size, the wave size, the phase list, and every duration. The defaults are the baseline shape, so a baseline re-run is the one-line command.

The harness checks two host prerequisites before it starts:

- `fs.inotify.max_user_instances` of at least 512 and `fs.inotify.max_user_watches` of at least 524288. The k3d nodes share the host kernel, and a few hundred Pods exhaust the defaults with confusing symptoms.
- A load cluster with more than one node and a raised kubelet `max-pods`. `make load-up` creates one server and two agents at 250 Pods each; the kubelet default of 110 caps a fleet long before memory does.

The harness is a release-time local gate, listed in the release checklist, not a CI job: a run takes an hour, and its numbers mean something only against the baseline on the same environment, which shared CI runners cannot reproduce.

## The baseline environment

| Item | Baseline |
|---|---|
| Host | one machine: 16 CPUs and 16 GiB in a WSL2 VM (Linux 6.18), Windows idle, nothing else running |
| Cluster | k3d v5.8.3, one server and two agents, kubelet `max-pods` 250 per node, Kubernetes v1.31.5+k3s1 (flannel, kube-router NetworkPolicy, local-path storage, one CoreDNS) |
| Chart | 2 gateway replicas, 2 controller replicas, no resource limits, the mock provider trusted for upstream and callbacks |
| Agent image | the e2e starter-go agent (the Go base image plus the starter handler), BestEffort |
| Product code | `51ce26a`, main at the #171 merge |
| Run | September 11, 2026, `make load` with every default |
| Baseline file | `test/load/baseline/2026-09-11.json`; its `notes` field records how the file was assembled |

## Baseline numbers

### Gateway

32 concurrent callers for 60 seconds per leg, against the in-cluster mock provider. Client latency is what the load generator observed; gateway-side is the gateway's own `kaalm_llm_request_duration_seconds` for the leg (its buckets are coarse, which is why the 50 ms leg reads 75 ms there and 51 ms at the client). Gateway peak is the metrics-API maximum summed over both replicas.

| Leg | rps | Client p50 / p95 / p99 (ms) | Gateway-side p50 / p95 / p99 (ms) | Gateway peak |
|---|---|---|---|---|
| Token tier, soft budget, immediate upstream | 4238 | 2.8 / 36.0 / 97.4 | 3.0 / 36.2 / 98.2 | 4.9 cores, 74 MiB |
| mTLS, soft budget, immediate upstream | 4626 | 2.6 / 31.9 / 88.2 | 3.0 / 32.1 / 93.3 | 5.9 cores, 90 MiB |
| mTLS, soft budget, 50 ms upstream | 621 | 51.3 / 53.7 / 54.9 | 75.0 / 97.5 / 99.5 | 5.3 cores, 65 MiB |
| mTLS, hard budget, immediate upstream | 4295 | 3.2 / 34.9 / 81.6 | 3.1 / 34.9 / 89.2 | 5.5 cores, 74 MiB |

Every request succeeded (about a quarter million per immediate leg).

- The gateway's own cost per request is about 3 ms at p50 on both tiers. The token tier is not slower than mTLS: a ServiceAccount token is validated once and cached.
- Against a 50 ms provider the gateway adds about 1.3 ms at p50. 621 rps is the arithmetic limit of 32 callers over a 51 ms round trip, not a gateway limit.
- Hard budget enforcement adds about 0.6 ms at p50 over soft on an immediate upstream.
- The p95 and p99 tails on the immediate legs (30 to 100 ms) are two replicas saturating a 16-core host that also runs the load generator and the mock. The gateway is CPU-bound there, not waiting on anything.

### Max-active ramp

Waves of 50 agents, persistence and hibernation off, none retired. Time-to-Ready is creation to the Ready condition; certificate is creation to cert-manager marking the Certificate Ready; Pod start is creation to the kubelet's start time; reconcile is the controller's `controller_runtime_reconcile_time_seconds` for the Agent reconciler over the wave.

| Wave | Fleet Ready | Wall (s) | Time-to-Ready p50 / p95 / max (s) | Certificate p50 / p95 (s) | Pod start p50 (s) | Start-to-Ready p50 / p95 (s) | Reconcile p50 / p99 (ms) | Controller / gateway RSS (MiB) | Host available (MiB) |
|---|---|---|---|---|---|---|---|---|---|
| 0 | 50 | 64 | 36 / 50 / 60 | 32 / 48 | 34 | 1 / 10 | 8.8 / 96.2 | 51 / 49 | 11417 |
| 1 | 100 | 70 | 42 / 59 / 66 | 36 / 55 | 37 | 1 / 10 | 10.8 / 95.7 | 54 / 54 | 10895 |
| 2 | 150 | 65 | 36 / 57 / 63 | 31 / 51 | 33 | 2 / 10 | 20.3 / 97.0 | 58 / 59 | 10227 |
| 3 | 200 | 72 | 39 / 60 / 66 | 35 / 54 | 36 | 1 / 11 | 23.3 / 97.3 | 63 / 64 | 9472 |
| 4 | 250 | 73 | 40 / 61 / 68 | 35 / 58 | 36 | 1 / 10 | 23.4 / 97.5 | 69 / 68 | 8697 |
| 5 | 300 | 73 | 41 / 60 / 68 | 38 / 57 | 39 | 1 / 11 | 26.5 / 97.6 | 73 / 75 | 7922 |
| 6 | 350 | 77 | 46 / 67 / 74 | 40 / 62 | 42 | 2 / 11 | 28.7 / 97.7 | 78 / 78 | 7058 |
| 7 | 400 | 81 | 51 / 70 / 80 | 45 / 67 | 47 | 2 / 11 | 30.5 / 98.3 | 81 / 85 | 6161 |
| 8 | stopped | 177 | 43 / 174 / 174 | 48 / 69 | 50 | 11 / 110 | 26.3 / 98.6 | 84 / 95 | 5193 |

**400 agents came up Running and Ready in eight waves, 13 minutes end to end.** The ninth wave hit the machine's limit: with about 5.2 GiB of host memory reported available, agent Pods began failing their probes and two entered CrashLoopBackOff, so the wave was trimmed and the later phases ran on the 400. The limit is the machine, not the operator: the controller's reconcile p99 stayed under 100 ms, its queue never backed up, and its RSS grew from 51 to 84 MiB across 400 agents.

Two numbers transfer beyond this machine:

- **Memory per running agent: 16.0 MiB of host memory**, all in (the starter-go container, its pause container, the containerd shim, and the kubelet's accounting), measured as the drop in host available memory over the fleet.
- **Starting a fleet is paced by certificate issuance.** Every agent gates on a cert-manager Certificate before its Pod exists, and issuance is 32 to 48 s of the 36 to 51 s p50 time-to-Ready, rising slowly with the fleet; the Pod starts within 2 s of the certificate and is Ready 1 to 2 s after that. A wave of 50 takes 64 to 81 s end to end, about 40 agents a minute against the default single-replica cert-manager.

### Hold and serve

The 400-agent fleet, each agent on its own async webhook channel, one message per agent per minute for three minutes (6.7 messages per second in aggregate), replies pushed to the callback receiver. The load generator saw every message accepted (1201 of 1201 answered `202`, acceptance p50 8.9 ms, p99 14.7 ms). On the gateway side:

| Measure | Value |
|---|---|
| Delivered / failed | 1181 / 19 (98.4% delivered, 1.6% failed after four attempts) |
| Callbacks delivered | 1200 of 1200 attempted, 1186 under 5 ms |
| Delivery time | p50 6.7 ms; 946 of 1202 under 25 ms; 90 between 1 s and 10 s; 157 over 10 s |
| Fleet during the hold | 400 Ready before and after, 0 container restarts, 0 readiness flaps |
| Gateway peak | 100 mCPU, 232 MiB |
| Controller peak | 15 mCPU, 93 MiB |

The delivery tail follows the delivery retry ladder (1 s, 5 s, 25 s, each attempt bounded by a 10 s read timeout): about 13% of first attempts fail and succeed on a retry, and the 157 over 10 s are first attempts that ran into the full timeout. The agent Pods were healthy throughout and the callback leg was uniformly fast, so the hop that stalls is gateway to agent. The gateway records no reason for a failed attempt, so the cause is open: [#172](https://github.com/win07xp/kaalm/issues/172) tracks the instrumentation and the first suspect (a 4-label Service name resolved under `ndots:5`, eight DNS queries per delivery). This is the baseline's first number to re-measure.

Channel activation runs at about one channel per second at this scale: the AgentChannel reconciler, like every Kaalm reconciler, reconciles one object at a time. The harness creates each agent's channel together with the agent so that work overlaps the ramp's waves.

### Fleet teardown

Deleting all 400 agents and their channels: every Pod gone in 58 s.

### Hibernation churn

100 persistence-enabled agents (a 1 GiB local-path PVC each) with a 10 s idle timeout and a 5 s hibernation delay. The harness waits for every agent to come up and then hibernate, then sends each agent one message per 90 s for 10 minutes, so every message finds its agent hibernated.

| Measure | Value |
|---|---|
| Fleet up | all 100 in 151 s; time-to-Ready p50 24 s, p95 127 s (certificate issuance for 100 at once queues) |
| First hibernation | Ready to Hibernated p50 18 s, p95 29 s; all 100 hibernated 15 s after the last came up |
| Messages | 667 sent at 1.11 per second, 667 accepted, 668 delivered, 0 failed, 668 callbacks |
| Wakes | 667 (every message was a cold wake), all with result `ready`; 667 hibernations followed |
| Wake latency | p50 4.7 s; 85 wakes in 1 to 2.5 s, 285 in 2.5 to 5 s, 137 in 5 to 10 s, 160 beyond 10 s (the histogram's last bucket) |
| Teardown | 100 agents and their PVCs gone in 13 s |

A cold wake is activation, a new Pod against the existing certificate and PVC, container start, readiness, and the gateway's delivery, which polls the agent Service every 2 s until it answers. The controller's idle evaluation runs on a 15 s activity cache, which is why Ready-to-Hibernated sits at 18 s with 10 s timers. The wake histogram tops out at 10 s, so the 24% beyond it have no upper bound here; the hold phase's delivery tail ([#172](https://github.com/win07xp/kaalm/issues/172)) is the same hop and the same instrumentation gap.

### Concurrent tasks

200 AgentTasks submitted at once (in 0.6 s), each reporting success on startup.

| Measure | Value |
|---|---|
| Outcome | 200 Succeeded, 0 retries |
| Makespan | 194 s for the batch, 62 tasks per minute |
| Creation to start | p50 117 s, p95 182 s (certificate issuance for 200 at once, then Pod start) |
| Start to completion | p50 5 s, p95 7 s |
| Teardown | 200 tasks gone in 19 s |

Task throughput on this environment is certificate issuance throughput: the run itself is 5 s, and the queue in front of it is two minutes at p50.

## What a real cluster changes

The baseline is one developer machine. The numbers that transfer are the per-unit ones (memory per running agent, the gateway's per-request cost, wake latency shape); the absolute fleet ceiling does not.

- **Provider latency.** Real providers answer in hundreds of milliseconds to seconds, so the gateway's own cost, which the immediate legs isolate, is a small fraction of every request. The 50 ms leg is the shape to compare against.
- **Memory and nodes.** The ramp stops where host memory runs out on one machine. On a real cluster the fleet ceiling is the sum of node capacity divided by the per-agent figure, plus whatever the agent image itself needs beyond the starter.
- **Certificate issuance.** Every agent gates on a cert-manager Certificate before its Pod exists, so starting a fleet is paced by cert-manager's issuance rate. A production cert-manager can be tuned and scaled; the baseline runs the default single replica.
- **The CNI.** k3d's flannel enforces NetworkPolicy through kube-router, whose ipset programming lags a freshly created Pod by up to about 20 seconds, which lands inside wake latency. Cilium and Calico program policies differently and typically faster.
- **Storage.** The local-path provisioner backs the churn fleet's PVCs and provisions each volume through a helper Pod. A CSI driver changes both the provisioning latency and the hibernate-and-wake cost.
- **The apiserver.** k3s runs a single embedded apiserver on SQLite-backed storage. The controller's reconcile latency and the hold phase's callback records are apiserver-bound at scale; a multi-member etcd behaves differently under the same write rate.

## See also

- [Observability](observability.md) for the metric catalog the harness reads.
- [Controller operations](../controller/operations.md#observability) and [LLM gateway operations](../gateways/llm/operations.md#observability) for what each metric means.
- [Vision and scope](../concepts/vision-and-scope.md) for the fleet shape the design targets.
