# AgentClass

AgentClass is a cluster-scoped policy resource. It describes the runtime configuration, isolation, resource defaults, and allowed providers for a category of agents. It is analogous to StorageClass: developers reference an AgentClass by name in their Agent or AgentTask spec, and the platform team controls what each class permits.

This split is the core of Kaalm's governance model. Developers pick a class; the class decides what images may run, how much compute they get, where their traffic may go, and which LLM providers they may call. When a live AgentClass is edited, the change propagates to running workloads through a controlled mechanism described in [AgentClass change handling](../controller/change-propagation.md#agentclass-change-handling). The reconciler that materializes an AgentClass into cluster objects is documented in [AgentClassReconciler](../controller/reconcilers.md#agentclassreconciler).

## Spec

The full spec, annotated:

```yaml
apiVersion: kaalm.io/v1beta1
kind: AgentClass
metadata:
  name: standard
spec:
  # Runtime backend. Only "pod" is supported in v1.
  # "agentSandbox" backend (Sandbox CRD integration) is planned for v1.1.
  runtime:
    backend: pod
    # Optional. When set, must name a RuntimeClass that exists on the cluster
    # (e.g., gvisor): Pod admission fails with "RuntimeClass not found"
    # otherwise. Omit (the default) to use the cluster's default container
    # runtime (runc in practice); stock clusters define no RuntimeClass
    # objects at all, so pinning e.g. "runc" here would break scheduling.
    # runtimeClassName: gvisor

  # Image policy
  image:
    # If set, only images matching one of these patterns may be used.
    # Empty list = any image allowed (not recommended).
    allowedImages:
      - "registry.internal.corp/agents/*"
      - "ghcr.io/myorg/agents/*:v*"
    # Default image if the Agent spec does not provide one.
    defaultImage: "registry.internal.corp/agents/base:v1"
    # Whether Agents of this class may mount a handler ConfigMap into a
    # reference base image (Agent.spec.handler). Default false: a mounted
    # handler injects code into an image this allowlist already approved,
    # so the capability is an explicit per-class grant. An Agent with
    # spec.handler set against a class where this is false is moved to
    # phase=Degraded (reason=HandlerMountNotAllowed) at reconcile time.
    # See rule 30 in Cross-Resource Validation.
    allowHandlerMounts: false
    pullPolicy: IfNotPresent
    imagePullSecrets:
      - name: registry-credentials

  # Resource defaults and caps. Agent.spec.resources may override within caps.
  resources:
    defaults:
      requests: { cpu: "500m", memory: "1Gi" }
      limits:   { cpu: "1",    memory: "2Gi" }
    maxLimits:
      cpu: "4"
      memory: "8Gi"

  # Persistence defaults
  persistence:
    # When false, Agents and AgentTasks of this class cannot request a PVC:
    # an Agent with persistence.enabled=true is moved to phase=Degraded
    # (reason=PersistenceNotAllowed), and an AgentTask with the same setting
    # is moved to phase=Failed (reason=PersistenceNotAllowed), at reconcile
    # time. See rule 24 in Cross-Resource Validation.
    enabled: true
    defaultSizeGi: 5
    maxSizeGi: 50
    storageClassName: "standard"   # k8s StorageClass
    # What happens to the per-Agent PVC when the Agent is deleted. Distinct from
    # PersistentVolume.persistentVolumeReclaimPolicy (which governs PV fate on
    # PVC deletion): this field controls PVC fate on Agent deletion.
    pvcRetention: Delete           # Delete | Retain

  # Provider access: which ModelProviders may agents of this class reference?
  # Empty list = no providers allowed.
  allowedProviders:
    - name: anthropic-shared
    - name: openai-fallback

  # Tool access (since v0.4.0): which ToolProviders may workloads of this
  # class be granted (rule 37)? Empty list = none, the same default as
  # allowedProviders.
  allowedToolProviders:
    - name: search-tools

  # Network policy hints (the controller translates these into NetworkPolicy resources)
  network:
    egress:
      # Allowed external destinations beyond the Kaalm gateway, expressed as
      # CIDR blocks. Agent containers call providers through the gateway; this
      # governs other direct egress (from v0.4.0, MCP tool traffic
    # preferentially rides the gateway's tool plane instead). Enforced on any CNI that implements
      # standard Kubernetes NetworkPolicy.
      allowedCIDRs:
        - "10.42.0.0/16"         # internal MCP subnet
        - "140.82.112.0/20"      # api.github.com (example; pin to actual ranges)
      # Optional DNS-based allowlist. Only enforced on CNIs that support FQDN
      # egress policies (e.g., Cilium, Calico Enterprise). On standard CNIs this
      # field is ignored and AgentClassReconciler emits a Warning event. Use
      # allowedCIDRs for portable enforcement; use allowedHosts in addition only
      # when you have a CNI that supports FQDN-based policy.
      allowedHosts:
        - "mcp.internal.corp"
        - "api.github.com"
    allowHostNetwork: false
    # Allow ingress from other agent Pods in the same namespace.
    # When true, the controller adds a NetworkPolicy ingress rule that allows
    # traffic from any Pod in the same namespace bearing the kaalm agent label.
    # Default false (deny all ingress except from the Kaalm gateway).
    allowSameNamespaceIngress: false

  # Pod security
  security:
    podSecurityContext:
      runAsNonRoot: true
      runAsUser: 10001
      seccompProfile: { type: RuntimeDefault }
    containerSecurityContext:
      allowPrivilegeEscalation: false
      readOnlyRootFilesystem: true
      capabilities: { drop: ["ALL"] }

  # Lifecycle defaults (overridable per-Agent within limits)
  lifecycle:
    defaultIdleTimeout: "30m"
    maxIdleTimeout: "24h"
    # Hard policy lever. When false, Agents of this class cannot opt in to
    # hibernation: an Agent with lifecycle.hibernationEnabled=true is moved
    # to phase=Degraded, reason=HibernationNotAllowed at reconcile time. See
    # rule 26 in Cross-Resource Validation.
    hibernationAllowed: true
    defaultHibernationDelay: "30m"  # how long an agent stays Idle before hibernating
    maxHibernationDelay: "2h"      # cap on per-Agent hibernationDelay overrides
    defaultWakeTimeout: "2m"       # default time gateway waits for Pod Ready on wake
    maxWakeTimeout: "5m"           # cap on per-Agent wakeTimeout overrides
    terminationGracePeriodSeconds: 60

  # Labels and annotations added to every Pod created under this class
  podMetadata:
    labels:
      cost-center: "platform"
    annotations: {}
```

The `PersistenceNotAllowed` and `HibernationNotAllowed` outcomes referenced in the comments are rules 24 and 26 in [Cross-Resource Validation](validation-and-defaulting.md#cross-resource-validation).

## Status

```yaml
status:
  observedGeneration: 3
  conditions:
    - type: Ready
      status: "True"
      reason: AllReferencesResolved
      message: "All referenced ModelProviders exist and are healthy"
      lastTransitionTime: "2026-04-05T12:00:00Z"
  agentsInUse: 14       # count of Agents currently using this class
  tasksInUse: 2         # count of AgentTasks currently using this class
```

The `Ready` condition reports whether every reference the class makes (its `allowedProviders` and `allowedToolProviders` lists) resolves to an existing provider. `agentsInUse` and `tasksInUse` count the Agents and AgentTasks currently using this class, which tells the platform team what a change to the class will affect.

## Design Notes

### An image allowlist is effectively mandatory

`allowedImages` is mandatory in practice for real clusters. An empty list means "any image", and validation will emit a warning when it sees one. Leave it empty only in throwaway environments.

### How `pvcRetention: Retain` works

AgentClass-derived per-Agent PVCs carry an ownerRef back to the Agent, like other [child resources](../runtime/child-resources.md). That means the default Kubernetes cascade garbage collection removes the PVC when the Agent is deleted.

To honor `Retain`, the Agent finalizer strips the PVC's ownerRef before the Agent's own finalizer is removed; cascade GC then leaves the PVC untouched. When the policy is `Delete`, the finalizer leaves the ownerRef in place and cascade GC removes the PVC. See [Finalizers](../controller/finalizers.md).

### `allowHandlerMounts` guards the image review boundary, not code execution

Anyone who can create an Agent can already run arbitrary code: any image matching `allowedImages`. What a mounted handler ([`Agent.spec.handler`](agent.md), consumed by the [reference base images](../runtime/base-images.md)) uniquely adds is code that *bypasses* image review: the platform team allowlisted `kaalm-agent-python`, not the handler source injected into it. `allowHandlerMounts` therefore defaults to `false`, and enabling it is the class-level statement that, for workloads of this category, namespace-level ConfigMap authorship is an acceptable code provenance. Classes for production fleets built from reviewed images leave it off; a starter or development class turns it on. Enforcement is rule 30 in [Cross-Resource Validation](validation-and-defaulting.md#cross-resource-validation), with the same recoverable `Degraded` handling as the persistence and hibernation gates, including on class drift: flipping this to `false` on a live class degrades existing Agents that mount handlers.

Stated as RBAC, so there is no room to misread it: **granting `allowHandlerMounts` makes ConfigMap write access in a namespace equivalent to code execution as that namespace's handler-mounting Agents**, with their ServiceAccount, their certificate identity, and their gateway access, taking effect at the next Pod recreation (including a wake from hibernation). Grant it only on classes whose namespaces already treat every ConfigMap author as a code author, and see the corresponding row in the [threat model](../security/threat-model.md#workload-isolation).

### Image pattern glob semantics

Patterns in `allowedImages` use Go's [`path.Match`](https://pkg.go.dev/path#Match) rules. Two consequences matter:

- **`*` does not cross path separators.** The wildcard matches any sequence of non-`/` characters. So `registry.internal.corp/agents/*` matches `registry.internal.corp/agents/foo:latest` but NOT `registry.internal.corp/agents/team/foo:latest`. Use explicit multi-segment patterns (e.g., `registry.internal.corp/agents/team/*`) for nested paths.
- **Digest references DO match globs.** `*` matches any run of non-`/` characters *including* `@` and `:`, so `registry.internal.corp/agents/*` matches `registry.internal.corp/agents/foo@sha256:…` as well as tagged refs. Where digest exclusion matters, anchor the tag (e.g., `registry.internal.corp/agents/*:v*` works because a hex digest contains no `v`, so digest refs cannot match) or list permitted digests explicitly.

### Network egress: `allowedCIDRs` vs. `allowedHosts`

`allowedCIDRs` is the portable primitive. It maps directly to `NetworkPolicy.egress.to.ipBlock.cidr`, which every CNI implementing Kubernetes NetworkPolicy supports.

`allowedHosts` (DNS names) cannot be expressed in standard `NetworkPolicy`. It requires a CNI with FQDN egress policies: Cilium (via `CiliumNetworkPolicy`) or Calico Enterprise. The AgentClassReconciler detects the cluster CNI on startup; if `allowedHosts` is set but no supported FQDN-policy CRD is present, a `Warning` event is emitted and `allowedHosts` is ignored.

Prefer `allowedCIDRs` for egress governance; layer `allowedHosts` on top only when the CNI supports it.

### `allowedProviders` is one gate in a chain, and only in the full lifecycle tier

`allowedProviders` is the access control mechanism for LLM providers at the class level, but it is not the whole story in either direction.

For a full-lifecycle Agent or AgentTask, the gateway enforces a chain, not a pair. Three tenancy layers must all admit the request: the workload's own `spec.providers`, this class's `allowedProviders`, and the target `ModelProvider.allowedNamespaces`. A fourth check, that the requested model exists in `ModelProvider.spec.models`, is a model-resolution prerequisite rather than a tenancy boundary, and it applies regardless.

In the gateway-only tier the class layer **disappears entirely**. Those callers are not Agents and reference no AgentClass, so there is nothing for `allowedProviders` to gate; `ModelProvider.allowedNamespaces` is the only tenancy check they face. Platform teams who need class-scoped provider policy must onboard workloads through the full lifecycle tier.

Since v0.4.0 the same chain exists for tools, gate for gate: the workload's `spec.tools`, this class's `allowedToolProviders` (rule 37), and the target `ToolProvider.allowedNamespaces` (rule 36), with the per-tool narrowing and catalog check (rule 38) layered on top. See [The Tool Plane](../gateways/tool-plane.md#grants).

See [Provider access gating](../concepts/tenancy-and-tiers.md#provider-access-gating) for the enforced chain end to end, including which gate produces which error, and [Provider Routing](../gateways/llm/provider-routing.md) for how the gateway resolves each layer.

### Defaults vs. maxLimits

Defaults are applied at reconcile time if the Agent does not specify a value. `maxLimits` are enforced regardless, and reject manifests that exceed them. A class can therefore be generous by default while still holding a hard ceiling.

### `imagePullSecrets` namespace resolution

AgentClass is cluster-scoped, but `imagePullSecrets[*].name` references a Secret, and Secrets live in namespaces. The reconciler resolves each entry in the **Agent's (or AgentTask's) namespace** at Pod-creation time, not in `kaalm-system`. Secrets are never copied across namespaces.

If any referenced Secret is missing from the target namespace, the Agent enters `Ready=False, reason=ImagePullSecretMissing` with a message naming the namespace and secret, and the Pod is not created. See rule 23 in [Cross-Resource Validation](validation-and-defaulting.md#cross-resource-validation) and the reconcile step in [AgentReconciler](../controller/reconcilers.md#agentreconciler) / [AgentTaskReconciler](../controller/reconcilers.md#agenttaskreconciler).

### `runtime.backend` is locked to `pod` in v1

`runtime.backend` only accepts `pod` in v1. The `agentSandbox` value (which creates Agent Sandbox `Sandbox` CRs instead of raw Pods) is deferred to v1.1.

The CRD schema enforces this with an enum on the `runtime.backend` field (`+kubebuilder:validation:Enum=pod`). Invalid values are therefore rejected at apply time rather than surfaced as a reconcile error, which gives the author immediate feedback, and adding `agentSandbox` later is an additive enum change on the frozen v1beta1 schema. See [Integration Points](../concepts/system-architecture.md#integration-points) for the planned integration design.
