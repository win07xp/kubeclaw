# Credential Handling

Kaalm handles three kinds of long-lived secret material: **LLM API keys**, which authenticate Kaalm to model providers, **tool server credentials** (since v0.4.0), which authenticate Kaalm's broker to MCP tool servers, and **channel credentials**, which authenticate inbound webhook callers to Kaalm and sign Kaalm's outbound callbacks. Their homes are deliberate. LLM keys and tool credentials live in one cluster-wide location and are never copied; channel credentials live in each agent's own namespace.

One rule spans all three: **agent containers never hold credential material.** The gateway is the only component that uses credentials on the data path, and it is a separate Pod in `kaalm-system`. That separation is what makes the isolation enforceable with nothing more exotic than a Kubernetes NetworkPolicy, covered in [Protecting Agent Containers from LLM Provider Access](#protecting-agent-containers-from-llm-provider-access).

TLS certificate material (agent serving certs, AgentTask client certs, and the CA trust chain) follows a separate lifecycle managed by cert-manager. See [In-cluster TLS (Bidirectional)](tls.md#in-cluster-tls).

## The Credential Map

One figure for the whole economy: every credential class, who mints it, who holds it, who verifies it, and what it unlocks. The lifecycle sections that follow and the figures on [TLS](tls.md) and [Workload Identity](../gateways/llm/workload-identity.md) are the deep dives.

![A component diagram mapping every credential class across four frames. A minters frame holds the platform engineer, cert-manager with trust-manager, and the kubelet. The platform engineer creates two Secrets in kaalm-system, the provider API key and the tool credential, and both flow to the gateway watch-based and never copied. cert-manager mints the per-workload leaf certificate inside the Agent or AgentTask Pod, 90 days renewed 30 early, loaded and reloaded on rotation by kaalm.gateway and the kaalm.http_client factories, which present it to the gateway over mTLS where the SAN names the workload; a note records that this verified identity unlocks provider and tool grants, the budget, and the audit name. trust-manager projects the public kaalm-ca ConfigMap into the namespace, which is how every caller knows it reached the real gateway. The kubelet projects a kaalm-gateway-audience ServiceAccount token into a tier-1 workload, whose bearer path through TokenReview yields namespace identity only. A framework SDK's placeholder api_key is drawn dashed and dies at the gateway, stripped. The gateway node verifies SAN plus source IP, TokenReview, and session ownership, strips inbound auth headers, mints workload-bound MCP session ids, and holds keys in process memory only. The only red edges leave the gateway for the LLM provider and the MCP tool server, injecting the API key and the tool credential, the only hop either ever travels.](../diagrams/credential-map.svg)

**Reading the diagram.** The frames are the argument. The only credential material inside a team namespace is identity: the workload's own leaf certificate and the public CA bundle it verifies the gateway with. Everything with spending power sits in `kaalm-system` and crosses exactly one hop, in red, on the gateway's upstream leg. And identity is what policy keys off: the verified SAN (or, for the gateway-only tier, the TokenReview namespace) selects the grants, the budget, and the audit name, which is why the base images' `kaalm.gateway` client and the `kaalm.http_client()` factories exist to present it, and follow its rotation, without handler code.

## Lifecycle of an LLM API Key

1. **Stored**: in a Secret in `kaalm-system` (for example `kaalm-system/anthropic-api-key`), created and managed by platform engineers.
2. **Referenced**: by `ModelProvider.spec.credentialsRef`. Read access is limited to two ServiceAccounts: the **gateway** (which uses the key on every proxied request) and the **operator** (which validates that the Secret exists and is well-formed, and uses the key for the ModelProviderReconciler's provider health probes, since `GET /v1/models` requires authentication; see [ModelProviderReconciler](../controller/reconcilers.md#modelproviderreconciler) step 2).
3. **Loaded**: the gateway reads the Secret on first use and then follows it through a Kubernetes watch (a single-object watch per referenced Secret, because the gateway holds `get` and `watch` but never `list`). Credentials are held in the gateway process memory, and transiently in the operator for the duration of a health probe. The operator does not retain key material between probes.
4. **Used**: the gateway injects the API key into upstream requests on behalf of agent containers. Agent containers never have access to the credential. They do not have the Secret mounted and cannot reach `kaalm-system` Secrets through the Kubernetes API.
5. **Rotated**: when the source Secret is updated, the gateway's Secret watch picks up the change and refreshes in-memory credentials without a restart.
6. **Never copied**: there are no per-agent or per-namespace copies of LLM credentials. The source Secret in `kaalm-system` is the single authoritative location.

Step 6 is the reason rotation is a single Secret update: with no fan-out copies, there is exactly one place to change and one watch to fire.

![A sequence diagram of the six-step LLM API key lifecycle, with a user namespace holding the Agent Pod and a kaalm-system frame holding the Secret, the operator ServiceAccount and the gateway ServiceAccount, and the LLM provider outside the cluster. A platform engineer creates the Secret in kaalm-system; exactly two ServiceAccounts may read it, both through a namespaced Role. The gateway loads it at startup and holds it in process memory; the operator reads it once per health probe and retains nothing between probes. On the used step the agent sends an LLM request carrying no credential, the gateway injects the API key on the upstream leg only, and the completion returns to the agent still carrying no credential; the agent cannot read kaalm-system Secrets at all. Rotation is a single in-place Secret update that fires the gateway's watch.](../diagrams/llm-key-lifecycle.svg)

**Reading the diagram.** The namespace frames are the argument. No credential-bearing arrow ever crosses out of `kaalm-system`: the key reaches the provider on the gateway's upstream leg and nowhere else. Read the three holders by retention, too, since they differ: the gateway keeps the key in memory for the life of the process, the operator only for the length of one probe, and the agent never at all.

## Lifecycle of a Tool Server Credential (since v0.4.0)

The tool credential is the LLM key's shape applied to the [tool plane](../gateways/tool-plane.md), and every property of that lifecycle transfers:

1. **Stored**: in a Secret in `kaalm-system`, created by platform engineers.
2. **Referenced**: by `ToolProvider.spec.credentialsRef`, resolved only from the operator namespace. The reference is optional: a nil `credentialsRef` is an unauthenticated tool server, and the broker injects nothing.
3. **Read**: by the same two ServiceAccounts as an LLM key. The gateway resolves it per brokered call through its cache and injects it as a bearer token on the upstream leg; the operator reads it transiently for the ToolProvider health probe and for `Ready` validation, retaining nothing.
4. **Used**: only on calls the broker admits. A call denied by the namespace gate, the grant chain, or session ownership dies before the credential is touched, so the credential is never spent authenticating a request policy already rejected.
5. **Rotated**: a single Secret update; the cache read means the next brokered call carries the new value. A credential the tool server rejects surfaces as `503 tool_unavailable` to the caller and a Warning event on the ToolProvider, never as an error inside an agent.
6. **Never copied**: no per-agent or per-namespace copies, nothing mounted into any pod.

Step 4 is the property the agent-visible half of the plane rests on: an agent's `spec.tools` grant names the ToolProvider, but the credential behind it has no path into the agent's namespace at all.

## Lifecycle of a Channel Credential (AgentChannel)

1. **Stored**: in a Secret in the agent's namespace (for example `team-support/discord-bot-credentials`), created by the platform team or a provisioning service.
2. **Referenced**: by the AgentChannel's webhook auth config: `spec.webhook.auth.secretRef` (inbound, bearer), `spec.webhook.auth.hmac.secretRef` (inbound, HMAC), and/or `spec.webhook.callbackAuth.secretRef` / `.hmac.secretRef` (outbound callback signing, required when `spec.webhook.callbackUrl` is set; see [rule 25](../resources/validation-and-defaulting.md#cross-resource-validation)). A platform channel (since v0.7.0) references one Secret through `spec.discord.credentialsRef` or `spec.whatsapp.credentialsRef`, whose keys are fixed by the type ([rule 40](../resources/validation-and-defaulting.md#cross-resource-validation)).
3. **Loaded**: the gateway watches `AgentChannel` resources directly. When it sees a new or updated AgentChannel, it reads the referenced Secret(s) from the agent's namespace using its scoped RBAC and holds them in-process: inbound `auth` material for the webhook adapter's verifier, outbound `callbackAuth` material for the adapter's `SendReply` signer. A platform channel's one `credentialsRef` Secret splits the same way: the Discord public key or the WhatsApp app secret and verify token back the inbound verifier, and the WhatsApp access token or the Discord bot token back `SendReply`. The operator ServiceAccount also has a parallel scoped read path on the same Secret(s) through a dynamic per-channel Role (see [Operator ServiceAccount](rbac.md#operator-serviceaccount)), used solely by the AgentChannelReconciler to validate that the configured `data` key exists; the operator does not retain credential material in memory.
4. **Rotated**: same watch-based mechanism as LLM credentials. The gateway watches the referenced Secret for changes and refreshes in-memory credentials without a restart.

Channel credentials are namespace-scoped for organizational isolation: each namespace contains only the credentials for its own agents' channels. They are created by the platform team or a provisioning service; developers do not need Secret access in their namespace.

![A sequence diagram of the channel credential lifecycle, drawn in the same grammar as the LLM API key figure but with the Secret sitting inside the agent's own namespace rather than kaalm-system, and a second namespace holding its own separate Secret. The AgentChannel's webhook auth config names the Secrets, and the AgentChannelReconciler mints two resourceNames-scoped Roles in the agent's namespace, one granting the gateway get and watch and one granting the operator get and watch, both owned by the AgentChannel and torn down with it. The gateway holds the material in process, split by direction: the inbound auth material feeds the webhook adapter's verifier and the outbound callbackAuth material feeds its SendReply signer. The operator reads the same Secret only to validate that the configured data key exists and retains nothing.](../diagrams/channel-credential-lifecycle.svg)

**Reading the diagram.** It is deliberately the mirror of [the LLM API key figure](#lifecycle-of-an-llm-api-key): same participants, same layout, one structural difference. The Secret has moved out of `kaalm-system` and into the agent's namespace, and it fans out one per namespace. Every other contrast follows from that move, including why the grant has to be minted per channel and garbage-collected with the AgentChannel instead of shipping as a plain namespaced Role. One Secret feeds both directions: the same object backs the inbound verifier and the outbound `SendReply` signer.

## Protecting Agent Containers from LLM Provider Access

Because the gateway is a separate Pod in `kaalm-system`, NetworkPolicy can cleanly enforce agent isolation without any per-container workarounds:

```yaml
# NetworkPolicy for agent Pods
policyTypes:
  - Ingress
  - Egress
ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: kaalm-system
      podSelector:
        matchLabels:
          app.kubernetes.io/name: kaalm-gateway
    ports:
      - port: 8080      # Agent HTTPS health/message port ($KAALM_HEALTH_PORT): gateway→agent channel message delivery
        protocol: TCP
egress:
  - to:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: kaalm-system
      podSelector:
        matchLabels:
          app.kubernetes.io/name: kaalm-gateway
    ports:
      - port: 8443      # All agent→gateway TLS traffic (LLM calls, heartbeats, task completion)
        protocol: TCP
  - to:                    # DNS, scoped to kube-dns in kube-system
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: kube-system
      podSelector:
        matchLabels:
          k8s-app: kube-dns
    ports:
      - port: 53
        protocol: UDP
      - port: 53
        protocol: TCP
```

Agent containers that attempt to call LLM providers directly are blocked at the NetworkPolicy level. No service mesh or L7-capable CNI is required for this guarantee: standard Kubernetes NetworkPolicy is sufficient because the enforcement is cross-Pod. Both gateway listeners serve TLS using the same `kaalm-gateway-tls` certificate: the LLM Gateway on port 8443 and the User Gateway on port 8080. External webhook traffic arrives through an Ingress configured for backend re-encrypt or TLS pass-through, so there is no plaintext hop anywhere in the data path.

### The DNS egress rule

The DNS egress rule in the preceding NetworkPolicy is scoped to `kubernetes.io/metadata.name: kube-system` + `k8s-app: kube-dns`, which matches the upstream kube-dns/CoreDNS labelling used by kubeadm, EKS, GKE, AKS, and the standard CoreDNS chart. Clusters whose DNS Pod uses a different namespace or label set (custom CoreDNS chart, NodeLocal DNSCache only) must override the selector. The reconciler exposes this as the Helm value [`controller.networkPolicy.dnsSelector`](../operations/deployment.md#helm-chart-contents) (an object with `namespaceLabels` and `podLabels` keys) on the synthesized per-agent NetworkPolicy.

The narrow scoping is deliberate: an untrusted agent must not be able to reach arbitrary Pods on port 53. The previous `namespaceSelector: {}` rule allowed exactly that and is no longer acceptable.
