# Request Handling

Every LLM call an agent makes travels through the LLM Gateway. The agent never talks to Anthropic, OpenAI, or Vertex directly: it talks to the gateway using the provider's own native API format, and the gateway authenticates the caller, authorizes the model, injects the real provider credential, forwards upstream, and accounts for the tokens spent.

This page walks the request path end to end, then covers the three mechanisms it depends on: how a model name identifies a provider, how the gateway works out which API format it is looking at, and how streaming responses are relayed and metered.

## Request Flow

![Sequence diagram of one LLM call through the gateway. The agent container makes an HTTPS request to $KAALM_GATEWAY_ENDPOINT using the provider's native path and a qualified providerRef/modelId name; bodies over maxLLMRequestBodyBytes are rejected with 413 request_too_large at the listener, before namespace identification. The gateway authenticates first, via mTLS client-cert SAN or a TokenReview-verified bearer token, and only then cross-checks the Pod at the request's source IP against its informer cache, a note marking this as defense in depth rather than the identity mechanism. It then resolves the Pod's ownerRef to the Agent and reads spec.providers, validates the model against ModelProvider.models and the namespace against allowedNamespaces, applies the budget check (degrade rewrites the model name, block returns an error), and applies the per-namespace-per-model token-bucket rate limit. Step 7 reads the provider credential from a Secret in kaalm-system; a highlighted note states the forwarded-header contract, which strips Authorization, x-api-key and api-key before injecting the credential, drops hop-by-hop headers per RFC 7230 section 6.1, and pins Accept-Encoding to identity because a relayed gzip would make usage extraction unreadable and zero all spend. The request is forwarded with the provider prefix stripped, falling back on failure. A second note marks that usage extraction happens on the return leg, naming the per-provider usage fields, before the gateway updates its in-process spend counter.](../../diagrams/llm-request-flow.svg)

Reading the diagram: two orderings carry the weight. Authentication precedes the source-IP cross-check (step 2), and the credential strip precedes the credential injection (step 7). Both are reversible-looking steps whose reversal would be a security bug, which is why they are drawn as separate arrows rather than folded together.

1. **Agent sends request.** The agent container makes an HTTPS request to `$KAALM_GATEWAY_ENDPOINT`, which resolves to the gateway Service in `kaalm-system`. The agent uses the upstream provider's native API path (for example `/v1/messages` for Anthropic, `/v1/chat/completions` for OpenAI-compatible) and includes a qualified model name in the request body (see [Model Identification](#model-identification) below). Request bodies above `gateway.maxLLMRequestBodyBytes` (Helm-configurable, default 4 MiB) are rejected with `413 request_too_large` at the listener, before namespace identification. This is the same defense-in-depth pattern as the User Gateway's `gateway.maxMessageBodyBytes` cap on `:8080`: reject oversized bodies before spending any work on them. See [LLM Gateway Error Responses](../api/errors.md#llm-gateway-error-responses) for the wire contract.

2. **Namespace identification.** The gateway authenticates the request first, via mTLS client-cert SAN (Mode 1) or a `TokenReview`-verified bearer token (Mode 2), to establish the caller's namespace. It then cross-checks that the Pod at the request's source IP, looked up in its Pod informer cache, is in that namespace. Authentication establishes the claim; the source-IP check confirms the claim came from where it should have. See [Namespace Identification](workload-identity.md) for both auth modes and the cross-check.

3. **Provider routing.** The gateway resolves the Pod's ownerRef to the Agent resource, reads `spec.providers` to determine which ModelProviders this agent is allowed to use, and parses the `provider/model` name from the request to identify the target ModelProvider. See [Provider Routing](provider-routing.md).

4. **Gateway validates.** It checks the namespace allowlist first, then the model. The caller's namespace must appear in the target ModelProvider's `allowedNamespaces`, or the request is rejected with `403 access_denied`. Only then does it confirm the requested model is listed in `spec.models`, rejecting with `400 invalid_request` if not. The tenancy check runs before the existence check so that a namespace not authorized to use a provider never learns which models it hosts. See [Provider Routing](provider-routing.md) for the full ordered chain.

5. **Budget check.** The gateway reads the current budget state for the agent's namespace. If a `degrade` policy applies, it rewrites the model name in the request. If `block` applies, it returns an error to the agent. See [Budget State Management](budgets-and-rate-limits.md#budget-state-management).

6. **Rate limit check.** A per-(namespace, model) token-bucket rate limiter is applied on requests/min and tokens/min. See [Rate Limiting](budgets-and-rate-limits.md#rate-limiting).

7. **Route to upstream.** The gateway attaches the provider credential, read directly from Secrets in `kaalm-system` (a static API-key header for Anthropic/OpenAI-style providers, an OAuth2 access token for Google Vertex; see [Credential Handling](provider-routing.md#credential-handling)), strips the provider prefix from the model name, and forwards the request under a strict **forwarded-header contract** (detailed below). If the upstream fails (connection error, 5xx, timeout), the gateway walks the fallback chain, trying the primary provider's `spec.fallback` entries, then each fallback's own fallback, up to `maxFallbackDepth` (default 3). See [Fallback Logic](fallback.md).

8. **Response returned.** The gateway relays the response to the agent container. For streaming responses (SSE), the gateway transparently relays each chunk as it arrives. See [Streaming Responses](#streaming-responses) below.

9. **Token counting.** The gateway extracts actual token usage from the provider response: `usage.input_tokens` / `usage.output_tokens` for Anthropic, `usage.prompt_tokens` / `usage.completion_tokens` for OpenAI, `usageMetadata.promptTokenCount` / `usageMetadata.candidatesTokenCount` for Google Vertex. For streaming responses, token usage is extracted from the usage-bearing SSE events instead (see [Streaming Responses](#streaming-responses)). Actual usage from the provider response is always preferred over pre-call estimation.

10. **Spend update.** The gateway updates the in-process spend counter for the namespace.

### The forwarded-header contract

Step 7 is the point where an agent's request becomes the gateway's request. Three header rules apply, and each exists for a concrete reason.

**All inbound authentication material is stripped before the provider credential is injected**: `Authorization`, `x-api-key`, and `api-key`. Injection alone does not displace them, because the header names differ per provider and per tier: Anthropic authenticates via `x-api-key`, while a gateway-only-tier caller arrives with `Authorization: Bearer <SA-token>`. Injecting the Anthropic key would leave the bearer token untouched. Without the explicit strip, a live audience-bound Kubernetes credential would be forwarded verbatim into third-party provider logs.

**Hop-by-hop headers are removed** per RFC 7230 §6.1: `Connection`, `TE`, `Upgrade`, and `Proxy-Authorization`. These are scoped to a single connection and must not be relayed across a proxy hop.

**`Accept-Encoding` is pinned to `identity`** so upstream response bodies arrive uncompressed. Go's transport only auto-decompresses gzip it negotiated itself, so relaying a caller's `Accept-Encoding: gzip` would make every response body opaque to usage extraction and silently zero all spend accounting. Pinning the header is what keeps step 9 able to read the response at all.

## Model Identification

Agents identify both the provider and the model in each LLM request using a **qualified model name** format: `{providerRef}/{modelId}`.

Examples:
- `anthropic-shared/claude-opus-4-6`: Claude Opus via the `anthropic-shared` ModelProvider
- `anthropic-shared/claude-sonnet-4-6`: Claude Sonnet via the same provider
- `openai-fallback/gpt-4o`: GPT-4o via the `openai-fallback` ModelProvider
- `local-vllm/llama-3-70b`: Llama 3 70B via a local vLLM instance registered as a ModelProvider

The gateway splits the model name on the **first** `/`: the prefix identifies the ModelProvider by `metadata.name`, and the suffix is the raw model ID that must appear in the ModelProvider's `models` list. Before forwarding upstream, the gateway strips the provider prefix and sends only the raw model ID, so the upstream Anthropic API receives `claude-opus-4-6`, not `anthropic-shared/claude-opus-4-6`.

This format uniquely identifies the (provider, model) pair and eliminates ambiguity when multiple ModelProviders offer models with similar names, for example a managed Anthropic endpoint and an OpenAI-compatible proxy both serving Claude models. The agent is always responsible for constructing the qualified `provider/model` name in its API calls.

The qualified name travels in the request body's `model` field for every served format. See [Request Format Detection](#request-format-detection).

## Request Format Detection

The agent sends LLM requests using the upstream provider's native API format. The gateway detects the request format from the **URL path** the agent uses:

- `/v1/messages` -> Anthropic format
- `/v1/chat/completions` -> OpenAI / OpenAI-compatible format (also used by vLLM, Ollama, LiteLLM)
- `/v1/completions` -> OpenAI legacy completions format

These three paths are the whole served inbound surface: any other path on the cluster listener is rejected with `400 invalid_request`. The gateway uses the detected format to parse the request (extracting the model name and other fields), then forwards to the upstream provider.

### The google-vertex type is reserved

The ModelProvider API accepts `spec.type: google-vertex`, but the type is not servable in this release: no Vertex-format inbound path is routed on the cluster listener, cross-format fallback cannot translate into it ([rule 12](../../resources/validation-and-defaulting.md#cross-resource-validation) keeps `google-vertex` chains same-type), and the gateway does not mint the OAuth2 tokens the platform requires in place of static API keys. The adapter's outbound pieces, usage extraction, URL-path model rewriting, and the `?alt=sse` streaming fixup, stay in the code for the finished feature.

Google has retired the Vertex AI brand: the platform is now the Gemini Enterprise Agent Platform, with the wire API carried forward unchanged, so `google-vertex` remains the stable enum value. Full support is a backlog item on the [roadmap](../../ROADMAP.md#beyond).

The gateway is **protocol-aware** in that it understands request/response shapes for supported provider types, which it needs for token extraction, model name parsing, and similar work. It translates between formats in one situation only: a fallback candidate of a different `spec.type` (since v0.7.0), where the request is rewritten into the candidate's format before the first byte and the response, streaming or not, is rewritten back; see [Crossing formats](fallback.md#crossing-formats) for the pairs, the matrix, and what cannot cross. Everywhere else the request path is passthrough: the primary is always spoken to in the caller's format, and same-type fallbacks are too.

## Streaming Responses

Most LLM usage involves streaming responses (Server-Sent Events, SSE), where the provider sends token-by-token output as a stream of chunks. The gateway supports streaming transparently.

![Sequence diagram of an SSE stream relayed through the gateway, using Anthropic's event names. Before forwarding, a note records the two adapter fixups: OpenAI-format streaming requests get stream_options include_usage injected when absent, since otherwise no usage is emitted at all, and Vertex :streamGenerateContent requests get ?alt=sse appended when absent, since otherwise Vertex returns a JSON-array stream and the SSE relay never engages. A divider marks that before the first chunk fallback is available, covering connection errors, timeouts before the first byte, and error responses returned before streaming begins. The provider then sends message_start carrying input_tokens, which the gateway relays. A second divider marks the first relayed chunk as the point of no return. Content chunks are relayed immediately in a loop with no buffering. If the stream completes, message_delta carries the cumulative output_tokens and message_stop carries no usage, and the gateway updates spend after the stream completes; a note records that a stream ending without usage metadata counts as zero spend and is logged at warning level. If the stream fails mid-way after the first chunk, the gateway closes the agent's SSE stream with an error event and does not fall back or retry, because the agent has already received partial output. A closing note records that budget is checked pre-call only, with no mid-stream enforcement.](../../diagrams/llm-streaming.svg)

Reading the diagram: the two dividers are the whole point. Everything above the second one is recoverable, because the agent has seen nothing yet. Everything below it is not, because the agent has already consumed bytes that a fallback provider would not have produced.

**Relay model.** The gateway acts as a pass-through proxy for SSE streams. When the upstream provider begins sending a streaming response (`Content-Type: text/event-stream`), the gateway relays each SSE chunk to the agent as it arrives. The gateway does not buffer the full response; chunks are forwarded immediately to preserve the low-latency benefit of streaming.

**Token counting.** The gateway inspects each SSE chunk as it relays it, accumulating usage metadata where the provider's stream format carries it. Per provider:

- **Anthropic**: `input_tokens` arrive on the `message_start` event and the cumulative `output_tokens` on the final `message_delta` event. `message_stop` carries no usage.
- **OpenAI-compatible**: a usage object appears in the final chunk preceding the `[DONE]` sentinel, but only when the request sets `stream_options: {"include_usage": true}`. The gateway therefore **injects that field into OpenAI-format streaming requests when absent**. The addition is backward-compatible: the extra terminal usage chunk has an empty `choices` array, which OpenAI client libraries tolerate, and it is relayed to the agent unchanged.
- **Google Vertex**: the adapter **appends `?alt=sse` to `:streamGenerateContent` requests when absent**, because Vertex otherwise returns a JSON-array stream rather than SSE, which would never engage the SSE relay. Usage arrives as `usageMetadata` (`promptTokenCount` / `candidatesTokenCount`) on the final streamed chunk.

The gateway extracts this data and updates spend counters after the stream completes, the same as step 9 in the non-streaming flow. A stream that ends without usage metadata (a misbehaving upstream) is counted as zero spend and logged at warning level. Under soft enforcement that is an accepted approximation; under [hard enforcement](budgets-and-rate-limits.md#hard-enforcement) it is named in the guarantee's fine print, because no gateway-side cap can meter spend the provider never reports. The log signal keeps it visible to operators either way.

**Budget checks.** Budget checks occur pre-call (step 5) using the last-known spend state, the same as non-streaming requests. No mid-stream budget enforcement is performed: once a stream has started, it runs to completion. This is the correct behavior, because aborting a stream mid-response would leave the agent with a partial, unusable response while still incurring provider charges for the full generation.

**Mid-stream failures.** If the upstream provider connection drops or errors mid-stream (after the first chunk has been relayed to the agent), the gateway closes the agent's SSE stream with an error event and does **not** attempt fallback. A partially-consumed stream cannot be retried: the agent has already received partial output, and replaying the request on a fallback provider would produce a different, potentially contradictory continuation. Fallback only applies to **pre-stream failures**: connection errors, timeouts before the first chunk, and error responses returned before streaming begins. See [Fallback Triggers](fallback.md#fallback-triggers).

**Provider adapter.** The `ProviderAdapter.ForwardRequest` method handles both streaming and non-streaming modes. The adapter detects streaming from the upstream response headers (`Content-Type: text/event-stream`, or `Transfer-Encoding: chunked` with SSE content) and returns a streaming reader that the gateway relays to the agent. Token extraction is adapter-specific: each adapter knows where usage metadata appears in its provider's SSE format. See [Provider Adapters](provider-routing.md#provider-adapters).
