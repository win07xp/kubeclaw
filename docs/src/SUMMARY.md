# Summary

[Introduction](introduction.md)
[Implementation Roadmap](ROADMAP.md)

# Orientation

- [Vision and Scope](concepts/vision-and-scope.md)
- [Core Concepts](concepts/core-concepts.md)
- [Personas and the Primary Scenario](concepts/personas.md)
- [System Architecture](concepts/system-architecture.md)
- [Multi-Tenancy and Adoption Tiers](concepts/tenancy-and-tiers.md)

# Resource Model

- [Resource Overview](resources/overview.md)
- [AgentClass](resources/agentclass.md)
- [ModelProvider](resources/modelprovider.md)
- [ToolProvider](resources/toolprovider.md)
- [Agent](resources/agent.md)
- [AgentTask](resources/agenttask.md)
- [AgentChannel](resources/agentchannel.md)
- [Validation and Defaulting](resources/validation-and-defaulting.md)

# Agent Runtime

- [The Runtime Contract](runtime/contract.md)
- [Reference Base Images](runtime/base-images.md)
- [Starter Templates](runtime/starter-templates.md)
- [Child Resources](runtime/child-resources.md)

# The Gateways

- [Gateway Overview](gateways/overview.md)
  - [Cluster Listener TLS](gateways/listener-tls.md)
- [HTTP API](gateways/api/overview.md)
  - [Channel Webhook](gateways/api/channel-webhook.md)
  - [Discord Channel](gateways/api/channel-discord.md)
  - [WhatsApp Channel](gateways/api/channel-whatsapp.md)
  - [Task Completion](gateways/api/task-complete.md)
  - [Agent Endpoints](gateways/api/agent-endpoints.md)
  - [Async Webhook Responses](gateways/api/async-responses.md)
  - [Internal Endpoints](gateways/api/internal-endpoints.md)
  - [Error Reference](gateways/api/errors.md)
- [LLM Gateway](gateways/llm/overview.md)
  - [Request Handling](gateways/llm/request-handling.md)
  - [Workload Identity](gateways/llm/workload-identity.md)
  - [Provider Routing and Adapters](gateways/llm/provider-routing.md)
  - [Budgets and Rate Limits](gateways/llm/budgets-and-rate-limits.md)
  - [Fallback Logic](gateways/llm/fallback.md)
  - [LLM Gateway Operations](gateways/llm/operations.md)
- [The Tool Plane](gateways/tool-plane.md)
- [User Gateway](gateways/user/overview.md)
  - [Platform Adapters and Channel Health](gateways/user/platform-adapters.md)
  - [Activation and Activity Tracking](gateways/user/activation-and-activity.md)
  - [User Gateway Operations](gateways/user/operations.md)

# The Controller

- [Operator Structure](controller/overview.md)
- [Reconcilers](controller/reconcilers.md)
- [Agent Lifecycle](controller/agent-lifecycle.md)
- [Hibernation and Wake](controller/hibernation-and-wake.md)
- [Change Propagation](controller/change-propagation.md)
- [AgentTask Lifecycle](controller/task-lifecycle.md)
- [Finalizers](controller/finalizers.md)
- [Errors, Events, and Testing](controller/operations.md)

# The Console

- [Console Overview](console/overview.md)

# Security

- [Security Model and Isolation](security/model.md)
- [RBAC and Authentication](security/rbac.md)
- [Credential Handling](security/credentials.md)
- [TLS and Certificates](security/tls.md)
- [Threat Model](security/threat-model.md)

# Operations

- [Deployment](operations/deployment.md)
- [API Versioning and Deprecation](operations/api-versioning.md)
- [Observability](operations/observability.md)
- [Load and Scale](operations/load-and-scale.md)

---

- [Acceptance Scenarios](appendix/scenarios.md)
- [Scenario Coverage](appendix/scenario-coverage.md)
- [Lifecycles at a Glance](appendix/lifecycles.md)
