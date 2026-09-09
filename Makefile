# Image URL to use all building/pushing image targets
IMG ?= controller:latest

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	$(CONTROLLER_GEN) rbac:roleName=manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: test
test: manifests generate fmt vet setup-envtest ## Run tests.
	@rm -f cover.out
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" go test -count=1 $$(go list ./... | grep -v /e2e) -coverprofile cover.out

# Minimum acceptable project-wide statement coverage (the union across all
# non-e2e packages, so cross-package tests get credit). Overridable: make
# cover-check COVERAGE_THRESHOLD=90
COVERAGE_THRESHOLD ?= 85

.PHONY: cover-check
# GOWORK=off: the gate measures the operator module exactly as its release
# builds compile it (module mode, its own go.sum). Workspace mode changes how
# -coverpkg attributes cross-package coverage (the union drops from ~87% to a
# false ~75% with identical tests). The agentruntime module has its own suite
# (make runtime-test).
cover-check: manifests generate fmt vet setup-envtest ## Run tests with union coverage and fail below COVERAGE_THRESHOLD%.
	@# Two stale-profile hazards deflate the union by many points, both from
	@# ranges of OLDER file revisions reading as uncovered: a pre-existing
	@# cover.out accumulates sections across runs (hence the rm), and cached
	@# test results from unchanged packages replay coverage blocks carrying
	@# old line geometry for -coverpkg dependencies that DID change (hence
	@# -count=1, which forces every package to re-run and re-instrument).
	@rm -f cover.out
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" \
		GOWORK=off go test -count=1 $$(GOWORK=off go list ./... | grep -v /e2e) \
		-coverpkg=$$(GOWORK=off go list ./... | grep -v /e2e | paste -sd,) -coverprofile cover.out
	@total=$$(go tool cover -func=cover.out | awk '/^total:/{print $$3}' | tr -d '%'); \
	awk -v t="$$total" -v thr="$(COVERAGE_THRESHOLD)" 'BEGIN{ \
		printf "total project coverage: %s%% (gate: %s%%)\n", t, thr; \
		if (t+0 < thr+0) { printf "::error::total coverage %s%% is below the %s%% gate\n", t, thr; exit 1 } \
		printf "coverage gate passed\n" }'

.PHONY: lint
lint: golangci-lint ## Run golangci-lint on every workspace module.
	$(GOLANGCI_LINT) run
	cd agentruntime && $(GOLANGCI_LINT) run
	cd examples/starter-go && $(GOLANGCI_LINT) run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint with fixes on every workspace module.
	$(GOLANGCI_LINT) run --fix
	cd agentruntime && $(GOLANGCI_LINT) run --fix
	cd examples/starter-go && $(GOLANGCI_LINT) run --fix

.PHONY: lint-config
lint-config: golangci-lint ## Verify golangci-lint linter configuration
	$(GOLANGCI_LINT) config verify

.PHONY: audit-drift
audit-drift: ## Report which #139 audit verdicts need a re-walk (hack/audit/manifest.txt).
	hack/audit/drift-check.sh

##@ Build

.PHONY: build
build: manifests generate fmt vet ## Build manager, gateway, and console binaries.
	go build -o bin/manager ./cmd/manager
	go build -o bin/gateway ./cmd/gateway
	go build -o bin/console ./cmd/console

.PHONY: run
run: manifests generate fmt vet ## Run the controller from your host.
	go run ./cmd/manager

.PHONY: run-gateway
run-gateway: ## Run the gateway from your host.
	go run ./cmd/gateway

# If you wish to build the manager image targeting other platforms you can use the --platform flag.
# (i.e. docker build --platform linux/arm64). However, you must enable docker buildKit for it.
# More info: https://docs.docker.com/develop/develop-images/build_enhancements/
.PHONY: docker-build
docker-build: ## Build docker image with the manager.
	$(CONTAINER_TOOL) build -t ${IMG} .

.PHONY: docker-push
docker-push: ## Push docker image with the manager.
	$(CONTAINER_TOOL) push ${IMG}

# PLATFORMS defines the target platforms for the manager image be built to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - be able to use docker buildx. More info: https://docs.docker.com/build/buildx/
# - have enabled BuildKit. More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image to your registry (i.e. if you do not set a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To adequately provide solutions that are compatible with multiple platforms, you should consider using this option.
PLATFORMS ?= linux/arm64,linux/amd64,linux/s390x,linux/ppc64le
.PHONY: docker-buildx
docker-buildx: ## Build and push docker image for the manager for cross-platform support
	# copy existing Dockerfile and insert --platform=${BUILDPLATFORM} into Dockerfile.cross, and preserve the original Dockerfile
	sed -e '1 s/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/; t' -e ' 1,// s//FROM --platform=\$$\{BUILDPLATFORM\}/' Dockerfile > Dockerfile.cross
	- $(CONTAINER_TOOL) buildx create --name kbinit-builder
	$(CONTAINER_TOOL) buildx use kbinit-builder
	- $(CONTAINER_TOOL) buildx build --push --platform=$(PLATFORMS) --tag ${IMG} -f Dockerfile.cross .
	- $(CONTAINER_TOOL) buildx rm kbinit-builder
	rm Dockerfile.cross

.PHONY: build-installer
build-installer: manifests generate kustomize ## Generate a consolidated YAML with CRDs and deployment.
	mkdir -p dist
	cd config/manager && $(KUSTOMIZE) edit set image controller=${IMG}
	$(KUSTOMIZE) build config/default > dist/install.yaml

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) apply -f -

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) delete --ignore-not-found=$(ignore-not-found) -f -

.PHONY: deploy
deploy: manifests kustomize ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	cd config/manager && $(KUSTOMIZE) edit set image controller=${IMG}
	$(KUSTOMIZE) build config/default | $(KUBECTL) apply -f -

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUSTOMIZE) build config/default | $(KUBECTL) delete --ignore-not-found=$(ignore-not-found) -f -

##@ Kaalm

CHART_DIR ?= charts/kaalm

.PHONY: chart-sync
chart-sync: manifests kustomize ## Sync generated CRDs (with the conversion stanza) and controller RBAC into the Helm chart.
	rm -f $(CHART_DIR)/crds/*.yaml
	$(KUSTOMIZE) build config/crd | go run ./hack/splitcrds $(CHART_DIR)/crds
	@mkdir -p $(CHART_DIR)/files
	@awk '/^rules:/{f=1;next} f' config/rbac/role.yaml > $(CHART_DIR)/files/controller-rules.yaml
	@echo "synced $$(ls $(CHART_DIR)/crds/*.yaml | wc -l) CRDs and the controller RBAC into $(CHART_DIR)/"

.PHONY: chart-lint
chart-lint: chart-sync ## Lint the Helm chart (after syncing CRDs).
	helm lint $(CHART_DIR)

.PHONY: chart-package
chart-package: chart-sync ## Package the chart into dist/ (VERSION defaults to Chart.yaml appVersion). Mirrors the release workflow.
	@V=$${VERSION:-$(CHART_APP_VERSION)}; mkdir -p dist; \
		helm package $(CHART_DIR) --version $$V --app-version $$V --destination dist

.PHONY: books
books: ## Build all mdBooks: the design book (docs/), the user guide (guide/), and the tutorial (learn/).
	mdbook build docs
	mdbook build guide
	mdbook build learn

.PHONY: k3d-up
k3d-up: ## Create a local k3d cluster with cert-manager and trust-manager for e2e.
	hack/k3d-up.sh

.PHONY: k3d-down
k3d-down: ## Delete the local k3d cluster.
	k3d cluster delete kaalm-dev

CLUSTER ?= kaalm-dev
CHART_APP_VERSION := $(shell grep '^appVersion:' charts/kaalm/Chart.yaml | awk '{print $$2}' | tr -d '"')
CONTROLLER_IMG ?= ghcr.io/win07xp/kaalm-controller:$(CHART_APP_VERSION)
GATEWAY_IMG ?= ghcr.io/win07xp/kaalm-gateway:$(CHART_APP_VERSION)
CONSOLE_IMG ?= ghcr.io/win07xp/kaalm-console:$(CHART_APP_VERSION)
AGENT_IMG ?= registry.test/agents/starter-go:e2e
MOCKPROVIDER_IMG ?= registry.test/mock/llm-provider:e2e
MOCKMCP_IMG ?= registry.test/mock/mcp-server:e2e
MOCKDISCORD_IMG ?= registry.test/mock/discord:e2e
MOCKWHATSAPP_IMG ?= registry.test/mock/whatsapp:e2e
# In-cluster names for the base images. The S16 spec references these, so the
# suite exercises the locally built images and nothing at test time can
# silently pull a published tag. The testdata YAML hardcodes them; change both
# together.
E2E_GO_BASE_IMG ?= registry.test/agents/kaalm-agent-go:e2e
E2E_PYTHON_BASE_IMG ?= registry.test/agents/kaalm-agent-python:e2e
# Preloaded so the NetworkPolicy-deny probe pod runs hermetically (no Docker Hub
# pull at test time, which would otherwise let that spec pass vacuously).
CURL_IMG ?= curlimages/curl:8.10.1
# The OTLP sink for the tracing e2e (S20); preloaded for the same hermeticity.
JAEGER_IMG ?= jaegertracing/all-in-one:1.62.0
PYTHON_AGENT_IMG ?= ghcr.io/win07xp/kaalm-agent-python:$(CHART_APP_VERSION)

.PHONY: python-test
python-test: ## Run the Python base image unit suite (the image's test stage; needs docker).
	docker build --target test images/agent-python

.PHONY: python-image
python-image: ## Build the kaalm-agent-python reference base image.
	docker build -t $(PYTHON_AGENT_IMG) images/agent-python

.PHONY: python-smoke
python-smoke: python-image ## Contract smoke against the built image: TLS, mTLS matrix, echo, mounted handler, fail-fast.
	hack/python-image-smoke.sh $(PYTHON_AGENT_IMG)

.PHONY: examples-smoke
examples-smoke: python-image ## Build the framework example images: FROM-rung examples against the local base, the task example standalone. Each Dockerfile import-checks its handler.
	docker build --build-arg BASE=$(PYTHON_AGENT_IMG) examples/langgraph-chat
	docker build --build-arg BASE=$(PYTHON_AGENT_IMG) examples/langgraph-tools
	docker build examples/langgraph-task

GO_AGENT_IMG ?= ghcr.io/win07xp/kaalm-agent-go:$(CHART_APP_VERSION)

.PHONY: runtime-test
runtime-test: ## Unit tests for the agentruntime module and the Go starter (native, -race).
	go test -race ./agentruntime/... ./examples/starter-go/...

.PHONY: go-agent-image
go-agent-image: ## Build the kaalm-agent-go base image (its test stage gates every build, so this also runs the suite).
	docker build -t $(GO_AGENT_IMG) -f images/agent-go/Dockerfile .

.PHONY: go-agent-smoke
go-agent-smoke: go-agent-image ## Contract smoke against the built image: TLS, mTLS matrix, echo, dedup across replacement.
	hack/go-image-smoke.sh $(GO_AGENT_IMG)

.PHONY: e2e-images
e2e-images: ## Build the controller, gateway, console, agent, base, and mock-provider images and import them into k3d.
	docker build -t $(CONTROLLER_IMG) --build-arg BINARY=manager .
	docker build -t $(GATEWAY_IMG) --build-arg BINARY=gateway .
	docker build -t $(CONSOLE_IMG) --build-arg BINARY=console .
	docker build -t $(MOCKPROVIDER_IMG) -f test/e2e/mockprovider/Dockerfile .
	docker build -t $(MOCKMCP_IMG) -f test/e2e/mockmcp/Dockerfile .
	docker build -t $(MOCKDISCORD_IMG) -f test/e2e/mockdiscord/Dockerfile .
	docker build -t $(MOCKWHATSAPP_IMG) -f test/e2e/mockwhatsapp/Dockerfile .
	docker build -t $(GO_AGENT_IMG) -f images/agent-go/Dockerfile .
	docker build -t $(AGENT_IMG) -f test/e2e/starter-go/Dockerfile --build-arg BASE=$(GO_AGENT_IMG) .
	docker build -t $(PYTHON_AGENT_IMG) images/agent-python
	docker tag $(GO_AGENT_IMG) $(E2E_GO_BASE_IMG)
	docker tag $(PYTHON_AGENT_IMG) $(E2E_PYTHON_BASE_IMG)
	docker pull $(CURL_IMG)
	docker pull $(JAEGER_IMG) || docker image inspect $(JAEGER_IMG) >/dev/null
	CLUSTER=$(CLUSTER) hack/k3d-import.sh $(CONTROLLER_IMG) $(GATEWAY_IMG) $(CONSOLE_IMG) $(MOCKPROVIDER_IMG) $(MOCKMCP_IMG) $(MOCKDISCORD_IMG) $(MOCKWHATSAPP_IMG) $(AGENT_IMG) $(E2E_GO_BASE_IMG) $(E2E_PYTHON_BASE_IMG) $(CURL_IMG) $(JAEGER_IMG)

.PHONY: e2e-deploy
e2e-deploy: chart-sync ## Install/upgrade the chart onto the current context.
	# Jaeger first: the chart is installed exporting to it (S20), and an
	# absent sink would make the exporter log retries for the whole suite.
	kubectl apply -f test/e2e/testdata/tracing-jaeger.yaml
	helm upgrade --install kaalm charts/kaalm -n kaalm-system --create-namespace \
		--set certManager.clusterResourceNamespace=cert-manager \
		--set gateway.trustClusterCAForUpstream=true \
		--set gateway.trustClusterCAForCallbacks=true \
		--set 'gateway.callbackUrl.allowlist={mock-provider.e2e.svc}' \
		--set console.enabled=true \
		--set controller.trustClusterCAForProbes=true \
		--set gateway.tracing.otlpEndpoint=http://jaeger.tracing-e2e.svc:4318 \
		--set gateway.platforms.discord.apiBaseUrl=http://mock-discord.e2e.svc:8080/api/v10 \
		--set gateway.platforms.whatsapp.apiBaseUrl=http://mock-whatsapp.e2e.svc:8080 \
		--wait --timeout 5m

.PHONY: dashboards-verify
dashboards-verify: ## Verify config/grafana against the live e2e cluster (run after make e2e): throwaway Prometheus+Grafana, provisioning, every panel query.
	hack/dashboards-verify.sh

# The released chart the upgrade e2e starts from. Release readiness (#115)
# bumps this to the newly released version after each release.
PREV_CHART_VERSION ?= 0.7.0

.PHONY: upgrade-images
upgrade-images: ## Build and import only what the upgrade e2e needs locally: the controller and gateway. The pre-upgrade world runs published $(PREV_CHART_VERSION) artifacts, and the upgrade recreates no workload Pods.
	docker build -t $(CONTROLLER_IMG) --build-arg BINARY=manager .
	docker build -t $(GATEWAY_IMG) --build-arg BINARY=gateway .
	CLUSTER=$(CLUSTER) hack/k3d-import.sh $(CONTROLLER_IMG) $(GATEWAY_IMG)

.PHONY: e2e-upgrade
e2e-upgrade: chart-sync ## One-shot S21 upgrade e2e: fresh cluster, install the released $(PREV_CHART_VERSION) chart, upgrade to the local build, assert nothing lost.
	-k3d cluster delete $(CLUSTER)
	hack/k3d-up.sh
	$(MAKE) upgrade-images
	UPGRADE_PREV_VERSION=$(PREV_CHART_VERSION) go test ./test/upgrade/... -tags upgrade -v -timeout 30m

.PHONY: e2e
e2e: ## One-shot k3d e2e: recreate the cluster, build+import images, install the chart, run the suite.
	# Always start from a fresh cluster. A long-lived k3d cluster stops
	# enforcing NetworkPolicies after enough churn, which fails the deny probe
	# and, worse, makes every allow-path assertion pass vacuously. The reuse
	# path in k3d-up.sh stays available for the inner loop (make e2e-deploy).
	-k3d cluster delete $(CLUSTER)
	hack/k3d-up.sh
	$(MAKE) e2e-images
	$(MAKE) e2e-deploy
	# -count=1 defeats go's test cache: the suite drives a live cluster the
	# cache knows nothing about, and a replayed transcript is not a gate.
	go test ./test/e2e/... -tags e2e -v -timeout 20m -count=1

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## Tool Binaries
KUBECTL ?= kubectl
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint

## Tool Versions
KUSTOMIZE_VERSION ?= v5.6.0
CONTROLLER_TOOLS_VERSION ?= v0.17.2
#ENVTEST_VERSION is the version of controller-runtime release branch to fetch the envtest setup script (i.e. release-0.20)
ENVTEST_VERSION ?= $(shell go list -m -f "{{ .Version }}" sigs.k8s.io/controller-runtime | awk -F'[v.]' '{printf "release-%d.%d", $$2, $$3}')
#ENVTEST_K8S_VERSION is the version of Kubernetes to use for setting up ENVTEST binaries (i.e. 1.31)
ENVTEST_K8S_VERSION ?= $(shell go list -m -f "{{ .Version }}" k8s.io/api | awk -F'[v.]' '{printf "1.%d", $$3}')
GOLANGCI_LINT_VERSION ?= v2.12.2

.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: setup-envtest
setup-envtest: envtest ## Download the binaries required for ENVTEST in the local bin directory.
	@echo "Setting up envtest binaries for Kubernetes version $(ENVTEST_K8S_VERSION)..."
	@$(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path || { \
		echo "Error: Failed to set up envtest binaries for version $(ENVTEST_K8S_VERSION)."; \
		exit 1; \
	}

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f $(1) || true ;\
GOBIN=$(LOCALBIN) go install $${package} ;\
mv $(1) $(1)-$(3) ;\
} ;\
ln -sf $(1)-$(3) $(1)
endef
