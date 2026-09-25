# Makefile for agentops.
#
# Maintainer note: on some machines a stale GOROOT in the environment can
# break the toolchain; if so, prefix invocations with `env -u GOROOT`
# (e.g. `env -u GOROOT make build`). That workaround is intentionally NOT
# baked into the recipes here.

# Local image tags (no registry — built straight into the local Docker/OrbStack
# image store and consumed via imagePullPolicy: IfNotPresent).
IMAGE         ?= agentops:dev
HARNESS_IMAGE ?= agentops-agent:dev
SIDECAR_IMAGE ?= agentops-sidecar:dev

# controller-gen, sqlc, and buf are declared as `tool` directives in go.mod,
# so we run them via `go tool` to pin the exact versions the module depends on.
CONTROLLER_GEN ?= go tool controller-gen
SQLC           ?= go tool sqlc
BUF            ?= go tool buf

# The harness module (harness/): the client-facing Harness API. A separate Go
# module, so every target that walks packages visits it too; GOWORK=off keeps a
# local go.work from resolving one module against the other.
HARNESS_DIR ?= harness
HARNESS_GO  ?= cd $(HARNESS_DIR) && GOWORK=off go

# Kustomize overlay to render/apply: dev or prod.
OVERLAY ?= dev

# Helm chart published as an OCI artifact by .github/workflows/helm.yaml.
HELM      ?= helm
CHART_DIR ?= deploy/helm

.PHONY: build test test-e2e vet generate generate-harness-api vocab vocab-check proto-lint tidy docker-build harness-build sidecar-build run kustomize deploy \
        helm-sync-crds helm-lint helm-template helm-package

## build: compile all packages.
build:
	go build ./...
	$(HARNESS_GO) build ./...

## test: run the test suite.
test:
	go test ./...
	$(HARNESS_GO) test ./...

## test-e2e: run the opt-in end-to-end harness tests (needs Docker + an
## Anthropic key in ANTHROPIC_API_KEY or ~/tmp/keys/claude_api_key.txt). These
## build the harness image and talk to the real Anthropic API and a public MCP
## server, so they're excluded from the default `test` target.
test-e2e:
	AGENTOPS_E2E=1 go test -tags e2e -timeout 15m ./internal/e2e/... $(ARGS)

## vet: run go vet over all packages.
vet:
	go vet ./...
	$(HARNESS_GO) vet ./...

## generate: regenerate deepcopy methods + CRD manifests (controller-gen),
## the sqlc query bindings (run from internal/chatops/db/, per its sqlc.yaml),
## the sidecar control protocol stubs (buf, from proto/sidecar), and the Harness
## API's string vocabularies and event payload shapes (vocab, which also fills
## the value lists in the .proto's comments), then its Connect stubs
## (generate-harness-api), so those comments reach the generated code.
generate: vocab generate-harness-api
	$(CONTROLLER_GEN) object:headerFile=/dev/null paths=./api/...
	$(CONTROLLER_GEN) crd paths=./api/... output:crd:artifacts:config=config/crd/bases
	cd internal/chatops/db && $(SQLC) generate
	$(BUF) generate
	$(MAKE) helm-sync-crds

## generate-harness-api: regenerate the Harness API's Go and Connect stubs
## (harness/api/pb) from proto/harnessapi, with the harness module's own
## pinned buf and plugins.
generate-harness-api:
	$(HARNESS_GO) tool buf generate --template buf.gen.connect.yaml

## vocab: regenerate the Harness API's string vocabularies and event payload
## shapes (harness/api, and the tables in docs/clients.md) from
## proto/vocabulary.yaml and proto/payloads.yaml.
vocab:
	$(HARNESS_GO) run ./cmd/vocabgen -root ..

## vocab-check: assert the committed artifacts still say what the sources say.
##
## The strings and the shapes are each one declaration with several audiences,
## and the failure mode this catches is the quiet one: a value or a field added
## to the Go side and to nothing else, which no test notices because every
## language still agrees with itself.
vocab-check:
	$(HARNESS_GO) run ./cmd/vocabgen -root .. -check

## helm-sync-crds: copy the generated CRDs into the Helm chart, wrapped in an
## `installCRDs` toggle. Kept in sync via `generate`; the kustomize base reads
## config/crd directly, while Helm needs its own templated copy.
helm-sync-crds:
	@{ \
	  echo '{{- if .Values.installCRDs }}'; \
	  echo '# AUTO-GENERATED — do not edit by hand.'; \
	  echo '# Synced from config/crd/bases/ by `make helm-sync-crds` (run after `make generate`).'; \
	  cat config/crd/bases/*.yaml; \
	  echo '{{- end }}'; \
	} > $(CHART_DIR)/templates/crds.yaml
	@echo "wrote $(CHART_DIR)/templates/crds.yaml"

## proto-lint: lint the protobuf sources.
proto-lint:
	cd proto && $(BUF) lint

## tidy: prune and verify go.mod / go.sum.
tidy:
	go mod tidy
	$(HARNESS_GO) mod tidy

## docker-build: build the app container image (local tag).
docker-build:
	docker build -t $(IMAGE) .

## harness-build: build a local agent harness image (HARNESS=claude-code|demo|...,
## one per deploy/harness/<agent> folder; context stays at deploy/harness so the
## shared git-checkout.sh is reachable).
HARNESS ?= claude-code
harness-build:
	docker build -t $(HARNESS_IMAGE) -f deploy/harness/$(HARNESS)/Dockerfile deploy/harness

## sidecar-build: build the sandbox sidecar proxy image (sidecar binary + envoy).
sidecar-build:
	docker build -f Dockerfile.sidecar -t $(SIDECAR_IMAGE) .

## run: build and run the binary locally.
run:
	go run ./cmd/agentops

## kustomize: render the Kustomize overlay to stdout (OVERLAY=dev|prod).
kustomize:
	kubectl kustomize deploy/overlays/$(OVERLAY)

## deploy: apply the Kustomize overlay to the current cluster (OVERLAY=dev|prod).
## Note: create the agentops-secrets Secret out-of-band first.
deploy:
	kubectl apply -k deploy/overlays/$(OVERLAY)

## helm-lint: lint the Helm chart.
helm-lint:
	$(HELM) lint $(CHART_DIR)

## helm-template: render the Helm chart to stdout with the minimum required values.
helm-template:
	$(HELM) template agentops $(CHART_DIR) \
	  --set slack.signingSecret=test \
	  --set slack.botToken=test \
	  --set config.oauthRedirectBaseURL=https://agentops.example.com

## helm-package: package the chart into a .tgz (CI pushes it to the OCI registry).
helm-package: helm-lint
	$(HELM) package $(CHART_DIR)
