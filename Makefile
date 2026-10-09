HARNESS_IMAGE  ?= agentops:dev
SLACKBOT_IMAGE ?= agentops-slackbot:dev
SIDECAR_IMAGE  ?= agentops-sidecar:dev
HARNESS        ?= claude-code
AGENT_IMAGE    ?= $(HARNESS):dev

MODULES        ?= harness slackbot
CLIENT_MODULES ?= $(filter-out harness,$(MODULES))

GOTOOL         ?= cd harness && GOWORK=off go tool
CONTROLLER_GEN ?= $(GOTOOL) controller-gen
SQLC           ?= GOWORK=off go tool sqlc
BUF            ?= $(GOTOOL) buf

HELM          ?= helm
CHARTS_DIR    ?= deploy/charts
PLATFORM_CHART ?= $(CHARTS_DIR)/agentops
SLACKBOT_CHART ?= $(CHARTS_DIR)/agentops-slackbot

APISTUB_BIN ?= $(CURDIR)/bin/apistub
BUF_BIN     ?= $(CURDIR)/bin/buf

.PHONY: build test test-e2e vet fmt-check boundary telemetry-in-sync generate pb-generate proto-lint proto-check \
        sdk-generate sdk-generate-ts sdk-generate-py sdk-generate-check sdk-test sdk-test-ts sdk-test-py \
        tidy docker-build harness-image slackbot-image harness-build sidecar-build run apistub \
        helm-sync-crds helm-lint helm-template helm-check-client-isolation helm-package

## build: compile every package in every module.
build:
	@out=$$(mktemp -d); trap 'rm -rf "$$out"' EXIT; \
	for m in $(MODULES); do echo "== $$m"; \
	  (cd $$m && GOWORK=off go build -o "$$out/" ./...) || exit 1; \
	done

## test: run every module's tests.
test:
	@for m in $(MODULES); do echo "== $$m"; (cd $$m && GOWORK=off go test ./...) || exit 1; done

## test-e2e: run the opt-in end-to-end tests (Docker; ARGS="-run ..." selects a suite).
test-e2e:
	cd harness && AGENTOPS_E2E=1 GOWORK=off go test -tags e2e -timeout 40m ./internal/e2e/... $(ARGS)

## vet: run go vet over every module.
vet:
	@for m in $(MODULES); do echo "== $$m"; (cd $$m && GOWORK=off go vet ./...) || exit 1; done

## fmt-check: fail if a Go file in any module is not gofmt-clean.
fmt-check:
	@bad=$$(gofmt -l $(MODULES)) || exit 1; \
	if [ -n "$$bad" ]; then echo "not gofmt-clean:"; echo "$$bad"; exit 1; fi; \
	echo "every Go file is gofmt-clean"

## boundary: fail if a client module imports anything in harness/ except harness/api.
boundary:
	@for m in $(CLIENT_MODULES); do \
	  deps=$$(cd $$m && GOWORK=off go list -deps -test ./...) || exit 1; \
	  bad=$$(echo "$$deps" | grep '^github.com/pomerium/agentops/harness/' \
	    | grep -vE '^github.com/pomerium/agentops/harness/api(/|$$)' || true); \
	  if [ -n "$$bad" ]; then \
	    echo "$$m reaches past harness/api:"; echo "$$bad"; exit 1; \
	  fi; \
	  echo "$$m depends only on harness/api"; \
	done

## telemetry-in-sync: fail if the two copies of the telemetry package differ.
telemetry-in-sync:
	@a=harness/internal/telemetry; b=slackbot/internal/telemetry; \
	tmp=$$(mktemp -d); trap 'rm -rf "$$tmp"' EXIT; \
	norm() { sed -e 's#slackbot/internal/telemetry#TELEMETRY#' -e 's#harness/internal/telemetry#TELEMETRY#' "$$1"; }; \
	for f in component.go component_test.go; do \
	  norm $$a/$$f > $$tmp/a; norm $$b/$$f > $$tmp/b; \
	  if ! diff -q $$tmp/a $$tmp/b >/dev/null; then \
	    echo "$$a/$$f and $$b/$$f have drifted"; exit 1; \
	  fi; \
	done; \
	echo "the two telemetry copies are identical"

## generate: regenerate the CRDs, the sqlc bindings, the protobuf code and both SDKs.
generate:
	$(CONTROLLER_GEN) object:headerFile=/dev/null paths=./apis/...
	$(CONTROLLER_GEN) crd paths=./apis/... output:crd:artifacts:config=../config/crd/bases
	cd harness/internal/sessionstore/sqlite && $(SQLC) generate
	$(MAKE) pb-generate
	$(MAKE) sdk-generate
	$(MAKE) helm-sync-crds

PB_OUT ?=

## pb-generate: regenerate the Go protobuf code from proto/.
pb-generate:
	$(BUF) generate $(if $(PB_OUT),-o $(PB_OUT)/go)
	$(BUF) generate --template buf.gen.connect.yaml $(if $(PB_OUT),-o $(PB_OUT)/go)

## helm-sync-crds: copy the generated CRDs into the platform chart.
helm-sync-crds:
	@{ \
	  echo '{{- if .Values.installCRDs }}'; \
	  awk '{ print } /^  annotations:$$/ { print "    helm.sh/resource-policy: keep" }' config/crd/bases/*.yaml; \
	  echo '{{- end }}'; \
	} > $(PLATFORM_CHART)/templates/crds.yaml
	@echo "wrote $(PLATFORM_CHART)/templates/crds.yaml"

## proto-lint: lint the protobuf sources.
proto-lint:
	$(BUF) lint ../proto

## proto-check: fail if the committed Go protobuf code or sqlc bindings are stale.
proto-check: proto-lint
	@tmp=$$(mktemp -d); trap 'rm -rf "$$tmp"' EXIT; set -e; \
	  $(MAKE) --no-print-directory pb-generate PB_OUT="$$tmp" >/dev/null; \
	  for d in $$(cd "$$tmp/go" && find . -type f -exec dirname {} \; | sort -u); do \
	    diff -r "$$tmp/go/$$d" "harness/$$d" || { echo "harness/$$d is stale; run make generate"; exit 1; }; \
	  done; \
	  (cd harness/internal/sessionstore/sqlite && $(SQLC) diff) || { echo "the sqlc bindings are stale; run make generate"; exit 1; }; \
	  echo "the Go protobuf code and the sqlc bindings match their sources"

## sdk-generate-check: fail if either SDK's generated code is stale.
sdk-generate-check:
	@tmp=$$(mktemp -d); trap 'rm -rf "$$tmp"' EXIT; set -e; \
	  $(MAKE) --no-print-directory sdk-generate PB_OUT="$$tmp" >/dev/null; \
	  diff -r "$$tmp/ts/src/gen" sdk/ts/src/gen || { echo "sdk/ts/src/gen is stale; run make sdk-generate"; exit 1; }; \
	  diff -r -x __pycache__ -x __init__.py "$$tmp/py/src/agentops_harness/gen" sdk/python/src/agentops_harness/gen \
	    || { echo "sdk/python/src/agentops_harness/gen is stale; run make sdk-generate"; exit 1; }; \
	  echo "both SDKs' protobuf code matches the protos"

## tidy: run go mod tidy in every module.
tidy:
	@for m in $(MODULES); do echo "== $$m"; (cd $$m && GOWORK=off go mod tidy) || exit 1; done

## docker-build: build the platform and Slack bot images.
docker-build: harness-image slackbot-image

## harness-image: build the platform image.
harness-image:
	docker build -f Dockerfile.harness -t $(HARNESS_IMAGE) .

## slackbot-image: build the Slack bot image.
slackbot-image:
	docker build -f Dockerfile.slackbot -t $(SLACKBOT_IMAGE) .

## harness-build: build an agent harness image from deploy/harness/$(HARNESS).
harness-build:
	docker build -t $(AGENT_IMAGE) -f deploy/harness/$(HARNESS)/Dockerfile deploy/harness

## sidecar-build: build the sandbox sidecar image.
sidecar-build:
	docker build -f Dockerfile.sidecar -t $(SIDECAR_IMAGE) .

## run: run the harness from source.
run:
	cd harness && GOWORK=off go run ./cmd/harness

## apistub: build the Harness API conformance server.
apistub:
	cd harness && GOWORK=off go build -o $(APISTUB_BIN) ./cmd/apistub

## sdk-generate: regenerate both SDKs' protobuf code.
sdk-generate: sdk-generate-ts sdk-generate-py

## sdk-generate-ts: regenerate the TypeScript SDK's protobuf code.
sdk-generate-ts:
	npm --prefix sdk/ts install --no-audit --no-fund
	npm --prefix sdk/ts run generate $(if $(PB_OUT),-- -o $(PB_OUT)/ts)

## sdk-generate-py: regenerate the Python SDK's protobuf code.
sdk-generate-py: $(BUF_BIN)
	cd sdk/python && uv sync --quiet --frozen
	cd sdk/python && PATH="$(CURDIR)/sdk/python/.venv/bin:$$PATH" $(BUF_BIN) generate $(if $(PB_OUT),-o $(PB_OUT)/py)

$(BUF_BIN):
	cd harness && GOWORK=off go build -o $(BUF_BIN) github.com/bufbuild/buf/cmd/buf

## sdk-test: run both SDK conformance suites against the stub.
sdk-test: sdk-test-ts sdk-test-py

## sdk-test-ts: run the TypeScript conformance suite.
sdk-test-ts: apistub
	npm --prefix sdk/ts install --no-audit --no-fund
	APISTUB_BIN=$(APISTUB_BIN) npm --prefix sdk/ts test

## sdk-test-py: run the Python conformance suite.
sdk-test-py: apistub
	cd sdk/python && APISTUB_BIN=$(APISTUB_BIN) uv run --frozen pytest -q

HELM_PLATFORM_VALUES ?= \
  --set config.agentic.asURL=https://agentic.example.com \
  --set config.harness.externalURL=https://harness.example.com \
  --set config.harness.assertionIssuer=harness.example.com \
  --set config.harness.api.assertionIssuer=harness-api.example.com

HELM_SLACKBOT_VALUES ?= \
  --set slack.signingSecret=test \
  --set slack.botToken=test \
  --set harnessAPI.url=https://harness-api.example.com

HELM_EXTRA_VALUES ?=

## helm-lint: lint both charts.
helm-lint:
	$(HELM) lint $(PLATFORM_CHART) $(HELM_PLATFORM_VALUES)
	$(HELM) lint $(SLACKBOT_CHART) $(HELM_SLACKBOT_VALUES)

## helm-template: render both charts with the minimum required values.
helm-template:
	$(HELM) template agentops $(PLATFORM_CHART) --namespace agentops-system $(HELM_PLATFORM_VALUES) $(HELM_EXTRA_VALUES)
	$(HELM) template agentops-slackbot $(SLACKBOT_CHART) --namespace agentops-slackbot $(HELM_SLACKBOT_VALUES)

## helm-check-client-isolation: fail if the Slack bot chart grants RBAC, mounts an AS token or claims a volume.
helm-check-client-isolation:
	@bot=$$($(HELM) template agentops-slackbot $(SLACKBOT_CHART) $(HELM_SLACKBOT_VALUES)) || exit 1; \
	rbac=$$($(HELM) template agentops $(PLATFORM_CHART) $(HELM_PLATFORM_VALUES) -s templates/rbac.yaml) || exit 1; \
	if echo "$$rbac" | grep -q 'slackbot'; then \
	  echo "the platform's RBAC names the Slack bot"; exit 1; \
	fi; \
	if echo "$$bot" | grep -qE 'kind: (Role|RoleBinding|ClusterRole|ClusterRoleBinding)'; then \
	  echo "the Slack bot chart renders RBAC"; exit 1; \
	fi; \
	if echo "$$bot" | grep -qE 'name: agentic-token|/var/run/agentic'; then \
	  echo "the Slack bot mounts an authorization-server token"; exit 1; \
	fi; \
	if echo "$$bot" | grep -qE 'persistentVolumeClaim|volumeClaimTemplates'; then \
	  echo "the Slack bot claims a volume"; exit 1; \
	fi; \
	echo "the Slack bot has no RBAC, no AS token volume and no volume claim"

## helm-package: package both charts into .tgz files.
helm-package: helm-lint
	$(HELM) package $(PLATFORM_CHART)
	$(HELM) package $(SLACKBOT_CHART)
