# The local test loop on the OrbStack cluster that the agentops quickstart
# installed (DEVELOPING.md, "In-cluster dev loop").
NS := sandbox-egress
POMERIUM_NS := agentops-pomerium
CA := $(shell mkcert -CAROOT)/rootCA.pem
POMERIUM_PROXY_IP = $(shell kubectl -n $(POMERIUM_NS) get svc pomerium-proxy -o jsonpath='{.spec.clusterIP}')
PY := ../../python

.PHONY: deploy undeploy token test-local image sandbox-image test-in-cluster

sandbox-image:
	docker build -t python-egress-sandbox:dev ../../sandbox/image

deploy: sandbox-image
	kubectl create namespace $(NS) --dry-run=client -o yaml | kubectl apply -f -
	kubectl -n $(NS) create secret generic pomerium-ca --from-file=ca.crt="$(CA)" --dry-run=client -o yaml | kubectl apply -f -
	kubectl kustomize . | sed 's/POMERIUM_PROXY_IP/$(POMERIUM_PROXY_IP)/' | kubectl apply -f -

undeploy:
	kubectl kustomize . | sed 's/POMERIUM_PROXY_IP/$(POMERIUM_PROXY_IP)/' | kubectl delete --ignore-not-found -f -

# A token for the agent's ServiceAccount, minted by the API server: the local
# stand-in for the projected token a pod gets.
token:
	@kubectl -n $(NS) create token agent --audience pomerium-egress --duration 1h

# The frameworks are mutually exclusive uv extras (see python/pyproject.toml),
# so each test module runs in its own environment. FRAMEWORKS= narrows it.
FRAMEWORKS ?= sdk deepagents openai smolagents adk pydantic-ai
test-local:
	@set -e; for fw in $(FRAMEWORKS); do \
	  extra=""; [ "$$fw" = sdk ] || extra="--extra $$fw"; \
	  echo "== $$fw"; \
	  (cd $(PY) && SANDBOX_API_URL=https://sandbox.localhost.pomerium.io \
	    POMERIUM_CA_CERT="$(CA)" \
	    POMERIUM_TOKEN="$$(kubectl -n $(NS) create token agent --audience pomerium-egress --duration 1h)" \
	    SANDBOX_NAMESPACE=$(NS) SANDBOX_WARMPOOL=python-egress \
	    uv run --extra test $$extra pytest -q -rs tests/test_$${fw//-/_}.py $(PYTEST_ARGS)); \
	done

image:
	docker build -t pomerium-egress-agent:dev $(PY)

test-in-cluster: image
	kubectl -n $(NS) delete job agent-tests --ignore-not-found
	kubectl kustomize . | sed 's/POMERIUM_PROXY_IP/$(POMERIUM_PROXY_IP)/' | kubectl apply -f -
	kubectl -n $(NS) wait --for=condition=complete --timeout=600s job/agent-tests || (kubectl -n $(NS) logs job/agent-tests; exit 1)
	kubectl -n $(NS) logs job/agent-tests
