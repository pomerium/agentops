# Developing agentops

This file is for people who change the code. For what AgentOps does and how to
install it, see [README.md](./README.md).

## What is in the repository

| Path | What it is |
|---|---|
| `harness/` | The Go module `github.com/pomerium/agentops/harness`: the platform, the two sandbox binaries, and `harness/api`, the package that clients import. |
| `slackbot/` | The Go module `github.com/pomerium/agentops/slackbot`: the Slack bot, a client of the Harness API. |
| `proto/` | The protobuf contracts. [`harnessapi/v1`](./proto/harnessapi/v1/harnessapi.proto) is the Harness API that clients call. [`agentlink/v1`](./proto/agentlink/v1/agentlink.proto) is the Agent Link between the platform and a sandbox sidecar. [`runner/v1`](./proto/runner/v1/runner.proto) is the pod-local API between the sidecar and the agent runner. |
| `sdk/ts`, `sdk/python` | The [TypeScript](./sdk/ts/README.md) and [Python](./sdk/python/README.md) clients of the Harness API. |
| `config/crd/bases` | The generated `AgentTemplate` and `ClientBinding` CRDs. |
| `deploy/` | The three Helm charts (the platform, the Slack bot, and the quickstart, which installs Pomerium, the platform and a demo agent), the quickstart scripts, the sandbox kustomize Component, example manifests, and the agent harness images. |
| `docs/` | Design notes: [run identity and the Agent Link](./docs/run-identity.md), [Slack threads](./docs/threads.md), [SDK conformance](./docs/sdk-conformance.md). |
| `examples/` | [`pomerium-egress-gateway`](./examples/pomerium-egress-gateway/README.md): the sidecar in workload mode on an agent-sandbox SandboxTemplate, Pomerium routes on both sides, and tests for five Python agent frameworks. Written in agent-sandbox's example style, to be offered upstream. |

The binaries:

| Binary | Source | Image | Role |
|---|---|---|---|
| `harness` | `harness/cmd/harness` | `pomerium/agentops` (`Dockerfile.harness`) | The platform. Serves the Harness API and the Agent Link, keeps the session store, creates SandboxClaims. |
| `sidecar` | `harness/cmd/sidecar` | `pomerium/agentops-sidecar` (`Dockerfile.sidecar`) | Runs in each sandbox pod. Gets the run token, runs envoy for the credential-injecting loopback listeners, dials out to the Agent Link and relays to the runner. |
| `agent-runner` | `harness/cmd/agent-runner` | `pomerium/agentops-sidecar` | The agent container's entrypoint. Starts the agent process and is its ACP client. |
| `slackbot` | `slackbot/cmd/slackbot` | `pomerium/agentops-slackbot` (`Dockerfile.slackbot`) | The Slack bot. |
| `apistub` | `harness/cmd/apistub` | none | The Harness API conformance server that the SDK tests run against. |

Each `main` builds its whole dependency graph in one function. Read the mains
first. The packages they wire:

| Package | Role |
|---|---|
| `harness/api` | The published client contract. `pb` holds the generated messages and Connect interfaces. The package itself holds the enum names, the error sentinels and their Connect codes. `client` is the Go client: bearer token, retries, error mapping and a resumable `Subscribe`. `server` holds the error interceptor and the subscription framing that every server implementation shares. |
| `harness/apis/v1alpha1` | The `AgentTemplate` and `ClientBinding` CRD types (`agents.pomerium.com/v1alpha1`). |
| `harness/internal/harnessapi` | The session core: create, approve, prompt, suspend and revive, sweeps, startup reconcile, the event log, client bindings and quotas. |
| `harness/internal/apiserver` | The Harness API over Connect on h2c: caller identity from the Pomerium assertion, `/healthz`, `/readyz`, per-verb metrics. |
| `harness/internal/agentlink` | The Agent Link gRPC server. It verifies the assertion Pomerium stamps on the route, matches it to the expected run, and carries the agent's I/O with byte-exact resume. |
| `harness/internal/agenticrun` | The client of Pomerium's agentic authorization server: create a run, read its status. |
| `harness/internal/sandbox` | Creates SandboxClaims, finds the pod, and opens or adopts a session over the Agent Link. |
| `harness/internal/agenttemplate` | Reads `AgentTemplate` and `ClientBinding` objects from a controller-runtime cache scoped to the pod's namespace. |
| `harness/internal/sessionstore`, `.../sqlite`, `.../storetest` | The store interface, its SQLite implementation (goose migrations, sqlc queries), and the shared store test suite. |
| `harness/internal/runner` | The agent runner: a gRPC server on a unix socket that starts `ACP_AGENT_CMD` and is its ACP client (`github.com/coder/acp-go-sdk`). |
| `harness/internal/sidecar/...` | The sidecar. `harnessclient` dials the Agent Link and relays to the runner. `agentic` keeps the run token (or the workload token) fresh. `envoyconfig` renders the envoy bootstrap and the token secret. `envparse` reads `SIDECAR_HTTP_*`. `server` runs envoy. |
| `harness/internal/pomeriumtls` | TLS and dialing for Pomerium routes: a private CA and an in-cluster dial address. |
| `harness/internal/apistub` | The scripted Harness API implementation behind `apistub`. |
| `harness/internal/config`, `slackbot/internal/config` | Each process's environment variables. |
| `harness/internal/telemetry`, `slackbot/internal/telemetry` | Two identical copies of the logging helper, one per module (`make telemetry-in-sync`). |
| `slackbot/internal/app` | The Slack client: one session per person per thread, the event stream rendered into the thread, the sweeper. |
| `slackbot/internal/gateway` | The HTTP endpoints (`/slack/events`, `/slack/interactivity`, `/healthz`), Slack signature verification, Block Kit. |
| `slackbot/internal/slackclient` | Outbound Slack Web API calls and rate limiting. Streaming updates coalesce, and a storm of them does not hold back lifecycle posts. |
| `slackbot/internal/channelmap` | The channel → `AgentTemplate` map, read from a mounted file and read again every 30 seconds. |
| `slackbot/internal/mdsplit` | Splits agent markdown into Slack-sized blocks. |

## Design invariants

Everything else depends on these. Do not break them casually.

**One replica.** The platform is a StatefulSet with `replicas: 1`. The session
store is SQLite on a PVC with one open connection (`SetMaxOpenConns(1)` in
`harness/internal/sessionstore/sqlite`). On startup,
`harnessapi.Service.ReconcileOnStartup` adopts each running session whose
sandbox is still there. It ends the others with `session_ended{interrupted}` on
their event log. Suspended sessions stay suspended.

**The platform knows nothing about Slack.** Running a session lives in
`harness/internal/harnessapi`. Showing one in Slack lives in
`slackbot/internal/app`. The `harness` module has no Slack dependency. The bot
reaches the platform only through the Harness API: it has no Kubernetes client,
no authorization-server token, no RBAC and no volume (`make boundary`,
`make helm-check-client-isolation`). If the bot needs something that is not a
verb or an event, the API is missing it.

**Events are the contract.** Each session has an append-only event log. The
first event has seq 1, and each next event has a seq one higher. The platform
takes seqs from a counter on the session row, so they survive suspend, revive
and a restart. A client resumes with `after_seq` set to the last seq it
received. New payload types are additive, and a client ignores a payload it does
not know. The SDK conformance suites test this
([`docs/sdk-conformance.md`](./docs/sdk-conformance.md)).

**Every client is registered.** The Harness API takes the caller's identity from
the assertion that Pomerium stamps on the Harness API route. A caller without a
matching `ClientBinding` is refused every verb. The binding's `templates` list is
all that the client may run, and its `quotas` apply when a session is created or
revived.

**Secrets never enter the agent container.**

- *The run token.* Only the `sidecar` container mounts the projected
  ServiceAccount token (audience `pomerium-agentic-as`). The sidecar exchanges it
  at the agentic authorization server for the run token, and envoy adds the run
  token to upstream requests. The agent reaches the LLM at
  `ANTHROPIC_BASE_URL=http://127.0.0.1:9999` and each MCP server at
  `http://127.0.0.1:91xx`, without credentials.
- *The LLM API key.* It is not in the pod. The Pomerium route for the LLM sets
  it. The agent's `ANTHROPIC_API_KEY` is a placeholder.
- *Git credentials.* A `secretKeyRef` on the template's `git-init` init
  container. [`deploy/harness/git-checkout.sh`](./deploy/harness/git-checkout.sh)
  gives the token to git through `GIT_ASKPASS`, never in argv. The init
  container exits before the agent starts.
- The pod sets `automountServiceAccountToken: false`.
  `harness/internal/sandbox/template_yaml_test.go` checks that the example
  templates give the agent container no `valueFrom` or `envFrom`.

**The runner is the ACP client, in the pod.** The platform does not exec into
pods: its RBAC grants `get` and `list` on pods and nothing more. The sidecar
dials out to the Agent Link through a Pomerium route. The runner starts
`/bin/sh -lc 'exec ${ACP_AGENT_CMD:-acp-agent}'` and speaks ACP to it over stdio.
The system prompt goes on ACP `session/new` as `_meta.systemPrompt.append`, not
in the environment. See [`docs/run-identity.md`](./docs/run-identity.md).

**A SandboxClaim carries no session data.** `BuildSandboxClaim` in
`harness/internal/sandbox/claim.go` sets the warm pool reference and labels,
and nothing else. The Sandbox's lease (`spec.shutdownTime`, `SANDBOX_LEASE`) is
the deadline. agent-sandbox skips the warm pool for a claim
that sets `env` or `volumeClaimTemplates`. So everything a pod needs before it
has a session belongs in the SandboxTemplate. The per-session configuration (the
MCP endpoints) arrives on the Agent Link attach stream.

**Container names are a contract.** The agent container is `agent`, the sidecar
is `sidecar`, and the checkout init container is `git-init` (constants in
`harness/internal/sandbox/claim.go`). The kustomize Component merges into
`agent` by name.

**TLS ends at Pomerium.** The Harness API and the Agent Link listen as plain
h2c; the admin listener (metrics) is plain HTTP, and the Service does not expose
it. Clients and sandboxes reach the platform only through Pomerium routes, and it
takes identity only from the verified `X-Pomerium-Jwt-Assertion`. The Agent Link route and the Harness API
route have different hosts, so they have different assertion issuers. The
platform refuses to start when `HARNESS_ASSERTION_ISSUER` equals
`HARNESS_API_ASSERTION_ISSUER`, because an assertion for one route would then be
accepted on the other. A startup line warns when the Agent Link listens on all
interfaces.

## Build, test, generate

The two modules share a `go.work` for editing. Every Makefile recipe and every
image build runs per module with `GOWORK=off`, so a build that only works inside
the workspace fails.

| Target | What it does |
|---|---|
| `make build` | `go build ./...` in each module. |
| `make test` | `go test ./...` in each module. The e2e suite does not run (see [End-to-end tests](#end-to-end-tests)). |
| `make vet` | `go vet ./...` in each module. |
| `make fmt-check` | Fails if a Go file in either module is not `gofmt`-clean. |
| `make boundary` | Fails if the Slack bot depends on a package under `harness/` outside `harness/api` (see [Two modules](#two-modules)). |
| `make telemetry-in-sync` | Fails if the two copies of the telemetry package differ. |
| `make generate` | Regenerates deepcopy methods and CRDs, the sqlc bindings, the Go protobuf code, both SDKs, and the chart's CRD copy. |
| `make pb-generate` | Regenerates the Go protobuf code from `proto/`. |
| `make proto-lint` | Lints `proto/` with buf. |
| `make proto-check` | Runs `proto-lint`, then fails if the committed Go protobuf code or sqlc bindings are stale. |
| `make sdk-generate` | Regenerates both SDKs' protobuf code (`sdk-generate-ts`, `sdk-generate-py`). |
| `make sdk-generate-check` | Fails if either SDK's generated code is stale. |
| `make sdk-test` | Builds `apistub` and runs both SDK conformance suites (`sdk-test-ts`, `sdk-test-py`). |
| `make apistub` | Builds `bin/apistub`. |
| `make tidy` | `go mod tidy` in each module. |
| `make run` | Runs the platform from source. It does not load `.env`. |
| `make test-e2e` | Runs the opt-in end-to-end tests. |
| `make docker-build` | Runs `harness-image` and `slackbot-image`. |
| `make harness-image` | Builds the platform image, `agentops:dev`. |
| `make slackbot-image` | Builds the Slack bot image, `agentops-slackbot:dev`. |
| `make sidecar-build` | Builds the sandbox sidecar image, `agentops-sidecar:dev`. |
| `make harness-build` | Builds an agent harness image from `deploy/harness/$(HARNESS)`, tagged `$(HARNESS):dev`. `HARNESS` defaults to `claude-code`. |
| `make helm-lint`, `helm-template`, `helm-check-client-isolation`, `helm-sync-crds`, `helm-package` | Chart work; see [Helm charts](#helm-charts). |

`HARNESS_IMAGE`, `SLACKBOT_IMAGE`, `SIDECAR_IMAGE` and `AGENT_IMAGE` override the
image tags.

CI runs `fmt-check`, `vet`, `boundary`, `telemetry-in-sync`, `proto-check`, `build` and
`test` in one job, and `sdk-generate-check`, `sdk-test-ts` and `sdk-test-py` in
another. A separate workflow runs `helm-lint`, `helm-template` (also with
persistence off) and `helm-check-client-isolation`.

On some machines a stale `GOROOT` export breaks the toolchain. Prefix the command
with `env -u GOROOT`, for example `env -u GOROOT make test`.

Generated code is committed. Run `make generate` after you change:

- `harness/apis/v1alpha1`: the deepcopy methods, the CRDs in `config/crd/bases`,
  and their copies in `deploy/charts/agentops/templates/crds.yaml` and
  `deploy/charts/agentops-quickstart/crds/`. Do not edit those by hand.
- `harness/internal/sessionstore/sqlite/query.sql` or `migrations/`: the sqlc
  bindings in `harness/internal/sessionstore/sqlite/sqlc`.
- `proto/agentlink` or `proto/runner`: the gRPC stubs in
  `harness/internal/agentlink/pb` and `harness/internal/runner/pb`.
- `proto/harnessapi`: the Connect stubs in `harness/api/pb` and both SDKs'
  generated code (`sdk/ts/src/gen`, `sdk/python/src/agentops_harness/gen`). The
  event payloads and every vocabulary (session states, reasons, end reasons,
  tool-call statuses, resolutions, error sentinels) are messages and enums in
  that one `.proto`, so every language names the same set.

controller-gen, sqlc, buf and the protoc plugins are `tool` directives in
`harness/go.mod`, run as `go tool <name>`. There is nothing to install, and the
module pins the versions. `make sdk-generate` and `make sdk-test` also need `npm`
and `uv`.

## Running locally

[`scripts/run-local.sh`](./scripts/run-local.sh) runs either process from
source. It loads `.env` from the repository root (plain `KEY=value` lines; the
file is gitignored), builds the process into `bin/` with `GOWORK=off`, and runs
it from the repository root, so relative paths in `.env` resolve there:

```sh
./scripts/run-local.sh            # the platform
./scripts/run-local.sh slackbot   # the Slack bot
```

The platform reads these variables
([`harness/internal/config/config.go`](./harness/internal/config/config.go)):

| Variable | Notes |
|---|---|
| `POD_NAMESPACE` | Required. The namespace it reads AgentTemplates and ClientBindings from and creates SandboxClaims in, for example `agentops-system`. |
| `DB_PATH` | Required. The SQLite file, for example `./agentops.db`. |
| `AGENTIC_AS_URL` | Required. An `https` URL: the Pomerium host of the agentic authorization server. |
| `HARNESS_EXTERNAL_URL` | Required. The Agent Link route. It must name the same route as the sandboxes' `SIDECAR_HARNESS_URL`. |
| `HARNESS_ASSERTION_ISSUER` | Required. The assertion issuer on the Agent Link route (its host). |
| `HARNESS_API_ASSERTION_ISSUER` | Required. The assertion issuer on the Harness API route. It must differ from `HARNESS_ASSERTION_ISSUER`. |
| `AGENTIC_TOKEN_FILE` | Default `/var/run/agentic/token`. Locally, point it at a file that holds a ServiceAccount token the authorization server accepts, for example the output of `kubectl -n agentops-system create token agentops --audience pomerium-agentic-as`. |
| `HARNESS_API_ADDR`, `HARNESS_GRPC_ADDR`, `HARNESS_ADMIN_ADDR` | Default `:8081`, `:8090` and `:9090`. |
| `LOG_LEVEL` | `debug`, `info` (default), `warn` or `error`. |

The Slack bot reads these
([`slackbot/internal/config/config.go`](./slackbot/internal/config/config.go)):

| Variable | Notes |
|---|---|
| `SLACK_SIGNING_SECRET` | Required. |
| `SLACK_BOT_TOKEN` | Required. |
| `HARNESS_API_URL` | Required. The Harness API route. |
| `HARNESS_API_TOKEN_FILE` | No default. When it is unset, the bot sends no bearer token. Locally, point it at a token for the bot's ServiceAccount, for example the output of `kubectl -n agentops-slackbot create token agentops-slackbot --audience pomerium-agentic-as`. The bot's ClientBinding then applies. |
| `SLACK_CHANNEL_MAP` | Default `/etc/agentops/channels.yaml`. Point it at a local file. With no file at the path, no channel starts a session. |
| `HTTP_ADDR` | Default `:8080`. |
| `LOG_LEVEL` | `debug`, `info` (default), `warn` or `error`. |

The channel map file has this shape:

```yaml
channels:
  C0123ABCDEF: runid
default: runid
```

Both config files have more optional variables: durations, dial addresses and
CA files.

The platform finds the cluster through your kubeconfig. The cluster needs the
two CRDs (`kubectl apply -f config/crd/bases`), or the platform fails at startup
because its cache cannot sync. A session needs more: agent-sandbox, and two
routes. The sandbox's sidecar dials the Agent Link through the Pomerium
route at `HARNESS_EXTERNAL_URL`, and clients reach the Harness API through its
own route, so both routes must reach your machine. Slack must reach the bot's
`HTTP_ADDR` at `/slack/events` and `/slack/interactivity`, for example through a
tunnel. In practice, run the platform in a cluster (see
[In-cluster dev loop](#in-cluster-dev-loop)) and use `run-local.sh` for the bot
or for startup checks.

### The transport, without a cluster

```sh
cd harness && go test ./internal/sidecar/harnessclient/
```

This package tests the platform-to-sandbox transport with no cluster, no
Pomerium and no Docker. It runs the real Agent Link server, the real sidecar
client, the real agent runner on a unix socket, and a fake ACP agent (the test
binary, started again as the agent). The server verifies genuine signed
assertions, stamped onto each stream the way the Pomerium route stamps them. The
tests cover attach and the session configuration, a full turn, a dropped tunnel
that resumes with no gaps and no duplicates, a restarted sidecar that joins the
running agent, an unknown run and a lost runner going terminal, a
`PERMISSION_DENIED` that refreshes the token and then gives up, and heartbeats.
It runs in seconds, so it is the fastest check for a protocol change.

## Clients

The Slack bot is the reference client. `sdk/ts` and `sdk/python` are typed
clients, and `harness/api/client` is the Go client. `apistub` holds the three of
them to the contract. It is the real apiserver and the real Connect transport in
front of a scripted in-memory implementation. A client can ask it for conditions that
a healthy server does not produce: a stream that dies mid-flight, a field this
build does not know, a log that ends before it says anything.

```sh
make apistub        # build bin/apistub
make sdk-test       # the TypeScript and Python suites against it
make sdk-generate   # regenerate both SDKs' protobuf code (make generate runs it)
```

See [`proto/harnessapi/v1/harnessapi.proto`](./proto/harnessapi/v1/harnessapi.proto)
and [`docs/sdk-conformance.md`](./docs/sdk-conformance.md).

## In-cluster dev loop

Use a local cluster that runs your local Docker images: OrbStack or Docker
Desktop Kubernetes, or kind after `kind load docker-image`. The quickstart
installs the stack; a values file points it at the local images.

1. Install agent-sandbox (`kubectl apply -f deploy/agent-sandbox.yaml`).

2. Build the images with local tags. Nothing is pushed:

   ```sh
   make docker-build   # agentops:dev and agentops-slackbot:dev
   make sidecar-build  # agentops-sidecar:dev
   make harness-build  # claude-code:dev; HARNESS=<dir> for another deploy/harness/<dir>
   ```

3. The domain `localhost.pomerium.io` and its subdomains resolve to `127.0.0.1`.
   On a cluster that serves a LoadBalancer Service on the host's port 443, such
   as OrbStack and Docker Desktop, that domain needs no DNS record. Make a
   certificate for `*.localhost.pomerium.io` with
   [mkcert](https://github.com/FiloSottile/mkcert) and put it in a TLS Secret:

   ```sh
   mkcert -cert-file tls.crt -key-file tls.key '*.localhost.pomerium.io'
   kubectl create namespace agentops-system
   kubectl -n agentops-system create secret tls pomerium-tls --cert=tls.crt --key=tls.key
   kubectl -n agentops-system create secret generic agentops-ca \
     --from-file=ca.crt="$(mkcert -CAROOT)/rootCA.pem"
   ```

4. Write a values file for the quickstart that points at the local tags and the
   mkcert certificate. Keep it out of the repository (`~/dev-values.yaml`):

   ```yaml
   hosts:
     authenticate: authenticate.localhost.pomerium.io
     anthropic: anthropic.localhost.pomerium.io
   tls:
     secret: agentops-system/pomerium-tls
   access:
     emails: [you@example.com]
   privateCA:
     secretName: agentops-ca
   sandbox:
     sidecar:
       image:
         repository: agentops-sidecar
         tag: dev
   agentops:
     image:
       repository: agentops
       tag: dev
     config:
       logLevel: debug
       agentic:
         asURL: https://agentic.localhost.pomerium.io
         caFile: /etc/agentops-ca/ca.crt
       harness:
         externalURL: https://harness.localhost.pomerium.io
         assertionIssuer: harness.localhost.pomerium.io
         api:
           assertionIssuer: harness-api.localhost.pomerium.io
     extraVolumes:
       - name: agentops-ca
         secret:
           secretName: agentops-ca
     extraVolumeMounts:
       - name: agentops-ca
         mountPath: /etc/agentops-ca
         readOnly: true
   ```

   Then install it, and run the `hello` agent as
   [INSTALL.md, Step 6](./INSTALL.md#step-6-run-the-hello-agent) says:

   ```sh
   kubectl apply --server-side --force-conflicts -f deploy/charts/agentops-quickstart/crds/
   helm dependency build --skip-refresh deploy/charts/agentops-quickstart
   helm upgrade --install agentops deploy/charts/agentops-quickstart \
     -n agentops-system -f ~/dev-values.yaml
   ```

   For the Slack bot, add it to `clients` in `~/dev-values.yaml` and install its
   chart as the README's [Add the Slack bot](./README.md#add-the-slack-bot)
   says, with `image.repository: agentops-slackbot` and `image.tag: dev`. For
   the claude-code agent, apply an overlay on [`deploy/sandbox`](./deploy/sandbox/README.md)
   as the README's [Add an agent](./README.md#add-an-agent) says.

5. After you rebuild a `:dev` image, restart what runs it. The tag does not
   change, so `helm upgrade` changes nothing:

   ```sh
   kubectl -n agentops-system rollout restart statefulset/agentops
   kubectl -n agentops-slackbot rollout restart deployment/agentops-slackbot
   ```

   A new sandbox pod uses a rebuilt sidecar or harness image. A pod that already
   waits in a warm pool does not.

6. After you change a SandboxTemplate, or rebuild an image that a pooled pod
   runs, delete the pooled Sandboxes so that the warm pools build them again:

   ```sh
   kubectl -n agentops-system delete sandboxes -l agents.x-k8s.io/warm-pool-sandbox
   ```

   A pooled pod keeps the template it started from, and the next session adopts
   it. agent-sandbox removes this label when a claim adopts a Sandbox, so the
   command does not touch a running session.

## Two modules

The repository is two Go modules, and the boundary between them is the product:

```
harness/    the platform. Its public surface is harness/api: the generated
            stubs, the sentinels and enums, the Go client. The rest is under
            harness/internal, which the compiler keeps out of reach.
slackbot/   the reference client. It requires harness and imports nothing
            but harness/api.
```

`go.work` joins them for editing and for `go test ./...` from either directory.
No image copies it, and every Makefile recipe runs with `GOWORK=off`.

`harness/apis/v1alpha1` is public too, because operators apply those CRDs, but a
client must not import it: a client knows nothing about Kubernetes.
`make boundary` lists the bot's dependencies and fails on any `harness/` package
other than `harness/api` and the packages under it, so an import of
`harness/apis` fails it.

## Helm charts

The charts are [`deploy/charts/agentops`](./deploy/charts/agentops) (the
platform), [`deploy/charts/agentops-slackbot`](./deploy/charts/agentops-slackbot)
(the bot) and [`deploy/charts/agentops-quickstart`](./deploy/charts/agentops-quickstart)
(Pomerium, the platform as a subchart, the sandbox side and the `hello` agent).
Their values files carry no comments; the chart READMEs
([platform](./deploy/charts/agentops/README.md),
[bot](./deploy/charts/agentops-slackbot/README.md),
[quickstart](./deploy/charts/agentops-quickstart/README.md)) document the values.

| Target | What it does |
|---|---|
| `make helm-deps` | Packages the platform chart into `deploy/charts/agentops-quickstart/charts/` (git-ignored). The lint and render targets run it. |
| `make helm-lint` | Lints the three charts with the minimum required values. |
| `make helm-template` | Renders the three charts. `HELM_EXTRA_VALUES` adds flags to the platform render, for example `HELM_EXTRA_VALUES="--set persistence.enabled=false"`. |
| `make helm-check-client-isolation` | Fails if the platform's RBAC names the bot, or if the bot chart renders RBAC, mounts an authorization-server token or claims a volume. |
| `make helm-check-quickstart-sandbox` | Fails if the quickstart chart's `hello` SandboxTemplate differs from [`deploy/quickstart/contract`](./deploy/quickstart/contract), which the agentops Component builds. Needs `kubectl` and `yq`. After a change to the Component, change `templates/sandbox.yaml` in the quickstart chart to match. |
| `make helm-sync-crds` | Copies `config/crd/bases` into `deploy/charts/agentops/templates/crds.yaml`, inside an `installCRDs` condition, and adds `helm.sh/resource-policy: keep` to each CRD so that `helm uninstall` leaves them. It also copies them into the quickstart chart's `crds/`. `make generate` runs it. |
| `make quickstart-sync-pomerium-crds` | Copies the Pomerium ingress controller's CRDs from its `experimental/agentic` branch (`POMERIUM_IC_REF`) into the quickstart chart's `crds/`. |
| `make helm-package` | Lints, then packages the three charts. |

## End-to-end tests

`harness/internal/e2e` is opt-in twice: the tests need the `e2e` build tag and
`AGENTOPS_E2E=1`. `go test ./...` runs none of them.

```sh
make test-e2e                      # every suite
make test-e2e ARGS='-run <Test>'   # one suite
```

`make test-e2e` runs `go test -tags e2e -timeout 40m ./internal/e2e/...` in
`harness/` with `AGENTOPS_E2E=1`.

| Test | What it checks | Needs |
|---|---|---|
| `TestClaudeHarnessConnectsAgnoMCP` | Builds the claude-code harness image and serves the production agent runner from the test process, which starts the agent in the container with `docker exec`. The agent must call a tool of the public Agno docs MCP server, finish the turn with no error and no permission request, and exit. With `AGENTOPS_E2E=1` set, a missing Anthropic key fails the test. | Docker, an Anthropic key (`ANTHROPIC_API_KEY` or `~/tmp/keys/claude_api_key.txt`), internet |
| `TestSandboxLifecyclePatchesK3s` | Calls the orchestrator's `ExtendLease`, `Suspend` and `Resume` against a real Sandbox. The lease (`spec.shutdownTime`) is set and moves forward, `operatingMode` switches to Suspended and back, Suspend keeps the lease, Resume renews it, and the pod template survives. | Docker (k3s) |
| `TestSandboxClaimSchemaK3sV1beta1` | The v1beta1 SandboxClaim CRD accepts the claim `BuildSandboxClaim` produces, and the stored claim has its warm-pool reference and no env, volume claim templates, lifecycle or endpoint data. | Docker (k3s) |
| `TestWorkloadIdentityK3s` | A pinned Pomerium that accepts Kubernetes ServiceAccount tokens admits a pod's own projected token, and answers 403 to a route for another ServiceAccount, another ServiceAccount's token, a token for another audience and a non-JWT bearer. The pod's agent container mounts no token. `POMERIUM_IMAGE` overrides the Pomerium image. | Docker (k3s), internet |

testcontainers reads the credential helpers in `~/.docker/config.json` before it
builds an image. If one of them fails (an expired cloud login, for example), the
Claude test fails before the build starts. `DOCKER_AUTH_CONFIG='{}'` makes
testcontainers skip that file.
