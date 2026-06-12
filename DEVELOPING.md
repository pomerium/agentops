# Developing agentops

This file is for people changing the code. For what the app does and how to
install it, see [README.md](./README.md).

## What it is

One Go module, two binaries. `cmd/agentops` is the app: a stateful Slack bot
that resolves an `AgentTemplate` CRD per @mention, brokers per-user OAuth to
the workflow's MCP servers, launches an agent-sandbox pod, and drives a
multi-turn ACP session from the Slack thread. `cmd/sidecar` is the
secret-isolating proxy that runs inside each sandbox pod.

`cmd/agentops/main.go` is the whole dependency graph in one function — read it
first. The components it wires:

| Package | Role |
|---|---|
| `internal/channels/slack/gateway` | HTTP endpoints (`/slack/events`, `/slack/interactivity`, `/oauth/callback`), signature verification, Block Kit rendering |
| `internal/channels/slack/client` | outbound Slack API, rate limiting (guaranteed lifecycle posts, last-one-wins coalescing of streaming updates) |
| `internal/chatops/session` | the brain: maps Slack threads to sessions, sequences auth → launch → prompt turns, sweeps idle sessions |
| `internal/chatops/store`, `internal/chatops/db` | SQLite persistence: goose migrations, sqlc-generated queries |
| `internal/agenttemplate` | `AgentTemplate` registry — a controller-runtime cache scoped to the pod namespace |
| `internal/mcpbroker` | per-user, per-server OAuth (authorization code + PKCE + dynamic client registration), token storage and refresh |
| `internal/sandbox` | creates `SandboxClaim`s, pod-execs the sidecar and agent, owns the ACP client (`github.com/coder/acp-go-sdk`) |
| `internal/sidecar`, `proto/sidecar` | the sidecar: a control server configured over gRPC-on-exec-stdio that renders envoy config for credential-injecting localhost listeners |
| `api/v1alpha1` | the `AgentTemplate` CRD types (`agents.pomerium.com/v1alpha1`); manifests generated into `config/crd` |
| `internal/mdsplit` | splits agent markdown into Slack-sized message blocks |

## Design invariants

These are the decisions everything else leans on. Don't break them casually.

**One replica, pod-pinned state.** The app is a StatefulSet with `replicas: 1`
and must stay that way: SQLite on a PVC opens with a single writer
(`MaxOpenConns=1`), ACP sessions are in-memory, and OAuth flow state (PKCE
verifiers) is local. A restart kills live sessions;
`Manager.ReconcileOnStartup` marks them dead in the store rather than
pretending they survived.

**Secrets never enter the agent container.** Three separate mechanisms, same
rule:

- *MCP tokens*: the orchestrator execs `sidecar serve` in the pod's `sidecar`
  container and streams it endpoint config over gRPC on the exec stdio.
  Envoy serves credential-free `127.0.0.1:91xx` listeners and injects the
  user's `Authorization` header upstream. The agent's ACP `session/new` gets
  the rewritten localhost URLs with no headers.
- *LLM API keys*: declared on the SandboxTemplate as `SIDECAR_HTTP_*` env on
  the sidecar container only; the agent gets a base-URL env pointing at the
  sidecar (e.g. `ANTHROPIC_BASE_URL=http://127.0.0.1:9999`).
- *Git credentials*: a `secretKeyRef` on the SandboxTemplate's `git-init`
  init container, consumed via `GIT_ASKPASS` (never argv) by
  `deploy/harness/git-checkout.sh`. The env dies with the init container
  before the agent starts. Nothing git-related passes through the app or the
  claim.

**The app owns the ACP client.** Sessions live exactly as long as the Slack
thread is active; each thread reply is a `Prompt` on the held session. The
agent's stdio is a Kubernetes pod-exec SPDY stream. Container names matter:
the claim targets env at containers `agent`, `sidecar`, and `git-init` by name
(constants in `internal/sandbox/claim.go`).

**The system prompt travels over ACP, not env.** The claude-agent-acp harness
ignores env-based prompts; `OpenSession` injects it via `session/new` `_meta`.

**Plain HTTP everywhere.** TLS termination is the ingress's problem.

`docs/SPEC.md` is the original design document. It is gitignored (all of
`docs/` is) and predates several renames — it says `WorkflowTemplate` where
the code says `AgentTemplate`, slash commands where the code uses @mentions,
and a top-level `db/` that is now `internal/chatops/db/`. Trust the code.

## Build, test, generate

```sh
make build      # go build ./...
make test       # go test ./... (e2e excluded; see below)
make vet
make generate   # controller-gen + sqlc + buf, then helm-sync-crds
```

Gotcha: on some machines a stale `GOROOT` export breaks the toolchain. Prefix
with `env -u GOROOT` (e.g. `env -u GOROOT make test`). Deliberately not baked
into the Makefile.

Generated code is committed. Run `make generate` after touching any of:

- `api/v1alpha1` → deepcopy methods + CRD manifests in `config/crd/bases`,
  also synced into the Helm chart (`deploy/helm/templates/crds.yaml`) by
  `helm-sync-crds` — never edit that file by hand;
- `internal/chatops/db/query.sql` or `migrations/` → sqlc bindings in
  `internal/chatops/db/sqlc`;
- `proto/sidecar` → gRPC stubs in `internal/sidecar/pb`.

controller-gen, sqlc, and buf are `tool` directives in `go.mod`, invoked as
`go tool <name>` — no separate install, versions pinned by the module.

## Running locally

The binary runs fine outside the cluster: `ctrlconfig.GetConfig()` falls back
to your kubeconfig, and sandbox launches use pod-exec, which works from a
laptop. You still need a real cluster (the agent-sandbox controller, the CRDs,
and the harness/sidecar images must be there) and Slack must be able to reach
your HTTP listener — put a tunnel in front and point the Slack app's request
URLs and `OAUTH_REDIRECT_BASE_URL` at it.

Put config in `.env` at the repo root (bare `KEY=value` lines, gitignored)
and run:

```sh
./scripts/run-local.sh   # sources .env with allexport, then go run ./cmd/agentops
```

Required keys: `SLACK_SIGNING_SECRET`, `SLACK_BOT_TOKEN`,
`OAUTH_REDIRECT_BASE_URL`, `POD_NAMESPACE`, `DB_PATH` (see
`internal/config/config.go` and the table in the README). Set
`LOG_LEVEL=debug` for HTTP request logs, the sandbox/ACP comms trace, and the
per-turn agent trace.

## In-cluster dev loop (Kustomize)

Build the images with local tags — no registry; a local cluster
(OrbStack/kind) consumes them directly via `imagePullPolicy: IfNotPresent`:

```sh
make docker-build                 # the app                     (agentops:dev)
make harness-build                # claude-code agent harness   (agentops-agent:dev)
make harness-build HARNESS=codex  # ...or any deploy/harness/<agent> folder
make sidecar-build                # sandbox sidecar proxy       (agentops-sidecar:dev)
```

Create the Slack credentials Secret out-of-band first — it is deliberately
excluded from Kustomize (copy `deploy/secret.example.yaml`). Then:

```sh
kubectl kustomize deploy/overlays/dev   # preview (or: make kustomize)
kubectl apply -k deploy/overlays/dev    # apply   (or: make deploy OVERLAY=dev)
```

`deploy/base` holds the core resources plus the CRD from `config/crd`; the
`dev`/`prod` overlays set the image tag and `OAUTH_REDIRECT_BASE_URL`. Keep
`replicas: 1` (see invariants above).

Helm chart work: `make helm-lint` / `make helm-template`; after `make
generate`, the chart's CRD copy is refreshed automatically.

## End-to-end tests

`internal/e2e` is opt-in twice over — the `e2e` build tag and
`AGENTOPS_E2E=1` — so `go test ./...` compiles it but runs nothing.

```sh
make test-e2e   # AGENTOPS_E2E=1 go test -tags e2e -timeout 15m ./internal/e2e/...
```

Two suites, runnable individually:

- `-run Agno` — drives the production `claude-code` harness image (built by
  the test via testcontainers) against the real Anthropic API and a public
  no-auth MCP server, asserting the agent actually calls an MCP tool. Needs
  Docker and an Anthropic key — `ANTHROPIC_API_KEY` or
  `~/tmp/keys/claude_api_key.txt`. Guards the "agent talks to the LLM but
  sees zero MCP servers" failure mode.
- `-run SidecarK3s` — the full production launch path (Orchestrator.Launch,
  SPDY pod-exec, gRPC-on-stdio sidecar config, envoy, the no-LLM demo
  harness) on a real k3s cluster via testcontainers. Only the claim→pod
  controller is faked. No LLM key needed.

Gotcha: the k3s testcontainers module sets `CgroupnsMode: host`, which
crash-loops pods under OrbStack's Docker. The test overrides it to `private`
(see the host-config customizer in `sidecar_k3s_test.go`) — keep that when
upgrading the module.
