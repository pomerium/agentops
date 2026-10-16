# AgentOps MCP server

An MCP server that runs AgentOps agents for MCP clients such as Claude and
ChatGPT. A user asks their client to run an agent; the server starts an AgentOps
session through the Harness API and hands the user Pomerium's approval page
through URL elicitation. The user approves the run there, as themselves, and
the client follows the run as an MCP task until the agent answers. The server
never approves on the user's behalf.

It is a reference app, kept apart from the core AgentOps packages: it talks to
the platform only over the Harness API, behind Pomerium, the same way any other
client does. It is built from
[nickytonline/mcp-typescript-template](https://github.com/nickytonline/mcp-typescript-template)
(TypeScript, Effect, Express and the MCP TypeScript SDK).

- [How it wires into AgentOps](#how-it-wires-into-agentops)
- [Tools](#tools)
- [Agent skill](#agent-skill)
- [Configuration](#configuration)
- [Run it locally](#run-it-locally)
- [Deploy to an AgentOps cluster](#deploy-to-an-agentops-cluster)
- [Development](#development)
- [CI/CD](#cicd)

## How it wires into AgentOps

```
MCP client ──OAuth──▶ Pomerium (mcp route) ──▶ agentops-mcp ──SA token──▶ Pomerium (harness-api route) ──▶ Harness API
                                                    │
user ◀── URL elicitation: Pomerium consent page ◀───┘
```

- **Inbound.** MCP clients reach the server through a Pomerium route with
  `mcp_server` set, so Pomerium is the OAuth authorization server for the
  client. Pomerium passes the signed-in user's identity in
  `X-Pomerium-Jwt-Assertion`, and the server scopes every task to that user.
- **Outbound.** The server is a Harness API client. It calls the Harness API
  through Pomerium with its pod's projected ServiceAccount token (audience
  `pomerium-agentic-as`). The quickstart values' `clients` list adds that
  ServiceAccount to the harness-api route policy and creates its
  ClientBinding, which decides which agent templates it may run.
- **Approval.** Launching a session puts the run in the awaiting-approval
  state. The server sends the user the run's approval URL, Pomerium's consent
  page, and waits on the session's events: approved, the agent's thoughts and
  tool calls, then the answer. An unapproved run expires after
  `AGENTIC_RUN_TTL` (15 minutes by default).

Tasks live in memory, so run one replica.

## Tools

### launch_hello_demo

Starts a run of the `hello` demo agent, with an optional `prompt`. How the
approval reaches the user depends on what the client supports, tried in this
order:

- **MCP Events:** if the user has a subscription to `run.approval_required` or
  `run.finished` that covers the run, the tool returns the `runId` and approval
  link at once, and the approval and the answer arrive as events (see
  [MCP Events](#mcp-events)).

- **MCP Tasks extension** (`io.modelcontextprotocol/tasks`, SEP-2663):
  the tool returns a task right away. `tasks/get` reports `working` while the
  sandbox launches, then `input_required` with a URL elicitation of the
  consent page. After approval the task goes back to `working` with live
  status ("Approved", thinking, tool calls) and ends `completed` with the
  agent's answer and session id. `tasks/cancel`, or declining the
  elicitation, ends the AgentOps session.
- **URL elicitation, no tasks:** the tool returns `input_required` with the
  URL elicitation and an HMAC-signed `requestState`, then answers when the
  agent finishes, waiting up to about 90 seconds per round.
- **Neither:** a plain result with the approval link and a `runId` to pass to
  `get_run_status`. Clients with only form elicitation (ChatGPT declares
  `elicitation: {}`) get this too: the server can't control how a client
  draws a form, and ChatGPT dots render them about 1px wide.

Dismissing an approval prompt (`cancel`) keeps the run and returns the link as
plain text; only declining it ends the run.

### get_run_status

Waits up to 45 seconds for a run (by `runId`) to move on, then reports the
approval link, progress or the agent's answer.

### list_pending_approvals

Lists every run this server started that is still waiting for approval, from
the platform's session list. It includes stale runs the server no longer
tracks, after a restart or from a client that gave up. Each entry has its
approval link, expiry, and whether `get_run_status` can still follow it.
Ending stale runs frees the client's `maxPendingApprovals` quota.

### list_agents

Lists the agent templates this server may run, from its ClientBinding.

### end_session

Ends an AgentOps session by `sessionId` and frees its pod.

### MCP Events

The server implements the draft
[MCP Events](https://github.com/modelcontextprotocol/experimental-ext-triggers-events/blob/main/docs/design-sketch-proposal.md)
extension the way
[ChatGPT](https://developers.openai.com/plugins/build/mcp-events) uses it
(dots and Work chats): `events` on `server/discover`, plus `events/list`,
`events/subscribe` and `events/unsubscribe`, with webhook delivery only.

| Event | Payload |
| --- | --- |
| `run.approval_required` | `run_id`, `session_id`, `template`, `approval_url`, `expires_at` |
| `run.finished` | `run_id`, `session_id`, `template`, `status` (`completed`, `failed` or `cancelled`), `answer` (up to 32 KiB), `answer_truncated`, `error` |

- Both accept an optional `run_id` filter, and go only to subscriptions of
  the user who launched the run. They aren't replayable (`cursor: null`): a
  run that finishes while the server is down sends no event, but
  `get_run_status` still reports it.
- **Subscribe:** the secret must be `whsec_` plus base64 of 24 to 64 bytes.
  The server sends a signed, single-use challenge to the callback and stores
  the subscription only if it's echoed back (cached for 24 hours per user and
  callback URL). Failures return `-32015` with `data.reason`. The id is
  derived from the user, callback URL, event and arguments, so repeated
  subscribes refresh one subscription. Lifetimes default to 24 hours and are
  clamped to between 5 minutes and 7 days; after a secret change, both
  secrets sign for an hour.
- **Delivery:** Standard Webhooks signatures plus `X-MCP-Subscription-Id`,
  at most 256 KiB, retried after 1 s, 5 s, 30 s and 2 min with the same
  event id. A `410` drops the subscription; `413` and other `4xx` aren't
  retried.
- **Callbacks** must be HTTPS and resolve only to public addresses, checked at
  connect time so DNS can't swap in a private one. Redirects aren't followed.
- Subscriptions persist to `EVENTS_STORE_FILE`.

Client support is thin while the extension is a draft: ChatGPT dots and Work
chats subscribe when the user asks them to watch for an event. Every other
client falls back to the paths above.

## Agent skill

The server publishes the [`agentops-demo`](skills/agentops-demo/SKILL.md)
Agent Skill, which explains the approval and task flow to the host. On
protocol revision `2026-07-28` or later, `server/discover` advertises
`io.modelcontextprotocol/skills`; clients call `skills/list` or `skills/get`
for `skill://agentops-demo/SKILL.md`, then read it with `resources/read`. See
[docs/AGENT_SKILLS.md](docs/AGENT_SKILLS.md).

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `HARNESS_API_URL` | `https://harness-api.agentops.example.com` | AgentOps Harness API, through Pomerium |
| `HARNESS_TOKEN_FILE` | `/var/run/secrets/agentops/token` | Projected ServiceAccount token, audience `pomerium-agentic-as`, re-read on each call |
| `HARNESS_TOKEN` | — | Static token for local development; overrides the file |
| `DEMO_TEMPLATE` | `hello` | AgentTemplate that `launch_hello_demo` runs |
| `STATE_SECRET` | random per process | HMAC key for `requestState` |
| `EVENTS_STORE_FILE` | empty (in memory) | JSON file for MCP Events subscriptions; holds signing secrets and is written with mode 0600 |
| `PORT` | `3000` | HTTP port |
| `SERVER_NAME`, `SERVER_VERSION` | `name` and `version` in `package.json` | MCP server identity |
| `LOG_LEVEL` | `info` | `error`, `warn`, `info` or `debug` |

## Run it locally

Node.js 24.11 or newer.

```sh
npm install
HARNESS_API_URL=https://harness-api.agentops.example.com \
HARNESS_TOKEN="$(kubectl -n agentops-mcp create token agentops-mcp --audience pomerium-agentic-as)" \
npm run dev
```

The server listens on `http://localhost:3000/mcp`; `GET /health` is a liveness
check. Without Pomerium in front there is no `X-Pomerium-Jwt-Assertion`, so
use the
[MCP Inspector](https://github.com/modelcontextprotocol/inspector)
(`npx @modelcontextprotocol/inspector`) for a quick look at the tools, and the
deployed route for the full approval flow.

`docker compose up --build` builds and runs the image with a health check.

## Deploy to an AgentOps cluster

[`deploy/agentops-mcp.yaml`](deploy/agentops-mcp.yaml) has the namespace,
ServiceAccount, Deployment and Service, plus a small PersistentVolumeClaim for
MCP Events subscriptions.
[`deploy/route.yaml`](deploy/route.yaml) is the Pomerium route at
`https://mcp.agentops.example.com/mcp`. Replace `agentops.example.com` with
your domain and set the route policy to your users.

1. Add the ServiceAccount to the quickstart values' `clients`, so the
   harness-api route accepts it and it gets a ClientBinding.
2. Create the `requestState` secret:

   ```sh
   kubectl create namespace agentops-mcp
   kubectl -n agentops-mcp create secret generic agentops-mcp \
     --from-literal=state-secret="$(openssl rand -hex 32)"
   ```

3. Build the image and load it into the cluster. On k3s:

   ```sh
   docker build -t agentops-mcp:dev .
   docker save agentops-mcp:dev | sudo k3s ctr images import -
   ```

4. Apply the manifests:

   ```sh
   kubectl apply -f deploy/agentops-mcp.yaml -f deploy/route.yaml
   kubectl -n agentops-mcp rollout restart deployment/agentops-mcp
   ```

Then add `https://mcp.agentops.example.com/mcp` to your MCP client as a remote
server and sign in through Pomerium.

## Development

```sh
npm run dev          # watch mode
npm run build        # typecheck, then bundle to dist/index.js
npm test             # Vitest
npm run lint         # Oxlint, with the vendored rules in tools/oxlint/anti-slop
npm run format       # Oxfmt
```

| Path | |
| --- | --- |
| `src/tools.ts` | Tool registration and logic |
| `src/demo-run.ts` | Maps one AgentOps session's events to one MCP task |
| `src/tasks.ts` | In-memory task store, scoped per user |
| `src/events.ts` | MCP Events: event catalog, subscriptions and delivery |
| `src/webhook.ts` | Standard Webhooks signing and the public-address-only callback POST |
| `src/harness.ts` | Harness API client |
| `src/identity.ts` | Reads the user from `X-Pomerium-Jwt-Assertion` |
| `src/skills.ts` | Agent Skills over MCP |
| `src/app.ts`, `src/index.ts` | Express wiring, startup and shutdown |
| `deploy/` | Kubernetes manifests and the Pomerium route |

[AGENTS.md](AGENTS.md) has the coding conventions and how to add a tool.

## CI/CD

This folder is a starting point to copy into its own repository, and
[`.github/`](.github) holds that repository's pipeline. GitHub runs workflows
only from the repository root, so none of them run inside pomerium/agentops.

| Workflow | What it does |
| --- | --- |
| [`ci.yml`](.github/workflows/ci.yml) | Lint, format check, tests, build, Agent Skill validation and a Docker build, on pushes and PRs to `main` |
| [`validate-commits.yml`](.github/workflows/validate-commits.yml) | Checks PR commits against [`.commitlintrc.json`](.commitlintrc.json) (Conventional Commits) |
| [`release.yml`](.github/workflows/release.yml) | On `main`, tests and builds, then semantic-release ([`.releaserc.json`](.releaserc.json)) tags a version and creates a GitHub release |
| [`publish-image.yml`](.github/workflows/publish-image.yml) | After a release, builds and pushes the image to `ghcr.io/<owner>/<repo>` (`sha-<commit>`, the release version, and `latest`) |

The image runs on any cluster that can pull from GHCR; point
`deploy/agentops-mcp.yaml` at it and drop `imagePullPolicy: Never`.
