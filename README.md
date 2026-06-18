# agentops

Run agentic workflows straight from Slack. **@mention the bot** with a workflow
name and a prompt — `@bot deploy-service ship it` — and it spins up an isolated
agent to do the work, turning the Slack thread into a live, multi-turn
conversation with that agent. When a workflow needs access to an external tool
(GitHub, Kubernetes, …), the bot walks **you** through a one-click connection the
first time, then reuses your own credentials on every run — so the agent always
acts as you, with exactly the access you granted.

Agents are developed like code because they *are* code: you iterate on a repo
of Skills locally with your coding agent, against the same MCP servers the
production sandbox attaches — and `git push` is the release.

It is a **private, single-workspace** Slack app: installed into one workspace,
**not** distributed via the Slack marketplace, and configured entirely through
Kubernetes resources. Each workflow is declared as an `AgentTemplate` custom
resource and runs as an isolated [`agent-sandbox`](https://github.com/kubernetes-sigs/agent-sandbox)
pod driven over the Agent Client Protocol (ACP).

For the architecture, design invariants, and local development how-to, see
[`DEVELOPING.md`](./DEVELOPING.md).

## What people build with it

An agent here is an `AgentTemplate`: a system prompt, the MCP servers it needs,
and a sandbox image — a harness plus, optionally, a git repo of Skills and
data. The patterns below are the ones with the strongest pull in team chat
today; the hosted products in each category all deliver into Slack, but none
of them run on your cluster or act as the person who asked.

- **Deploy / release ChatOps** — `@bot deploy-service ship the latest api
  build`: the agent proposes a rollout, an approve/deny button gates it in the
  thread, and it runs with the *deployer's* RBAC rather than a god-mode bot
  token. Shipped example:
  [`agenttemplate-deploy-service.yaml`](./deploy/examples/agenttemplate-deploy-service.yaml).
- **Incident & on-call triage** — `@bot oncall-triage why is checkout 5xx'ing`:
  reads pods, events, and metrics with the on-call engineer's own scopes and
  posts findings in the alert thread. The incident data never leaves your infra.
- **Knowledge Q&A / support deflection** — answer the recurring questions in
  `#help-*` channels from Notion/Confluence/Drive MCP servers, grounded in
  what the *asker* is allowed to read, without indexing internal docs into a
  third-party SaaS.
- **Data pulls** — `@bot data-pull weekly actives by plan`: runs the query
  through a warehouse MCP server as the analyst and posts the table — or a
  real `.xlsx`, since file upload is the natural delivery mechanism for agent
  output in Slack.
- **Docs & decks from a thread** — "summarize this thread as a one-pager",
  "make a 5-slide deck of what shipped this sprint": document Skills produce
  `.docx`/`.pptx`/`.pdf` artifacts posted back to the channel, which makes
  agents useful to teammates who will never open a terminal.
- **Release notes** — draft from GitHub + Linear activity, publish to Notion;
  the publish step triggers the per-user OAuth connect on first use.
- **Access requests** — `@bot grant-access give Jen read on staging logs`:
  propose a downscoped grant, approve or deny in the thread.

### Dev to prod is `git push`

The workflow's repo is the deployment artifact. You develop an agent the way
you already work: locally, in that repo, with your coding-agent CLI, iterating
on `SKILL.md` files against the *same MCP servers* the production sandbox
attaches. When a skill behaves, commit and push — the sandbox checks the repo
out at its pinned ref, so the next @mention runs exactly what you perfected,
for everyone in the workspace, including teammates who will never open a
terminal. There is no separate publish step, no skills marketplace, no waiting
for a "share" button: review is a pull request, rollout is a merge, rollback
is a revert.

Skills are plain folders (`SKILL.md` + scripts), so existing collections work
as-is — bake them into the harness image or the workflow's repo. Good starting
points: [`anthropics/skills`](https://github.com/anthropics/skills) (the
`docx`/`xlsx`/`pptx`/`pdf` document skills plus `slack-gif-creator`, which is
built for Slack's GIF constraints) and
[`garrytan/gstack`](https://github.com/garrytan/gstack) (role-based
plan→review→ship workflows whose stage gates map directly onto the in-thread
approval buttons).

## Using the bot

1. **Start a workflow.** In any channel the bot has been invited to, @mention it
   with a workflow name followed by your prompt:

   ```
   @bot deploy-service roll out the latest api build
   ```

   The first word after the mention is the workflow name; the rest is the initial
   prompt. The bot reacts to your message with :hourglass_flowing_sand: while it
   gets ready, swapping it for :white_check_mark: when the agent is live (or
   :x: if something went wrong).

   You can also **loop the bot into an ongoing discussion** by @mentioning it
   inside an existing thread. The bot reads the thread so far as context (message
   texts only — who said what is not passed to the agent), starts a new session
   thread in the channel, and cross-links the two: the origin thread gets a link
   to the session thread, and the first answer is linked back into the origin
   thread.

2. **Connect your tools (first time only).** If the workflow needs a tool you
   haven't connected yet, the bot posts an auth link just for you. Click it,
   approve access, and you're done — your credentials are remembered, so you won't
   be asked again for that tool.

3. **Have the conversation.** Once the agent is running, the thread becomes a live
   session. Each reply you post in the thread is another turn; the agent's output
   streams back into the thread, and when it wants to run a sensitive action it
   asks for approval with buttons right in the thread.

4. **You're in control.** Only the person who started a session can drive it —
   replies and approval clicks from others in the channel are ignored, because the
   agent runs with *your* tool credentials.

5. **Wrap up.** A session ends when the agent finishes, when it sits idle too long,
   or when you stop it explicitly — and the underlying sandbox is torn down.

## Configuration

Process configuration comes from environment variables (see
`internal/config/config.go`):

| Variable | Required | Default | Description |
|---|---|---|---|
| `SLACK_SIGNING_SECRET` | yes | — | Verifies inbound Slack requests (from a Secret). |
| `SLACK_BOT_TOKEN` | yes | — | Authenticates outbound Slack API calls (from a Secret). |
| `OAUTH_REDIRECT_BASE_URL` | yes | — | Externally reachable base URL; the redirect URI is this + `/oauth/callback` (e.g. `https://agentops.example.com`). |
| `POD_NAMESPACE` | yes | — | Namespace the app operates in; set via `fieldRef` `metadata.namespace`. |
| `DB_PATH` | yes | — | SQLite database file path (e.g. `/data/agentops.db` on the mounted PVC). |
| `HTTP_ADDR` | no | `:8080` | Listen address for the gateway server. |
| `SESSION_TTL` | no | `1h` | Go duration bounding a sandbox's lifetime. |
| `AGENT_STREAM_INTERVAL` | no | `2.5s` | Go duration pacing how often the in-progress agent reply message may be replaced in Slack. Larger values reduce flicker on long turns (each update re-renders the message); the turn's final output is always delivered. |
| `LOG_LEVEL` | no | `info` | `debug`, `info`, `warn`, or `error`. Set `debug` to see HTTP request logs, the sandbox/ACP comms trace (method/update kinds only), and the per-turn agent trace — tool calls, thoughts, and superseded narration, tagged with the sandbox and turn. |
| `AGENT_COMMAND` | no | — | Command exec'd in the sandbox to start the ACP agent over stdio; empty uses the orchestrator default. |

Per-workflow configuration is supplied as CRDs (`AgentTemplate` and the
referenced `SandboxTemplate`), not environment variables.

## Install

Three things land in the cluster, in order: the upstream **agent-sandbox**
controller and CRDs (prerequisite), **this app** (Helm, or Kustomize for local
dev), and at least one **agent harness** image with its `SandboxTemplate` +
`AgentTemplate` pair (see [Agent harnesses](#agent-harnesses) and
[Defining an agent](#defining-an-agent)).

### 1. Install agent-sandbox (prerequisite)

Sandboxes are [`agent-sandbox`](https://github.com/kubernetes-sigs/agent-sandbox)
pods; its controller and CRDs (`Sandbox`, `SandboxTemplate`, `SandboxClaim`)
must be present before the app can launch anything. Upstream publishes plain
manifests (no Helm chart); the controller lands in namespace
`agent-sandbox-system`:

```sh
VERSION=v0.4.6  # latest: curl -s https://api.github.com/repos/kubernetes-sigs/agent-sandbox/releases/latest | jq -r .tag_name
kubectl apply -f "https://github.com/kubernetes-sigs/agent-sandbox/releases/download/${VERSION}/manifest.yaml"    # Sandbox CRD + controller
kubectl apply -f "https://github.com/kubernetes-sigs/agent-sandbox/releases/download/${VERSION}/extensions.yaml"  # SandboxTemplate, SandboxClaim, SandboxWarmPool
```

Its API groups (`agents.x-k8s.io`, `extensions.agents.x-k8s.io`) are
name-similar to — but distinct from — this app's `agents.pomerium.com`.

### 2. Install the app — Helm

A Helm chart lives at [`deploy/helm`](./deploy/helm) and is published as an OCI
artifact to `oci://registry-1.docker.io/pomerium/agentops` on every release
(and as `0.0.0-git.<sha>` on each push to `main`). It installs the
StatefulSet, RBAC, Service, the Slack credentials Secret, and the
`AgentTemplate` CRD.

```sh
helm install agentops oci://registry-1.docker.io/pomerium/agentops \
  --namespace agentops --create-namespace \
  --set slack.signingSecret=... \
  --set slack.botToken=... \
  --set config.oauthRedirectBaseURL=https://agentops.example.com
```

Set `existingSecret.name` to reference a Secret you manage yourself (keys
`SLACK_SIGNING_SECRET` and `SLACK_BOT_TOKEN`) instead of passing the tokens to
the chart. See [`deploy/helm/values.yaml`](./deploy/helm/values.yaml) for the
full set of options. Lint/render locally with `make helm-lint` /
`make helm-template`.

A TLS-terminating ingress (Pomerium or any reverse proxy) is expected in front
of the app's plain-HTTP Service; ingress configuration is out of scope. Slack
must be able to reach it at the Event Subscriptions / Interactivity URLs (see
[Slack app setup](#slack-app-setup)).

### 3. Set up an agent harness

Pick (or build) a harness image from [Agent harnesses](#agent-harnesses), then
apply its `SandboxTemplate`, the LLM-credentials Secret it references, and an
`AgentTemplate` that uses it — worked examples under
[`deploy/examples/`](./deploy/examples) are described in
[Defining an agent](#defining-an-agent).

## Agent harnesses

A harness is the container image a sandbox runs: a coding agent that speaks the
[Agent Client Protocol](https://agentclientprotocol.com) on stdio, exec'd by
the orchestrator as `/bin/sh -lc 'exec ${ACP_AGENT_CMD}'`. The contract (see
[`deploy/harness/claude-code/Dockerfile`](./deploy/harness/claude-code/Dockerfile),
the e2e-tested reference): a shell, git, the ACP agent on `PATH`, a non-root
uid-1000 user (the pod's `fsGroup` grants it the workspace volume), a writable
`HOME` and `/workspace`, and an idle `CMD` so the pod stays Ready between
sessions. Auto-approval ("yolo") must be on — permission prompts would
otherwise block Slack sessions.

One folder per agent under [`deploy/harness/`](./deploy/harness), built with
`make harness-build HARNESS=<agent>`:

| Harness | Agent | ACP integration | Auth env | Unattended mode | Status |
|---|---|---|---|---|---|
| [`claude-code`](./deploy/harness/claude-code) | Claude Code (Anthropic) | [`@agentclientprotocol/claude-agent-acp`](https://github.com/agentclientprotocol/claude-agent-acp) adapter | `ANTHROPIC_API_KEY` | `bypassPermissions` (refused as root unless `IS_SANDBOX=1`) | **reference, e2e-tested** |
| [`codex`](./deploy/harness/codex) | Codex CLI (OpenAI) | [`@zed-industries/codex-acp`](https://github.com/zed-industries/codex-acp) adapter | `OPENAI_API_KEY` / `CODEX_API_KEY` | `-c approval_policy=never -c sandbox_mode=danger-full-access` | ACP handshake verified; no live-LLM e2e yet |
| [`gemini`](./deploy/harness/gemini) | Gemini CLI (Google) | native: `gemini --acp` | `GEMINI_API_KEY` | `--approval-mode yolo` | ACP handshake verified; no live-LLM e2e yet |
| [`opencode`](./deploy/harness/opencode) | [OpenCode](https://opencode.ai) | native: `opencode acp` | `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, … | baked `permission: allow` config | ACP handshake verified; no live-LLM e2e yet |
| [`pi`](./deploy/harness/pi) | [pi](https://github.com/earendil-works/pi) (Mario Zechner) | community [`pi-acp`](https://github.com/svkozak/pi-acp) adapter | `ANTHROPIC_API_KEY`, … | no tool gating by design (container-first) | experimental — adapter is an MVP; handshake verified |
| [`hermes`](./deploy/harness/hermes) | [Hermes Agent](https://github.com/NousResearch/hermes-agent) (Nous Research) | native: `hermes-acp` (`hermes-agent[acp]`) | `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `NOUS_API_KEY`, … | **no documented always-approve** — validate before unattended use | experimental — handshake verified |
| [`demo`](./deploy/harness/demo) | none — canned output, no LLM | acp-go-sdk example agent | — | n/a | pipeline testing |

Any other ACP-capable agent works the same way — Goose (`goose acp`), Qwen Code
(`qwen --acp`), OpenHands (`openhands acp`), Kimi CLI, Copilot CLI, … — see the
[ACP agent registry](https://agentclientprotocol.com/get-started/registry) for
the full list of potentially supported harnesses, and follow the `claude-code`
Dockerfile pattern.

How the model API key reaches the agent is the `SandboxTemplate`'s choice. The
shipped [`claude-code` example](./deploy/examples/sandboxtemplate-claude-code.yaml)
keeps it out of the agent container entirely: the sidecar's envoy proxies the
Anthropic API and injects the key as a header upstream (`SIDECAR_HTTP_*` env).
For other providers, either wire an equivalent sidecar endpoint and point the
agent's base-URL env at it, or — simpler but weaker isolation — inject the
provider key directly into the `agent` container env.

## Continuous integration & releases

GitHub Actions workflows under [`.github/workflows`](./.github/workflows):

- **`test.yaml`** — `go vet`, build, and unit tests on every push to `main` and
  pull request. (The opt-in `e2e` suite is excluded — it needs Docker and an
  Anthropic key.)
- **`docker.yaml`** — builds the app (`pomerium/agentops`) and sidecar
  (`pomerium/agentops-sidecar`) images for `linux/amd64,linux/arm64`. PRs build
  only; pushes to `main` publish `:main` and `:git-<sha>`; tags `vX.Y.Z`
  publish `:vX.Y.Z` and `:latest`.
- **`helm.yaml`** — lints/renders the chart on PRs that touch `deploy/helm`,
  publishes a `0.0.0-git.<sha>` dev chart on push to `main`, and the release
  version when a GitHub release is published.

Cutting a release: push a `vX.Y.Z` tag and publish a GitHub release for it —
the tag push builds the release images and the published release pushes the
matching chart (chart `appVersion` is set to the tag, so it pulls the
same-tagged image).

These require two repository secrets: **`DOCKERHUB_USER`** and
**`DOCKERHUB_TOKEN`** (a DockerHub access token with push rights to the
`pomerium` org), used for both image and chart pushes.

## Defining an agent

An agent users can invoke from Slack is an `AgentTemplate` (group
`agents.pomerium.com/v1alpha1`) plus the `SandboxTemplate` it references. The
worked examples under [`deploy/examples/`](./deploy/examples):

**`AgentTemplate`** — a workflow users invoke as `@bot <metadata.name> ...`:

| Example | Invoke as | What it shows |
| --- | --- | --- |
| [`agenttemplate-deploy-service.yaml`](./deploy/examples/agenttemplate-deploy-service.yaml) | `deploy-service` | System prompt, required MCP servers (github + k8s), and a workflow-specific `sandboxTemplateRef` that bakes the git working context. |
| [`agenttemplate-gstack.yaml`](./deploy/examples/agenttemplate-gstack.yaml) | `gstack` | Bakes the `garrytan/gstack` "AI software factory" skills repo, paired with product/dev MCP servers (Linear, Notion, GitHub, PostHog). |
| [`agenttemplate-gcloud.yaml`](./deploy/examples/agenttemplate-gcloud.yaml) | `gcloud` | Bakes Google's official `google/skills` repo, each product skill paired with Google's matching first-party Cloud MCP server (Cloud Run, BigQuery, GKE, …), plus a `sessionConfig` picking the model. |

**`SandboxTemplate`** — how a sandbox is baked (harness + sidecar + git context):

| Example | Referenced by | What it shows |
| --- | --- | --- |
| [`sandboxtemplate-claude-code.yaml`](./deploy/examples/sandboxtemplate-claude-code.yaml) | generic | The `claude-code` harness (`envVarsInjectionPolicy: Allowed`); the `agent` container sets `ACP_AGENT_CMD`, and all secrets live in the `sidecar` (embedded envoy injects per-user MCP OAuth tokens and the `ANTHROPIC_API_KEY` as upstream headers — the agent never sees a token). |
| [`sandboxtemplate-pomerium-zero-claude-code.yaml`](./deploy/examples/sandboxtemplate-pomerium-zero-claude-code.yaml) | private-repo pattern | Workflow-specific: claude-code harness plus a `git-init` init container baking the repo URL/ref and a **private**-repo git-credentials `secretKeyRef`. Copy as the starting point for workflows that bake a private repo (e.g. `deploy-service`'s hypothetical `deploy-runbooks-claude-code`). |
| [`sandboxtemplate-gstack-claude-code.yaml`](./deploy/examples/sandboxtemplate-gstack-claude-code.yaml) | `gstack` | Workflow-specific: bakes a **public** repo (no credentials Secret — `git-checkout` does an unauthenticated shallow fetch). |
| [`sandboxtemplate-google-skills-claude-code.yaml`](./deploy/examples/sandboxtemplate-google-skills-claude-code.yaml) | `gcloud` | Workflow-specific: bakes the public `google/skills` repo into `/workspace`. |

**Secret** — LLM credentials a `SandboxTemplate` references:

| Example | What it shows |
| --- | --- |
| [`secret.claude-code.example.yaml`](./deploy/examples/secret.claude-code.example.yaml) | The `claude-code-credentials` Secret holding `ANTHROPIC_API_KEY`, consumed by the sidecar via `SIDECAR_HTTP_*` env vars. |

The agent is selected from Slack by the template's `metadata.name`. Key
`AgentTemplate` spec fields: `systemPrompt`, `requiredMCPServers`
(`{name, url}`), `sandboxTemplateRef`, and `sessionConfig` (below).

### Harness session configuration (`sessionConfig`)

`spec.sessionConfig` tunes the harness per workflow — most usefully the model —
using the ACP-standard mechanism: the harness advertises its configuration
options in the `session/new` response, and the app applies each entry via
`session/set_config_option` right after the session opens. The ACP spec has no
dedicated "set model" method; model selection is a config option like any
other (reserved option category `model`).

```yaml
spec:
  sessionConfig:
    model: opus    # claude-code resolves aliases (opus/sonnet/haiku) or full model IDs
    effort: high   # reasoning effort
```

Keys are option ids, values are the option's value id — or `"true"`/`"false"`
for boolean options. The claude-code harness
([`@agentclientprotocol/claude-agent-acp`](https://github.com/agentclientprotocol/claude-agent-acp))
advertises `model`, `effort`, and `mode`; other ACP harnesses advertise their
own sets.

Application is **strict**: if the harness doesn't advertise a configured
option id, or rejects the value, the session fails to launch and the error
(naming the bad id and listing what the harness does support) is surfaced in
the Slack thread. A misspelled `modell:` fails loudly instead of producing a
session that silently ignores the template's intent. `model` is always
applied first, since the harness rebuilds dependent options (e.g. the valid
`effort` levels) when the model changes.

## Slack app setup

The bot is driven by @mentions in channels. The app detects the mention in the
**message** events it already needs for thread replies (Slack delivers a channel
@mention as a normal message whose text contains the bot's `<@id>`), so a
separate `app_mention` subscription is **not** required.

- **Invite the bot to the channel** so it receives message events there.
- **Subscribe to message events** (Event Subscriptions → request URL
  `https://<your-host>/slack/events`): `message.channels` (and
  `message.groups`/`message.im`/`message.mpim` for those surfaces).
- **Enable Interactivity** (request URL `https://<your-host>/slack/interactivity`)
  for the tool-permission buttons.
- **Bot token scopes:** `chat:write`, `reactions:write`, and the history scopes
  matching the subscribed surfaces (`channels:history`, `groups:history`, …).
  The bot's user id is discovered at startup via `auth.test`.

This is a self-hosted app, and you need to create a Slack application by visiting https://api.slack.com/apps and clicking Create Application. 
Choose Create from Manifest option as it is easiest. Then Install to Workspace to obtain the bot token. 

Then, configure the following env variables with the values from Slack application either via secret reference or providing them during Helm install

- `SLACK_SIGNING_SECRET`
- `SLACK_BOT_TOKEN`

Make sure you confirm the Event URL in the slack as it requires application to be running and respond to a challenge sent by Slack - otherwise your application would not receive the notifications from slack when new messages arrive.

### Manifest

```json
{
    "display_information": {
        "name": "pom"
    },
    "features": {
        "bot_user": {
            "display_name": "Pomerium",
            "always_online": false
        }
    },
    "oauth_config": {
        "scopes": {
            "bot": [
                "app_mentions:read",
                "channels:history",
                "chat:write",
                "reactions:write",
                "im:history",
                "groups:history",
                "mpim:history"
            ]
        },
        "pkce_enabled": false
    },
    "settings": {
        "event_subscriptions": {
            "request_url": "https://slack-agent.example.com/slack/events",
            "bot_events": [
                "message.channels",
                "message.groups",
                "message.im",
                "message.mpim"
            ]
        },
        "interactivity": {
            "is_enabled": true,
            "request_url": "https://slack-agent.example.com/slack/interactivity"
        },
        "org_deploy_enabled": false,
        "socket_mode_enabled": false,
        "token_rotation_enabled": false,
        "is_mcp_enabled": true
    }
}
```
