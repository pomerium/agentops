# agentops

AgentOps runs coding agents on Kubernetes for the person who asks for one. It
has three parts:

- **The platform** (`harness/`, image `pomerium/agentops`) runs sessions. It
  serves the **Harness API**, which clients use to start sessions, send prompts
  and read events. It launches each session's agent in an
  [agent-sandbox](https://github.com/kubernetes-sigs/agent-sandbox) pod, and it
  serves the **Agent Link**, which the pod dials back to.
- **Clients** drive sessions through the Harness API. The **Slack bot**
  (`slackbot/`, image `pomerium/agentops-slackbot`) is the reference client: a
  Slack thread is a conversation with an agent. You can write others.
- **The sandbox pod** runs an agent that speaks the
  [Agent Client Protocol](https://agentclientprotocol.com) (ACP), next to a
  sidecar (image `pomerium/agentops-sidecar`) that holds the pod's credentials.
  The agent container holds none.

Pomerium is in front of all of it. Before an agent starts, the person who asked
for it approves a run on a Pomerium consent page. The run is sealed to that one
pod. The pod can reach only DNS and Pomerium, and each Pomerium route decides
whether it accepts the run's token. The exception is a pod that checks out a git
repository when it starts. It needs an extra egress rule for HTTPS to the git
host, and that rule lets its agent reach public HTTPS hosts directly too (see
[Dev to prod is `git push`](#dev-to-prod-is-git-push)).

More documentation:

- [DEVELOPING.md](./DEVELOPING.md): architecture, building and testing.
- [docs/run-identity.md](./docs/run-identity.md): how a run is sealed to a pod,
  and the Pomerium configuration behind it.
- [docs/threads.md](./docs/threads.md): how the Slack bot maps threads to
  sessions: ownership, joining a colleague's thread, continuing a paused
  conversation.

## What people build with it

An agent is an `AgentTemplate`: a system prompt, the MCP servers it needs, and a
sandbox image (an agent harness and, optionally, a git repository of Skills and
data). Some patterns:

- **Deploy ChatOps.** `@bot ship the latest api build` in `#deploys`. The agent
  proposes a rollout, waits for a go-ahead in the thread, and acts with the
  deployer's access. Example:
  [`agenttemplate-deploy-service.yaml`](./deploy/examples/agenttemplate-deploy-service.yaml).
- **Incident triage.** `@bot why is checkout 5xx'ing` in `#oncall`. The agent
  reads pods, events and metrics through MCP servers, with the on-call
  engineer's own upstream accounts, and posts what it finds in the thread.
- **Knowledge Q&A.** The agent answers questions in `#help-*` channels from
  Notion, Confluence or Drive MCP servers, limited to what the asker can read.
- **Data pulls.** `@bot weekly actives by plan` in `#data-requests`. The agent
  queries a warehouse MCP server as the analyst and posts the table.
- **Release notes.** The agent drafts notes from GitHub and Linear activity and
  publishes them to Notion.

### Dev to prod is `git push`

An agent's git repository is its deployment artifact. You develop the agent in
that repository with your own coding agent, and you test its `SKILL.md` files
against the same MCP servers that the sandbox uses. The sandbox's `git-init`
init container checks out the repository at `GIT_REPO_REF` when the pod starts.
A merge therefore reaches every new pod: review is a pull request, rollout is a
merge, rollback is a revert. Pre-started pods in a warm pool keep the checkout
they started with (see [Warm pools](#warm-pools)).

The checkout needs an egress rule of its own. The agentops Component's network
policy admits only DNS and Pomerium, and agent-sandbox applies it to the whole
pod, init containers included, so a `git-init` cannot reach github.com with that
policy alone. [`deploy/examples/git-egress.yaml`](./deploy/examples/git-egress.yaml)
is a JSON 6902 patch that appends one egress rule: TCP 443 to `0.0.0.0/0` except
the RFC 1918 ranges (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`).
[`deploy/examples/kustomization.yaml`](./deploy/examples/kustomization.yaml)
applies it to the three templates with a `git-init`.

This is a trade-off. The rule applies to the whole pod, so the agent container
can also reach any public HTTPS host directly, without Pomerium and without the
run token. To narrow it, replace `0.0.0.0/0` with your git host's address
ranges. A template without a `git-init` does not need the rule and keeps the
DNS-and-Pomerium-only policy.

Skills are plain folders (`SKILL.md` and scripts). Put them in the harness image
or in the agent's repository. Collections such as
[`anthropics/skills`](https://github.com/anthropics/skills) and
[`garrytan/gstack`](https://github.com/garrytan/gstack) are folders of this
kind; the `gstack` example agent checks out the second one.

## Using the bot

1. **Start a session.** In a channel that is bound to an agent (see
   [Binding a channel](#binding-a-channel)), mention the bot with your prompt:

   ```
   @bot roll out the latest api build
   ```

   The channel decides which agent runs. The text after the mention is the
   prompt. The bot reacts to your message with :hourglass_flowing_sand: while it
   prepares the session, and changes the reaction to :rocket: when the agent is
   ready or to :x: if the launch failed. One status message in the thread shows
   the state of the session.

2. **Approve the run.** The bot sends you a direct message with a button that
   opens Pomerium's consent page. The page shows what the agent asks for,
   including its MCP servers, with a Connect link for each upstream account you
   have not authorized yet. Nothing runs before you approve. The request goes to
   you only, so no other channel member can approve it. An unapproved run lapses
   after `AGENTIC_RUN_TTL` (15 minutes by default).

3. **Talk to the agent.** In a thread that only you use, each reply is a turn.
   The agent's output streams into the thread. A :waiting: reaction (a custom
   emoji, see [Slack app setup](#slack-app-setup)) stays on the thread's first
   message while a turn runs. If the agent asks permission for a
   tool call, the bot posts buttons in the thread. Only the owner of the session
   can click them, and a request that nobody answers is cancelled after 5
   minutes.

4. **Share the thread.** A session belongs to the person who started it. When
   someone else mentions the bot in your thread, they join it: they get their
   own session, pod and approval, and their agent receives the thread so far as
   context (message texts only, not who wrote them). Their agent's answers go to
   the same thread. When a second person joins, the thread becomes
   multiplayer: from then on a mention is a turn and a plain message is
   discussion, for everyone. In a multiplayer thread, each turn starts with what
   was said in the thread since that session's previous turn. A mention in any
   thread where you have no session, such as an ongoing discussion, also joins
   it. See [docs/threads.md](./docs/threads.md).

5. **Pause and continue.** A session with no turn for `SESSION_IDLE_TTL` (15
   minutes by default) is paused, and its pod is freed. The bot warns you
   `SESSION_IDLE_WARN_LEAD` (2 minutes by default) before that: in the thread,
   or by direct message in a multiplayer thread. The workspace volume and the
   agent's own transcript are kept. Reply in the thread (mention the bot in a multiplayer
   thread) to continue the conversation. The continued session runs in a new
   pod, so it needs a new approval. A paused workspace is released after
   `SUSPENDED_TTL` (24 hours by default). A session also ends when the agent
   process exits, when `SESSION_TTL` (1 hour from the session's start or its
   latest continuation) runs out, or when its run is revoked or expires. The status message says
   that the session ended.

## Install

The install has seven steps, in this order. The examples use these Pomerium
hosts; replace them with your own:

| Host | Route to |
|---|---|
| `agentic.example.com` | the agentic authorization server (AS) |
| `harness.example.com` | the Agent Link (port 8090 of the platform) |
| `harness-api.example.com` | the Harness API (port 8081 of the platform) |
| `slack.example.com` | the Slack bot's webhooks |
| `anthropic.example.com` | the Anthropic API |

They also use these namespaces:

| Namespace | Contents |
|---|---|
| `agent-sandbox-system` | the agent-sandbox controller |
| `pomerium` | Pomerium. The sandbox network policy expects it here; [step 4](#4-set-up-the-sandbox-side) shows how to change that. |
| `agentops-system` | the platform release; the sandboxes (SandboxTemplates, SandboxWarmPools, Sandbox pods and the `sandbox-agent` ServiceAccount); AgentTemplates; ClientBindings |
| `agentops-slackbot` | the Slack bot release, its Slack credentials Secret and its channel map |

The platform reads and creates resources only in its own namespace. Put the
sandboxes, AgentTemplates and ClientBindings where you install the platform.

### 1. Install agent-sandbox

```sh
kubectl apply -f deploy/agent-sandbox.yaml
```

[`deploy/agent-sandbox.yaml`](./deploy/agent-sandbox.yaml) is the upstream
agent-sandbox v1.0.3 release asset `sandbox-with-extensions.yaml`, unchanged. It
creates the `agent-sandbox-system` namespace, the controller, and the `Sandbox`,
`SandboxTemplate`, `SandboxClaim` and `SandboxWarmPool` CRDs. The platform is
built against v1.0.3 and its `v1beta1` API (groups `agents.x-k8s.io` and
`extensions.agents.x-k8s.io`). [`deploy/agent-sandbox.md`](./deploy/agent-sandbox.md)
covers changing the version.

### 2. Configure Pomerium as the agentic authorization server

Pomerium needs:

- `runtime_flags: {agentic: true, mcp: true}`;
- a persistent databroker. An in-memory databroker loses every run when
  Pomerium restarts;
- a human identity provider that issues refresh tokens. The approver needs one
  for the consent page to succeed;
- an identity provider for Kubernetes ServiceAccount tokens (`cluster` below).
  Its audience must match the projected tokens of the platform, the sandbox
  sidecar and the Slack bot, which is `pomerium-agentic-as` by default;
- the global `jwt_claims_headers` below. Without it the Agent Link refuses every
  sandbox with `assertion carries no run_id`;
- the routes below.

```yaml
runtime_flags:
  agentic: true
  mcp: true

jwt_claims_headers:
  run_id: run_id
  act.kubernetes.io.namespace: act.kubernetes.io.namespace
  act.kubernetes.io.serviceaccount.name: act.kubernetes.io.serviceaccount.name
  act.kubernetes.io.pod.name: act.kubernetes.io.pod.name
  act.kubernetes.io.pod.uid: act.kubernetes.io.pod.uid

identity_providers:
  cluster:
    issuer: "kubernetes:///"
    audiences: ["pomerium-agentic-as"]
    supported_algs: ["RS256"]

routes:
  # The agentic AS. The platform creates runs here.
  - from: https://agentic.example.com
    prefix: /agentic/runs
    to: pomerium://agentic
    bearer_token_format: jwt
    identity_providers: [cluster]
    preserve_host_header: true
    policy:
      allow:
        and:
          - claim/kubernetes.io.namespace: agentops-system
          - claim/kubernetes.io.serviceaccount.name: agentops

  # The agentic AS. Sandbox sidecars get their run token here.
  - from: https://agentic.example.com
    path: /agentic/token
    to: pomerium://agentic
    bearer_token_format: jwt
    identity_providers: [cluster]
    policy:
      allow:
        and:
          - claim/kubernetes.io.namespace: agentops-system
          - claim/kubernetes.io.serviceaccount.name: sandbox-agent

  # The agentic AS. People approve runs here.
  - from: https://agentic.example.com
    path: /agentic/approve
    to: pomerium://agentic
    pass_identity_headers: true
    preserve_host_header: true
    mcp: { client: {} }
    policy:
      allow:
        and:
          - domain: example.com

  # The Agent Link. Sandbox sidecars dial it with the run token.
  - from: https://harness.example.com
    to: h2c://agentops.agentops-system.svc.cluster.local:8090
    bearer_token_format: agentic_run_token
    timeout: 0s
    idle_timeout: 0s
    pass_identity_headers: true
    policy:
      allow:
        and:
          - claim/act.kubernetes.io.namespace: agentops-system
          - claim/act.kubernetes.io.serviceaccount.name: sandbox-agent

  # The Harness API. One entry per client ServiceAccount.
  - from: https://harness-api.example.com
    to: h2c://agentops.agentops-system.svc.cluster.local:8081
    bearer_token_format: jwt
    identity_providers: [cluster]
    timeout: 0s
    idle_timeout: 0s
    pass_identity_headers: true
    policy:
      allow:
        or:
          - claim/sub: system:serviceaccount:agentops-slackbot:agentops-slackbot

  # The Slack bot's webhooks. Slack signs each request and the bot checks it.
  - from: https://slack.example.com
    path: /slack/events
    to: http://agentops-slackbot.agentops-slackbot.svc.cluster.local:80
    allow_public_unauthenticated_access: true
  - from: https://slack.example.com
    path: /slack/interactivity
    to: http://agentops-slackbot.agentops-slackbot.svc.cluster.local:80
    allow_public_unauthenticated_access: true

  # One route per upstream the agent reaches with the run token: the LLM API,
  # each MCP server. Pomerium adds the real API key; the pod never has it.
  - from: https://anthropic.example.com
    to: https://api.anthropic.com
    bearer_token_format: agentic_run_token
    timeout: 0s
    set_request_headers:
      x-api-key: "<your Anthropic API key>"
    remove_request_headers: [Authorization]
    policy:
      allow:
        and:
          - domain: example.com
          - claim/act.kubernetes.io.serviceaccount.name: sandbox-agent
```

Notes on these routes:

- The Agent Link and the Harness API are separate hosts. Pomerium sets the
  assertion's `iss` and `aud` from the route host, so separate hosts keep a
  sandbox's assertion from being valid on the Harness API. The platform refuses
  to start if the two issuers are the same.
- Long-lived gRPC and Connect streams need `timeout: 0s` and `idle_timeout: 0s`.
  Do not use `allow_websockets` on these routes: it forces the upstream to
  HTTP/1.1 and every stream fails.
- The Harness API policy matches the raw `sub` of the client's token. The
  ClientBinding in [step 5](#5-define-an-agent-and-register-the-bot) uses the
  subject that Pomerium mints, which has the identity provider's name in front.
- A route accepts a run token only if it declares
  `bearer_token_format: agentic_run_token`. An MCP server route needs it too.
- The Slack bot's token has the `cluster` provider's audience by default
  (`harnessAPI.tokenAudience`, `pomerium-agentic-as`). The bot mounts no token
  for the AS routes, and the two AS routes that take ServiceAccount tokens
  (`/agentic/runs`, `/agentic/token`) pin the namespace and the ServiceAccount
  in their policies, so they refuse the bot's token. To keep the audiences apart,
  set `harnessAPI.tokenAudience` to a value of its own and give the Harness API
  route an identity provider that accepts it.

[docs/run-identity.md](./docs/run-identity.md) explains each AS route and the
Agent Link route in detail.

### 3. Install the platform

```sh
helm install agentops oci://registry-1.docker.io/pomerium/agentops \
  --version X.Y.Z \
  --namespace agentops-system --create-namespace \
  --set config.agentic.asURL=https://agentic.example.com \
  --set config.harness.externalURL=https://harness.example.com \
  --set config.harness.assertionIssuer=harness.example.com \
  --set config.harness.api.assertionIssuer=harness-api.example.com
```

Always pass `--version`. CI publishes the chart for each GitHub release (version
`X.Y.Z` for the tag `vX.Y.Z`) and a development chart, version
`0.0.0-git-<sha7>`, for each push to `main` that changes `deploy/charts/**`
(see [Continuous integration and releases](#continuous-integration-and-releases)).

To install from a checkout instead, give the chart's path. A chart from a
checkout uses the `appVersion` in its `Chart.yaml` as the image tag, so set
`image.tag` to an image that exists: `main`, the `git-<sha8>` tag of a commit on
`main`, or your own build.

```sh
helm install agentops deploy/charts/agentops \
  --namespace agentops-system --create-namespace \
  --set image.tag=git-<sha8> \
  --set config.agentic.asURL=https://agentic.example.com \
  --set config.harness.externalURL=https://harness.example.com \
  --set config.harness.assertionIssuer=harness.example.com \
  --set config.harness.api.assertionIssuer=harness-api.example.com
```

The four `config.*` values are required. The two issuers are the hosts of the
Agent Link and Harness API routes. If the platform cannot reach the public hosts
from inside the cluster, set `config.agentic.dialAddress` to the in-cluster
Pomerium Service. If Pomerium uses a private CA, mount it and set
`config.agentic.caFile`. Every value is in
[the chart's README](./deploy/charts/agentops/README.md).

The chart installs the AgentTemplate and ClientBinding CRDs with
`helm.sh/resource-policy: keep`, so `helm uninstall` leaves the CRDs and every
AgentTemplate and ClientBinding in place (see
[the chart's README](./deploy/charts/agentops/README.md#crds)). It does not
install agent-sandbox, Pomerium, the sandboxes or any client.

### 4. Set up the sandbox side

You need three things:

- a harness image for the agent. Build one from
  [`deploy/harness/`](./deploy/harness) with
  `make harness-build HARNESS=claude-code` (tag `claude-code:dev`) and push it to
  a registry your cluster can pull from. CI does not publish harness images;
- the sidecar image `pomerium/agentops-sidecar`, which CI publishes;
- a kustomize overlay with your SandboxTemplate, a SandboxWarmPool for it, the
  agentops Component, and a patch with your Pomerium hosts.

[`deploy/examples`](./deploy/examples) is a complete overlay with four
SandboxTemplates and their pools. Three of the templates have a `git-init`, so
the overlay also applies [`git-egress.yaml`](./deploy/examples/git-egress.yaml)
to them (see [Dev to prod is `git push`](#dev-to-prod-is-git-push)). Edit
[`endpoints.yaml`](./deploy/examples/endpoints.yaml) and the `images:` in its
`kustomization.yaml`, then:

```sh
kubectl apply -k deploy/examples
```

Your own overlay has the same shape:

```yaml
# kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: agentops-system
resources:
  - sandboxtemplate.yaml
  - warmpool.yaml
components:
  - github.com/pomerium/agentops//deploy/components/agentops?ref=vX.Y.Z
images:
  - name: agentops/sidecar
    newName: pomerium/agentops-sidecar
    newTag: vX.Y.Z
patches:
  - path: endpoints.yaml
    target:
      kind: SandboxTemplate
      annotationSelector: agents.pomerium.com/inject=true
```

- `vX.Y.Z` is a release tag. To build from a commit on `main` instead, use the
  commit as the `ref` and its `git-<sha8>` sidecar image as `newTag`.
- `sandboxtemplate.yaml` holds only what belongs to your harness: the `agent`
  container's image, `ACP_AGENT_CMD` and the workspace volume. Annotate it
  `agents.pomerium.com/inject: "true"`. The Component adds the rest (see
  [The sandbox pod contract](#the-sandbox-pod-contract)).
  [`deploy/sandbox`](./deploy/sandbox) has one such template, its warm pool and
  an AgentTemplate.
- `warmpool.yaml` is a SandboxWarmPool that references the template.
- `namespace: agentops-system` is necessary: the Component's `sandbox-agent`
  ServiceAccount and `agentops-egress-only` NetworkPolicy have no namespace of
  their own.
- `endpoints.yaml` sets the hosts the sidecar dials, as in
  [`deploy/examples/endpoints.yaml`](./deploy/examples/endpoints.yaml):

| Sidecar variable | Value |
|---|---|
| `SIDECAR_HTTP_ANTHROPIC_UPSTREAM_URL` | the LLM route, `https://anthropic.example.com` |
| `SIDECAR_AGENTIC_AS_URL` | the AS host, the same as `config.agentic.asURL` |
| `SIDECAR_HARNESS_URL` | the Agent Link route, the same as `config.harness.externalURL`. The platform cannot see the pod's environment and cannot check this. |
| `SIDECAR_HTTP_ANTHROPIC_DIAL_ADDRESS`, `SIDECAR_AGENTIC_AS_DIAL_ADDRESS`, `SIDECAR_HARNESS_DIAL_ADDRESS` | the `host:port` to dial instead of the URL's host, usually the in-cluster Pomerium Service, while Host and SNI stay public. Optional only if the public hosts, resolved inside the pod, reach a Pomerium pod that the network policy admits. |
| `SIDECAR_AGENTIC_CA_FILE` | optional: a private CA for Pomerium. The Agent Link uses it too, unless `SIDECAR_HARNESS_CA_FILE` is set. |

The network policy admits Pomerium pods in one namespace and nothing else (see
below). Every address the sidecar dials, these and an AgentTemplate's MCP
`dialAddress`, must therefore end at a Pomerium pod in that namespace.

The [Component's README](./deploy/components/agentops/README.md) explains the
merge and the workload mode (`agents.pomerium.com/inject: "workload"`).

**Pomerium in another namespace.** The Component's network policy lets the pod
reach kube-dns in `kube-system` and pods labeled
`app.kubernetes.io/name: pomerium` on port 8443 in the namespace `pomerium`, and
nothing else. If your Pomerium runs elsewhere, add a JSON 6902 patch to the
overlay:

```yaml
patches:
  - target:
      kind: SandboxTemplate
      annotationSelector: agents.pomerium.com/inject in (true,workload)
    patch: |-
      - op: replace
        path: /spec/networkPolicy/egress/1/to/0/namespaceSelector/matchLabels/kubernetes.io~1metadata.name
        value: my-pomerium-namespace
  - target:
      kind: NetworkPolicy
      name: agentops-egress-only
    patch: |-
      - op: replace
        path: /spec/egress/1/to/0/namespaceSelector/matchLabels/kubernetes.io~1metadata.name
        value: my-pomerium-namespace
```

The second patch applies to workload mode. Patch
`/spec/networkPolicy/egress/1/to/0/podSelector` or
`/spec/networkPolicy/egress/1/ports/0/port` the same way if your Pomerium pods
have another label or port.

### 5. Define an agent and register the bot

The install runs the `deploy-service` agent
([`agenttemplate-deploy-service.yaml`](./deploy/examples/agenttemplate-deploy-service.yaml),
see [Defining an agent](#defining-an-agent)). It uses the `claude-code` pool
from `deploy/examples`, which has no `git-init`.

A ClientBinding registers one client of the Harness API. The platform refuses
every call from a client that has no ClientBinding.

| Field | Meaning |
|---|---|
| `subject` | The client's subject as Pomerium mints it: `<identity provider>/<sub>`, for example `cluster/system:serviceaccount:agentops-slackbot:agentops-slackbot`. |
| `templates` | The AgentTemplates the client may run. An empty list allows none. |
| `quotas.maxLiveSessions` | Maximum sessions that are not ended. |
| `quotas.maxPendingApprovals` | Maximum sessions that are still launching or waiting for approval. |
| `quotas.createRatePerMinute` | Maximum new sessions per minute. |

Before you apply them, edit both files:

- In `agenttemplate-deploy-service.yaml`, replace the example MCP URLs
  (`https://github-mcp.example.com/mcp`, `https://k8s-mcp.example.com/mcp`) with
  your own Pomerium routes. The sidecar sends the run token as the bearer to
  every `requiredMCPServers` URL, so each URL must be a Pomerium route that you
  control and that declares `bearer_token_format: agentic_run_token`. Never put
  a third-party host there.
- In [`clientbinding-slack.yaml`](./deploy/examples/clientbinding-slack.yaml),
  `templates` lists `runid` (the agent in [`deploy/sandbox`](./deploy/sandbox))
  and `deploy-service`. Add every other agent that your channels map to. Check
  that `subject` matches your identity provider's name and the bot's release
  and namespace.

Then apply them in the platform's namespace:

```sh
kubectl apply -f deploy/examples/agenttemplate-deploy-service.yaml
kubectl apply -f deploy/examples/clientbinding-slack.yaml
```

### 6. Install the Slack bot

First create the Slack app ([Slack app setup](#slack-app-setup)) and install it
to your workspace. Copy [`deploy/secret.example.yaml`](./deploy/secret.example.yaml)
to `slack-secret.yaml` and fill in the app's signing secret and bot token, then:

```sh
kubectl create namespace agentops-slackbot
kubectl apply -f slack-secret.yaml
helm install agentops-slackbot oci://registry-1.docker.io/pomerium/agentops-slackbot \
  --version X.Y.Z \
  --namespace agentops-slackbot \
  --set harnessAPI.url=https://harness-api.example.com \
  --set slack.existingSecret=agentops-slackbot-slack \
  --set slack.channels.C0123ABCDEF=deploy-service
```

The chart versions are the same as in [step 3](#3-install-the-platform). From a
checkout, use `helm install agentops-slackbot deploy/charts/agentops-slackbot`
with the same flags, without `--version`, and set `image.tag` as in step 3.

Each `slack.channels` entry binds a channel ID to an AgentTemplate. The bot
authenticates to the Harness API route with a projected ServiceAccount token.
Its subject comes from the release: the release `agentops-slackbot` in the
namespace `agentops-slackbot` is
`system:serviceaccount:agentops-slackbot:agentops-slackbot`, the subject in the
route policy (step 2) and the ClientBinding (step 5). Every value is in
[the chart's README](./deploy/charts/agentops-slackbot/README.md).

When the bot starts, it calls the Harness API. The platform logs each client's
subject once, the first time it admits the client:

```sh
kubectl -n agentops-system logs sts/agentops | grep "admitted a client"
```

The `client_id` in that line must equal `spec.subject` in the ClientBinding. If
it does not, correct the binding and apply it again.

### 7. Connect Slack

When the bot runs and the Slack routes reach it:

1. In the Slack app's **Event Subscriptions**, verify the request URL
   `https://slack.example.com/slack/events`. Slack sends a challenge, and the
   bot answers it.
2. Invite the bot to each bound channel.
3. Mention the bot in one of them.

## Configuration

Both processes read environment variables only. Each chart sets them from its
values, and each chart's README lists every value, its default and the variable
it sets:

- the platform: [`deploy/charts/agentops`](./deploy/charts/agentops/README.md),
  which sets the variables that
  [`harness/internal/config/config.go`](./harness/internal/config/config.go)
  reads;
- the Slack bot:
  [`deploy/charts/agentops-slackbot`](./deploy/charts/agentops-slackbot/README.md),
  which sets the variables that
  [`slackbot/internal/config/config.go`](./slackbot/internal/config/config.go)
  reads.

Agents are configured with cluster resources: see
[Defining an agent](#defining-an-agent).

## Defining an agent

An agent is an `AgentTemplate` (`agents.pomerium.com/v1alpha1`) in the
platform's namespace. It references a `SandboxWarmPool` by name, and the pool
references a `SandboxTemplate`. A client asks for an agent by the template's
`metadata.name`; the Slack bot gets the name from its channel map.

Spec fields
([`harness/apis/v1alpha1/agenttemplate_types.go`](./harness/apis/v1alpha1/agenttemplate_types.go)):

| Field | Required | What it does |
|---|---|---|
| `warmPoolRef.name` | yes | The `SandboxWarmPool` that the session's `SandboxClaim` uses. |
| `systemPrompt` | no | The system prompt. It goes to the agent in the ACP `session/new` request, not in the environment. |
| `requiredMCPServers` | no | `{name, url}` and an optional `dialAddress` for each MCP server. The sidecar serves each one on a loopback port and adds the run token as the bearer, so `url` must be a Pomerium route that you control. `dialAddress` follows the rule for the sidecar's dial addresses ([step 4](#4-set-up-the-sandbox-side)). The consent page shows the list, with a Connect link for each upstream account that the approver has not authorized yet. |
| `sessionConfig` | no | ACP session options set after the session opens. See [below](#harness-session-configuration-sessionconfig). |

A run carries no grant. `requiredMCPServers` is what the approver sees, not what
the run token may reach. Each Pomerium route decides that itself: it accepts a
run token only if it declares `bearer_token_format: agentic_run_token`, and its
policy decides which runs it admits, usually by the approver's identity and the
`act.*` claims of the sealed pod.

The sidecar adds the run token to every request to every `requiredMCPServers`
URL. Each URL must therefore be a Pomerium route that you control, never a
third-party host, which would receive the run token. The examples use
placeholder hosts under `example.com`; replace them with your routes.

Examples in [`deploy/examples/`](./deploy/examples):

| Example | Name | What it shows |
|---|---|---|
| [`agenttemplate-deploy-service.yaml`](./deploy/examples/agenttemplate-deploy-service.yaml) | `deploy-service` | A system prompt and two MCP servers, on the generic `claude-code` pool. |
| [`agenttemplate-gstack.yaml`](./deploy/examples/agenttemplate-gstack.yaml) | `gstack` | Checks out the `garrytan/gstack` skills repository, with Linear, Notion, GitHub and PostHog MCP servers. |
| [`agenttemplate-gcloud.yaml`](./deploy/examples/agenttemplate-gcloud.yaml) | `gcloud` | Checks out Google's `google/skills` repository, with Google Cloud MCP servers, a `sessionConfig` that selects the model, and a `dialAddress`. |

A minimal one, using the template and pool in [`deploy/sandbox`](./deploy/sandbox):

```yaml
apiVersion: agents.pomerium.com/v1alpha1
kind: AgentTemplate
metadata:
  name: runid
  namespace: agentops-system
spec:
  systemPrompt: |
    You are a demo agent invoked from Slack.
  warmPoolRef:
    name: claude-code-runid
```

### Warm pools

Each session creates a `SandboxClaim` that has only the warm-pool reference.
Everything specific to the session reaches the sidecar over
the Agent Link. So a claim can adopt a pre-started pod from the pool, and
`replicas` sets the trade-off:

- `replicas: 0`: no pod waits, and each launch starts a pod. The pools in
  `deploy/examples` use this.
- `replicas: N`: N started pods wait. The platform can ask for approval only
  after it knows which pod the run is sealed to, so a cold start also delays the
  approval message.

A pooled pod keeps the SandboxTemplate it started from, and its git checkout.
After you change a template, delete the pooled Sandboxes so the pool makes new
ones.

### Binding a channel

The Slack bot decides which agent a mention runs from the channel. The map from
channel ID to AgentTemplate name is the bot's own configuration, set in its Helm
values and mounted from a ConfigMap. The platform never sees a channel.

```yaml
slack:
  channels:
    C0123ABCDEF: deploy-service   # channel ID -> AgentTemplate name
    C0456GHIJKL: gcloud
  defaultChannelTemplate: ""      # the agent for a channel not listed above
```

Leave `defaultChannelTemplate` empty unless every channel the bot is invited to
may start that agent. To find a channel's ID in Slack, open the channel name and
then "View channel details".

A mention in a channel with no binding gets a reply that no agent is configured.
A binding to an AgentTemplate that does not exist, or that the bot's
ClientBinding does not list, gets a reply that the binding is a configuration
problem. The bot re-reads the map every 30 seconds, so an edit to the ConfigMap
takes effect without a restart, after the kubelet updates the mounted file. A
`helm upgrade` that changes `slack.channels` restarts the pod.

### Harness session configuration (`sessionConfig`)

`spec.sessionConfig` sets options that the harness advertises, most usefully
the model. The harness lists its options in the ACP `session/new` response, and
the runner in the pod sets each configured one with
`session/set_config_option`. ACP has no separate method for the model: `model`
is an option like any other.

```yaml
spec:
  sessionConfig:
    model: opus    # claude-code accepts aliases (opus, sonnet, haiku) or full model IDs
    effort: high
```

Keys are option IDs. Values are the ID of the option's value, or `"true"` or
`"false"` for a boolean option. The claude-code harness
([`@agentclientprotocol/claude-agent-acp`](https://github.com/agentclientprotocol/claude-agent-acp))
advertises `model`, `effort` and `mode`. Other harnesses advertise their own.

The check is strict. If the harness does not advertise an option, or refuses a
value, the session fails to launch. The thread says only that the bot could
not start the workspace. The platform log (`activate failed`) has the useful
error: it names the bad option and lists the options the harness supports.
`model` is set first, because the harness can change other options, such as the
valid `effort` levels, when the model changes.

## Agent harnesses

A harness is the container image of the sandbox's `agent` container: an agent
that speaks ACP on stdio. The container's entrypoint is **agent-runner**. It
serves a unix socket on an emptyDir that it shares with the sidecar, and when
the platform asks, it starts `/bin/sh -lc 'exec ${ACP_AGENT_CMD:-acp-agent}'`
and is that agent's ACP client. The runner is in the sidecar image. An init
container copies it to the shared emptyDir, so a harness image needs no
agentops binary.

A harness image needs `/bin/sh`, `git`, the ACP agent on `PATH`, a non-root user
(uid 1000 in the shipped images; the pod's `fsGroup` gives it the workspace
volume), and a writable `HOME` and `/workspace`. The image's own `CMD` does not
matter: the SandboxTemplate replaces the command with the runner.
[`deploy/harness/claude-code/Dockerfile`](./deploy/harness/claude-code/Dockerfile)
is the reference.

For unattended use, the agent must not stop for permission. The runner asks
every agent for the ACP session mode `bypassPermissions` and continues if the
agent refuses it. Each harness below sets its own unattended mode. A permission
request that still comes through appears as buttons in the thread, and it is
cancelled after 5 minutes without an answer.

The shipped harnesses add two `bash` scripts, so their images need `bash`:
`agent-entrypoint` (the `ACP_AGENT_CMD`: it adds the workspace to git's
`safe.directory`, because the volume's owner is not the agent's user, and then
starts the ACP agent) and `git-checkout` (the command of the `git-init` init
container, described below). If `ACP_AGENT_CMD` names a binary, you need
neither.

There is one folder per harness in [`deploy/harness/`](./deploy/harness). Build
one with `make harness-build HARNESS=<folder>` (tag `<folder>:dev`). CI does not
publish harness images.

| Harness | Agent | ACP integration | API key variables | Unattended mode | Status |
|---|---|---|---|---|---|
| [`claude-code`](./deploy/harness/claude-code) | Claude Code (Anthropic) | [`@agentclientprotocol/claude-agent-acp`](https://github.com/agentclientprotocol/claude-agent-acp) adapter | `ANTHROPIC_API_KEY` | `bypassPermissions` session mode (refused when the agent runs as root) | reference; the e2e suite runs it |
| [`codex`](./deploy/harness/codex) | Codex CLI (OpenAI) | [`@zed-industries/codex-acp`](https://github.com/zed-industries/codex-acp) adapter | `OPENAI_API_KEY` or `CODEX_API_KEY` | `-c approval_policy=never -c sandbox_mode=danger-full-access` | no tests |
| [`gemini`](./deploy/harness/gemini) | Gemini CLI (Google) | native: `gemini --acp` | `GEMINI_API_KEY` | `--approval-mode yolo` | no tests |
| [`opencode`](./deploy/harness/opencode) | [OpenCode](https://opencode.ai) | native: `opencode acp` | `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, … | built-in config: `permission.{edit,bash,webfetch}: allow` | no tests |
| [`pi`](./deploy/harness/pi) | [pi](https://github.com/earendil-works/pi) | community [`pi-acp`](https://github.com/svkozak/pi-acp) adapter | `ANTHROPIC_API_KEY`, … | pi does not gate tools | experimental: the adapter is an MVP |
| [`hermes`](./deploy/harness/hermes) | [Hermes Agent](https://github.com/NousResearch/hermes-agent) (Nous Research) | native: `hermes-acp` (`hermes-agent[acp]`) | `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `NOUS_API_KEY`, … | no documented always-approve setting: test it before unattended use | experimental |
| [`demo`](./deploy/harness/demo) | none: fixed output, no LLM | the `acp-go-sdk` example agent | none | not applicable: it sends a sample permission request | for testing the launch path |

`demo` is not a real harness. It is an Alpine image with no `bash`, no
`git-checkout` and no `USER` (the pod's `securityContext` makes it non-root).
Use it to test the launch path without an LLM key, not as a template.

The agentops Component routes only the Anthropic API through Pomerium: it sets
`ANTHROPIC_BASE_URL` to the sidecar's port 9999. A harness that uses another
provider needs its own `SIDECAR_HTTP_<NAME>_*` endpoint and base URL in its
SandboxTemplate.

Other ACP agents work the same way, for example Goose (`goose acp`), Qwen Code
(`qwen --acp`) and OpenHands (`openhands acp`). The
[ACP agent registry](https://agentclientprotocol.com/get-started/registry) lists
more. Follow the `claude-code` Dockerfile.

### The sandbox pod contract

The agentops Component
([`deploy/components/agentops/inject.yaml`](./deploy/components/agentops/inject.yaml))
adds all of this to each SandboxTemplate annotated
`agents.pomerium.com/inject: "true"`:

- `serviceAccountName: sandbox-agent` (the ServiceAccount that the Pomerium
  policies name) and `automountServiceAccountToken: false`, so the agent
  container has no Kubernetes credentials;
- `dnsPolicy: ClusterFirst`, so the sidecar can resolve the in-cluster Pomerium
  Service;
- a `networkPolicy`: egress to kube-dns and Pomerium only, ingress from the
  agent-sandbox router only;
- an init container `agentops-init` from the sidecar image that runs
  `agent-runner install /opt/agentops/agent-runner` into a shared emptyDir;
- on the `agent` container: `command: ["/opt/agentops/agent-runner"]`,
  `ANTHROPIC_BASE_URL=http://127.0.0.1:9999`, a placeholder `ANTHROPIC_API_KEY`
  and the runner's socket directory;
- a `sidecar` container that runs `sidecar serve user-identity`, with a
  projected ServiceAccount token (audience `pomerium-agentic-as`, 600 seconds)
  at `/var/run/agentic`. Only the sidecar mounts it. Its Anthropic endpoint on
  port 9999 adds the run token to each request.

Your template supplies the `agent` container's image, `ACP_AGENT_CMD` and the
workspace volume at `/workspace`. Your overlay supplies the sidecar's hosts. The
container names are part of the contract: `agent`, `sidecar` and, for a git
checkout, `git-init`.

The LLM API key can reach the agent in two ways. The SandboxTemplate chooses:

- **At Pomerium** (what the Component sets up): the sidecar adds the run token,
  and the Pomerium route checks it and sets the real key with
  `set_request_headers`. The pod has no LLM key.
- **In the sidecar**: `SIDECAR_HTTP_<NAME>_HEADER_<HEADER>` on the sidecar
  container, from a `secretKeyRef`. The key is in the pod but not in the agent
  container. Example Secret:
  [`secret.claude-code.example.yaml`](./deploy/examples/secret.claude-code.example.yaml).

For a **git checkout**, add a `git-init` init container that runs
[`git-checkout`](./deploy/harness/git-checkout.sh) (in every shipped harness
image except `demo`), with `GIT_REPO_URL`, `GIT_REPO_REF` (default `HEAD`) and,
for a private repository, `GIT_TOKEN` and optionally `GIT_USERNAME` from a
`secretKeyRef`. The script gives git the credentials through `GIT_ASKPASS`,
never on the command line. The init container exits before the agent starts, so
the token is never in the agent's environment, the platform or the
`SandboxClaim`. If the workspace already has a checkout, the script does
nothing.
[`sandboxtemplate-pomerium-zero-claude-code.yaml`](./deploy/examples/sandboxtemplate-pomerium-zero-claude-code.yaml)
is an example. The Component's network policy blocks the checkout, so a
template with a `git-init` also needs an egress rule such as
[`git-egress.yaml`](./deploy/examples/git-egress.yaml). That rule lets the
agent container reach public HTTPS hosts too (see
[Dev to prod is `git push`](#dev-to-prod-is-git-push)).

## Writing a client

The Slack bot is one client of the Harness API. Any program can be another. A
client needs:

- a token that the Harness API's Pomerium route accepts (the Slack bot uses a
  projected ServiceAccount token), and an entry for it in that route's policy;
- a ClientBinding that names the subject Pomerium mints for it and lists the
  AgentTemplates it may run.

[`proto/harnessapi/v1/harnessapi.proto`](./proto/harnessapi/v1/harnessapi.proto)
is the contract: `CreateSession`, `Prompt`, `RespondPermission`, `EndSession`,
`GetSession`, `ListSessions`, `ListTemplates`, `ListEvents` and `Subscribe`,
over Connect.

| Language | Client | Example |
|---|---|---|
| TypeScript | [`sdk/ts`](./sdk/ts) (`@pomerium/agentops-harness`) | [`hello-session.ts`](./sdk/ts/examples/hello-session.ts) |
| Python | [`sdk/python`](./sdk/python) (`agentops-harness`) | [`hello_session.py`](./sdk/python/examples/hello_session.py) |
| Go | [`harness/api/client`](./harness/api/client) | the Slack bot, [`slackbot/cmd/slackbot`](./slackbot/cmd/slackbot) |

`make apistub` builds a conformance server, `bin/apistub`: the real API server
and Connect transport in front of a scripted in-memory implementation. It needs
no cluster and no credentials, and it can produce the failures a client must
handle, such as a stream that drops mid-flight or a field the client does not
know. [`docs/sdk-conformance.md`](./docs/sdk-conformance.md) lists them.
`make sdk-test` runs both SDKs' conformance suites against it.

## Slack app setup

You create the Slack app yourself. At <https://api.slack.com/apps>, select
**Create New App**, then **From a manifest**. Paste the manifest below with your
own request URLs, then select **Install to Workspace** to get the bot token. The
signing secret is under **Basic Information**.

Give the two credentials to the bot chart, as `slack.signingSecret` and
`slack.botToken` or in a Secret named by `slack.existingSecret` with the keys
`SLACK_SIGNING_SECRET` and `SLACK_BOT_TOKEN`.

The app is private to one workspace. The bot drops events from other workspaces.
It learns its own user ID and workspace ID at startup with `auth.test`.

The bot finds mentions in **message** events: Slack delivers a channel mention
as a message whose text contains `<@bot-id>`. It needs no `app_mention`
subscription and ignores `app_mention` events.

- **Event Subscriptions**: request URL `https://<your-host>/slack/events`, bot
  events `message.channels`, `message.groups`, `message.im` and `message.mpim`.
  Slack checks the URL with a challenge, so the bot must be running and
  reachable before the URL saves.
- **Interactivity**: request URL `https://<your-host>/slack/interactivity`, for
  the permission buttons.
- **Bot token scopes**: `chat:write` (thread messages and direct messages),
  `reactions:write` (the progress reactions), and the history scope for each
  subscribed surface (`channels:history`, `groups:history`, `im:history`,
  `mpim:history`). The bot reads threads with `conversations.replies`.
- **A custom emoji named `waiting`.** The bot marks a running turn with a
  `:waiting:` reaction, and `waiting` is not a standard Slack emoji. Add a
  custom emoji with that name to the workspace. Without it, Slack rejects the
  reaction and the bot logs this warning, with `"emoji":"waiting"`, on every
  turn:

  ```
  add reaction failed; grant the bot the reactions:write scope to show launch progress
  ```

  The warning names the scope, but when only the `waiting` reaction fails, the
  cause is the missing emoji, not a missing scope.

### Manifest

```json
{
  "display_information": {
    "name": "AgentOps"
  },
  "features": {
    "bot_user": {
      "display_name": "AgentOps",
      "always_online": false
    }
  },
  "oauth_config": {
    "scopes": {
      "bot": [
        "channels:history",
        "groups:history",
        "im:history",
        "mpim:history",
        "chat:write",
        "reactions:write"
      ]
    }
  },
  "settings": {
    "event_subscriptions": {
      "request_url": "https://slack.example.com/slack/events",
      "bot_events": [
        "message.channels",
        "message.groups",
        "message.im",
        "message.mpim"
      ]
    },
    "interactivity": {
      "is_enabled": true,
      "request_url": "https://slack.example.com/slack/interactivity"
    },
    "org_deploy_enabled": false,
    "socket_mode_enabled": false,
    "token_rotation_enabled": false
  }
}
```

## Continuous integration and releases

GitHub Actions workflows in [`.github/workflows`](./.github/workflows):

- **`test.yaml`**, on pushes to `main` and on pull requests: `make vet`,
  `make boundary`, `make telemetry-in-sync`, `make proto-check`, `make build`
  and `make test` (without the opt-in e2e suite), and the SDK checks
  (`make sdk-generate-check`, `make sdk-test-ts`, `make sdk-test-py`).
- **`docker.yaml`** builds `pomerium/agentops`, `pomerium/agentops-slackbot`
  and `pomerium/agentops-sidecar`. Pull requests build `linux/amd64` only and
  push nothing. Pushes to `main` publish `:main` and `:git-<sha8>` for
  `linux/amd64` and `linux/arm64`. Tags `vX.Y.Z` publish `:vX.Y.Z`, `:latest`
  and `:git-<sha8>`. Harness images are not built.
- **`helm.yaml`** lints and renders both charts on pull requests, including
  `make helm-check-client-isolation`. It publishes both charts to
  `oci://registry-1.docker.io/pomerium` on each push to `main` that changes
  `deploy/charts/**` (version `0.0.0-git-<sha7>`, `appVersion` `git-<sha8>`,
  the image tag of the same commit), and on each published GitHub release
  (version = the tag without `v`, `appVersion` = the tag).

On a pull request, the jobs run only if no other open pull request is stacked
on it ([`stack-top.yaml`](./.github/workflows/stack-top.yaml)).

To release, push a `vX.Y.Z` tag and publish a GitHub release for it. The tag
builds the images, and the release publishes the charts, whose `appVersion`
selects the images of the same tag.

The workflows need two repository secrets: `DOCKERHUB_USER` and
`DOCKERHUB_TOKEN` (a DockerHub access token that can push to the `pomerium`
organization).
