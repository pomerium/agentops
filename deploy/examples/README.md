# Example sandboxes, agent templates and client binding

Example manifests for the namespace `agentops-system`, where the platform runs
and creates its SandboxClaims. Copy them and change what is particular to your
environment.

| File | Kind | Applied with |
|---|---|---|
| [`kustomization.yaml`](kustomization.yaml) | Kustomization | `kubectl apply -k deploy/examples` |
| [`endpoints.yaml`](endpoints.yaml) | SandboxTemplate patch | the kustomization |
| [`git-egress.yaml`](git-egress.yaml) | SandboxTemplate patch (JSON 6902) | the kustomization, on the three templates with a `git-init` |
| [`sandboxtemplate-claude-code.yaml`](sandboxtemplate-claude-code.yaml) | SandboxTemplate | the kustomization |
| [`sandboxtemplate-pomerium-zero-claude-code.yaml`](sandboxtemplate-pomerium-zero-claude-code.yaml) | SandboxTemplate | the kustomization |
| [`sandboxtemplate-gstack-claude-code.yaml`](sandboxtemplate-gstack-claude-code.yaml) | SandboxTemplate | the kustomization |
| [`sandboxtemplate-google-skills-claude-code.yaml`](sandboxtemplate-google-skills-claude-code.yaml) | SandboxTemplate | the kustomization |
| `sandboxwarmpool-*.yaml` (four files) | SandboxWarmPool | the kustomization |
| [`agenttemplate-deploy-service.yaml`](agenttemplate-deploy-service.yaml) | AgentTemplate | `kubectl apply -f` |
| [`agenttemplate-gcloud.yaml`](agenttemplate-gcloud.yaml) | AgentTemplate | `kubectl apply -f` |
| [`agenttemplate-gstack.yaml`](agenttemplate-gstack.yaml) | AgentTemplate | `kubectl apply -f` |
| [`clientbinding-slack.yaml`](clientbinding-slack.yaml) | ClientBinding | `kubectl apply -f` |
| [`secret.claude-code.example.yaml`](secret.claude-code.example.yaml) | Secret | `kubectl apply -f`, after you copy it and fill in the key |

## Order

1. Install agent-sandbox ([`../agent-sandbox.yaml`](../agent-sandbox.yaml)) and
   the platform chart in `agentops-system`. The chart installs the
   `AgentTemplate` and `ClientBinding` CRDs.
2. Create the Secrets that the templates reference. Only
   `sandboxtemplate-pomerium-zero-claude-code.yaml` needs one:

   ```sh
   kubectl -n agentops-system create secret generic pomerium-zero-git-credentials \
     --from-literal=GIT_TOKEN=<token>
   ```

3. Edit `endpoints.yaml` and the `images:` in `kustomization.yaml`, then apply
   the sandboxes:

   ```sh
   kubectl apply -k deploy/examples
   ```

4. Replace the example MCP URLs in `agenttemplate-deploy-service.yaml` with
   your own Pomerium routes (see [The agent templates](#the-agent-templates)),
   then apply it. It names the `claude-code` pool, which has no `git-init`.
   Each AgentTemplate names a warm pool, so apply the pools first or at the
   same time:

   ```sh
   kubectl apply -f deploy/examples/agenttemplate-deploy-service.yaml
   ```

   `agenttemplate-gcloud.yaml` and `agenttemplate-gstack.yaml` work the same
   way. Their pools check out a git repository, so their pods carry the
   `git-egress.yaml` rule.

5. Edit the ClientBinding for the Slack bot. Its `templates` lists `runid` and
   `deploy-service`; add every other agent that your channels map to. Then
   apply it:

   ```sh
   kubectl apply -f deploy/examples/clientbinding-slack.yaml
   ```

   Once the Slack bot runs, check its subject in the platform log (see
   [`clientbinding-slack.yaml`](#clientbinding-slackyaml)).

## The sandboxes

### `kustomization.yaml`

The kustomization that a user writes: four SandboxTemplates and their pools, the
agentops Component ([`../components/agentops`](../components/agentops/README.md)),
one patch for where the sidecars dial, one patch for git egress, and
`namespace: agentops-system`. The component merges the sidecar, the runner, the
projected token, the network policy and the `sandbox-agent` ServiceAccount into
each template that carries `agents.pomerium.com/inject: "true"`.

`images:` maps the placeholders to the local tags that
`make harness-build HARNESS=claude-code` (`claude-code:dev`) and
`make sidecar-build` (`agentops-sidecar:dev`) produce. Point them at your
registry for a real cluster. The sidecar is published as
`pomerium/agentops-sidecar`. Harness images are not published; build and push
your own from [`../harness`](../harness).

### `endpoints.yaml`

A strategic-merge patch, keyed by container and variable name, on the `sidecar`
container. The kustomization applies it to every injected template, so its
`metadata.name` is not used. Every URL in it is a Pomerium route:

| Variable | Meaning |
|---|---|
| `SIDECAR_HTTP_ANTHROPIC_UPSTREAM_URL` | The LLM route. It admits the run token (`bearer_token_format: agentic_run_token`) and sets the real API key, so no key exists in the pod. |
| `SIDECAR_AGENTIC_AS_URL` | The agentic authorization server, where the sidecar exchanges its projected token for the run token. |
| `SIDECAR_HARNESS_URL` | The Agent Link route. It must name the same route as the platform's `HARNESS_EXTERNAL_URL` (chart value `config.harness.externalURL`), because that route's policy admits the sandbox's attach. |
| `*_DIAL_ADDRESS` | The in-cluster Pomerium Service to connect to. TLS SNI and the `Host` header still come from the URL. Set your own Service. Remove the three lines only if the public hosts, resolved inside the pod, reach a Pomerium pod that the network policy admits. |

Where Pomerium presents a private CA, also mount it into the sidecar and set
`SIDECAR_AGENTIC_CA_FILE`. The component's README has
[the patch](../components/agentops/README.md#what-you-write). If Pomerium does
not run in the namespace `pomerium`, add
[the network policy patch](../components/agentops/README.md#pomerium-in-another-namespace)
to the kustomization.

The network policy admits Pomerium pods in one namespace and nothing else in the
cluster. Every address the sidecar dials, these three and an AgentTemplate's MCP
`dialAddress`, must end at a Pomerium pod in that namespace.

### `git-egress.yaml`

A JSON 6902 patch that appends one egress rule to the template's
`networkPolicy`: TCP 443 to `0.0.0.0/0` except `10.0.0.0/8`, `172.16.0.0/12`
and `192.168.0.0/16`. The component's policy admits only DNS and Pomerium, and
agent-sandbox applies it to the whole pod, init containers included. Without
this rule a `git-init` cannot reach github.com. The kustomization applies the
patch to `pomerium-zero-claude-code`, `gstack-claude-code` and
`google-skills-claude-code`.

The rule also applies to the whole pod. In those three templates the agent
container can reach any public HTTPS host directly, without Pomerium and
without the run token. To narrow the rule, replace `0.0.0.0/0` with your git
host's address ranges. `claude-code` has no `git-init`, gets no patch, and keeps
the DNS-and-Pomerium-only policy.

### SandboxTemplates

All four run the claude-code harness from
[`../harness/claude-code`](../harness/claude-code) and share this shape:

- the annotation `agents.pomerium.com/inject: "true"`, which the component
  selects;
- one container, named `agent`. The component merges into it by that name;
- `runAsUser: 1000`, `runAsGroup: 1000`, `runAsNonRoot: true`. The claude-code
  image runs as its `node` user, uid 1000. `fsGroup: 1000` makes the workspace
  volume writable for it;
- `ACP_AGENT_CMD=agent-entrypoint`: what the runner starts when the platform
  asks. It must start a process that speaks ACP on stdin and stdout;
- `CLAUDE_CONFIG_DIR=/workspace/.agentops/claude`: the agent's transcript goes on
  the workspace PVC, so it survives a suspend and a revive. Without it a paused
  conversation cannot be resumed;
- `CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1`: turns off telemetry, error
  reports and update checks. In `claude-code` they could only fail, because the
  pod reaches only DNS and Pomerium. In the templates with `git-egress.yaml` they
  would reach the public internet;
- a `workspace` volume claim template (1Gi), mounted at `/workspace`. The ACP
  session's working directory is `/workspace`.

| Template | Use it for |
|---|---|
| `claude-code` | Workflows without a repository. No init container. Egress to DNS and Pomerium only. |
| `pomerium-zero-claude-code` | A private repository checked out into the workspace: `https://github.com/pomerium/pomerium-zero-skills.git`, with credentials from the Secret `pomerium-zero-git-credentials`. Gets `git-egress.yaml`. |
| `gstack-claude-code` | A public repository: `https://github.com/garrytan/gstack.git`. No credentials. Gets `git-egress.yaml`. |
| `google-skills-claude-code` | A public repository: `https://github.com/google/skills.git`. No credentials. Gets `git-egress.yaml`. |

A template that checks out a repository is particular to one workflow, because
the repository and its credentials are part of it. To bake your own, copy the
pomerium-zero pair (template and pool), change the names, the URL and the
Secret, and add the copy's name to the `git-egress.yaml` target in
`kustomization.yaml`.

`harness/internal/sandbox/template_yaml_test.go` checks these files: one
container named `agent`, no secrets on it, `CLAUDE_CONFIG_DIR` on a PVC, a
required `GIT_TOKEN` reference on the private-repo template, and no Secret on
the public-repo ones.

### The `git-init` contract

The checkout runs in an init container named `git-init`, from the harness
image. Its command is `git-checkout`, which the image installs from
[`../harness/git-checkout.sh`](../harness/git-checkout.sh). It must mount the
workspace at `/workspace`. The pod needs the egress rule from
[`git-egress.yaml`](#git-egressyaml), or the fetch cannot reach the git host.

| Variable | Meaning |
|---|---|
| `GIT_REPO_URL` | The repository. If it is unset, the script does nothing. |
| `GIT_REPO_REF` | The ref to fetch. Default `HEAD`. |
| `GIT_TOKEN` | Optional. From a `secretKeyRef`, never a literal. The script gives it to git through `GIT_ASKPASS`, so it is not in argv and not in the stored git config. |
| `GIT_USERNAME` | Optional. Default `x-access-token`, the username for GitHub token authentication. |
| `WORKSPACE_DIR` | Optional. Default `/workspace`. |

The script does a shallow fetch of the ref and checks it out. If the workspace
already holds a `.git`, it does nothing, so a revived sandbox keeps its
checkout. The init container exits before the agent container starts, so the
token never reaches the agent.

In `pomerium-zero-claude-code` the `GIT_TOKEN` reference is not optional. A
missing Secret fails the pod instead of falling back to an unauthenticated
clone. `GIT_USERNAME` comes from the same Secret and is optional.

### SandboxWarmPools

One pool per template, with the same name. A SandboxClaim names a pool, not a
template, and an AgentTemplate selects its pool by `spec.warmPoolRef.name`.

The pools have `replicas: 0`: nothing is pre-started, and every claim starts a
fresh pod. Raise `replicas` to keep pods started and waiting. The platform's
claims carry no `env` and no `volumeClaimTemplates`, so agent-sandbox lets them
adopt a waiting pod. A launch cannot ask for approval until it knows which pod
the run is sealed to, so a warm pod also shortens the wait for the approval
prompt. Each waiting pod costs its resource requests while it waits.

With `replicas` above 0, a pooled pod keeps the template it started from. After
you change a template, delete the pooled Sandboxes:

```sh
kubectl -n agentops-system delete sandboxes -l agents.x-k8s.io/warm-pool-sandbox
```

## The agent templates

An `AgentTemplate` is one workflow that a client can run by name. The fields
([`harness/apis/v1alpha1/agenttemplate_types.go`](../../harness/apis/v1alpha1/agenttemplate_types.go)):

| Field | Meaning |
|---|---|
| `spec.systemPrompt` | Sent to the agent on ACP `session/new`. A client can append to it per session. |
| `spec.sessionConfig` | ACP session configuration options, set after the session starts, keyed by option id. The value is the option's value id for a select option, or `"true"`/`"false"` for a boolean one. An option id the harness does not advertise, or a value it rejects, fails the launch. |
| `spec.requiredMCPServers` | A list of `name`, `url` and optional `dialAddress`. `name` is unique in the template. The approver sees the list on Pomerium's consent page, with a Connect link for each upstream account not yet authorized. It grants nothing: each Pomerium route decides whether it accepts the run token. The sidecar serves each server on a loopback port from 9100 up, adds the run token as the bearer, and forwards to `url`. The agent gets the loopback URLs with no credentials. |
| `spec.requiredMCPServers[].url` | A Pomerium route that you control and that declares `bearer_token_format: agentic_run_token`. Every request to it carries the run token, so it must never be a third-party host. |
| `spec.requiredMCPServers[].dialAddress` | Optional. The `host:port` the sidecar connects to for this server, for example the in-cluster Pomerium Service. TLS SNI and the `Host` header still come from `url`. Leave it unset only if the host of `url`, resolved inside the pod, reaches a Pomerium pod that the network policy admits. |
| `spec.warmPoolRef.name` | The SandboxWarmPool in the same namespace. |

A channel runs a template through the Slack bot's chart values
(`slack.channels`, channel ID → template name, and `slack.defaultChannelTemplate`).
The bot's ClientBinding must list each of those templates.

| File | Template | Pool | Notes |
|---|---|---|---|
| `agenttemplate-deploy-service.yaml` | `deploy-service` | `claude-code` | No working context. Two placeholder MCP servers. To give it a repository, copy the pomerium-zero template and pool with your own repository, add the copy to the `git-egress.yaml` target, and point `warmPoolRef` at the copy. |
| `agenttemplate-gcloud.yaml` | `gcloud` | `google-skills-claude-code` | Google's skills repository as working context. `sessionConfig` sets `model: sonnet`. Three placeholder MCP servers; `cloud-run` shows `dialAddress`. |
| `agenttemplate-gstack.yaml` | `gstack` | `gstack-claude-code` | The gstack repository as working context. Four placeholder MCP servers: Linear, Notion, GitHub and PostHog. |

Every MCP URL in these files is a placeholder under `example.com`. Before you
apply a template, replace each one with a Pomerium route that you control and
that accepts the run token (`bearer_token_format: agentic_run_token`). The
sidecar sends the run token to every `requiredMCPServers` URL.

## `clientbinding-slack.yaml`

A `ClientBinding` registers one client of the Harness API and says what it may
run. The platform team writes it; a client cannot create, widen or read its own.
Pomerium decides who a client is (the Harness API route's identity providers and
policy). The binding decides what that client may do. A client with no binding
is refused every verb.

The fields
([`harness/apis/v1alpha1/clientbinding_types.go`](../../harness/apis/v1alpha1/clientbinding_types.go)):

| Field | Meaning |
|---|---|
| `spec.subject` | The subject of the assertion Pomerium stamps on the Harness API route. |
| `spec.templates` | The AgentTemplate names the client may run. Empty grants none. A suspended session whose template leaves the list cannot be continued. |
| `spec.quotas.maxLiveSessions` | Caps the client's sessions in any non-terminal state. Checked when a session is created. |
| `spec.quotas.maxPendingApprovals` | Caps the client's sessions that wait for approval, launches in flight included. Checked when a session is created or revived. A session that nobody approves still holds a pod for the whole approval window. |
| `spec.quotas.createRatePerMinute` | Caps how fast the client creates and revives sessions. Each launch puts an approval request in front of a person. |

A quota that is unset or 0 does not apply. A request over a quota fails with
`SENTINEL_QUOTA_EXCEEDED`.

**The subject is provider-prefixed. Take it from the platform log.** Pomerium
mints the subject as `<identity provider>/<sub>`. For the bot's projected
ServiceAccount token, verified by an identity provider named `cluster`, it is
`cluster/system:serviceaccount:agentops-slackbot:agentops-slackbot`. That is not
the string the route's policy matches: the policy sees the raw `sub` of the
presented token, `system:serviceaccount:agentops-slackbot:agentops-slackbot`,
with no prefix. A wrong policy is a 403 that the bot reports. A wrong binding
subject is a binding that never applies, and every verb is refused. The platform
logs each client's subject once, at Info, the first time it sees the client:

```sh
kubectl -n agentops-system logs sts/agentops | grep "admitted a client"
```

The bot must be running: the line appears when the bot first calls the Harness
API, even before a binding exists. Copy the `client_id` from that line into
`spec.subject`.

The example lists `runid` (from [`../sandbox`](../sandbox/README.md)) and
`deploy-service`. List every template that the bot's channels map to, or those
channels fail.

## `secret.claude-code.example.yaml`

The Secret `claude-code-credentials` with one key, `ANTHROPIC_API_KEY`. The
example templates do not use it: the LLM route in Pomerium sets the key. Use it
only if the sidecar must add the key itself. Then reference it from the
`sidecar` container as `SIDECAR_HTTP_ANTHROPIC_HEADER_X_API_KEY` through a
`secretKeyRef`, and the sidecar sends it upstream as the `x-api-key` header.
Never put it on the `agent` container.

Copy the file, fill in the key, and apply it out of band. Keep real values out
of version control.
