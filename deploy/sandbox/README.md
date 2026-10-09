# Run-identity demo sandbox base

A kustomize base in the namespace `agentops-system` with every per-environment
value left out. It holds:

| File | Object |
|---|---|
| [`sandboxtemplate.yaml`](sandboxtemplate.yaml) | SandboxTemplate `claude-code-runid`: the claude-code harness, annotated `agents.pomerium.com/inject: "true"`. |
| [`warmpool.yaml`](warmpool.yaml) | SandboxWarmPool `claude-code-runid`, `replicas: 1`. |
| [`agenttemplate.yaml`](agenttemplate.yaml) | AgentTemplate `runid`, which runs on that pool. |

**This is not deployable on its own.** The harness image is a placeholder
(`agentops/harness:overlay-must-set`). Without the agentops Component
([`../components/agentops`](../components/agentops/README.md)) the template is a
pod with no sidecar. Without an endpoints patch the sidecar has nowhere to dial.
An overlay adds all three.

## An overlay

```yaml
# kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: agentops-system
resources:
  - github.com/pomerium/agentops//deploy/sandbox?ref=vX.Y.Z
components:
  - github.com/pomerium/agentops//deploy/components/agentops?ref=vX.Y.Z
images:
  - name: agentops/harness
    newName: my-registry/claude-code
    newTag: "1.0"
  - name: agentops/sidecar
    newName: pomerium/agentops-sidecar
    newTag: vX.Y.Z
patches:
  - path: endpoints.yaml
    target:
      kind: SandboxTemplate
      annotationSelector: agents.pomerium.com/inject=true
```

`endpoints.yaml` is the same patch as
[`../examples/endpoints.yaml`](../examples/endpoints.yaml): the three hosts the
sidecar dials and the in-cluster dial addresses. The component's README lists
[the variables](../components/agentops/README.md#what-you-write) and when the
dial addresses can be left out.
The overlay's `namespace:` also places the component's ServiceAccount and
NetworkPolicy. Build the harness image from
[`../harness/claude-code`](../harness/claude-code) with
`make harness-build HARNESS=claude-code`; CI does not publish harness images.

Then apply it and register the template with the client that runs it:

```sh
kubectl apply -k .
```

The bot's ClientBinding must list `runid` in `spec.templates`, as
[`../examples/clientbinding-slack.yaml`](../examples/clientbinding-slack.yaml)
does, and a Slack channel must map to it (chart value `slack.channels`).

## What belongs where

Anything a pod needs **before it has a session** belongs in the SandboxTemplate.
A warm-pool pod is built from it long before any SandboxClaim exists. The
per-session part reaches the sidecar on the Agent Link attach stream. The
platform's claims carry no environment variables, because agent-sandbox skips
the warm pool for a claim that sets `env` or `volumeClaimTemplates`.

In the component, the same for every harness and environment:

- the `sidecar` container and the `agentops-init` init container;
- the `agent` container's runner `command`, `ANTHROPIC_BASE_URL`, the credential
  placeholder and the agentops mounts;
- the projected `pomerium-agentic-as` token, the runner's emptyDirs, the network
  policy, the `sandbox-agent` ServiceAccount;
- the sidecar's loopback port, token file and runner socket.

In this base, particular to the claude-code harness:

- the `agent` container's image placeholder, `ACP_AGENT_CMD`,
  `CLAUDE_CONFIG_DIR` and `CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC`;
- the pod security context (uid and gid 1000, `fsGroup: 1000`) and the
  `workspace` PVC at `/workspace`;
- the warm pool and the agent template.

In the overlay, particular to the environment:

- **images**, through `images:`. This works on the CRD only because the
  component carries `images.yaml`;
- **the hosts the sidecar dials**: `SIDECAR_HTTP_ANTHROPIC_UPSTREAM_URL`,
  `SIDECAR_AGENTIC_AS_URL`, `SIDECAR_HARNESS_URL`;
- **the in-cluster dial addresses**, usually the Pomerium Service:
  `SIDECAR_HTTP_ANTHROPIC_DIAL_ADDRESS`, `SIDECAR_AGENTIC_AS_DIAL_ADDRESS`,
  `SIDECAR_HARNESS_DIAL_ADDRESS`. The network policy admits only Pomerium pods,
  so leave them out only if the public hosts, resolved inside the pod, reach a
  Pomerium pod that the policy admits;
- **a private CA**, where Pomerium presents one (`SIDECAR_AGENTIC_CA_FILE` and
  its mount);
- **a network policy patch**, if Pomerium does not run in the namespace
  `pomerium` ([how](../components/agentops/README.md#pomerium-in-another-namespace)).
  The quickstart's Pomerium runs in `agentops-pomerium`;
- **a patch that drops the component's `sandbox-agent` ServiceAccount**, on an
  install made with the quickstart, whose release owns it (see
  [`../examples/kustomization.yaml`](../examples/kustomization.yaml));
- **resource requests**, where something bills for them;
- **the agent's MCP servers**, because they are per-environment hostnames.

The channel → agent map is not here. It is the bot's own configuration, set
through its chart values.

## The agent template and its MCP servers

`runid` has a system prompt and a pool, and no `requiredMCPServers`. Add them in
the overlay with a patch on the AgentTemplate:

```yaml
apiVersion: agents.pomerium.com/v1alpha1
kind: AgentTemplate
metadata:
  name: runid
spec:
  requiredMCPServers:
    - name: gke
      url: https://gke-mcp.example.com/mcp
```

`requiredMCPServers` is a disclosure, not a grant. The approver sees the list on
Pomerium's consent page, with a Connect link for each upstream account they have
not authorized yet. Each route decides on its own whether it accepts the run
token (`bearer_token_format: agentic_run_token`) and which runs its policy
admits.

The sidecar sends the run token as the bearer to every `url` in the list. Each
one must be a Pomerium route that you control, never a third-party host. A
`dialAddress`, if you set one, must reach a Pomerium pod that the network policy
admits, like the sidecar's own dial addresses.

## The workspace and the transcript

The agent writes its conversation transcript under `CLAUDE_CONFIG_DIR`
(`/workspace/.agentops/claude`). That directory is on the workspace PVC, so the
transcript survives a suspend and a revive, and a paused thread can continue. If
`CLAUDE_CONFIG_DIR` points anywhere off the PVC, the transcript dies with the
pod and no session can be revived. The failure then shows as a conversation that
cannot be resumed, not as a configuration error.
`harness/internal/sandbox/template_yaml_test.go` checks this for the example
templates.

This template has no `git-init`, so the pod reaches only DNS and Pomerium, and
Claude Code's telemetry, error reports and update checks can only fail.
`CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1` turns them off. If you add a
`git-init`, the pod also needs an egress rule for the git host
([how, and what it costs](../components/agentops/README.md#a-git-init-needs-more-egress)).

## The warm pool

`replicas: 1` keeps one pod started and waiting, with its images pulled and the
runner installed. The next claim adopts it. A launch
cannot ask for approval until it knows which pod the run is sealed to, so a
warm pod also shortens the wait for the approval prompt. An adopted pod does not
go back to the pool, and the controller starts a replacement.

A pooled pod keeps the template it started from. After you change the template,
or rebuild an image with the same tag, delete the pooled Sandbox so that the pool
builds it again:

```sh
kubectl -n agentops-system delete sandboxes -l agents.x-k8s.io/warm-pool-sandbox
```

Otherwise the next session adopts a pod built from the old template. For
example, a pod from before `CLAUDE_CONFIG_DIR` gives a session with no
transcript. Setting `spec.updateStrategy.type: Recreate` on the pool makes
agent-sandbox replace stale Sandboxes after a template change; the default,
`OnReplenish`, leaves them until they are deleted or claimed.

## Patches merge by name

The overlay's SandboxTemplate patches are strategic merges that name the
container and the variable, for example `containers: [{name: sidecar, env:
[{name: SIDECAR_HARNESS_URL, ...}]}]`. The component's `schema.json` supplies
those merge keys, so a patch lands on the right entry in any list order, with no
positional paths. See [the component's README](../components/agentops/README.md).
