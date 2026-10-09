# agentops kustomize Component

The agentops half of a sandbox pod, as a
[kustomize Component](https://kubectl.docs.kubernetes.io/guides/config_management/components/).
You write a SandboxTemplate for your harness. The component adds the sidecar, the
runner, the projected token, the network policy and the service account.

The component is the same in every environment. It names no host, no dial
address and no CA. Your overlay sets those with a patch.

## What you write

```yaml
# kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: agentops-system
resources:
  - template.yaml
components:
  - github.com/pomerium/agentops//deploy/components/agentops?ref=vX.Y.Z
images:
  - name: agentops/sidecar
    newName: pomerium/agentops-sidecar
    newTag: vX.Y.Z
patches:
  - path: endpoints.yaml
```

Set `namespace:` to the platform's namespace. The platform creates its
SandboxClaims there, and the component's ServiceAccount and NetworkPolicy carry
no namespace of their own, so `namespace:` puts them next to your templates.

```yaml
# template.yaml: only what is particular to your harness
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxTemplate
metadata:
  name: my-agent
  annotations:
    agents.pomerium.com/inject: "true"
spec:
  podTemplate:
    spec:
      securityContext: {runAsUser: 1000, runAsGroup: 1000, runAsNonRoot: true, fsGroup: 1000}
      containers:
        - name: agent
          image: my-registry/my-harness:1.2
          env:
            - name: ACP_AGENT_CMD
              value: agent-entrypoint
          volumeMounts:
            - name: workspace
              mountPath: /workspace
  volumeClaimTemplates:
    - metadata: {name: workspace}
      spec: {accessModes: [ReadWriteOnce], resources: {requests: {storage: 1Gi}}}
```

```yaml
# endpoints.yaml: where the sidecar dials, as a name-keyed patch
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxTemplate
metadata:
  name: my-agent
spec:
  podTemplate:
    spec:
      containers:
        - name: sidecar
          env:
            - {name: SIDECAR_HTTP_ANTHROPIC_UPSTREAM_URL, value: https://anthropic.example.com}
            - {name: SIDECAR_AGENTIC_AS_URL, value: https://agentic.example.com}
            - {name: SIDECAR_HARNESS_URL, value: https://harness.example.com}
            - {name: SIDECAR_HTTP_ANTHROPIC_DIAL_ADDRESS, value: pomerium-proxy.pomerium.svc.cluster.local:443}
            - {name: SIDECAR_AGENTIC_AS_DIAL_ADDRESS, value: pomerium-proxy.pomerium.svc.cluster.local:443}
            - {name: SIDECAR_HARNESS_DIAL_ADDRESS, value: pomerium-proxy.pomerium.svc.cluster.local:443}
```

| Variable | Required | Meaning |
|---|---|---|
| `SIDECAR_HTTP_ANTHROPIC_UPSTREAM_URL` | yes | The LLM route. The agent's `ANTHROPIC_BASE_URL` points at the sidecar's loopback port 9999, and the sidecar forwards to this URL with the run token. The route admits the run token and sets the real API key. |
| `SIDECAR_AGENTIC_AS_URL` | yes | The agentic authorization server. The sidecar exchanges its projected token there for the run token. |
| `SIDECAR_HARNESS_URL` | yes | The Agent Link route. It must name the same route as the platform's `HARNESS_EXTERNAL_URL` (chart value `config.harness.externalURL`). |
| `SIDECAR_HTTP_ANTHROPIC_DIAL_ADDRESS`, `SIDECAR_AGENTIC_AS_DIAL_ADDRESS`, `SIDECAR_HARNESS_DIAL_ADDRESS` | see below | The `host:port` to connect to instead of the URL's host, usually the in-cluster Pomerium Service. TLS SNI and the `Host` header still come from the URL, so Pomerium matches the public route and the traffic stays in the cluster. |
| `SIDECAR_AGENTIC_CA_FILE` | no | A PEM bundle for a private Pomerium CA. The sidecar uses it for the authorization server, for the Agent Link (unless `SIDECAR_HARNESS_CA_FILE` is set), and for every endpoint that injects the run token. Mount the file into the `sidecar` container. |

`sidecar serve user-identity` refuses to start unless both
`SIDECAR_AGENTIC_AS_URL` and `SIDECAR_HARNESS_URL` are set.

The network policy admits only the Pomerium pods in one namespace (see
[The network policy](#the-network-policy)). Every address the sidecar dials, the
three above and an AgentTemplate's MCP `dialAddress`, must end at one of those
pods. The dial addresses are optional only if the public host names, resolved
inside the pod, reach a Pomerium pod that the policy admits. Otherwise set them.

A private CA is one more name-keyed patch:

```yaml
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxTemplate
metadata:
  name: my-agent
spec:
  podTemplate:
    spec:
      containers:
        - name: sidecar
          env:
            - {name: SIDECAR_AGENTIC_CA_FILE, value: /etc/pomerium-ca/ca.crt}
          volumeMounts:
            - {name: pomerium-ca, mountPath: /etc/pomerium-ca, readOnly: true}
      volumes:
        - name: pomerium-ca
          configMap: {name: pomerium-ca}
```

Only templates annotated `agents.pomerium.com/inject: "true"` get the run-mode
pod. Any other SandboxTemplate in the build stays as it is.

The agent container must be named `agent`. The component merges the runner into
the container of that name. Other containers in your template, such as a
`git-init` init container, stay as they are. The network policy still applies to
them (see [A `git-init` needs more egress](#a-git-init-needs-more-egress)).

## What the component adds

| File | Applies to | What it adds |
|---|---|---|
| [`inject.yaml`](inject.yaml) | SandboxTemplates annotated `"true"` | The run-mode pod (below). |
| [`sandbox-networkpolicy.yaml`](sandbox-networkpolicy.yaml) | SandboxTemplates annotated `"true"` or `"workload"` | The template's `networkPolicy`. |
| [`egress-sandboxtemplate.yaml`](egress-sandboxtemplate.yaml) | SandboxTemplates annotated `"workload"` | A standalone egress sidecar ([workload mode](#workload-mode)). |
| [`egress-workload.yaml`](egress-workload.yaml) | Deployments, StatefulSets, DaemonSets and Jobs annotated `"workload"` | The same egress sidecar, and the label that the egress NetworkPolicy selects. |
| [`egress-networkpolicy.yaml`](egress-networkpolicy.yaml) | always | The `agentops-egress-only` NetworkPolicy. It selects nothing until a workload carries its label. |
| [`sa.yaml`](sa.yaml) | always | The `sandbox-agent` ServiceAccount. |
| [`schema.json`](schema.json), [`images.yaml`](images.yaml) | the build | Merge keys and image paths for the SandboxTemplate CRD (see [How the merge works](#how-the-merge-works)). |

In run mode, `inject.yaml` adds:

- `serviceAccountName: sandbox-agent`. The pod runs as this ServiceAccount, and
  the sidecar's projected token attests it, so Pomerium route policies can match
  it (`claim/act.kubernetes.io.serviceaccount.name: sandbox-agent`).
- `automountServiceAccountToken: false`. No container gets a Kubernetes API
  token. The only token in the pod is the projected one, audience
  `pomerium-agentic-as`, mounted into the `sidecar` container alone at
  `/var/run/agentic`. It is useful only for the run-token exchange.
- `dnsPolicy: ClusterFirst`. When a template sets neither a network policy nor
  a DNS policy, agent-sandbox gives the pod public resolvers (`dnsPolicy: None`
  with 8.8.8.8 and 1.1.1.1). With them the sidecar cannot resolve an in-cluster
  name such as the Pomerium Service. The component's network policy already
  turns that default off, and `ClusterFirst` keeps cluster DNS independent of
  it.
- The `agentops-init` init container. It copies `agent-runner` from the sidecar
  image onto a shared emptyDir (`agent-runner install`), so harness images need
  no change. The sidecar image is distroless and has no `cp` or shell, so the
  binary copies itself.
- On the `agent` container: the runner as `command`, `ANTHROPIC_BASE_URL`
  pointing at the sidecar's loopback port, a placeholder `ANTHROPIC_API_KEY`, the
  runner binary, and the runner's socket directory. The runner serves a unix
  socket and starts `ACP_AGENT_CMD` when the platform asks.
- The `sidecar` container, running `sidecar serve user-identity`: the loopback
  port for the LLM endpoint, run-token injection on it, the token file and the
  runner socket.
- The annotation `kubectl.kubernetes.io/default-container: agent`. After the
  merge, `agent` is not always the first container, and the annotation keeps
  `kubectl logs` and `kubectl exec` on it.

The pod declares no ports and accepts no connections. The sidecar dials out to
the Agent Link and reaches the runner over the unix socket.

## The network policy

The component sets the SandboxTemplate's `networkPolicy` in full. A template
without one gets agent-sandbox's default: the public internet minus private
address ranges. That default blocks every in-cluster peer, the Pomerium Service
included, so the sidecar cannot reach the authorization server, never gets a run
token, and never attaches.

The component's policy admits two egress peers and nothing else:

- cluster DNS: pods `k8s-app: kube-dns` in `kube-system`, UDP and TCP 53;
- Pomerium: pods `app.kubernetes.io/name: pomerium` in the namespace `pomerium`,
  TCP 8443.

Everything the sandbox does leaves through the sidecar and a Pomerium route: the
model, the MCP servers, the Agent Link. The agent has no other way out. A
`git clone`, a package install or a URL fetch from inside the agent fails,
unless the template adds an egress rule
([A `git-init` needs more egress](#a-git-init-needs-more-egress)). The
policy names peers by label, not by CIDR, because Service and Pod ranges differ
per cluster. Kubernetes evaluates it after the Service's address translation, so
the port is the Pomerium pod's 8443, not the Service's 443.

Ingress admits only the agent-sandbox router (`app: sandbox-router` in
`agent-sandbox-system`), as agent-sandbox's own default does.

These lists have no merge key, so the component's policy replaces any policy your
template sets. To add a peer, patch the policy in your overlay, as below.

### A `git-init` needs more egress

agent-sandbox applies the policy to the whole pod, init containers included. A
`git-init` init container that clones from github.com therefore fails with this
policy alone. A template with a `git-init` needs one more egress rule, such as
[`deploy/examples/git-egress.yaml`](../../examples/git-egress.yaml): a JSON 6902
patch that appends TCP 443 to `0.0.0.0/0` except `10.0.0.0/8`, `172.16.0.0/12`
and `192.168.0.0/16`. [`deploy/examples/kustomization.yaml`](../../examples/kustomization.yaml)
applies it by template name:

```yaml
# kustomization.yaml, added to patches:
  - path: git-egress.yaml
    target:
      kind: SandboxTemplate
      name: (pomerium-zero|gstack|google-skills)-claude-code
```

The rule applies to the whole pod too. With it, the agent container can reach
any public HTTPS host directly, without Pomerium and without the run token. To
narrow it, replace `0.0.0.0/0` with your git host's address ranges. A template
without a `git-init` does not need the rule and keeps the DNS-and-Pomerium-only
policy.

### Pomerium in another namespace

Patch the policy in your overlay. The Pomerium peer is the second egress rule.
This JSON 6902 patch points it at `pomerium-system`:

```yaml
# kustomization.yaml, added to patches:
  - target:
      kind: SandboxTemplate
      annotationSelector: agents.pomerium.com/inject in (true,workload)
    patch: |-
      - op: replace
        path: /spec/networkPolicy/egress/1/to/0/namespaceSelector/matchLabels/kubernetes.io~1metadata.name
        value: pomerium-system
```

For plain workloads in workload mode, patch the NetworkPolicy object too:

```yaml
  - target:
      kind: NetworkPolicy
      name: agentops-egress-only
    patch: |-
      - op: replace
        path: /spec/egress/1/to/0/namespaceSelector/matchLabels/kubernetes.io~1metadata.name
        value: pomerium-system
```

The same rule holds the pod labels
(`/spec/networkPolicy/egress/1/to/0/podSelector/matchLabels`) and the port
(`/spec/networkPolicy/egress/1/ports/0/port`), if your Pomerium pods differ.
kustomize applies your overlay's patches after the component's, so the paths
exist when the patch runs.

## How the merge works

[`inject.yaml`](inject.yaml) is a strategic-merge patch. The SandboxTemplate
CRD's schema has no patch-merge keys. Without help, every list in the patch would
replace your list: your `agent` container would lose its image and env, and your
volumes would be gone. [`schema.json`](schema.json) tells kustomize that
`spec.podTemplate.spec` is a core `PodSpec`. It `$ref`s kustomize's built-in
`io.k8s.api.core.v1.PodSpec`, so the pod spec merges as a Deployment's does:
`name` for `containers`, `initContainers`, `volumes` and `env`, `mountPath` for
`volumeMounts`, `containerPort` for `ports`. As a result:

- your `agent` container keeps its image, env and mounts, and gains what the
  component adds;
- `sidecar` and `agentops-init` are added next to your own containers;
- your volumes are kept;
- your overlay's patches also merge by name, in any list order, so setting
  `SIDECAR_HARNESS_URL` is a patch that names that variable.

The schema is part of the Component, so adding the component is all you need. It
adds one definition next to the built-in ones, and built-in kinds in the same
build merge as usual.

[`images.yaml`](images.yaml) tells the `images:` transformer where a
SandboxTemplate keeps its image references. Without it the transformer knows only
the built-in workload kinds, leaves a CRD's images alone, and the build deploys
the `agentops/sidecar:overlay-must-set` placeholder.

The fields the component sets win over yours: `command` on `agent`,
`serviceAccountName`, `dnsPolicy`, `automountServiceAccountToken: false`, and the
whole `networkPolicy`.

## Changing a template

A warm-pool pod is built from the SandboxTemplate as it was when the pod
started, and the next session adopts it. After you change a template, delete the
pooled Sandboxes so that the pool builds them again:

```sh
kubectl -n agentops-system delete sandboxes -l agents.x-k8s.io/warm-pool-sandbox
```

agent-sandbox removes that label when a claim adopts a Sandbox, so the command
does not touch a running session. A pool with `replicas: 0` holds no pods, and
every claim starts a fresh pod from the current template. A pool with
`spec.updateStrategy.type: Recreate` replaces its stale Sandboxes after a
template change by itself; the default, `OnReplenish`, leaves them in place.
Neither notices an image rebuilt under the same tag.

Everything a pod needs before it has a session belongs in the template. The
platform's claims carry no `env` and no `volumeClaimTemplates`, because
agent-sandbox skips the warm pool for a claim that sets either. The per-session
part, the MCP endpoints, reaches the sidecar on the Agent Link attach stream.

## Workload mode

`agents.pomerium.com/inject: "workload"` adds only a standalone egress sidecar,
running `sidecar serve workload-identity`
([workload mode](../../../docs/run-identity.md#workload-mode)). It injects the
pod's own projected ServiceAccount token and needs no platform, run or runner.
Your containers, command and ServiceAccount stay as they are. The ServiceAccount
is the identity that Pomerium's policy matches, so choose it per workload.

The annotation works on a SandboxTemplate
([`egress-sandboxtemplate.yaml`](egress-sandboxtemplate.yaml)) and on a
Deployment, StatefulSet, DaemonSet or Job
([`egress-workload.yaml`](egress-workload.yaml)). Both add the same things:

- a `sidecar` native sidecar: an init container with `restartPolicy: Always`,
  which needs Kubernetes 1.29 or later. It starts before your containers and
  does not keep a Job from completing;
- a second projected token, audience `pomerium-egress`, at `/var/run/egress`,
  mounted into the sidecar only. The run-mode token (audience
  `pomerium-agentic-as`) is not mounted. The sidecar refuses to start if the
  workload audience is `pomerium-agentic-as`;
- `automountServiceAccountToken: false`;
- egress limited to DNS and Pomerium. A SandboxTemplate gets it as its inline
  `networkPolicy`. A plain workload gets the label
  `agents.pomerium.com/egress: workload`, which
  [`egress-networkpolicy.yaml`](egress-networkpolicy.yaml) selects. That policy
  covers egress only, so a workload can still serve its own traffic.

You add the endpoints as a name-keyed patch on the `sidecar` init container:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: nightly-report
spec:
  template:
    spec:
      initContainers:
        - name: sidecar
          env:
            - {name: SIDECAR_HTTP_ANTHROPIC_PORT, value: "9999"}
            - {name: SIDECAR_HTTP_ANTHROPIC_UPSTREAM_URL, value: https://anthropic.example.com}
            - {name: SIDECAR_HTTP_ANTHROPIC_INJECT_RUN_TOKEN, value: "true"}
            - {name: SIDECAR_HTTP_ANTHROPIC_DIAL_ADDRESS, value: pomerium-proxy.pomerium.svc.cluster.local:443}
```

Point your app at `http://127.0.0.1:9999`. In workload mode, `INJECT_RUN_TOKEN`
injects the workload JWT.

Not covered:

- A bare Pod (`spec`) or a CronJob (`spec.jobTemplate.spec.template`). They keep
  the pod spec at a different path, and a strategic-merge patch follows the
  path. Copy the fragment from `egress-workload.yaml` to the right path in a
  patch of your own.
- A ready gate. The app can start before envoy listens: a startup probe cannot
  reach the sidecar's `127.0.0.1` listeners, and the image has no shell to run
  one. Make the app's first request retry.
