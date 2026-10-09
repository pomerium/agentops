# agentops Helm chart

The AgentOps platform. The chart runs the `harness` binary (`/usr/local/bin/harness` in the image `pomerium/agentops`) as a StatefulSet with one replica. The platform serves:

- the **Harness API** (Connect over h2c) on port 8081, with `/healthz` and `/readyz`. Clients reach it only through a Pomerium route;
- the **Agent Link** (gRPC over h2c) on port 8090. Sandbox sidecars dial it through a Pomerium route;
- an admin listener on port 9090 with `/metrics`. The Service does not expose it.

The platform creates `SandboxClaim`s for agent-sandbox and reads `AgentTemplate`s and `ClientBinding`s, all in its own namespace. By convention that namespace is `agentops-system`, and the sandboxes, AgentTemplates and ClientBindings go there too.

The chart installs:

- the StatefulSet, with SQLite at `/data/agentops.db` on a PersistentVolumeClaim;
- a Service with the ports `grpc` (8090) and `api` (8081);
- a ServiceAccount, and a Role and RoleBinding (`rbac.create`);
- the `AgentTemplate` and `ClientBinding` CRDs (`installCRDs`).

The chart does not install agent-sandbox, Pomerium, the sandbox side (SandboxTemplates, SandboxWarmPools and the `sandbox-agent` ServiceAccount, which come from the kustomize Component in [`deploy/components/agentops`](../../components/agentops)), AgentTemplates, ClientBindings, or any client. The Slack bot has its own chart, [`agentops-slackbot`](../agentops-slackbot).

The full installation, including the Pomerium routes, is in the repository's [README](../../../README.md#install).

## Install

```sh
helm install agentops oci://registry-1.docker.io/pomerium/agentops \
  --version X.Y.Z \
  --namespace agentops-system --create-namespace \
  --set config.agentic.asURL=https://agentic.example.com \
  --set config.harness.externalURL=https://harness.example.com \
  --set config.harness.assertionIssuer=harness.example.com \
  --set config.harness.api.assertionIssuer=harness-api.example.com
```

Always pass `--version`. CI publishes the chart for each GitHub release (version `X.Y.Z` for the tag `vX.Y.Z`, `appVersion` the tag) and a development chart, version `0.0.0-git-<sha7>` with `appVersion` `git-<sha8>`, for each push to `main` that changes `deploy/charts/**`.

To install from a checkout, give the chart's path. The image tag then defaults to the `appVersion` in `Chart.yaml`, so set `image.tag` to an image that exists, such as `main` or the `git-<sha8>` tag of a commit on `main`:

```sh
helm install agentops deploy/charts/agentops \
  --namespace agentops-system --create-namespace \
  --set image.tag=git-<sha8> \
  --set config.agentic.asURL=https://agentic.example.com \
  --set config.harness.externalURL=https://harness.example.com \
  --set config.harness.assertionIssuer=harness.example.com \
  --set config.harness.api.assertionIssuer=harness-api.example.com
```

Install the agent-sandbox controller and its CRDs (v1.0.3, `v1beta1` API) first. From the repository root: `kubectl apply -f deploy/agent-sandbox.yaml` ([the file](../../agent-sandbox.yaml)).

## Required values

The chart does not render without these:

| Value | Example | Meaning |
|---|---|---|
| `config.agentic.asURL` | `https://agentic.example.com` | Base URL of the Pomerium host that serves the agentic authorization server (AS). |
| `config.harness.externalURL` | `https://harness.example.com` | URL of the Pomerium route to the Agent Link. |
| `config.harness.assertionIssuer` | `harness.example.com` | Host of that route. |
| `config.harness.api.assertionIssuer` | `harness-api.example.com` | Host of the Pomerium route to the Harness API. It must differ from `config.harness.assertionIssuer`: Pomerium sets `iss` and `aud` from the route host, so with one host an Agent Link assertion would also be valid on the Harness API. |

## Values

| Key | Default | Description |
|---|---|---|
| `nameOverride` | `""` | Replaces the chart name in resource names and in the `app.kubernetes.io/name` label. |
| `fullnameOverride` | `""` | Replaces the resource name. By default it is the release name if that contains `agentops`, otherwise `<release>-agentops`. The ServiceAccount has this name, so the AS route where the platform creates runs must admit it. |
| `image.repository` | `pomerium/agentops` | Image repository. |
| `image.tag` | `""` | Image tag. Empty uses the chart's `appVersion`. |
| `image.digest` | `""` | Image digest (`sha256:…`). When set, the chart uses it instead of the tag. |
| `image.pullPolicy` | `IfNotPresent` | Image pull policy. |
| `imagePullSecrets` | `[]` | Pull secrets for the pod. |
| `config.logLevel` | `info` | `LOG_LEVEL`: `debug`, `info`, `warn` or `error`. |
| `config.sessionTTL` | `1h` | `SESSION_TTL`: a session that is not suspended ends when this much time has passed since it last started launching: at creation, or when a continuation revived it. The wait for approval counts; time spent suspended does not. |
| `config.sessionIdleTTL` | `15m` | `SESSION_IDLE_TTL`: a running session with no turn for this long is suspended. Its pod is freed and its workspace kept. |
| `config.sessionIdleWarnLead` | `2m` | `SESSION_IDLE_WARN_LEAD`: how long before the idle suspend the session gets an `idle_warning` event. There is no warning unless this is less than `config.sessionIdleTTL`. |
| `config.suspendedTTL` | `24h` | `SUSPENDED_TTL`: how long a suspended session's workspace (Sandbox and volume) is kept, from the suspend. Then the workspace is released and the session ends. |
| `config.sandboxLease` | `1h` | `SANDBOX_LEASE`: the shutdown time the platform sets on each Sandbox and moves forward while the session is active. If the platform stops, agent-sandbox still shuts the Sandbox down at that time. Keep it longer than `config.sessionIdleTTL`. |
| `config.agentic.asURL` | `""` | Required. `AGENTIC_AS_URL`: base URL of the AS host. The platform creates and reads runs under `/agentic/runs`. |
| `config.agentic.dialAddress` | `""` | `AGENTIC_AS_DIAL_ADDRESS`: `host:port` to connect to for the AS, for example the in-cluster Pomerium Service, while Host and SNI stay those of `asURL`. It is also the default for both JWKS fetches. |
| `config.agentic.runTTL` | `15m` | `AGENTIC_RUN_TTL`: how long a new run waits for approval. The platform waits this plus 2 minutes for the sandbox to connect. Must be positive. |
| `config.agentic.caFile` | `""` | `AGENTIC_CA_FILE`: PEM CA bundle for Pomerium's TLS certificate, for the AS and, by default, both JWKS fetches. Mount the file with `extraVolumes` and `extraVolumeMounts`. Empty uses the system roots. |
| `config.agentic.audience` | `pomerium-agentic-as` | Audience of the projected ServiceAccount token at `/var/run/agentic/token` (`AGENTIC_TOKEN_FILE`). The platform sends it as the bearer on the AS. It must be an audience of the Pomerium identity provider on that route. |
| `config.agentic.tokenExpirationSeconds` | `600` | Lifetime of that token. The kubelet rotates it, and the platform reads the file for each request. |
| `config.harness.externalURL` | `""` | Required. `HARNESS_EXTERNAL_URL`: URL of the Agent Link route. Its host is the expected `aud` of the assertion. Sandboxes dial `SIDECAR_HARNESS_URL`, which must name the same route. |
| `config.harness.assertionIssuer` | `""` | Required. `HARNESS_ASSERTION_ISSUER`: expected `iss` of the assertion that Pomerium adds on the Agent Link route (the route's host). The platform gets the keys from `https://<issuer>/.well-known/pomerium/jwks.json`. |
| `config.harness.attachGrace` | `2m` | `HARNESS_ATTACH_GRACE`: how long a session waits for its sidecar to reconnect after the Agent Link connection drops. Must be positive. |
| `config.harness.attachWarnAfter` | `90s` | `HARNESS_ATTACH_WARN_AFTER`: if a sandbox has not connected this long after the platform started waiting for it, the platform logs a warning. It sends one `launch_stalled` event per launch: at that moment if the run is already approved, or else on the first run poll after the one that sees the approval, if the sandbox still has not connected. Must be positive. |
| `config.harness.heartbeatInterval` | `20s` | `HARNESS_HEARTBEAT_INTERVAL`: the heartbeat interval the platform gives to sidecars. Must be positive. |
| `config.harness.heartbeatMissLimit` | `3` | `HARNESS_HEARTBEAT_MISS_LIMIT`: how many heartbeats can be missed before the connection counts as lost. Must be a positive integer. |
| `config.harness.api.assertionIssuer` | `""` | Required. `HARNESS_API_ASSERTION_ISSUER`: expected `iss` of the assertion on the Harness API route (the route's host). The platform takes the client's identity from the assertion's subject. |
| `config.harness.api.assertionAudience` | `""` | `HARNESS_API_ASSERTION_AUDIENCE`: expected `aud` on the Harness API route. Empty uses the host of `config.harness.api.assertionIssuer`. |
| `serviceAccount.annotations` | `{}` | Annotations on the ServiceAccount. |
| `rbac.create` | `true` | Create the Role and RoleBinding (see [RBAC](#rbac)). Set `false` to manage them yourself. |
| `installCRDs` | `true` | Install the `AgentTemplate` and `ClientBinding` CRDs with the release, annotated `helm.sh/resource-policy: keep` (see [CRDs](#crds)). |
| `service.type` | `ClusterIP` | Service type. |
| `service.grpcPort` | `8090` | Agent Link port: container port, Service port and `HARNESS_GRPC_ADDR`. |
| `service.apiPort` | `8081` | Harness API port: container port, Service port and `HARNESS_API_ADDR`. The probes use it. |
| `service.adminPort` | `9090` | Admin port: container port and `HARNESS_ADMIN_ADDR`. It serves `/metrics` and is not on the Service. |
| `persistence.enabled` | `true` | Put `/data` on a PersistentVolumeClaim. With `false`, `/data` is an emptyDir and the database does not survive the pod. |
| `persistence.storageClass` | `""` | Storage class of the claim. Empty uses the cluster default. |
| `persistence.size` | `1Gi` | Size of the claim. |
| `persistence.accessModes` | `[ReadWriteOnce]` | Access modes of the claim. |
| `resources` | `{}` | Container resources. |
| `podAnnotations` | `{}` | Annotations on the pod. |
| `extraEnvVars` | `[]` | More environment variables, as a list of `EnvVar`. |
| `extraVolumes` | `[]` | More pod volumes. |
| `extraVolumeMounts` | `[]` | More container volume mounts. |
| `nodeSelector` | `kubernetes.io/os: linux` | Node selector. |
| `tolerations` | `[]` | Tolerations. |
| `affinity` | `{}` | Affinity. |
| `priorityClassName` | `""` | Priority class. |
| `podSecurityContext` | non-root, uid, gid and fsGroup 65532, `RuntimeDefault` seccomp | Pod security context. |
| `securityContext` | no privilege escalation, read-only root filesystem, all capabilities dropped | Container security context. |

### Durations

Durations are Go durations (`90s`, `15m`, `1h`). For `config.sessionTTL`, `config.sessionIdleTTL`, `config.sessionIdleWarnLead`, `config.suspendedTTL` and `config.sandboxLease`, `0` means the default and a negative value turns the behavior off:

| Value | Negative means |
|---|---|
| `config.sessionTTL` | no limit on a session's lifetime |
| `config.sessionIdleTTL` | idle sessions are not suspended |
| `config.sessionIdleWarnLead` | no idle warning |
| `config.suspendedTTL` | suspended workspaces are kept |
| `config.sandboxLease` | no shutdown time on the Sandbox, so a sandbox can outlive a stopped platform |

The platform does not start if `config.agentic.runTTL`, `config.harness.attachGrace`, `config.harness.attachWarnAfter` or `config.harness.heartbeatInterval` is zero or negative.

### Variables set by the chart

| Variable | Value |
|---|---|
| `POD_NAMESPACE` | the pod's namespace. The platform works only in this namespace. |
| `DB_PATH` | `/data/agentops.db` |
| `AGENTIC_TOKEN_FILE` | `/var/run/agentic/token`, the projected token |

### Variables without a value

These have no chart value. Set them with `extraEnvVars` if you need them:

| Variable | Default | Meaning |
|---|---|---|
| `HARNESS_ASSERTION_AUDIENCE` | the host of `config.harness.externalURL` | Expected `aud` on the Agent Link route. |
| `HARNESS_ASSERTION_JWKS_URL` | derived from `config.harness.assertionIssuer` | JWKS URL for the Agent Link route. |
| `HARNESS_ASSERTION_DIAL_ADDRESS` | `config.agentic.dialAddress` | `host:port` to connect to for that JWKS fetch. |
| `HARNESS_ASSERTION_CA_FILE` | `config.agentic.caFile` | CA bundle for that JWKS fetch. |
| `HARNESS_API_ASSERTION_JWKS_URL` | derived from `config.harness.api.assertionIssuer` | JWKS URL for the Harness API route. |
| `HARNESS_API_ASSERTION_DIAL_ADDRESS` | `config.agentic.dialAddress` | `host:port` to connect to for that JWKS fetch. |
| `HARNESS_API_ASSERTION_CA_FILE` | `config.agentic.caFile` | CA bundle for that JWKS fetch. |

## RBAC

The Role, in the release namespace, grants:

| API group | Resources | Verbs |
|---|---|---|
| `agents.pomerium.com` | `agenttemplates`, `clientbindings` | get, list, watch |
| `extensions.agents.x-k8s.io` | `sandboxtemplates` | get, list, watch |
| `extensions.agents.x-k8s.io` | `sandboxclaims` | create, get, list, watch, delete |
| `agents.x-k8s.io` | `sandboxes` | get, list, watch, patch |
| core | `pods` | get, list |

## One replica

The StatefulSet always has one replica. The database is SQLite with one writer, on a `ReadWriteOnce` volume.

## CRDs

The CRDs are templates in this chart, not files in `crds/`, so `helm upgrade` updates them. Each carries `helm.sh/resource-policy: keep`, which `make helm-sync-crds` adds when it copies them from [`config/crd/bases`](../../../config/crd/bases).

- `helm uninstall` leaves both CRDs, and every AgentTemplate and ClientBinding in the cluster, in place.
- To remove them, delete the CRDs by hand. This also deletes every AgentTemplate and ClientBinding:

  ```sh
  kubectl delete crd agenttemplates.agents.pomerium.com clientbindings.agents.pomerium.com
  ```

- To manage the CRDs outside the release, set `installCRDs: false` and apply `config/crd/bases` yourself (`kubectl apply -f config/crd/bases`). This is safe on a fresh install. It is also safe on an existing release: when `helm upgrade` stops rendering the CRDs, the `keep` policy stops Helm from deleting them.

## After you install

The chart's notes list these steps. The route settings in step 2 are not in the notes.

1. Check that the pod is ready:

   ```sh
   kubectl --namespace agentops-system get pods -l app.kubernetes.io/instance=agentops
   ```

2. Route two separate Pomerium hosts to the release:

   | Route | `from` | `to` |
   |---|---|---|
   | Agent Link | `config.harness.externalURL` | `h2c://agentops.agentops-system.svc.cluster.local:8090` |
   | Harness API | `https://<config.harness.api.assertionIssuer>` | `h2c://agentops.agentops-system.svc.cluster.local:8081` |

   Both routes need `pass_identity_headers: true`, `timeout: 0s` and `idle_timeout: 0s`. The Agent Link route uses `bearer_token_format: agentic_run_token`. The Harness API route uses `bearer_token_format: jwt` and the identity provider that verifies your clients' tokens. The AS route where the platform creates runs (`/agentic/runs`) must admit the ServiceAccount `system:serviceaccount:agentops-system:agentops`. The repository's [README](../../../README.md#2-configure-pomerium-as-the-agentic-authorization-server) has the complete Pomerium configuration.

3. Give each client a ClientBinding in `agentops-system`. The platform refuses every call from a client without one. The platform logs the subject of each client it admits:

   ```sh
   kubectl --namespace agentops-system logs sts/agentops | grep "admitted a client"
   ```

   [`deploy/examples/clientbinding-slack.yaml`](../../examples/clientbinding-slack.yaml) is the binding for the Slack bot.

4. Sandboxes run in `agentops-system`. Apply a SandboxTemplate, a SandboxWarmPool and an AgentTemplate there. [`deploy/sandbox`](../../sandbox) and [`deploy/examples`](../../examples) have examples.
