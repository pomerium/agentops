# agentops-slackbot Helm chart

The AgentOps Slack bot, the reference client of the Harness API. The chart runs the `slackbot` binary (`/usr/local/bin/slackbot` in the image `pomerium/agentops-slackbot`) as a Deployment. The bot receives Slack's requests on `/slack/events` and `/slack/interactivity` (container port 8080, Service port 80) and starts and drives sessions through the Harness API's Pomerium route. A Slack thread is a conversation with an agent; [docs/threads.md](../../../docs/threads.md) explains how.

By convention the release is named `agentops-slackbot` and goes in the namespace `agentops-slackbot`.

The chart installs:

- the Deployment (one replica, `strategy: Recreate`);
- a Service on port 80;
- a ServiceAccount;
- a ConfigMap `<fullname>-channels` with the channel map;
- a Secret with the Slack credentials, unless you set `slack.existingSecret`.

The chart does not install the platform (chart [`agentops`](../agentops)), the Pomerium routes, the bot's ClientBinding (it goes in the platform's namespace), or the Slack app.

## What the pod does not have

The bot is a client of the Harness API and nothing more:

- **No RBAC.** The chart creates no Role or RoleBinding. The bot does not call the Kubernetes API.
- **No Kubernetes API token.** The pod sets `automountServiceAccountToken: false`. Its only token is a projected ServiceAccount token at `/var/run/harness-api/token`, with the audience `harnessAPI.tokenAudience`, which it sends to the Harness API route.
- **No token for the authorization server.** The bot does not talk to Pomerium's agentic authorization server; the platform creates runs. The pod mounts no token for the authorization server's routes. The default `harnessAPI.tokenAudience`, `pomerium-agentic-as`, is the same audience that the platform's and the sandboxes' tokens use with Pomerium's identity provider for ServiceAccount tokens (`cluster` in the repository's [README](../../../README.md#2-configure-pomerium-as-the-agentic-authorization-server)). The authorization server's routes for ServiceAccount tokens pin the namespace and the ServiceAccount in their policies, so they refuse the bot's token. To keep the audiences separate, set `harnessAPI.tokenAudience` to a value of its own and give the Harness API route an identity provider that accepts it.
- **No volume.** The bot writes nothing to disk. Sessions are in the platform, and the bot keeps what it needs about each thread in Slack message metadata. After a restart it lists its live sessions on the Harness API and continues from there.

`make helm-check-client-isolation` fails if the chart renders RBAC, mounts the authorization-server token (a volume named `agentic-token`, or anything at `/var/run/agentic`) or claims a volume. The Helm workflow runs it on pull requests.

## One replica, `Recreate`

Each bot process follows every live session of its client and renders the session's events into the thread. Two pods would post every event twice. `replicas` is therefore fixed at 1, and `strategy: Recreate` stops the old pod before the new one starts, so an upgrade never runs two.

## Install

Create the Slack app first: the repository's [README](../../../README.md#slack-app-setup) has the manifest. Then put its signing secret and bot token in a Secret, from a copy of [`deploy/secret.example.yaml`](../../secret.example.yaml):

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

Or let the chart create the Secret:

```sh
helm install agentops-slackbot oci://registry-1.docker.io/pomerium/agentops-slackbot \
  --version X.Y.Z \
  --namespace agentops-slackbot --create-namespace \
  --set harnessAPI.url=https://harness-api.example.com \
  --set slack.signingSecret="$SLACK_SIGNING_SECRET" \
  --set slack.botToken="$SLACK_BOT_TOKEN" \
  --set slack.channels.C0123ABCDEF=deploy-service
```

Always pass `--version`. CI publishes the chart for each GitHub release (version `X.Y.Z` for the tag `vX.Y.Z`, `appVersion` the tag) and a development chart, version `0.0.0-git-<sha7>` with `appVersion` `git-<sha8>`, for each push to `main` that changes `deploy/charts/**`.

To install from a checkout, give the chart's path. The image tag then defaults to the `appVersion` in `Chart.yaml`, so set `image.tag` to an image that exists, such as `main` or the `git-<sha8>` tag of a commit on `main`:

```sh
helm install agentops-slackbot deploy/charts/agentops-slackbot \
  --namespace agentops-slackbot \
  --set image.tag=git-<sha8> \
  --set harnessAPI.url=https://harness-api.example.com \
  --set slack.existingSecret=agentops-slackbot-slack \
  --set slack.channels.C0123ABCDEF=deploy-service
```

## Required values

The chart does not render without these:

| Value | Meaning |
|---|---|
| `harnessAPI.url` | URL of the Harness API's Pomerium route, for example `https://harness-api.example.com`. |
| `slack.signingSecret` and `slack.botToken`, or `slack.existingSecret` | The Slack credentials. An existing Secret must be in the release namespace and have the keys `SLACK_SIGNING_SECRET` and `SLACK_BOT_TOKEN`. |

Without `slack.channels` or `slack.defaultChannelTemplate` the bot runs but no channel starts a session.

## Values

| Key | Default | Description |
|---|---|---|
| `nameOverride` | `""` | Replaces the chart name in resource names and in the `app.kubernetes.io/name` label. |
| `fullnameOverride` | `""` | Replaces the resource name. By default it is the release name if that contains `agentops-slackbot`, otherwise `<release>-agentops-slackbot`. The ServiceAccount has this name, and so it sets the bot's subject on the Harness API. |
| `image.repository` | `pomerium/agentops-slackbot` | Image repository. |
| `image.tag` | `""` | Image tag. Empty uses the chart's `appVersion`. |
| `image.digest` | `""` | Image digest (`sha256:…`). When set, the chart uses it instead of the tag. |
| `image.pullPolicy` | `IfNotPresent` | Image pull policy. |
| `imagePullSecrets` | `[]` | Pull secrets for the pod. |
| `logLevel` | `info` | `LOG_LEVEL`: `debug`, `info`, `warn` or `error`. |
| `slack.signingSecret` | `""` | `SLACK_SIGNING_SECRET`, in the Secret the chart creates. The bot uses it to verify that requests come from Slack. Not used when `slack.existingSecret` is set. |
| `slack.botToken` | `""` | `SLACK_BOT_TOKEN` (`xoxb-…`), in the Secret the chart creates. Not used when `slack.existingSecret` is set. |
| `slack.existingSecret` | `""` | Name of a Secret in the release namespace with the keys `SLACK_SIGNING_SECRET` and `SLACK_BOT_TOKEN`. When set, the chart creates no Secret. |
| `slack.channels` | `{}` | Map of Slack channel ID to AgentTemplate name. A mention in a listed channel starts that agent. |
| `slack.defaultChannelTemplate` | `""` | AgentTemplate for a channel that `slack.channels` does not list. Empty means such a channel starts nothing. Any channel the bot is invited to can start this agent. |
| `slack.streamInterval` | `""` | `AGENT_STREAM_INTERVAL`, a Go duration: the minimum time between edits of an agent reply that is still streaming, per channel. Empty uses the bot's default, `2.5s`. |
| `harnessAPI.url` | `""` | Required. `HARNESS_API_URL`: URL of the Harness API's Pomerium route. |
| `harnessAPI.dialAddress` | `""` | `HARNESS_API_DIAL_ADDRESS`: `host:port` to connect to, for example the in-cluster Pomerium Service, while Host and SNI stay those of `harnessAPI.url`. Empty resolves the URL's host. |
| `harnessAPI.caFile` | `""` | `HARNESS_API_CA_FILE`: PEM CA bundle for the route's TLS certificate. Mount the file with `extraVolumes` and `extraVolumeMounts`. Empty uses the system roots. |
| `harnessAPI.tokenAudience` | `pomerium-agentic-as` | Audience of the projected ServiceAccount token at `/var/run/harness-api/token` (`HARNESS_API_TOKEN_FILE`). The bot sends it as the bearer on the Harness API route. It must be an audience of the Pomerium identity provider on that route. |
| `harnessAPI.tokenExpirationSeconds` | `600` | Lifetime of that token. The kubelet rotates it, and the bot reads the file for each request. |
| `serviceAccount.annotations` | `{}` | Annotations on the ServiceAccount. |
| `service.type` | `ClusterIP` | Service type. |
| `service.port` | `80` | Service port. It forwards to container port 8080 (`HTTP_ADDR`). |
| `resources` | `{}` | Container resources. |
| `podAnnotations` | `{}` | More annotations on the pod. The chart also sets checksums of the channel map and of the Secret it creates. |
| `extraEnvVars` | `[]` | More environment variables, as a list of `EnvVar`. |
| `extraVolumes` | `[]` | More pod volumes. |
| `extraVolumeMounts` | `[]` | More container volume mounts. |
| `nodeSelector` | `kubernetes.io/os: linux` | Node selector. |
| `tolerations` | `[]` | Tolerations. |
| `affinity` | `{}` | Affinity. |
| `priorityClassName` | `""` | Priority class. |
| `podSecurityContext` | non-root, uid, gid and fsGroup 65532, `RuntimeDefault` seccomp | Pod security context. |
| `securityContext` | no privilege escalation, read-only root filesystem, all capabilities dropped | Container security context. |

### Variables set by the chart

| Variable | Value |
|---|---|
| `SLACK_SIGNING_SECRET`, `SLACK_BOT_TOKEN` | from the Secret |
| `HTTP_ADDR` | `:8080` |
| `SLACK_CHANNEL_MAP` | `/etc/agentops/channels.yaml`, from the ConfigMap |
| `HARNESS_API_TOKEN_FILE` | `/var/run/harness-api/token`, the projected token |

### The channel map

The ConfigMap `<fullname>-channels` holds `channels.yaml`:

```yaml
channels:
  "C0123ABCDEF": "deploy-service"
default: "gcloud"   # only when slack.defaultChannelTemplate is set
```

The bot reads the file at startup and again every 30 seconds. A bot that starts with a file it cannot read or parse exits with an error. A running bot that cannot read or parse the file on a later read keeps the previous map and logs `re-reading the channel map failed; keeping the previous one`. If the file is missing, at startup or on a later read, the map is empty and no channel starts a session. An edit to the ConfigMap takes effect without a restart, after the kubelet updates the mounted file. A `helm upgrade` that changes `slack.channels` or `slack.defaultChannelTemplate` changes the checksum on the pod and restarts it.

To find a channel's ID in Slack, open the channel name and then "View channel details". A mention in a channel with no binding gets a reply that no agent is configured.

## After you install

The chart's notes list these steps:

1. Check that the pod is ready:

   ```sh
   kubectl --namespace agentops-slackbot get pods -l app.kubernetes.io/instance=agentops-slackbot
   ```

2. Route Slack to the bot. A Pomerium route that terminates TLS sends `/slack/events` and `/slack/interactivity` to `http://agentops-slackbot.agentops-slackbot.svc.cluster.local:80`. Slack cannot sign in, so the route allows unauthenticated access; the bot checks Slack's signature on each request.

3. Allow the bot on the Harness API (`harnessAPI.url`):

   - The Pomerium route admits the token of the ServiceAccount `system:serviceaccount:agentops-slackbot:agentops-slackbot` with the audience `harnessAPI.tokenAudience`, for example with `claim/sub: system:serviceaccount:agentops-slackbot:agentops-slackbot`. That policy matches the token's raw `sub`.
   - A ClientBinding in the platform's namespace names the subject that Pomerium mints, which has the identity provider's name in front: for a provider named `cluster`, `cluster/system:serviceaccount:agentops-slackbot:agentops-slackbot`. See [`deploy/examples/clientbinding-slack.yaml`](../../examples/clientbinding-slack.yaml). The platform logs the exact subject when it first admits the bot: `kubectl -n agentops-system logs sts/agentops | grep "admitted a client"`.

4. If neither `slack.channels` nor `slack.defaultChannelTemplate` is set, the notes say that no channel is bound to an agent.

Then, in the Slack app, verify the Event Subscriptions request URL, invite the bot to each bound channel and mention it there.
