# Installing AgentOps

This guide is written for a coding agent that installs AgentOps for a person.
A person can follow it too. It installs the
[`agentops-quickstart`](./deploy/charts/agentops-quickstart) Helm chart into a
Kubernetes cluster:

- **Pomerium**, the [Pomerium ingress controller](https://github.com/pomerium/ingress-controller)
  (`pomerium/ingress-controller:experimental-agentic`), in a namespace of its
  own. It is the agentic authorization server, and every route is an Ingress.
- **The AgentOps platform**, which serves the Harness API and the Agent Link.
- **The sandbox side and the `hello` demo agent.** `hello` needs no LLM: it
  answers every prompt with a fixed text.
- **The client `quickstart-client`**, a ServiceAccount that the last step
  uses to run one `hello` session through the Harness API.

At the end, the person approves one run in a browser and sees the `hello`
agent's answer. Then the install works from end to end.

## Rules for the agent

- Show each command that changes the cluster before you run it, and say what it
  does in one sentence.
- Interview the person for every setting in
  [Step 2](#step-2-interview-the-person). Do not guess a domain, a certificate
  or an approver.
- Never print an API key, a token or a Secret's data. Do not put an API key in a
  file in a git repository.
- Stop at the first check that fails. Use [When a check fails](#when-a-check-fails),
  tell the person what you found, and fix it with them before you go on.
- Use one kubeconfig context for every command. If the person names one, add
  `--context <context>` to each `kubectl` command and `--kube-context <context>`
  to each `helm` command.
- Run the commands from the root of a checkout of
  `https://github.com/pomerium/agentops`. Clone it if there is none.

## Step 1: Check the prerequisites

1. Check the tools: `kubectl`, `helm` (3.8 or later), `curl` and `jq`.
   `openssl` is optional.

2. Show the person the cluster, and ask them to confirm it is the right one:

   ```sh
   kubectl config current-context
   kubectl cluster-info
   ```

3. Check agent-sandbox. Each of these CRDs must exist and serve `v1beta1`:

   ```sh
   kubectl get crd sandboxes.agents.x-k8s.io sandboxtemplates.extensions.agents.x-k8s.io \
     sandboxclaims.extensions.agents.x-k8s.io sandboxwarmpools.extensions.agents.x-k8s.io \
     -o custom-columns=NAME:.metadata.name,SERVED:'.spec.versions[?(@.served==true)].name'
   kubectl get deployments -A -l app=agent-sandbox-controller \
     -o custom-columns=NAMESPACE:.metadata.namespace,IMAGE:'.spec.template.spec.containers[0].image'
   ```

   If the CRDs are missing, ask the person before you install agent-sandbox. It
   is cluster-wide:

   ```sh
   kubectl apply -f deploy/agent-sandbox.yaml
   kubectl -n agent-sandbox-system rollout status deployment/agent-sandbox-controller
   ```

   The platform is built against the controller version in
   `deploy/agent-sandbox.yaml` (v1.0.3). If the cluster runs another version,
   tell the person, and point them at `deploy/agent-sandbox.md`.

## Step 2: Interview the person

Interview the person for the settings below before you change anything.

- If your agent has a tool that asks the user structured or multiple-choice
  questions, use it. Claude Code's `AskUserQuestion` is one. Ask each round
  below as one call: at most four questions, each with the suggested options,
  the recommended one first. The person can always type another answer.
- Without such a tool, ask each round as one chat message, and wait for the
  answers.
- Find the options in the cluster first, where the table says how, so the
  person picks rather than types.

### Round 1: namespaces and domain

| Question | Options to offer |
|---|---|
| The namespace of the platform, the sandboxes and the demo agent | `agentops-system` (recommended) |
| The namespace of Pomerium. It must differ from the platform's. | `agentops-pomerium` (recommended). If it exists already, see [Step 4](#step-4-install). |
| The DNS domain of the hosts, for example `agentops.example.com` | Domains that the TLS Secrets in the cluster cover (see Round 2), if any. Otherwise the person types one. |
| The host names | `authenticate`, `agentic`, `harness`, `harness-api` and `anthropic` under the domain (recommended), or the person names each one |

What the hosts are for:

| Host | Route to |
|---|---|
| `authenticate` | Pomerium's sign-in |
| `agentic` | the agentic authorization server: `/agentic/approve` for approvers, `/agentic/runs` for the platform, `/agentic/token` for sandboxes |
| `harness` | the Agent Link, which sandboxes dial |
| `harness-api` | the Harness API, for clients such as the Slack bot |
| `anthropic` | the Anthropic API, with the API key, for approved runs |

Each host must be a different, valid DNS name.

### Round 2: certificate

| Question | Options to offer |
|---|---|
| The TLS Secret with the certificate for every host, as `namespace/name` | The Secrets of type `kubernetes.io/tls` in the cluster whose certificate covers the hosts (commands below), the best match first |
| Who signed the certificate | A public CA (recommended for a real domain); a private CA, with the CA certificate in a PEM file; a private CA, with the CA in a Secret in the platform namespace |

To find the TLS Secrets, and the names each certificate covers:

```sh
kubectl get secrets -A --field-selector type=kubernetes.io/tls \
  -o custom-columns=NAMESPACE:.metadata.namespace,NAME:.metadata.name
kubectl -n <namespace> get secret <name> -o jsonpath='{.data.tls\.crt}' \
  | base64 -d | openssl x509 -noout -text | grep -o 'DNS:[^,]*'
```

A name `*.<parent>` covers a host `<label>.<parent>`, one label only. A
wildcard certificate for `*.<domain>` covers all five hosts. If no Secret covers
the hosts, stop: the person must make one first, for example with cert-manager.

### Round 3: approvers and the LLM route

| Question | Options to offer |
|---|---|
| The email addresses of the approvers | The person's own address (`git config user.email`, recommended); another address. Several are allowed. |
| The email domains of the approvers | None (recommended); the domain of the person's address. Each domain admits everyone with an address there: never offer a public one such as `gmail.com`. |
| A route to the Anthropic API, for agents that use Claude | Not now (recommended: `hello` needs no LLM); yes, with an API key |

There must be at least one approver address or domain. Ask for the API key
only after the person chooses the route, and do not echo it.

### Confirm

Show the person every answer in one summary, with the five hosts, and ask
them to confirm before Step 3.

## Step 3: Write the values file

Write the values file outside any git repository, for example
`~/agentops-values.yaml`. The install runs from the checkout, so keep the file's
absolute path in a variable:

```sh
export VALUES="$HOME/agentops-values.yaml"
```

Write this to `$VALUES`, and replace each `<...>`:

```yaml
hosts:
  authenticate: <authenticate host>
  anthropic: <anthropic host>
tls:
  secret: <tls-namespace>/<tls-name>
access:
  domains: [<domain>, ...]
  emails: [<address>, ...]
pomerium:
  namespace: <pomerium namespace>
agentops:
  config:
    agentic:
      asURL: https://<agentic host>
      dialAddress: pomerium-proxy.<pomerium namespace>.svc.cluster.local:443
    harness:
      externalURL: https://<harness host>
      assertionIssuer: <harness host>
      api:
        assertionIssuer: <harness-api host>
```

For a private CA, first put the CA in a Secret in the platform namespace, if it
is a file:

```sh
kubectl create namespace <platform namespace> --dry-run=client -o yaml | kubectl apply -f -
kubectl -n <platform namespace> create secret generic agentops-ca --from-file=ca.crt=<CA file>
```

Then add this to the values file. With a Secret that exists already, use its
name in both `secretName` fields, and its key in `key` and as the file name at
the end of `caFile`. The platform reads the CA at `caFile`, and the volume
names each file after its key:

```yaml
privateCA:
  secretName: agentops-ca
  key: ca.crt
agentops:
  config:
    agentic:
      caFile: /etc/agentops-ca/ca.crt
  extraVolumes:
    - name: agentops-ca
      secret:
        secretName: agentops-ca
  extraVolumeMounts:
    - name: agentops-ca
      mountPath: /etc/agentops-ca
      readOnly: true
```

Merge the `agentops:` keys with the ones above. Do not write the key twice.

For the Anthropic route, add `anthropic: {enabled: true}` to the values file,
and put the key in a second file that only the person can read:

```sh
umask 077
printf 'anthropic:\n  apiKey: %s\n' "<key>" > "$HOME/agentops-anthropic.yaml"
```

Pass that file to the first install only. The chart keeps the key in a Secret,
and an upgrade without `apiKey` keeps it.

The [chart's README](./deploy/charts/agentops-quickstart/README.md) lists every
other value.

## Step 4: Install

If the Pomerium namespace exists already and Helm did not create it for this
release, add `pomerium: {createNamespace: false}` to the values file.

```sh
kubectl apply --server-side --force-conflicts -f deploy/charts/agentops-quickstart/crds/
helm dependency build --skip-refresh deploy/charts/agentops-quickstart
helm upgrade --install agentops deploy/charts/agentops-quickstart \
  --namespace <platform namespace> --create-namespace \
  -f "$VALUES"
```

For the Anthropic route, add `-f "$HOME/agentops-anthropic.yaml"` to the last
command.

- The first command applies the CRDs of Pomerium and of AgentOps. Helm
  installs the CRDs in a chart's `crds/` only once and never updates them, so
  apply them again before each upgrade. The `Pomerium` CRD is cluster-wide. If
  the cluster runs another Pomerium ingress controller, tell the person first.
  The new CRD only adds fields.
- The second command packages the platform chart into the quickstart chart. It
  is a local dependency, so `--skip-refresh` skips the network.
- If the chart refuses a value, its error message says what to set.

## Step 5: Check the install

1. The pods are ready:

   ```sh
   kubectl -n <pomerium namespace> rollout status statefulset/pomerium --timeout=300s
   kubectl -n <platform namespace> rollout status statefulset/agentops --timeout=300s
   kubectl -n <platform namespace> get sandboxwarmpool hello
   ```

   `READY` of the warm pool becomes `1` within a few minutes, when the first
   `hello` sandbox has started.

2. Pomerium has applied its settings and every route:

   ```sh
   kubectl get pomerium agentops -o json | jq '{generation: .metadata.generation,
     settings: .status.settingsStatus, routes: (.status.ingress | map_values({observedGeneration, reconciled}))}'
   ```

   Each `reconciled` is `true`, and `settings.observedGeneration` equals
   `generation`. This can take up to a minute after the install. An entry with
   `reconciled: false` names the problem.

3. The Service has an external address:

   ```sh
   kubectl -n <pomerium namespace> get service pomerium-proxy
   ```

   It is a LoadBalancer. Its `EXTERNAL-IP` stays `<pending>` on a cluster
   without a load balancer.

4. Pomerium answers, before DNS is ready. Forward a local port to the Service,
   and ask for the approval page through it. Expect `302`, a redirect to sign-in:

   ```sh
   kubectl -n <pomerium namespace> port-forward service/pomerium-proxy 8443:443 &
   sleep 3
   curl -sS -o /dev/null -w '%{http_code}\n' \
     --connect-to <agentic host>:443:127.0.0.1:8443 \
     https://<agentic host>/agentic/approve
   kill %1
   ```

   With a private CA, add `--cacert <CA file>`. Local clusters such as OrbStack
   and Docker Desktop publish a LoadBalancer on `127.0.0.1`, not on its
   external address.

5. DNS. Each host must resolve to the external address. Tell the person which
   records to make if they do not: one wildcard record for `*.<domain>`, or one
   record for each host. Then the same `curl` without `--connect-to` answers
   `302`.

## Step 6: Run the hello agent

This step needs DNS, and a browser for the person.

Get a token for `quickstart-client`, and set the Harness API URL. Each command
below uses these two variables. If your shell does not keep variables between
commands, run each command in one line with them.

```sh
export TOKEN="$(kubectl -n <platform namespace> create token quickstart-client --audience pomerium-agentic-as --duration 30m)"
export API=https://<harness-api host>
```

The Harness API is [Connect](https://connectrpc.com) with JSON: each call is a
`POST` to `$API/harnessapi.v1.HarnessAPIService/<method>`. With a private CA, add
`--cacert <CA file>` to each `curl`.

1. List the agents this client may run. Expect `hello`:

   ```sh
   curl -sS -X POST "$API/harnessapi.v1.HarnessAPIService/ListTemplates" \
     -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{}'
   ```

2. Create a session:

   ```sh
   curl -sS -X POST "$API/harnessapi.v1.HarnessAPIService/CreateSession" \
     -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
     -d '{"template":"hello","conversationRef":"hello-1","approvalPrompt":"Run the AgentOps hello demo agent?","initialPrompt":"Hello! Did the install work?"}' \
     | jq -r .session.id
   ```

   Keep the session id.

3. Read the session's events. Repeat every few seconds:

   ```sh
   curl -sS -X POST "$API/harnessapi.v1.HarnessAPIService/ListEvents" \
     -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
     -d '{"ref":{"sessionId":"<session id>"}}' | jq -c '.events[] | del(.sessionId, .timestamp)'
   ```

   - `stateChanged` goes `PENDING`, `LAUNCHING`, `AWAITING_APPROVAL`.
   - `approvalRequired.approvalUrl` is the approval link. Give it to the person.
     They open it, sign in as an approver and approve. The run waits for 15
     minutes.
   - `approved`, then `stateChanged` to `RUNNING`.
   - `agentMessage.text` is the agent's answer. Show it to the person. It begins
     with `Hello from AgentOps!` and names the sandbox pod.
   - `turnCompleted` ends the turn. The install works.

4. End the session:

   ```sh
   curl -sS -X POST "$API/harnessapi.v1.HarnessAPIService/EndSession" \
     -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
     -d '{"ref":{"sessionId":"<session id>"}}'
   ```

## Step 7: Report

Tell the person:

- what you installed, in which namespaces, with which hosts;
- the result of each check, and the `hello` agent's answer;
- what is left for them, such as DNS records;
- where their values file is, and that the same `kubectl apply` and
  `helm upgrade` commands from [Step 4](#step-4-install) upgrade the install;
- the next steps, from the [README](./README.md):
  [add an agent](./README.md#add-an-agent),
  [add the Slack bot](./README.md#add-the-slack-bot), or write a client with
  [the SDKs](./README.md#writing-a-client).

## When a check fails

| What you see | What to look at |
|---|---|
| The chart refuses to render | Its message names the value. Fix the values file and run `helm upgrade` again. |
| `pomerium-0` does not start | `kubectl -n <pomerium namespace> describe pod pomerium-0` and `kubectl -n <pomerium namespace> logs statefulset/pomerium`. A volume that is not writable needs `pomerium.persistence.initPermissions: true`. |
| A route has `reconciled: false` | Its error in the `Pomerium` status. Often the TLS Secret is missing or does not cover the host. |
| `curl` answers `000` | Nothing answers at that address: the Service has no address yet, or a firewall. With `--connect-to`, DNS is not the cause. |
| `curl` answers `404` | The host reaches a Pomerium that has no such route: DNS points at another Pomerium. |
| A Harness API call answers `403` with an HTML page | Pomerium refused the client: the `harness-api` route admits only the ServiceAccounts in `clients` and `quickstart-client`. Check the namespace and the token's audience. |
| A Harness API call answers JSON with `permission_denied` | The platform refused the client: its ClientBinding. `kubectl -n <platform namespace> logs statefulset/agentops \| grep "admitted a client"` shows the subject it saw. |
| The session stays in `LAUNCHING` | `kubectl -n <platform namespace> get sandboxes,sandboxclaims,pods`, and the platform log. |
| `launchStalled` after the approval | The sandbox's sidecar cannot reach Pomerium, or does not trust its certificate: `kubectl -n <platform namespace> logs <hello pod> -c sidecar`. |
| The approval page refuses the person | They are not an approver: `access.domains` and `access.emails` in the values file. |

## Uninstall

```sh
helm uninstall agentops -n <platform namespace>
```

This deletes the release's objects, and the Pomerium namespace too if the chart
created it. It leaves the CRDs and the platform's PersistentVolumeClaim.
