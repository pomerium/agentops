# agentops-quickstart Helm chart

The AgentOps base stack in one release:

- **Pomerium**: the [Pomerium ingress controller](https://github.com/pomerium/ingress-controller) in all-in-one mode, from the image `pomerium/ingress-controller:experimental-agentic`, which contains the agentic authorization server (AS). It runs in a namespace of its own (`pomerium.namespace`), and each route is an Ingress there.
- **The platform**: the [`agentops`](../agentops) chart, as a subchart. Its values are under `agentops.*`.
- **The sandbox side**: the `sandbox-agent` ServiceAccount, and the `hello` demo agent (AgentTemplate, SandboxTemplate and SandboxWarmPool).
- **Clients**: the ServiceAccount `quickstart-client` with its ClientBinding, and a ClientBinding and a route policy entry for each entry in `clients`.

The release goes in the platform's namespace, by convention `agentops-system`. The chart does not install agent-sandbox, a TLS certificate, real agents or the Slack bot.

The repository's [README](../../../README.md#install) describes the installation, and the routes and objects in detail.

## Install

[INSTALL.md](../../../INSTALL.md) is the step-by-step install, written for a coding agent; a person can follow it too. In short, write the values (here `~/my-values.yaml`, outside the checkout), then:

```sh
kubectl apply --server-side --force-conflicts -f deploy/charts/agentops-quickstart/crds/
helm dependency build deploy/charts/agentops-quickstart
helm upgrade --install agentops deploy/charts/agentops-quickstart \
  --namespace agentops-system --create-namespace -f ~/my-values.yaml
```

The first command applies the CRDs. Helm installs the CRDs in `crds/` on the first install only, and never updates them. `helm dependency build` packages the [`agentops`](../agentops) chart into `charts/`.

The smallest values file:

```yaml
hosts:
  authenticate: authenticate.agentops.example.com
  anthropic: anthropic.agentops.example.com
tls:
  secret: cert-manager/agentops-wildcard
access:
  domains: [example.com]
agentops:
  config:
    agentic:
      asURL: https://agentic.agentops.example.com
    harness:
      externalURL: https://harness.agentops.example.com
      assertionIssuer: harness.agentops.example.com
      api:
        assertionIssuer: harness-api.agentops.example.com
```

The chart takes the AS, Agent Link and Harness API hosts from the platform's values (`agentops.config.*`), so each host is in one place. The chart does not render when a required value is missing, or when two values disagree. Its error message says what to set.

## Required values

| Value | Example | Meaning |
|---|---|---|
| `hosts.authenticate` | `authenticate.agentops.example.com` | Host of Pomerium's authenticate service. It must differ from every route host. |
| `hosts.anthropic` | `anthropic.agentops.example.com` | Host of the Anthropic route. The sidecars always point at it; the route exists only with `anthropic.enabled`. |
| `tls.secret` | `cert-manager/agentops-wildcard` | `namespace/name` of the TLS Secret with a certificate for every host. Pomerium reads it in any namespace. |
| `access.domains` or `access.emails` | `[example.com]` | Who may approve runs: the policy of the approval route and of the Anthropic route. |
| `agentops.config.agentic.asURL` | `https://agentic.agentops.example.com` | URL of the AS host. Its host is the host of the three AS routes. |
| `agentops.config.harness.externalURL` | `https://harness.agentops.example.com` | URL of the Agent Link route. |
| `agentops.config.harness.assertionIssuer` | `harness.agentops.example.com` | The host of `agentops.config.harness.externalURL`. |
| `agentops.config.harness.api.assertionIssuer` | `harness-api.agentops.example.com` | Host of the Harness API route. It must differ from the Agent Link host. |

## Values

| Key | Default | Description |
|---|---|---|
| `hosts.authenticate` | `""` | Required. See above. |
| `hosts.anthropic` | `""` | Required. See above. |
| `tls.secret` | `""` | Required. See above. |
| `privateCA.secretName` | `""` | A Secret in the release namespace with the CA certificate of a private CA. The `hello` sidecar mounts it and sets `SIDECAR_AGENTIC_CA_FILE`. The platform needs it too: mount it with `agentops.extraVolumes` and `agentops.extraVolumeMounts`, and set `agentops.config.agentic.caFile`. The chart fails if `caFile` is empty. |
| `privateCA.key` | `ca.crt` | The key of the CA certificate in that Secret. |
| `access.domains` | `[]` | Email domains of the approvers. |
| `access.emails` | `[]` | Email addresses of the approvers. |
| `pomerium.namespace` | `agentops-pomerium` | Namespace of Pomerium, its routes and its upstream Services. |
| `pomerium.createNamespace` | `true` | Create the namespace with the release. `helm uninstall` then deletes it, and Pomerium's databroker with it. Set `false` for a namespace that exists. |
| `pomerium.image.repository` | `pomerium/ingress-controller` | Image repository. |
| `pomerium.image.tag` | `experimental-agentic` | Image tag. CI of the ingress controller publishes it from its `experimental/agentic` branch, and the tag moves with the branch. |
| `pomerium.image.digest` | the build of 2026-09-24 | Image digest. When set, the chart uses it instead of the tag. Set it to `""` to follow the tag, with `pullPolicy: Always`. |
| `pomerium.image.pullPolicy` | `IfNotPresent` | Image pull policy. |
| `pomerium.imagePullSecrets` | `[]` | Pull secrets for the Pomerium pod. |
| `pomerium.configName` | `agentops` | Name of the cluster-scoped `Pomerium` object with the global settings (the controller's `--pomerium-config`). |
| `pomerium.ingressClass.name` | `pomerium-agentops` | The IngressClass of the routes. |
| `pomerium.ingressClass.controller` | `pomerium.io/ingress-controller-agentops` | The controller name (`--name`). It differs from the default, so another Pomerium ingress controller in the cluster never takes these Ingresses, and this one never takes its. |
| `pomerium.identityProvider.provider` | `hosted` | The identity provider for people. `hosted` is Pomerium's hosted one, which needs no configuration. |
| `pomerium.identityProvider.url` | `""` | The provider URL, for providers that need one. |
| `pomerium.identityProvider.secret` | `""` | `namespace/name` of a Secret with `client_id` and `client_secret`. |
| `pomerium.identityProvider.scopes` | `[]` | Scopes to request. |
| `pomerium.runtimeFlags` | `{}` | More runtime flags. The chart always sets `agentic` and `mcp`. |
| `pomerium.extraArgs` | `[]` | More arguments for the controller, for example `--debug-port=9200`. |
| `pomerium.service.type` | `LoadBalancer` | Type of the Service `pomerium-proxy` (ports 443 and 80). |
| `pomerium.service.annotations` | `{}` | Annotations on that Service, for example for a cloud load balancer. |
| `pomerium.service.loadBalancerIP` | `""` | A fixed load balancer IP, where the cloud supports it. |
| `pomerium.persistence.size` | `1Gi` | Size of the databroker's PersistentVolumeClaim. |
| `pomerium.persistence.storageClass` | `""` | Storage class of the claim. Empty uses the cluster default. |
| `pomerium.persistence.initPermissions` | `false` | Add an init container that runs as root and gives the volume to uid 65532. Set it only for a storage class that ignores `fsGroup` and makes volumes that only root can write. |
| `pomerium.issuerDiscoveryBinding` | `true` | Bind Pomerium's ServiceAccount to `system:service-account-issuer-discovery`, so it can read the cluster's OIDC discovery document. Most clusters bind that role to every ServiceAccount already. |
| `pomerium.resources` | requests 300m CPU and 200Mi, limit 1Gi | Container resources. |
| `pomerium.nodeSelector` | `kubernetes.io/os: linux` | Node selector. |
| `pomerium.tolerations` | `[]` | Tolerations. |
| `pomerium.affinity` | `{}` | Affinity. |
| `anthropic.enabled` | `false` | Add the Anthropic route: `https://<hosts.anthropic>` to `api.anthropic.com`, with the API key and without the run token. |
| `anthropic.apiKey` | `""` | The API key. The chart keeps it in the Secret `anthropic-api-key` in the Pomerium namespace. Empty keeps the key of that Secret, if it exists. |
| `anthropic.existingSecret` | `""` | A Secret in the Pomerium namespace with the key in `x-api-key`, instead of `apiKey`. |
| `sandbox.sidecar.image.repository` | `pomerium/agentops-sidecar` | The sidecar image, in the `hello` template. |
| `sandbox.sidecar.image.tag` | `""` | Its tag. Empty uses `agentops.image.tag`, so the sidecar and the platform come from one build. |
| `sandbox.sidecar.image.digest` | `""` | Its digest. When set, the chart uses it instead of the tag. |
| `sandbox.sidecar.image.pullPolicy` | `IfNotPresent` | Its pull policy. |
| `hello.enabled` | `true` | Install the `hello` demo agent. |
| `hello.name` | `hello` | Name of its AgentTemplate, SandboxTemplate and SandboxWarmPool. |
| `hello.image.repository` | `python` | Image of its `agent` container. The agent is a Python script from a ConfigMap. |
| `hello.image.tag` | `3.13-alpine` | Its tag. |
| `hello.image.digest` | `""` | Its digest. |
| `hello.image.pullPolicy` | `IfNotPresent` | Its pull policy. |
| `hello.warmPool` | `1` | Started pods that wait for a `hello` session. |
| `verifyClient.enabled` | `true` | Create the ServiceAccount `quickstart-client` and its ClientBinding, the client that [INSTALL.md](../../../INSTALL.md#step-6-run-the-hello-agent) runs the `hello` agent with. |
| `verifyClient.name` | `quickstart-client` | Its name. |
| `verifyClient.templates` | `[]` | The AgentTemplates it may run. Empty: `hello`. |
| `verifyClient.quotas` | 5 live, 2 pending, 10 per minute | Its quotas. |
| `clients` | `[]` | More clients. Each entry has `serviceAccount.namespace`, `serviceAccount.name`, and optionally `name` (of the ClientBinding; default the ServiceAccount's name), `templates` (default `[hello]`) and `quotas`. The chart adds the subject to the Harness API route's policy and makes the ClientBinding. |
| `agentops.fullnameOverride` | `agentops` | The platform's resource names. The AS route for runs admits the platform's ServiceAccount, whatever its name. |
| `agentops.config.agentic.dialAddress` | `pomerium-proxy.agentops-pomerium.svc.cluster.local:443` | The in-cluster Pomerium Service: `pomerium-proxy.<pomerium.namespace>.svc.cluster.local:443`. The default matches the default `pomerium.namespace`; change both together. |
| `agentops.config.agentic.audience` | `pomerium-agentic-as` | The audience of every projected token: the platform's, the sandboxes' and the clients'. Pomerium's `cluster` identity provider accepts this audience. |
| `agentops.installCRDs` | `false` | The platform chart's CRDs. This chart has them in `crds/`, so Helm can map the AgentTemplate and ClientBindings on the first install. Keep `false`. |
| `agentops.image.tag` | `main` | The platform image tag. |
| `agentops.*` | | Every other value of the [`agentops`](../agentops/README.md#values) chart. |

## Clients

A client of the Harness API needs a token that the Harness API route accepts, and a ClientBinding. An entry in `clients` gives it both:

```yaml
clients:
  - serviceAccount:
      namespace: agentops-slackbot
      name: agentops-slackbot
    templates: [hello, deploy-service]
    quotas:
      maxLiveSessions: 50
      maxPendingApprovals: 10
      createRatePerMinute: 30
```

The route's policy matches the raw subject of the client's projected ServiceAccount token, `system:serviceaccount:<namespace>:<name>`. The ClientBinding names the subject that Pomerium mints, `cluster/system:serviceaccount:<namespace>:<name>`. The token's audience must be `agentops.config.agentic.audience`.

## More routes

Each route is an Ingress of the class `pomerium.ingressClass.name` in `pomerium.namespace`, with Pomerium's settings in `ingress.pomerium.io/*` annotations. The controller reads Ingresses in that namespace only. An Ingress backend must be a Service in the Ingress's namespace, so an upstream in another namespace, or outside the cluster, needs an `ExternalName` Service there. [`templates/routes.yaml`](templates/routes.yaml) has the chart's routes; the repository's [README](../../../README.md#add-the-slack-bot) has a route for the Slack bot.

## The hello agent

[`files/hello-agent.py`](files/hello-agent.py) is an ACP agent in Python with no dependencies. The runner starts it with `python3 -u /opt/hello/agent.py`. It answers every prompt with the same text: a greeting, the pod's name, what worked, and the next steps. It can resume a session, so a paused `hello` session can continue. `harness/internal/runner/helloagent_test.go` runs it through the real runner.

Its SandboxTemplate is the pod that the agentops Component ([`deploy/components/agentops`](../../components/agentops)) builds. [`deploy/quickstart/contract`](../../quickstart/contract) is the same template as a kustomize overlay, and `make helm-check-quickstart-sandbox` checks that the two agree.

## CRDs

`crds/` has the ingress controller's CRDs (`Pomerium`, `PomeriumService`, `PolicyFilter`) and the platform's (`AgentTemplate`, `ClientBinding`). Helm installs them on the first install, skips the ones that exist, and never updates or deletes them. Apply them before each upgrade: `kubectl apply --server-side --force-conflicts -f deploy/charts/agentops-quickstart/crds/`. The install script does.

- `make quickstart-sync-pomerium-crds` copies the ingress controller's CRDs from its `experimental/agentic` branch (`POMERIUM_IC_REF`).
- `make helm-sync-crds` copies the platform's CRDs from `config/crd/bases`.

The `Pomerium` CRD is cluster-scoped and shared with any other Pomerium ingress controller in the cluster. The experimental CRD only adds fields.

## Uninstall

```sh
helm uninstall agentops -n agentops-system
```

This deletes the release's objects, and the Pomerium namespace with Pomerium's databroker, if the chart created it. It leaves the CRDs, every AgentTemplate and ClientBinding not made by the chart, and the platform's PersistentVolumeClaim.
