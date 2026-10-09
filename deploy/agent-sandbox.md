# agent-sandbox

AgentOps runs its sandboxes on
[agent-sandbox](https://github.com/kubernetes-sigs/agent-sandbox).
[`agent-sandbox.yaml`](agent-sandbox.yaml) is a verbatim copy of the upstream
v1.0.3 release asset `sandbox-with-extensions.yaml`: the `agent-sandbox-system`
namespace, the four CRDs (`sandboxes.agents.x-k8s.io` and the three
`extensions.agents.x-k8s.io` CRDs: `sandboxclaims`, `sandboxtemplates`,
`sandboxwarmpools`), the RBAC and the controller. The platform builds against
the `v1beta1` API of the same version (`sigs.k8s.io/agent-sandbox v1.0.3` in
[`harness/go.mod`](../harness/go.mod)).

The `agents.x-k8s.io` groups belong to agent-sandbox. AgentOps's own CRDs are
in `agents.pomerium.com`.

## A fresh cluster

```sh
kubectl apply -f deploy/agent-sandbox.yaml
kubectl -n agent-sandbox-system rollout status deployment/agent-sandbox-controller
```

## Changing the version

Fetch the release asset again, and keep it verbatim:

```sh
curl -fL -o deploy/agent-sandbox.yaml \
  https://github.com/kubernetes-sigs/agent-sandbox/releases/download/v1.0.3/sandbox-with-extensions.yaml
```

Do not export the objects from a live cluster instead. An export carries that
cluster's defaulted fields. Move `sigs.k8s.io/agent-sandbox` in `harness/go.mod`
to the same version.
