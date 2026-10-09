{{- define "quickstart.labels" -}}
helm.sh/chart: {{ include "agentops.chart" . }}
app.kubernetes.io/part-of: agentops
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "quickstart.pomeriumSelector" -}}
app.kubernetes.io/name: pomerium
app.kubernetes.io/component: proxy
{{- end }}

{{- define "quickstart.image" -}}
{{- if .digest -}}
{{- printf "%s@%s" .repository .digest -}}
{{- else -}}
{{- printf "%s:%s" .repository .tag -}}
{{- end -}}
{{- end }}

{{- define "quickstart.platformContext" -}}
{{- toJson (dict "Values" .Values.agentops "Release" (dict "Name" .Release.Name) "Chart" (dict "Name" "agentops")) -}}
{{- end }}

{{- define "quickstart.platformName" -}}
{{- include "agentops.fullname" (include "quickstart.platformContext" . | fromJson) -}}
{{- end }}

{{- define "quickstart.platformServiceAccount" -}}
{{- include "agentops.serviceAccountName" (include "quickstart.platformContext" . | fromJson) -}}
{{- end }}

{{- define "quickstart.agenticHost" -}}
{{- (urlParse .Values.agentops.config.agentic.asURL).host -}}
{{- end }}

{{- define "quickstart.harnessHost" -}}
{{- (urlParse .Values.agentops.config.harness.externalURL).host -}}
{{- end }}

{{- define "quickstart.harnessAPIHost" -}}
{{- .Values.agentops.config.harness.api.assertionIssuer -}}
{{- end }}

{{- define "quickstart.proxyAddress" -}}
{{- printf "pomerium-proxy.%s.svc.cluster.local:443" .Values.pomerium.namespace -}}
{{- end }}

{{- define "quickstart.clients" -}}
{{- $default := ternary (list .Values.hello.name) (list) .Values.hello.enabled -}}
{{- $clients := list -}}
{{- with .Values.verifyClient -}}
{{- if .enabled -}}
{{- $clients = append $clients (dict "name" .name "namespace" $.Release.Namespace "account" .name "templates" (.templates | default $default) "quotas" .quotas) -}}
{{- end -}}
{{- end -}}
{{- range .Values.clients -}}
{{- $sa := .serviceAccount | default dict -}}
{{- $clients = append $clients (dict "name" (.name | default $sa.name) "namespace" $sa.namespace "account" $sa.name "templates" (ternary .templates $default (kindIs "slice" .templates)) "quotas" .quotas) -}}
{{- end -}}
{{- toJson $clients -}}
{{- end }}

{{- define "quickstart.approverCriteria" -}}
{{- $criteria := list -}}
{{- range .Values.access.domains -}}
{{- $criteria = append $criteria (dict "domain" (dict "is" .)) -}}
{{- end -}}
{{- range .Values.access.emails -}}
{{- $criteria = append $criteria (dict "email" (dict "is" .)) -}}
{{- end -}}
{{- toJson $criteria -}}
{{- end }}

{{- define "quickstart.approverPolicy" -}}
{{- toYaml (dict "allow" (dict "or" (include "quickstart.approverCriteria" . | fromJsonArray))) -}}
{{- end }}

{{- define "quickstart.runTokenPolicy" -}}
{{- $ns := .Release.Namespace -}}
{{- $rules := list -}}
{{- range include "quickstart.approverCriteria" . | fromJsonArray -}}
{{- $and := list . (dict "claim/act.kubernetes.io.namespace" $ns) (dict "claim/act.kubernetes.io.serviceaccount.name" "sandbox-agent") -}}
{{- $rules = append $rules (dict "allow" (dict "and" $and)) -}}
{{- end -}}
{{- toYaml $rules -}}
{{- end }}

{{- define "quickstart.anthropicKey" -}}
{{- if .Values.anthropic.apiKey -}}
{{- .Values.anthropic.apiKey | b64enc -}}
{{- else -}}
{{- $old := lookup "v1" "Secret" .Values.pomerium.namespace "anthropic-api-key" -}}
{{- if and $old $old.data -}}
{{- index $old.data "x-api-key" -}}
{{- end -}}
{{- end -}}
{{- end }}

{{- define "quickstart.validate" -}}
{{- $v := .Values -}}
{{- $harness := include "quickstart.harnessHost" . -}}
{{- if not (regexMatch "^[a-z0-9-]+/[a-z0-9.-]+$" $v.tls.secret) -}}
{{- fail "tls.secret is required: the namespace/name of the TLS Secret with the wildcard certificate for every host, for example cert-manager/agentops-wildcard." -}}
{{- end -}}
{{- if eq $v.pomerium.namespace .Release.Namespace -}}
{{- fail "pomerium.namespace must differ from the release namespace: Pomerium and its routes need a namespace of their own." -}}
{{- end -}}
{{- if not $v.hosts.authenticate -}}
{{- fail "hosts.authenticate is required: the host of Pomerium's authenticate service, for example authenticate.agentops.example.com." -}}
{{- end -}}
{{- if not $v.hosts.anthropic -}}
{{- fail "hosts.anthropic is required: the host of the LLM route that sandboxes dial, for example anthropic.agentops.example.com. The route itself exists only with anthropic.enabled." -}}
{{- end -}}
{{- $hosts := list $v.hosts.authenticate (include "quickstart.agenticHost" .) $harness (include "quickstart.harnessAPIHost" .) $v.hosts.anthropic -}}
{{- if ne (len $hosts) (len (uniq $hosts)) -}}
{{- fail (printf "every host must be different: %s" (join ", " $hosts)) -}}
{{- end -}}
{{- if not (or $v.access.domains $v.access.emails) -}}
{{- fail "access.domains or access.emails is required: the people who may approve runs, for example --set access.domains={example.com}." -}}
{{- end -}}
{{- if ne $v.agentops.config.harness.assertionIssuer $harness -}}
{{- fail (printf "agentops.config.harness.assertionIssuer must be %q, the host of agentops.config.harness.externalURL." $harness) -}}
{{- end -}}
{{- if not $v.agentops.config.agentic.dialAddress -}}
{{- fail (printf "agentops.config.agentic.dialAddress is required: set it to %s, the in-cluster Pomerium Service." (include "quickstart.proxyAddress" .)) -}}
{{- end -}}
{{- if and $v.privateCA.secretName (not $v.agentops.config.agentic.caFile) -}}
{{- fail "privateCA.secretName is set but agentops.config.agentic.caFile is not: mount the CA into the platform with agentops.extraVolumes and agentops.extraVolumeMounts and point caFile at it." -}}
{{- end -}}
{{- if and $v.anthropic.enabled (not $v.anthropic.existingSecret) (not (include "quickstart.anthropicKey" .)) -}}
{{- fail "anthropic.enabled needs anthropic.apiKey or anthropic.existingSecret." -}}
{{- end -}}
{{- range $v.clients -}}
{{- if not (and .serviceAccount .serviceAccount.namespace .serviceAccount.name) -}}
{{- fail "each entry in clients needs serviceAccount.namespace and serviceAccount.name." -}}
{{- end -}}
{{- end -}}
{{- end -}}
