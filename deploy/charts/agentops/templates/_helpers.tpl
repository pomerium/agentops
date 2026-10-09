{{- define "agentops.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "agentops.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "agentops.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "agentops.labels" -}}
helm.sh/chart: {{ include "agentops.chart" . }}
{{ include "agentops.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "agentops.selectorLabels" -}}
app.kubernetes.io/name: {{ include "agentops.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "agentops.serviceAccountName" -}}
{{- include "agentops.fullname" . }}
{{- end }}

{{- define "agentops.image" -}}
{{- $i := .Values.image -}}
{{- if $i.digest -}}
{{- printf "%s@%s" $i.repository $i.digest -}}
{{- else -}}
{{- printf "%s:%s" $i.repository ($i.tag | default .Chart.AppVersion) -}}
{{- end -}}
{{- end }}

{{- define "agentops.validateValues" -}}
{{- if not .Values.config.agentic.asURL -}}
{{- fail "config.agentic.asURL is required: the base URL of the Pomerium host that serves the agentic authorization server." -}}
{{- end -}}
{{- if not .Values.config.harness.externalURL -}}
{{- fail "config.harness.externalURL is required: the Pomerium route that sandboxes dial to reach the Agent Link." -}}
{{- end -}}
{{- if not .Values.config.harness.assertionIssuer -}}
{{- fail "config.harness.assertionIssuer is required: the issuer of the assertions Pomerium stamps on the Agent Link route." -}}
{{- end -}}
{{- if not .Values.config.harness.api.assertionIssuer -}}
{{- fail "config.harness.api.assertionIssuer is required: the issuer of the assertions Pomerium stamps on the Harness API route." -}}
{{- end -}}
{{- if eq .Values.config.harness.api.assertionIssuer .Values.config.harness.assertionIssuer -}}
{{- fail "config.harness.api.assertionIssuer must differ from config.harness.assertionIssuer: Pomerium mints iss and aud from the route host, so with one issuer an Agent Link assertion is also a Harness API credential." -}}
{{- end -}}
{{- end -}}
