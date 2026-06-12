{{/*
Expand the name of the chart.
*/}}
{{- define "agentops.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name. Truncated at 63 chars for the DNS
naming spec. If the release name contains the chart name it is used as-is.
*/}}
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

{{/*
Chart name and version as used by the chart label.
*/}}
{{- define "agentops.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "agentops.labels" -}}
helm.sh/chart: {{ include "agentops.chart" . }}
{{ include "agentops.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "agentops.selectorLabels" -}}
app.kubernetes.io/name: {{ include "agentops.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
ServiceAccount name.
*/}}
{{- define "agentops.serviceAccountName" -}}
{{- include "agentops.fullname" . }}
{{- end }}

{{/*
Secret holding the Slack credentials: an existing one if provided, otherwise
the chart-managed Secret named after the release.
*/}}
{{- define "agentops.secretName" -}}
{{- with .Values.existingSecret.name -}}
{{- . -}}
{{- else -}}
{{- include "agentops.fullname" . -}}
{{- end -}}
{{- end -}}

{{/*
Validate required values.
*/}}
{{- define "agentops.validateValues" -}}
{{- if and (not .Values.existingSecret.name) (or (not .Values.slack.signingSecret) (not .Values.slack.botToken)) -}}
{{- fail "Set slack.signingSecret and slack.botToken, or point existingSecret.name at a Secret with keys SLACK_SIGNING_SECRET and SLACK_BOT_TOKEN." -}}
{{- end -}}
{{- if not .Values.config.oauthRedirectBaseURL -}}
{{- fail "config.oauthRedirectBaseURL is required (the externally reachable base URL for the Slack OAuth redirect)." -}}
{{- end -}}
{{- end -}}
