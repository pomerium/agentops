{{- define "slackbot.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "slackbot.fullname" -}}
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

{{- define "slackbot.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "slackbot.labels" -}}
helm.sh/chart: {{ include "slackbot.chart" . }}
{{ include "slackbot.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "slackbot.selectorLabels" -}}
app.kubernetes.io/name: {{ include "slackbot.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "slackbot.image" -}}
{{- $i := .Values.image -}}
{{- if $i.digest -}}
{{- printf "%s@%s" $i.repository $i.digest -}}
{{- else -}}
{{- printf "%s:%s" $i.repository ($i.tag | default .Chart.AppVersion) -}}
{{- end -}}
{{- end }}

{{- define "slackbot.secretName" -}}
{{- default (include "slackbot.fullname" .) .Values.slack.existingSecret -}}
{{- end -}}

{{- define "slackbot.validateValues" -}}
{{- if and (not .Values.slack.existingSecret) (or (not .Values.slack.signingSecret) (not .Values.slack.botToken)) -}}
{{- fail "Set slack.signingSecret and slack.botToken, or set slack.existingSecret to a Secret with the keys SLACK_SIGNING_SECRET and SLACK_BOT_TOKEN." -}}
{{- end -}}
{{- if not .Values.harnessAPI.url -}}
{{- fail "harnessAPI.url is required: the Pomerium route of the Harness API." -}}
{{- end -}}
{{- end -}}
