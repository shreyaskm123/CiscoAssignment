{{/*
Full name of this chart's resources, e.g. telemetry-collector.
*/}}
{{- define "collector.fullname" -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}