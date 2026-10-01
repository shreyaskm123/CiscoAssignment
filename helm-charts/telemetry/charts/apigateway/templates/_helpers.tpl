{{/*
Full name of this chart's resources, e.g. telemetry-apigateway.
*/}}
{{- define "apigateway.fullname" -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}