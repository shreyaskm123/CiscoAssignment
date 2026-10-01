{{/*
Full name of this chart's resources, e.g. telemetry-streamer.
*/}}
{{- define "streamer.fullname" -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}