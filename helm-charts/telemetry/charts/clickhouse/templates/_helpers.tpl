{{/*
Full name of this chart's resources, e.g. telemetry-clickhouse.
*/}}
{{- define "clickhouse.fullname" -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Headless Service giving every ClickHouse replica a stable DNS name
(<pod>.<headless>.<ns>.svc.cluster.local). Replicas use these names to fetch
parts from each other and to find the other members of the cluster.
*/}}
{{- define "clickhouse.headlessName" -}}
{{- printf "%s-headless" (include "clickhouse.fullname" .) -}}
{{- end -}}

{{/* Name of the ClickHouse Keeper StatefulSet, e.g. telemetry-clickhouse-keeper. */}}
{{- define "clickhouse.keeperName" -}}
{{- printf "%s-keeper" (include "clickhouse.fullname" .) -}}
{{- end -}}

{{/* Headless Service giving every Keeper member a stable DNS name. */}}
{{- define "clickhouse.keeperHeadlessName" -}}
{{- printf "%s-headless" (include "clickhouse.keeperName" .) -}}
{{- end -}}

{{/*
Pod labels. Keeper is deliberately NOT labelled name=clickhouse: the
NetworkPolicies select ClickHouse by that label, and Keeper must not inherit
the client-facing rules.
*/}}
{{- define "clickhouse.selectorLabels" -}}
app.kubernetes.io/name: clickhouse
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "clickhouse.keeperSelectorLabels" -}}
app.kubernetes.io/name: clickhouse-keeper
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
