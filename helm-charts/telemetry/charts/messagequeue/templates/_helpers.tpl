{{/*
Full name of this chart's resources, e.g. telemetry-messagequeue.
*/}}
{{- define "messagequeue.fullname" -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Service that clients (streamer, collector) connect to. It resolves to whichever
pod currently holds the leader lease, because its selector matches the
component label the elected pod sets on itself.
*/}}
{{- define "messagequeue.leaderName" -}}
{{- printf "%s-leader" (include "messagequeue.fullname" .) -}}
{{- end -}}

{{/*
Headless Service covering every replica. A follower uses its pod's stable DNS
name under this Service to reach the elected leader, so a replica's changing IP
is resolved by DNS instead of being baked into configuration.
*/}}
{{- define "messagequeue.peersName" -}}
{{- printf "%s-peers" (include "messagequeue.fullname" .) -}}
{{- end -}}

{{/*
Selector matching the elected leader only.
*/}}
{{- define "messagequeue.leaderSelector" -}}
app.kubernetes.io/name: messagequeue
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: leader
{{- end -}}

{{/*
Selector matching the StatefulSet pods themselves (role-agnostic). This is what
the StatefulSet's own selector uses, so that the elected pod relabelling itself
does not fight the controller: a StatefulSet's spec.selector must match its own
pods, so it cannot include the mutable component label.
*/}}
{{- define "messagequeue.podSelector" -}}
app.kubernetes.io/name: messagequeue
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "messagequeue.serviceAccountName" -}}
{{- include "messagequeue.fullname" . -}}
{{- end -}}