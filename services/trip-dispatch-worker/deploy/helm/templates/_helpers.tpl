{{- define "trip-dispatch-worker.fullname" -}}
{{ .Release.Name }}
{{- end -}}

{{- define "trip-dispatch-worker.labels" -}}
app.kubernetes.io/name: trip-dispatch-worker
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "trip-dispatch-worker.selectorLabels" -}}
app.kubernetes.io/name: trip-dispatch-worker
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
