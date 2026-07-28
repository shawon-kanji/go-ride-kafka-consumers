{{- define "driver-request-handler.fullname" -}}
{{ .Release.Name }}
{{- end -}}

{{- define "driver-request-handler.labels" -}}
app.kubernetes.io/name: driver-request-handler
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "driver-request-handler.selectorLabels" -}}
app.kubernetes.io/name: driver-request-handler
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
