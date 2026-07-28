{{- define "cab-request-handler.fullname" -}}
{{ .Release.Name }}
{{- end -}}

{{- define "cab-request-handler.labels" -}}
app.kubernetes.io/name: cab-request-handler
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "cab-request-handler.selectorLabels" -}}
app.kubernetes.io/name: cab-request-handler
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
