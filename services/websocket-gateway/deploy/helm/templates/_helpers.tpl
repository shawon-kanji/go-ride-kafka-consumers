{{- define "websocket-gateway.fullname" -}}
{{ .Release.Name }}
{{- end -}}

{{- define "websocket-gateway.labels" -}}
app.kubernetes.io/name: websocket-gateway
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "websocket-gateway.selectorLabels" -}}
app.kubernetes.io/name: websocket-gateway
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
