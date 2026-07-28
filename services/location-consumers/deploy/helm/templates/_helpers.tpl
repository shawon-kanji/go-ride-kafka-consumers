{{- define "location-consumers.fullname" -}}
{{ .Release.Name }}
{{- end -}}

{{- define "location-consumers.labels" -}}
app.kubernetes.io/name: location-consumers
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "location-consumers.selectorLabels" -}}
app.kubernetes.io/name: location-consumers
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
