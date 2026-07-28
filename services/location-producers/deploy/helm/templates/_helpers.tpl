{{- define "location-producers.fullname" -}}
{{ .Release.Name }}
{{- end -}}

{{- define "location-producers.labels" -}}
app.kubernetes.io/name: location-producers
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "location-producers.selectorLabels" -}}
app.kubernetes.io/name: location-producers
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
