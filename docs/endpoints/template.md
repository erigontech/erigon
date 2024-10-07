<!-- 
{{ .Disclaimer }}
-->

# RPC Endpoints

> {{ .Disclaimer }}

Here you will find the list of all supported JSON RPC endpoints.
If the endpoint is not in the list below, it means this specific endpoint is not supported yet, feel free to open an issue requesting it to be added and please explain the reason why you need it.
{{ range .EndpointGroups }}
## {{ .Key }}
{{ range .Value }}
- {{ . }}
{{- end }}
{{ end }}