{{$svrType := .ServiceType}}
{{$svrName := .ServiceName}}

{{- range .MethodSets}}
const AsyncOperation{{$svrType}}{{.OriginalName}} = "{{$svrName}}.{{.OriginalName}}"
const AsyncOperation{{$svrType}}{{.OriginalName}}Result = "{{$svrName}}.{{.OriginalName}}.Result"
{{- end}}

type {{.ServiceType}}AsyncServer interface {
{{- range .MethodSets}}
	{{- if ne .Comment ""}}
	{{.Comment}}
	{{- end}}
	{{.Name}}(context.Context, *{{.Request}}) (*{{.Reply}}, error)
{{- end}}
}

func Register{{.ServiceType}}AsyncServer(s *async.Server, srv {{.ServiceType}}AsyncServer) {
	{{- range .Methods}}
	s.MustRegister(_{{$svrType}}_{{.Name}}{{.Num}}_Async_Handler(srv), AsyncOperation{{$svrType}}{{.OriginalName}}, AsyncOperation{{$svrType}}{{.OriginalName}}Result)
	{{- end}}
}

{{range .Methods}}
func _{{$svrType}}_{{.Name}}{{.Num}}_Async_Handler(srv {{$svrType}}AsyncServer) func(ctx async.Context) error {
	return func(ctx async.Context) error {
		var in {{.Request}}
		if err := ctx.Decode(&in); err != nil {
			return err
		}
		out, err := srv.{{.Name}}(ctx, &in)
		if err != nil {
			return err
		}
		return ctx.Encode(out)
	}
}
{{end}}
