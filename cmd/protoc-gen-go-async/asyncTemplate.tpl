{{$svrType := .ServiceType}}
{{$svrName := .ServiceName}}
{{range .MethodSets}}
const AsyncOperation{{$svrType}}{{.OriginalName}} = "{{$svrName}}.{{.OriginalName}}"
{{end}}

type {{.ServiceType}}AsyncServer interface {
{{range .MethodSets}}
    {{.Name}}(context.Context, *{{.Request}}) (*{{.Reply}}, error)
{{end}}
}

func Register{{.ServiceType}}AsyncServer(r *async.Runtime, srv {{.ServiceType}}AsyncServer, opts ...async.SubscriptionOption) error {
{{range .Methods}}
    if err := request.Register(r, AsyncOperation{{$svrType}}{{.OriginalName}}, srv.{{.Name}}, opts...); err != nil { return err }
{{end}}
    return nil
}

type {{.ServiceType}}AsyncClient struct { client *request.Client }
func New{{.ServiceType}}AsyncClient(r *async.Runtime) *{{.ServiceType}}AsyncClient {
    return &{{.ServiceType}}AsyncClient{client: request.NewClient(r)}
}
{{range .MethodSets}}
func (c *{{$svrType}}AsyncClient) {{.Name}}(ctx context.Context, in *{{.Request}}, opts ...request.CallOption) (string, error) {
    return c.client.Submit(ctx, AsyncOperation{{$svrType}}{{.OriginalName}}, in, opts...)
}
func Decode{{$svrType}}{{.Name}}Result(m async.Message) (request.Result[{{.Reply}}], error) {
    return request.DecodeResultFor[{{.Reply}}](m, AsyncOperation{{$svrType}}{{.OriginalName}})
}
{{end}}
