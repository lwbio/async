# async

RabbitMQ 专用 Go 异步消息库：异步请求与结果通知、事件发布订阅、确认发布、有限重试、死信处理、自动恢复和 OpenTelemetry。

本次重构采用全新协议及 Go 接口，不兼容旧版 `async/`、`Conn` 和 Publisher/Subscriber 接口。需要 Go 1.25.7+、RabbitMQ 4.2+；可靠队列使用 quorum。

## 快速开始

```go
r, err := async.New(async.Config{URL: brokerURL, Namespace: "orders"})
// 检查 err；在 Start 前注册处理器及结果订阅。
err = request.Register(r, "orders.Create", service.Create)
replyTo, err := request.Listen(r, service.ReceiveResult)
err = r.Start(ctx)
id, err := request.NewClient(r).Submit(ctx, "orders.Create", input,
    request.WithReplyTo(replyTo))
// id 是请求 ID；err 是发布结果，不是业务执行结果。
// 退出时使用带期限的上下文调用 r.Stop(shutdownCtx)。
```

完整、包含错误处理的可运行示例位于 [examples/echoapp](examples/echoapp/main.go)，类型化客户端和服务注册由 protobuf 生成。

运行时支持通过 `Config.MeterProvider` 接入 OpenTelemetry Metrics；指标含发布及等待耗时、按订阅的处理耗时/失败、重试、死信和恢复。详见 [监控与压测](docs/observability.md)；`make bench` 提供微基准和可选真实 RabbitMQ 压测。

事件使用 `pubsub.NewPublisher(r).Publish` 和 `pubsub.Subscribe`。订阅名称默认根据处理器生成，无须必填；需要稳定身份或区分闭包实例时使用 `async.WithSubscription`。同一订阅共享队列，不同订阅独立接收事件。

## 接入 Go Kratos

`*async.Runtime` 实现 Kratos 的 `transport.Server`，可直接交由应用管理：

```go
// 创建 r，并在启动前完成 request.Register、pubsub.Subscribe 等注册。
app := kratos.New(
    kratos.Name("orders"),
    kratos.Server(r),
    kratos.StopTimeout(10*time.Second),
)
if err := app.Run(); err != nil {
    log.Fatal(err)
}
```

由 Kratos 调用 `Start` 和 `Stop`，不要重复手动启动运行时。`Start` 建立连接和订阅后返回；后台消费持续运行，应用退出时按 Kratos 的停止期限排空。

## 行为约定

- `Submit` 返回请求 ID，不等待业务结果；没有结果目标时为单向请求。
- 重复投递可能发生，业务负责幂等；`async.MessageFromContext` 提供消息 ID 和重试信息。
- 普通错误及 panic 默认额外重试三次（1／10／60 秒）；`async.Permanent` 指定永久错误，最终失败进入死信。
- 确认丢失返回 `async.ErrUnknown`；调用方决定是否以同一个 ID 重试。
- 默认每订阅并发 8、预取 16，不保证业务完成顺序；可通过订阅选项调整。
- 延迟请求需显式启用插件能力，其可靠性低于常规投递路径。

## 开发

```bash
make check       # 两个 Go 模块的 build、race test、vet
make generate    # 需要 protoc、protoc-gen-go；生成插件和示例绑定
```

集成测试需要 `ASYNC_TEST_AMQP_URL`，未设置时跳过。参见 [部署与验证](docs/operations.md)、[架构](docs/architecture.md)、[消息协议](docs/protocol.md) 和 [术语表](GLOSSARY.md)。

GitHub 自动检查、三节点故障测试、报告下载和定时压测见 [CI 说明](docs/ci.md)。

明确的消费/转移故障按订阅退避恢复；确认未知仍按连接恢复。发布并发可用 `Config.PublishConcurrency` 显式调整（默认 1）。死信查询、默认 dry-run 的限速重放及审计见 [死信运维](docs/deadletters.md)。
