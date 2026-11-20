# 监控与压测

## 接入 OpenTelemetry Metrics

`async.Config.MeterProvider` 接受应用管理的 `metric.MeterProvider`；未设置时使用全局 provider。库不创建 exporter、不开放 HTTP 端口，也不负责关闭 provider。没有 SDK/exporter 时为低开销 no-op；原有 `Stats()` 不变。

在应用中创建 SDK provider（版本与本库 OpenTelemetry 保持一致），通过 OTLP exporter 或 Prometheus reader 导出。例如，已经创建 `reader` 后：

```go
provider := sdkmetric.NewMeterProvider(
    sdkmetric.WithReader(reader),
    // 应用在 Resource 中设置 service.name、service.instance.id、部署环境等属性。
)
r, err := async.New(async.Config{
    URL: brokerURL, Namespace: "orders", MeterProvider: provider,
})
// 检查 err，注册订阅，然后启动 r。
// 退出时先 r.Stop(shutdownCtx)，再 provider.Shutdown(exportCtx)；
// 使用独立的有期限上下文，避免停止超时导致最后一批指标无法导出。
```

`sdkmetric` 对应 `go.opentelemetry.io/otel/sdk/metric`。也可以在创建 Runtime 前调用 `otel.SetMeterProvider(provider)`，但显式传入更便于多实例隔离及测试。Tracing 仍通过 `Config.Tracing` 独立配置。

### 指标

Instrumentation scope：`github.com/lwbio/async`。时长单位均为秒；直方图提供显式桶边界，reader/view 可以覆盖。

| 名称 | 类型 | 含义 |
| --- | --- | --- |
| `async.publish.count` | Counter | 发布尝试，包含 API 提前拒绝及内部结果、重试、死信发布 |
| `async.publish.duration` | Histogram | 发布耗时，包含通道等待、发送和 broker 确认 |
| `async.publish.wait.duration` | Histogram | 等待串行发布通道的时间，包含等待期间取消；API 提前拒绝没有该样本 |
| `async.handler.count` | Counter | 实际业务处理器调用次数 |
| `async.handler.duration` | Histogram | 处理器调用耗时，含失败与 panic |
| `async.process.count` | Counter | 消息处理次数，含重投，覆盖协议校验、处理器、转移及 ACK |
| `async.process.duration` | Histogram | 单次完整消息处理耗时 |
| `async.process.in_flight` | UpDownCounter | 正在处理的消息数，不含 broker 待处理及预取缓冲消息 |
| `async.retry.count` | Counter | 已确认的重试转移次数 |
| `async.dead_letter.count` | Counter | 已确认的死信转移次数，包括非法协议 |
| `async.recovery.attempts` | Counter | 断线后重新连接并恢复拓扑的尝试次数，不含首次启动 |
| `async.recovery.duration` | Histogram | 一轮恢复的总耗时，包含退避；成功、停止或取消时记录 |
| `async.subscription.recovery.attempts` | Counter | 对应订阅重新声明拓扑及创建消费 channel 的尝试次数 |
| `async.subscription.recovery.duration` | Histogram | 一轮订阅局部恢复耗时，包含退避，直到重新创建消费 channel 或停止 |
| `async.subscription.recovering` | UpDownCounter | 处于局部恢复等待中的订阅数；属性包含 kind 和 subscription |

共同属性为 `async.namespace`；消息指标还有 `async.kind`（request/result/event/unknown），消费指标还有 `async.subscription`（启动前注册的稳定身份）。不把消息 ID、关联 ID、负载、错误文本或任意操作名放入指标属性，避免高基数和敏感信息泄漏。

有 `async.outcome` 的指标使用以下值：

- publish：`success`、`unroutable`、`unknown`、`nack`、`unavailable`、`canceled`、`timeout`、`error`。已发送后取消优先归为 `unknown`，不伪装成确定取消。
- handler：`success`、`error`、`permanent_error`、`panic`、`canceled`。
- process：`success`、`retry`、`dead_letter`、`invalid_protocol`、`error`、`canceled`。转移或 ACK 失败归为 error；handler 成功不代表 process 成功。
- recovery.attempts：`success` 或 `error`；recovery.duration：`success`、`stopped`、`canceled`。

订阅局部恢复指标没有 outcome 标签。重新建立 channel 不代表业务故障已解决；持续缺失的结果目标可能让它再次进入退避。局部恢复期间 Runtime 可保持 Ready，需结合 subscription.recovering 和恢复尝试的增长判断订阅健康。

重试/死信计数表示转移发布得到确认，不代表原消息 ACK 必然成功；确认与 ACK 间隙可能重复计数。指标不是业务账本。`publish.count` 包含内部消息，不能直接当作业务请求吞吐。

### 日志、追踪和告警

- handler 失败日志包含 namespace、消息 ID、订阅、重试次数、错误类型；panic 额外记录调用栈，不记录 panic 值。默认不记录业务错误文本、负载或 broker URL；业务可以在处理器内自行记录经脱敏的详情。
- 恢复结束日志记录 outcome 和总耗时。恢复时长直方图在恢复结束才更新，持续故障期间还应结合 `Runtime.State()`、`LastError()`、恢复尝试计数和 broker 监控判断健康状况。
- 消息 ID 用日志和 trace 关联，不用作 metric 标签。consumer span 补充订阅、重试次数、重投标记，转移或 ACK 失败也标为 error；现有上下文传播保留。
- 优先告警：unknown/nack/unroutable 增长、持续无法恢复、死信持续增长、handler 高失败率、发布等待 P95/P99 上升。
- RabbitMQ 自身的 ready/unacked 消息数、最老消息年龄、磁盘/内存告警、连接流控和 quorum 副本健康仍需 broker 侧监控；运行时指标不能替代队列监控。

## 压测

### 不依赖 broker 的微基准

```bash
GOTOOLCHAIN=go1.25.7 go test -run '^$' \
  -bench '^BenchmarkHandlerInstrumentation$' -benchmem -count=3
```

此项分别测量空 handler 在 no-op metrics 和启用 SDK 聚合（ManualReader、不导出）下的调用开销，不包含真实 exporter 的开销。

### 真实 RabbitMQ

只使用**可丢弃 RabbitMQ 4.2+ broker**，不要对生产 broker 执行。基准使用随机命名空间，结束后停止运行时并删除自己的业务/结果队列、死信队列及事件 exchange；强制终止时可能残留资源，建议销毁测试容器。

```bash
ASYNC_TEST_AMQP_URL=amqp://async:local-test@127.0.0.1:5672/ \
GOTOOLCHAIN=go1.25.7 go test -run '^$' \
  -bench '^BenchmarkRabbitMQ' -benchmem -benchtime=10s -count=3 -cpu=1,8

# 默认预取 16、每订阅并发 8；可以分别调整，预取必须不小于并发。
ASYNC_TEST_AMQP_URL=amqp://async:local-test@127.0.0.1:5672/ \
ASYNC_BENCH_CONCURRENCY=16 ASYNC_BENCH_PREFETCH=32 \
GOTOOLCHAIN=go1.25.7 make bench

# 对比 SDK 聚合开销（ManualReader、不包含 exporter）。
ASYNC_TEST_AMQP_URL=amqp://async:local-test@127.0.0.1:5672/ \
ASYNC_BENCH_METRICS=sdk GOTOOLCHAIN=go1.25.7 go test -run '^$' \
  -bench '^BenchmarkRabbitMQ' -benchmem -benchtime=10s -count=3 -cpu=1,8
```

覆盖 256B 和 4KiB 原始负载，无业务编解码开销：

- `BenchmarkRabbitMQPublish`：确认发布延迟及吞吐；计时结束后继续等待消费完成，消费排空不计入发布吞吐。
- `BenchmarkRabbitMQRequestResult`：请求发布、处理、结果发布、接收结果的完整往返。使用同一 Runtime，反映请求与结果争用单条发布通道的现状。
- Go `RunParallel` 默认以 GOMAXPROCS 数量作为并发调用数；`-cpu` 调整的是基准客户端并行度，环境变量调整的是订阅消费并发。
- 输出包含 `requests/s`、`p50-us`、`p95-us`、`p99-us`、`unknown`、`unroutable`、`B/op` 和 `allocs/op`。分位数使用固定内存对数桶，报告桶上界，通常至多高估约 10%；亚微秒样本按 1µs、超过约 10 小时的样本按最后一桶处理。
- 默认显式使用 no-op provider，`ASYNC_BENCH_METRICS=sdk` 启用 SDK 聚合；运行时和基准客户端在同一进程，分配指标包含期间后台消费的开销，不仅是 Publish 调用本身。
- `ASYNC_BENCH_METRICS=json` 配合 `ASYNC_BENCH_METRICS_FILE=/path/to/metrics.jsonl` 启用实际 JSON exporter，每秒导出一次，结束时 flush。它测量文件导出，不代表 OTLP/Prometheus 或生产采集器的开销。
- `ASYNC_BENCH_PUBLISHERS` 设置确认发布 channel 数（默认 1、最大 32）；`ASYNC_BENCH_HANDLER_DELAY=10ms` 可模拟慢处理器。`goroutines-delta` 是计时前后进程 goroutine 数的差值，可能受后台退出影响，不能单独证明无泄漏。
- 任一发布或等待结果失败会导致基准失败，不静默排除错误。按消息操作采集延迟；Go 的 ns/op 是并行运行总时间/操作数，不等于单条消息延迟。

固定 Go/RabbitMQ 版本、CPU/内存、磁盘、网络 RTT、quorum 副本数、并行度和负载大小后再对比。容量基准不要开 `-race`，正确性检查单独执行。单节点本机数据不能外推为三节点生产容量；默认 no-op metrics 的结果也不能替代实际 exporter 配置下的压测。

`make bench` 同时执行微基准及 broker 基准；没有测试 URL 时 broker 基准明确跳过。压测属于闭环负载模型，不能据此证明突发流量和持续积压下的延迟上界。

本地实际执行结果见 [基线记录](benchmark-baseline.md)。

### 可复现三节点容量采集

```bash
# 输出目录应是新目录；脚本自己创建和清理三个隔离节点。
GOTOOLCHAIN=go1.25.7 bash scripts/benchmark-cluster.sh /path/to/new-report

# 比较可选的四通道，不修改默认配置。
GOTOOLCHAIN=go1.25.7 ASYNC_BENCH_PUBLISHERS=4 \
  bash scripts/benchmark-cluster.sh /path/to/another-report
```

默认每项 10 秒、重复 3 次、客户端并行度 1/8、JSON 文件 exporter。可用 `ASYNC_BENCH_TIME`、`ASYNC_BENCH_COUNT`、`ASYNC_BENCH_CPUS` 调整；固定次数可写 `ASYNC_BENCH_TIME=60x`。这些参数不是负载保证。

基准会核对业务/结果队列**实际拥有三个副本**，否则失败。输出目录包含 benchmark 文本、环境信息、指标 JSONL、CPU/heap profile、对应测试二进制及节点日志。profile 包含准备及等待阶段的进程行为，不只是 handler。脚本本身运行时间可能远大于各项计时之和，执行器超时应覆盖建群和多轮采集，不要把截断报告当成通过。

JSON 指标为周期累计快照，统计时同一 namespace/属性集取最后一份，不能把所有快照直接相加。这些本地三节点仍共享一台主机，不代表目标网络、磁盘及故障域；生产 exporter 和长时间稳定性测试需要目标环境参数，不能在本机宣称已完成。
