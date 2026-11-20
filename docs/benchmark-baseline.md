# 可观测性与容量基线

日期：2026-11-21。此记录为本地短时基准，不是生产容量承诺。

## 环境与命令

- Linux/amd64，AMD Ryzen 7 6800H。
- Go 1.23.10，OpenTelemetry 1.36.0。
- Docker `rabbitmq:4.2-management`，实际 RabbitMQ 4.2.9，单节点 quorum，持久消息，宿主机 loopback 端口。
- 同一 Runtime 发布及消费；每订阅并发 8、预取 16，禁用业务重试；无业务编解码和真实 exporter。
- broker 上存在此前集成测试遗留的空闲队列；硬件、磁盘及容器不是专用生产环境。基准期间没有并行运行集成测试。

```bash
GOTOOLCHAIN=go1.23.10 ASYNC_TEST_AMQP_URL=... go test -run '^$' \
  -bench '^Benchmark(HandlerInstrumentation|RabbitMQ)' \
  -benchmem -benchtime=2s -count=2 -cpu=1,8
```

## 结果

以下为两次运行的范围。Broker 基准使用 no-op metrics。

| 场景 | 负载 | 客户端并行度 | requests/s | P95（ms，近似桶上界） |
| --- | --- | --- | --- | --- |
| 发布确认 | 256B | 1 | 75.20–75.57 | 26.86–29.54 |
| 发布确认 | 256B | 8 | 70.38–76.25 | 135.74–149.31 |
| 发布确认 | 4KiB | 1 | 69.17–71.19 | 29.54 |
| 发布确认 | 4KiB | 8 | 71.47–72.90 | 149.31 |
| 请求—结果 | 256B | 1 | 45.13–46.00 | 39.32 |
| 请求—结果 | 256B | 8 | 43.89–48.48 | 218.60–240.46 |
| 请求—结果 | 4KiB | 1 | 42.33–43.00 | 39.32 |
| 请求—结果 | 4KiB | 8 | 40.72–42.25 | 240.46–264.51 |

所有 broker 基准的 unknown、unroutable 均为 0，没有发布或等待结果失败。压测结束后检查 broker 队列清单，未发现 `bench_` 前缀残留队列。

空 handler 的观测包装：

| Metrics 模式 | GOMAXPROCS | ns/op | B/op | allocs/op |
| --- | --- | --- | --- | --- |
| no-op | 1 | 1348–1404 | 1008 | 8 |
| no-op | 8 | 1191–1238 | 1008 | 8 |
| SDK 聚合，无 exporter | 1 | 2997–3378 | 1008 | 8 |
| SDK 聚合，无 exporter | 8 | 2586–2756 | 1008 | 8 |

微基准自身为串行循环，GOMAXPROCS=8 不代表 8 个 handler 并发。这里测量的是包装后的总开销，不是相对未观测 handler 的增量。

另以 `ASYNC_BENCH_METRICS=sdk`、256B、`-benchtime=1s -count=1 -cpu=1,8` 做了 SDK 聚合路径冒烟压测：发布确认分别为 73.05 / 67.66 requests/s，请求—结果分别为 47.87 / 43.61 requests/s，unknown/unroutable 均为 0。单次短测不用于判断模式间的性能差异。

## 解读与后续验证

本环境增加发布调用并发没有明显提高吞吐，却增加单条消息延迟，与当前“单条在途确认发布”的实现一致。但该结果不能独立证明全部耗时来自通道等待，broker 确认、存储和容器调度也可能占主要比例。

后续应在目标部署环境，使用实际 exporter，结合 `async.publish.wait.duration` 和 `async.publish.duration` 做长时间、三节点 quorum 及突发积压测试，再决定是否引入发布 channel 池。短时闭环基准不覆盖网络分区、磁盘告警和长期资源泄漏。
