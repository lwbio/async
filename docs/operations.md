# 部署与验证

运行时要求 RabbitMQ 4.2 或更新版本，并在启动时检查版本及声明参数。当前验证基线为 4.2。生产环境建议至少三个节点及足够磁盘，quorum 少数节点故障的容错能力依赖队列实际副本数；单节点测试不能证明集群容错。

运行时显式声明持久 quorum 队列、持久消息、`x-overflow=reject-publish` 和 `x-delivery-limit=-1`。业务重试次数由协议控制，避免 broker 的默认 delivery limit 在重复基础设施故障时丢弃消息。每个重试队列设置固定 `x-message-ttl`、默认 DLX、原订阅 routing key 和 `x-dead-letter-strategy=at-least-once`。业务及死信队列不使用自动 DLX 转移，应用确认转发成功后再 ACK。

不要用 operator policy 覆盖这些安全属性；AMQP 声明不能全面审计管理员策略。部署时检查 feature flags 及有效 policy，并监控磁盘、队列积压及 quorum 健康。库不修改整个集群的 policy，也不自动删除遗留队列。同名订阅的所有实例须使用相同绑定、并发及重试配置；跨进程配置一致性由部署配置保证。

发布等待默认最多 10 秒。没有路由返回 `ErrUnroutable`；broker NACK 返回 `ErrNack`（不代表所有目标均未收到，重试仍应复用消息 ID）；已发送但未确认时返回 `ErrUnknown`。断线期间返回 `ErrUnavailable`。`Start` 完成表示本进程拓扑已建立，不表示所有远端订阅都已就绪。

`Config.PublishConcurrency` 默认 1，允许显式设置为 1–32，增加时会使用更多发布 channel，但每条仍只有一个在途确认；异常/未知确认仍可能使共享连接失效，不表示故障被隔离到某个发布 channel。调优前先执行 [三节点容量基准](observability.md)，不要直接套用本机短测参数。死信检查与受控重放见 [死信运维](deadletters.md)。

`Stop(ctx)` 先停止接收，等待在途处理和正在确认的发布；到期取消 handler 上下文并关闭连接。处理器必须响应取消。明确的重试／结果转移失败、消费 channel 故障或 broker 取消只重建对应订阅，原消息保持未确认，通过 1–30 秒指数退避恢复；消息处理成功后重置退避。网络故障、确认未知仍使连接代际失效。业务失败重试次数有限，基础设施恢复可能导致更多重复调用。

目标队列暂时缺失时，quorum 至少一次死信转发会保留消息并定期重试。验证的 RabbitMQ 4.2 环境中 `dead_letter_worker_publisher_confirm_timeout` 默认为 180000 毫秒，因此目标恢复后可能仍需等待数分钟；1／10／60 秒业务重试延迟不是故障恢复期限。CI 仅在隔离 broker 中把该间隔设为 1000 毫秒。

`Runtime.State()`、`LastError()` 和 `Stats()` 提供状态和累计计数。统一追踪覆盖请求、结果、事件及转移；日志包含消息和订阅身份，默认不记录负载。消费者须对业务副作用和结果接收实施幂等。

OpenTelemetry Metrics 接入、指标口径、告警建议及容量基准见 [监控与压测](observability.md)。

局部订阅恢复期间 `Runtime.State()` 可以仍为 Ready；此状态不能替代按订阅的健康监控。`LastError()` 保留最近一次故障，不是当前所有订阅的健康汇总。

## 可选延迟请求

安装与 RabbitMQ 版本匹配的 delayed-message 插件，并设置 `EnableDelay: true`。运行时声明 `<namespace>.v1.delay`；缺少插件将启动失败。延迟请求的 broker 确认不保证到期可路由，插件无 mandatory 保证且单节点存储，不适用于与常规路径同等级的可靠性要求。普通重试不依赖插件。

## 本地验证

```bash
make check
make generate                 # 需要 protoc 和 protoc-gen-go
docker run --rm --name async-test -p 127.0.0.1:5672:5672 \
  -e RABBITMQ_DEFAULT_USER=async -e RABBITMQ_DEFAULT_PASS=local-test \
  rabbitmq:4.2-management
```

另一个终端执行：

```bash
ASYNC_TEST_AMQP_URL=amqp://async:local-test@127.0.0.1:5672/ go test -race ./...
AMQP_URL=amqp://async:local-test@127.0.0.1:5672/ go run ./examples/echoapp
```

集成测试使用随机命名空间，仅针对隔离测试 broker 运行；部分测试会删除自己的目标队列以验证转移恢复。测试产生的队列保留在测试容器中，销毁容器即可清理。未设置测试 URL 时，集成测试明确跳过。

`scripts/test-cluster.sh` 自动创建并清理三节点 Docker 集群，检查队列实际具有三个副本，验证少数／多数副本故障及恢复。已安装匹配插件的测试 broker 可设置 `ASYNC_TEST_DELAY_PLUGIN=1` 验证业务延迟。单独设置 `ASYNC_TEST_RESTART_NODE` 并仅运行 `TestIntegrationBrokerRestart` 可重启指定的隔离 Docker broker；不要与其他集成测试同时执行。
