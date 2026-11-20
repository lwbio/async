# 消息协议 v1

协议不兼容旧版。命名空间由 1–64 个字母、数字、下划线或连字符组成。

| AMQP 字段 | 内容 |
| --- | --- |
| `message_id` | 逻辑消息 ID，重试保持不变；调用方可提供，否则生成 UUID |
| `correlation_id` | 结果对应的请求 ID |
| `reply_to` | 请求的结果队列；空表示单向请求 |
| `content_type` | `application/json` 或 `application/proto`；可注册其他 codec |
| `type` | `request`、`result` 或 `event` |
| `delivery_mode` | `2`，持久化 |
| `body` | codec 编码的负载；失败结果可为空 |
| `headers.async-version` | AMQP int32，固定为 `1` |
| `headers.async-kind` | 消息种类，与 type 一致 |
| `headers.async-operation` | 完整操作名或事件名 |
| `headers.async-attempt` | AMQP int32，首次为 `0`，每次业务重试递增 |
| `headers.async-error-code` | 失败结果的稳定错误码，成功时为空 |
| `headers.async-error-message` | 可公开的错误说明 |
| `headers.traceparent`、`tracestate`、`baggage` | 由配置的 OpenTelemetry propagator 注入 |

结果 ID 为 `SHA256("async.v1.result\x00" + requestID)` 的小写十六进制串。业务必须保持同一请求的结果一致；重发该结果使用相同 ID。

队列名称为 `<namespace>.v1.<kind>.<identityHash>`，其中 identityHash 为身份字符串 SHA256 的前 16 字节十六进制。请求身份为完整操作名，事件／结果订阅身份为完整处理器名或显式覆盖值。后缀 `.retry.N` 为第 N 次重试队列，`.dead` 为死信队列。

事件 exchange 为 `<namespace>.v1.events`，类型 topic，routing key 为完整事件名。订阅可绑定多个精确名称或 RabbitMQ `*`／`#` 模式。重试直接返回原订阅队列，不重新广播。请求和结果通过默认 exchange 按队列名称直接投递。

结果接收方先启动订阅，再把返回的队列目标交给请求方。多种结果类型共享订阅时，使用原始 `Runtime.Register` 并按 operation 调用生成的结果解码函数。

AMQP type 和 async-kind 必须一致。畸形协议不信任 reply_to，原始负载转入订阅死信队列；未知 codec 由类型化处理器作为永久错误处理。失败结果处理失败不会生成新的结果。

单个请求的结果发布、原消息 ACK 和死信转移不是原子事务，可能重复；消息 ID 并不自动实现业务去重。调用方通过 `async.MessageFromContext(ctx)` 获取 ID 和 attempt 来实施幂等。

## 可选运维元数据

死信转移附加 string 类型的 `async-dead-reason`、`async-dead-at`（UTC RFC3339Nano）、`async-dead-subscription` 及可选 `async-dead-error-code`；不添加原始异常文本或堆栈。这些不是业务失败结果字段，不改变请求不得携带 `async-error-code` 的约束。

受控重放保留消息及关联身份，附加 `async-replay-id`、`async-replay-at`、`async-replay-source`；只有显式选择时才把 `async-attempt` 重置为 int32(0)。重放移除已过时的死信原因与 broker 死亡历史，直接投递到原订阅队列，不经过事件广播 exchange。详见 [死信运维](deadletters.md)。

以上是 v1 的可选 headers，旧消息可以缺省，读取方不应将其作为授权、去重或原子移动完成的证明。
