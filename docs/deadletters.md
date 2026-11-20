# 死信检查与受控重放

`cmd/asyncctl` 是独立运维工具，不启动业务处理器，不自动创建队列。只操作显式指定命名空间及订阅对应的死信队列，重放目标始终是原订阅队列，事件不重新广播。

## 查询与筛选

通过环境变量提供 broker URL，避免凭据进入进程参数。账号只应拥有必要命名空间的权限。

```bash
# 请求的 identity 是操作名；事件/结果的 identity 是稳定订阅名。
AMQP_URL=... go run ./cmd/asyncctl inspect \
  --namespace orders --kind request --identity orders.Create --limit 20

AMQP_URL=... go run ./cmd/asyncctl inspect \
  --namespace orders --kind event --identity invoices \
  --reason retries_exhausted --limit 100
```

输出包含源/目标队列、broker ready 消息数、扫描数量及消息 ID、关联 ID、操作、重试次数、负载字节数、协议有效性和死信原因；不输出负载或 broker URL。

从现在起，死信转移附带 `async-dead-reason`、`async-dead-at`、`async-dead-subscription` 和可选稳定错误码。reason 为 `invalid_protocol`、`permanent_error` 或 `retries_exhausted`，不保存原始异常文本。旧死信缺少这些元数据时仍可检查，reason 显示为空。

**AMQP 没有完全无副作用的 peek**：工具以手动确认方式获取样本，退出时关闭 channel，让 broker 重入队。消息不会因 dry-run 被 ACK，但顺序和 redelivery 标记可能变化；quorum 的重入队在另一连接的统计中可能短暂延迟。需要零扰动浏览时，应采用 broker 管理接口/离线副本，不应把这个命令当作纯数据库查询。

## 默认 dry-run，显式执行

```bash
# 不指定 --apply 时，仅检查，不移动消息。
AMQP_URL=... go run ./cmd/asyncctl replay \
  --namespace orders --kind request --identity orders.Create --limit 20

# 确认业务修复、幂等和目标订阅就绪后再实际重放。
AMQP_URL=... go run ./cmd/asyncctl replay \
  --namespace orders --kind request --identity orders.Create \
  --limit 20 --rate 5 --apply --actor operator-name \
  --audit /secure/path/replay.jsonl --timeout 60s
```

- `--limit` 是扫描上限（1–1000），包括过滤掉或无法重放的消息，不是保证成功移动的数量。
- `--rate` 是实际重放速率上限（默认 10 条/秒，最大 1000），发布、审计及确认耗时可能进一步降低速度。
- 默认保留业务 Attempt。仅在明确希望重新给予重试预算时加 `--reset-retries`。
- 始终保留 MessageID、CorrelationID、操作、负载和结果目标。附加独立 ReplayID、重放时间和源队列，移除旧死信原因和 broker 的死亡历史字段。
- 缺少目标队列时拒绝 apply；协议损坏或 kind 不匹配的消息只记录 skipped_invalid，不确认、不强制重放。
- 同一个坏消息可能反复进入死信。不要通过更换 MessageID 绕过业务幂等，也不要盲目循环重放。

## 审计和故障恢复

实际重放必须提供 `--actor` 与私有普通审计文件（权限 0600）。工具追加 JSONL，不覆盖历史记录；每条记录同步到磁盘。

顺序为：持久审计 intent → mandatory 确认发布到原队列 → 持久审计 confirmed → ACK 源死信 → 审计 moved。审计不包含负载、密码、原始错误文本。

发布失败、结果未知、confirmed 审计失败或源 ACK 失败时，保留原消息的恢复机会并退出。若发布已经成功但后续步骤失败，原队列和死信队列可能同时出现同 ID 消息；这是允许的重复，不是事务移动。moved 表示客户端 ACK 调用成功，也不是 broker 给 ACK 再做确认。审计与 broker 不能原子提交，最后一条 moved 记录失败时报告会包含已完成数量；后续处置仍必须以消息身份和业务幂等为依据。

broker 操作受整体期限约束，取消会关闭独立连接并释放未确认消息；本地文件系统的阻塞写入/fsync 无法通过 context 强行中断。文件系统审计需要可用磁盘，错误时会失败关闭，不为了“继续重放”跳过审计。
