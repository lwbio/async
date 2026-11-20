# 重构验证记录

验证日期：2026-11-21（Asia/Shanghai）。环境：Go 1.23.10、RabbitMQ 4.2.9、delayed-message 插件 4.2.0；broker 均为本次创建的隔离 Docker 容器。

## 已验证

- `make generate`：从统一 annotation schema 生成两份绑定，并生成示例客户端、服务注册和结果解码函数。
- `make check`：根模块及生成器模块的 build、race test、vet。
- JSON 与 protobuf 类型化请求／结果完整往返；示例程序实际运行。
- 协议版本、消息身份、事件模式匹配、codec 错误输入及 panic 分类。
- 暂时性失败有限重试、永久失败结果及死信转移。
- 结果目标缺失后恢复，原请求重新投递并使用稳定结果 ID。
- TTL 到期时目标队列缺失，消息被 broker 保留，目标恢复后成功转移。
- 同名订阅共享消费，不同订阅独立接收事件。
- 不可路由反馈；TCP 代理丢弃 broker 确认时返回 `ErrUnknown`。
- 连续多次断线后重建拓扑与消费者；broker 实际重启后的恢复。
- 发布 channel 单独关闭时连接代际失效，不会长期停留在不可用的 Ready 状态。
- 正常退出排空、超时退出取消处理，以及跨连接代际的并发上限。
- 匹配版本插件的可选延迟请求实际延迟投递。
- 三节点 quorum：检查实际副本数，暂停一个副本后仍能发布；暂停两个副本时不误报成功；恢复后继续发布。
- `scripts/test-cluster.sh` 完整建群、测试及资源清理流程。
- Kratos v2.7.3：`Runtime` 的 `transport.Server` 编译期断言，真实 `kratos.App` 的启动失败传播、消息处理和停止生命周期（含 race 检查）。

断线恢复与跨代际并发测试额外连续运行三次，均通过。测试以消息 ID 判断恢复送达，不把故障期间允许的重复投递误判为失败。

## 验证中确认的约束

RabbitMQ 4.2.9 的死信发布确认重试间隔在测试环境默认为 180 秒。最初 45 秒的等待不足；通过 broker 活状态确认消息仍被保留后，修正了测试期限，并仅在隔离测试 broker 将间隔缩短为 1 秒。生产恢复时间不能由业务重试 TTL 推断。

本记录不代表生产吞吐、长期资源使用或所有网络分区组合已获验证。当前每运行时单条在途发布，重构阶段未做容量评估，后续短时基准见下文；可选延迟插件仍具有已记录的单节点存储和路由反馈限制。管理员策略一致性、业务幂等及不响应取消的处理器由部署方和业务方负责。

## 可观测性与压测增量验证

同日补充 OpenTelemetry Metrics、脱敏的处理器失败类型及 panic 调用栈、发布通道等待观测，以及微基准和真实 broker 压测。未改变投递协议和确认语义。

- 使用 Go 1.23.10、隔离 RabbitMQ 4.2.9 执行两个模块的 build、race test、vet。
- ManualReader 测试覆盖发布分类、提前拒绝、handler 结果、panic 日志不泄漏原值、ACK 失败及在途指标归零。
- 实际 broker 验证成功、重试、死信及通道等待指标；修正测试把“暂时无在途消息”误作“重试处理结束”的等待条件后，新增集成测试连续 race 运行十次通过。
- 无 broker 的取消测试覆盖等待发布通道时取消也会记录等待时间。
- 完成 no-op/SDK 聚合微基准，以及 256B/4KiB、客户端并行度 1/8 的 broker 基准。详情和局限见 [基线记录](benchmark-baseline.md)。
- 本次没有重跑 delayed-message 插件、broker 重启或三节点 quorum 专项测试，也没有实际 exporter、长期稳定性或生产容量验证。

## Go 1.25.7 升级验证

根模块和生成器模块的最低 Go 版本均升级为 1.25.7，使用 `GOTOOLCHAIN=go1.25.7 make check` 完成两个模块的 build、race test、vet。此次未设置 `ASYNC_TEST_AMQP_URL`，broker 集成测试跳过；未重跑容量基准，上述 Go 1.23.10 历史结果保留原版本标记。

## GitHub CI 增量验证

- 两份 workflow 通过 actionlint 1.7.7（含 ShellCheck 0.10.0 对内联脚本的检查），集群脚本通过 Bash 语法检查及 ShellCheck。
- 在临时副本中使用固定 protoc 31.1 / protoc-gen-go 1.36.12 执行 `make generate`，生成结果与当前绑定一致，未覆盖工作区文件。
- Go 1.25.7 下两个模块的 race + JSON 测试输出和覆盖率 profile 均正常生成；两个模块的 `go mod tidy` 不产生变化。
- 实际运行三节点 quorum 故障测试通过；验证了清理前各节点日志保存到指定 artifact 目录。
- 本地验证发现以 root 执行 broker 启动健康检查存在 Erlang cookie 所有权竞态，导致 broker 启动失败；CI 健康检查和诊断改为以 `rabbitmq` 用户执行。
- 本地随机端口容器的重启验证失败后，最小重现确认 Docker 会重新分配随机宿主端口，导致客户端连接旧端口；CI 使用固定的 5672 映射。两种问题都未通过放宽业务测试超时规避。
- 改用固定宿主端口和 `rabbitmq` 用户健康检查后，Go 1.25.7 下带真实 broker 的 `make check` 通过，真实容器重启恢复测试连续三次通过。
- no-op 和 SDK 模式的 broker 基准均通过 `-benchtime=1x -cpu=1,8` 冒烟验证；这不是完整容量测量，不替换历史性能基线。

以上为本地执行记录；GitHub runner、Actions 缓存、artifact 上传和定时触发仍须提交推送后由 GitHub 验证。CI 范围及未覆盖项见 [CI 说明](ci.md)。

## 按顺序实施的可靠性、容量和运维增量

环境为 Go 1.25.7、隔离 RabbitMQ 4.2.9，未访问生产 broker。

- **故障隔离**：明确的消费/转移故障改为订阅级恢复；缺失结果目标不打断正常订阅，保持原连接代际。channel 关闭、队列删除、局部退避时停机保留消息等回归测试通过；主要隔离/退出/并发用例连续三轮 race 通过。跨局部消费代际复用并发许可，不响应取消的旧 handler 不会使并发上限失效。
- **容量采集**：补充三节点脚本，逐队列核实实际三副本，使用实际 JSON exporter，保存指标、CPU/heap profile、环境和节点日志。完成 1/4 发布 channel 的固定样本短测；首次截断及一次运行中脚本编辑产生的无效采集均单独排除，最终结果见 [三节点容量对比](capacity-quorum.md)。
- **有界发布并发**：默认仍为 1，显式配置支持 1–32。并发混合正常路由/不可路由的 100 次发布测试通过，确认未知的单/四通道代理测试通过；pool 取消和 drain 保留许可的测试通过。
- **真实故障**：单/四通道均通过三节点少数/多数副本失效及恢复测试；单/四通道均通过固定端口容器的真实 broker 重启测试。此验证不覆盖所有网络分区和 broker 流控组合。
- **死信工具**：默认 dry-run 保留消息；apply 需 actor 与私有持久审计。实际 broker 测试覆盖身份保留、显式 Attempt 重置、确认发布后源 ACK、审计失败前不发布、发布后审计失败保留源消息。CLI 的私有审计文件、审计阶段和输出脱敏也通过实际 broker 测试；工具、维护实现和 transport 测试连续三轮 race 通过。
- 带真实 broker 的两个模块 `make check` 通过；workflow 通过 actionlint（含内联 ShellCheck），两个集群脚本通过 ShellCheck。新增运维 headers 为协议 v1 可选扩展，不需要修改 protobuf 绑定。

目标部署环境、真实业务负载、目标 OTLP/Prometheus exporter、长时间资源稳定性和突发积压容量仍未提供/验证。单机三节点短测不能替代这些验收；发布并发未自动提高默认值。此轮未重跑 delayed-message 插件专项。
