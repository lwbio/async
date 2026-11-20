# GitHub CI

## 必需检查

`.github/workflows/go.yml` 在推送到 `main`、面向 `main` 的 PR 和手动触发时执行四组检查：

| Job | 检查内容 |
| --- | --- |
| `Check (runtime)` / `Check (generator)` | 两个模块分别检查依赖文件、build、vet、race test 和覆盖率；runtime job 还检查全仓库 Go 格式和 workflow 语法 |
| `Generated bindings` | 固定 protoc 31.1 和 protoc-gen-go 1.36.12，重新生成 annotation 和示例绑定，检查已跟踪文件变化及新增文件 |
| `RabbitMQ integration` | 在隔离 RabbitMQ 4.2.9 上运行根模块的 race 测试；随后单独运行真实容器重启恢复测试 |
| `Three-node quorum faults` | 建立三节点 quorum，检查实际副本数及少数/多数节点失效时的行为，退出时清理容器和网络 |

两个模块均使用根 `go.mod` 声明的 Go 版本（当前 1.25.7）。`GOTOOLCHAIN=local` 避免检查过程中隐式换版本；`GOFLAGS=-mod=readonly` 防止 build/test 隐式修改依赖。`go mod tidy` 作为独立检查允许更新文件，但只要产生 diff 就失败。

生成工具升级时，应同时更新 workflow 中的版本、protoc 下载 SHA-256，并重新生成提交派生文件。protoc 从官方 release 下载并校验哈希，不使用系统不固定版本的包。

### 报告与失败诊断

- `check-runtime`、`check-generator`：JSON 测试结果、覆盖率 profile 和摘要。
- `rabbitmq-integration`：集成/重启测试 JSON 结果，失败时尽力收集 broker 日志、容器运行/健康状态和队列状态。
- `quorum-faults`：集群脚本输出及清理前保存的各节点日志。脚本也支持本地设置 `ASYNC_TEST_ARTIFACT_DIR` 保存日志。
- 正确性报告保留 7 天，不设置覆盖率百分比门槛。

本地复现重启测试时必须显式指定固定宿主端口（例如 `-p 127.0.0.1:5672:5672`）。Docker 的随机端口映射可能在 `restart` 后变化，原 `ASYNC_TEST_AMQP_URL` 随之失效，不能把它误判为运行时恢复失败。

workflow 只有 `contents: read` 权限；同一 workflow/ref 的新运行取消旧运行。各 job 设置超时，矩阵中单个模块失败不取消另一个模块。服务健康检查等待 RabbitMQ 应用实际启动，而不只检查 Erlang 节点存活。健康检查及诊断命令以 `rabbitmq` 用户执行，避免启动期间 root 诊断进程创建不可读的 Erlang cookie。

如果启用分支保护，应把上表中的具体 job 名设置为 required checks。CI 只会在包含这些工作区修改的提交推送后运行；本地修改本身不会触发 GitHub。

## 定时与手动压测

`.github/workflows/benchmark.yml` 每周一 **03:23 UTC** 和手动触发时运行。默认分支上的 workflow 才会执行 schedule；分支上的新 workflow 需要先合入默认分支。

- 分别测量 no-op 和实际 JSON 文件 exporter 模式，以及 1/4 条发布 channel；每个矩阵 job 使用独立的单节点 RabbitMQ 4.2.9。JSON 模式同时上传指标 JSONL，不代表生产采集器的开销。
- 运行微基准、256B/4KiB 发布确认及请求—结果往返，客户端并行度 1/8，每项 3 秒、重复 3 次。
- 不开 race，不设置吞吐/延迟阈值。测试错误仍会使 job 失败；共享 runner 的性能波动不能作为性能回归结论。
- Artifact 包含原始 Go benchmark 输出、Go/broker 版本、CPU/系统信息、存储驱动，以及失败时的 broker 日志和容器状态，保留 30 天。

压测口径和本地运行方式见 [监控与压测](observability.md)。真实 exporter、生产磁盘、三节点容量和长时间稳定性需要独立验证。

## 尚未覆盖

CI 不安装 delayed-message 插件，其专项测试会明确跳过。插件版本及单节点存储限制仍须按 [部署文档](operations.md) 验证。覆盖率用于诊断，不证明所有故障组合均已验证。
