# 三节点容量对比：单通道与四通道

日期：2026-11-21。Linux/amd64、Ryzen 7 6800H、Go 1.25.7、RabbitMQ 4.2.9-alpine。三个隔离 Docker 节点共享宿主机，业务/结果队列的实际副本数逐一检查为 3；持久消息、每订阅并发 8、预取 16，启用 OpenTelemetry 1.36.0 JSON 文件 exporter（每秒一次）。

## 方法

```bash
GOTOOLCHAIN=go1.25.7 ASYNC_BENCH_TIME=60x ASYNC_BENCH_COUNT=1 \
ASYNC_BENCH_CPUS=1,8 ASYNC_BENCH_PUBLISHERS=1 \
  bash scripts/benchmark-cluster.sh /path/to/serial

GOTOOLCHAIN=go1.25.7 ASYNC_BENCH_TIME=60x ASYNC_BENCH_COUNT=1 \
ASYNC_BENCH_CPUS=1,8 ASYNC_BENCH_PUBLISHERS=4 \
  bash scripts/benchmark-cluster.sh /path/to/pool4
```

每场景固定 60 次，单轮短测，没有业务编解码或真实业务逻辑；两个配置在独立重建的集群上顺序执行，不同时施压。本地报告分别保存于 `/tmp/opencode/async-capacity-pool1-final` 和 `/tmp/opencode/async-capacity-pool4`，包含原始输出、指标、profile 和节点日志，不将二进制/profile 提交到仓库。

## 8 并发结果

| 场景 | 负载 | 1 通道 requests/s | 4 通道 requests/s | 1 通道 P95 ms | 4 通道 P95 ms |
| --- | --- | --- | --- | --- | --- |
| 发布确认 | 256B | 50.25 | 173.2 | 198.73 | 63.32 |
| 发布确认 | 4KiB | 46.81 | 162.3 | 198.73 | 57.57 |
| 请求—结果 | 256B | 24.76 | 70.83 | 426.00 | 164.24 |
| 请求—结果 | 4KiB | 27.78 | 81.40 | 320.06 | 123.40 |

P95 为对数桶近似上界，不是精确采样分位数。客户端并行度为 1 时，增加 channel 没有一致收益，符合单个调用无法利用池并行度的预期。

所有场景 unknown/unroutable 为 0。对 exporter 的周期累计快照按 namespace/属性集去重、取最后一份后，两个配置各有 732 条发布观测（包含校准及内部结果消息）：单通道平均等待 63.407ms、总发布耗时 82.571ms；四通道平均等待 11.654ms、总发布耗时 33.429ms。这是多个场景的汇总，不应视为某个业务的 SLA。

## 决策与限制

数据支持增加可选、有界的并行确认 channel；每条仍只放一条在途消息以保持 Return/confirm 对应。默认配置仍是 1，不自动扩容，也不根据单轮数据声称生产提升固定倍数。

最初的定时校准采集被执行器 120 秒超时截断，未用作此表；随后一轮基准本身完整通过，但脚本在运行期间被修改而发生末尾解析错误，也未用作此表。上述表使用最后一次完整成功的单通道和四通道脚本运行。

这不是目标部署环境，也不是长时间资源稳定性、突发积压或网络/磁盘故障容量验证。JSON 文件 exporter 不代表目标 OTLP/Prometheus 采集器；两个配置有宿主机噪声和执行顺序偏差。目标环境需重复测量、固定业务负载/磁盘/网络及 exporter，并运行长时间测试后再选并发。
