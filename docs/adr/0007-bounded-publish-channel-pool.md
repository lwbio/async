# 可选的有界确认发布通道池

三节点短测显示发布通道等待占主要耗时，允许通过 `Config.PublishConcurrency` 显式配置 1–32 条确认发布 channel，默认仍为 1。每条 channel 只允许一条在途消息，独立配对 Return/confirm，而不是在同一 channel 内并行发送后猜测 Return 的归属。

这增加了吞吐和启动时的 channel 数，且不提供跨 channel 顺序保证。任一发布 channel 的确认不确定或关闭仍使整个共享 Session 失效，保留原有保守的未知结果语义；自动扩容或只重建可疑发布 channel 暂不实现。单机三节点短测不是生产选型依据，部署应结合确认延迟、流控和容量验证选择并发。
