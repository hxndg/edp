# Argo Workflows 当前实现

本文只描述现行代码。旧 Pipes/pod/saga 文档已归档，不可作为设计依据。

## 分工

1. Kafka sensor 按 `(manifest_op, processing_type)` 组批并创建独立 Dagster run，不查询业务表。
2. run 只选择 `status=ready` 的业务项，从 PG 一次 JOIN 冻结 processing definition 与不可变执行 Profile，再批量 acquire claim，写最后 run/profile 并递增业务执行次数。
3. run 为每个业务项写 staging input，提交一个 Workflow CR。
4. Argo 使用 Profile 指定的版本化 WorkflowTemplate 和镜像运行 task。
5. Workflow 终态后，Dagster 读取 manifest 和最终 `PodOutcome`；成功子集由单写者提交 Iceberg。
6. Dagster 在一个 PG 事务中批量写 done/failed、最终错误摘要并释放仍归当前 run 的 claim。

## Workflow 构造

`common/argo_workflows.py` 只动态生成：

- 确定性 Workflow 名：Dagster run 前缀 + 全部业务 id 摘要；
- task items：完整 `biz_id`、命令、staging prefix 和三档内存；
- batch parallelism、单 task 超时和 worker image。

模板 YAML 在 `deploy/k8s/31-argo-worker-template.yaml`；当前名字是
`edp-worker-batch-v1`。模板与 `worker_execution_profile` 都只新增、不原地修改，
`processing_type_definition.active_execution_profile_id` 是唯一可切换指针。DAG 使用
`failFast: false`，单个 task 最终失败不会取消同批其它 task。

## 退出与重试

worker 始终写结构化 manifest。`engines/worker/exit_policy.py` 把 manifest 翻译为：

- `0`：成功；
- `10`：确定性业务错误，不重试；
- `20`：可重试基础设施错误；
- `137` 或其它异常退出：由 Argo 识别并重试。

每个 retry 前先删旧 manifest，避免上一 attempt 污染最终判断。模板最多执行三次，
内存按 `memory-0 → memory-1 → memory-2` 升档。task retry 次数不写 PG。

## 最终观测

`PodOutcome` 以 Pod 输入中的完整 `biz-id` 绑定业务项，不依赖 task 顺序。多个 retry
Pod 按结束时间选最后一次作为最终 phase/message/exit code，同时保留：

- `retry_count`
- 所有 `pod_names`
- 所有 MinIO `log_uris`

Dagster 日志记录完整 outcome；PG 业务行保存最终错误摘要、`last_dagster_run_id`、
`last_execution_profile_id` 和 `execution_attempt_count`。Workflow annotation 记录 processing type、profile 与 image。

## 一致性

`execution_claim.run_id` 是 fencing token。heartbeat、最终业务更新和 release 都检查
当前 run；旧 owner 不能覆盖接管者。Iceberg 写仍由 Dagster run 单写者执行，worker
不连 PG，也不碰 Iceberg catalog。

心跳超时本身不等于失败。`reconciliation_schedule` 每 5 分钟创建一个正常 Dagster
run，查询 stale claim 对应的 Dagster run 和 Argo Workflow；只有两边都不活跃才置
`failed/EXECUTION_LOST`、释放 claim 并写 alert。它不创建处理 run，也不投 Kafka；
恢复只能由用户通过 retry API 显式执行 `failed → ready`。
