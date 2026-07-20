# Pod Fan-out 实现讲解：Pipes worker + run 单写者 commit + 错误码与重试 + K8s 权限

> **历史归档（不可作为现行设计）**：本文描述已删除的 Pipes fan-out。现行 Argo
> WorkflowTemplate、task retry 与薄 claim 见 `docs/argo-workflows-code-review.md`。

对应 README 3.6.3。本文讲四件事：

1. 这套 fan-out 的代码怎么分工、每个模块干什么（Dagster Pipes 形态）；
2. 内存模型：worker 流式解析、run pod 分块 commit，两端峰值内存都与数据量解耦；
3. 错误码体系与重试：谁产生状态码、怎么传回来、按码决定自动重试还是等人工；
4. K8s 权限（ServiceAccount / Role / RBAC）怎么配、为什么这么配，以及一次真实的 403 事故复盘。

---

## 一、总体架构：控制面与数据面的切分

一个 ingest 微批（≤200 个 upload）由**一个 Dagster run pod** 处理，但 run pod 只做"控制面"的活；批内每个 upload 最重的计算（下载 MCAP → 流式解析 → 清洗 → 切片 → 写 Lance）外包给**一个独立的 worker pod**，用 **Dagster Pipes（`PipesK8sClient`）** 拉起：

```text
run pod（控制面 + 单写者）                     worker pods（每 upload 一个）
──────────────────────────────────           ─────────────────────────────
claim_many（saga 互斥）
写 input.json 到 staging  ───────────────▶   下载 MCAP 到本地盘 → 流式解析/清洗
Pipes 起 pod（线程池并发一波 ≤20 个）             → 切片写 Lance → 厚表分块写 staging parquet
  worker 日志实时流回本 run 的 compute log
  监视线程：刷 saga 心跳 + 抓 pod 终态快照
收 manifest.json（薄表行 + error_code 内联）◀──  业务失败也写清单（带状态码）后正常退出
  无清单 → 按 pod 终态推断 OOM/超时/丢失
每表每批一次 Iceberg commit（事务内分块追加）
succeed_many + session done；失败按码进重试层
```

两条不随实现变的硬约束：

- **Iceberg commit 与 Saga 所有权收敛在 run pod 单写者**。worker 各自 commit 等于退回"每上传一次 commit"，200 个写者还会在 catalog 的乐观并发控制上互相冲突重试，saga 的 fencing 也保不住互斥。
- **worker 完全无状态**：只碰对象存储（staging / raw / quarantine 前缀）和 Lance 卷，**不连 PG、不碰 Iceberg catalog、不调 K8s API**。worker 怎么死都不影响一致性——没有清单就等于没干过。

为什么用 Pipes 而不是裸 K8s Job（此前的实现）：`PipesK8sClient` 把 worker 的 stdout **实时流回 run 的 compute log**——在 Dagster UI 里看一个 run 就能看到它所有 worker 的日志，不用 `kubectl logs` 逐个追；worker 完成时还能通过 pipes 消息上报结构化小结（`report_custom_message`）。**但 Pipes 只是观测通道，不是数据契约**：薄表行、厚表文件引用、错误码的真相仍在 staging 的 `manifest.json` 里——数据不受日志行长度限制，且 worker 与编排框架保持松耦合（本地 `python -m engines.worker.ingest_parse` 直接运行，无 pipes 环境时自动退化为普通进程）。

---

## 二、模块讲解

### 2.1 `engines/worker/staging.py`：run ↔ worker 的交接约定

双方的数据通道是 MinIO 上的一个前缀，每个 (run, upload) 一个，天然隔离、可并发：

```text
staging/{run_id}/{upload_id}/input.json      run pod 写：worker 的全部输入
staging/{run_id}/{upload_id}/manifest.json   worker 写：结果清单（薄表行 + error_code 内联）
staging/{run_id}/{upload_id}/bronze_imu.parquet   worker 写：厚表数据文件（多 row group）
staging/{run_id}/{upload_id}/silver_imu.parquet
```

- **厚薄分工**：`bronze_imu`/`silver_imu` 每 upload 成百上千行，走 parquet 文件；`raw_file`/`episode`/`sample` 等索引行每 upload 只有几行，直接内联在 `manifest.json` 的 `thin_rows` 里。
- **可见性**：staging 前缀不属于任何 Iceberg 表。Iceberg 的规则是"不被快照引用的文件不存在"（README 4.5），所以 worker 写到这里的东西对任何读者都不可见，直到 run pod 把行注册进表。
- `iter_parquet_batches` 逐 row group 产出 parquet 内容，是 run pod 分块收数的读取端（见 2.4）。
- JSON 里的 datetime 用 `{"__dt__": iso}` 标记往返，双方不需要知道哪些字段是时间类型。

### 2.2 `engines/worker/ingest_parse.py`：worker 入口（流式 + 带码）

`python -m engines.worker.ingest_parse --upload-id ... --run-id ... --staging-prefix ...`。顶层先 `_maybe_open_pipes()`：有 pipes bootstrap 环境变量（`PipesK8sClient` 注入）就打开会话，没有就当普通进程跑。

**内存模型：两遍扫描，峰值与文件大小/时长解耦。**

1. **pass 1（`_download_and_scan`）**：每个文件下载到本地盘（`boto3.download_file` 流式落盘，不整块进内存）→ 流式 sha256 → 流式统计 imu 消息数和 min/max ts。结构性损坏在这里暴露：append 模式坏文件写 quarantine 前缀后继续（全坏才整单失败 `DATA_EMPTY`）；correct 模式任何坏文件整单失败——修正已有数据只写一半时间窗比不写更糟。
2. **pass 2（`_emit_file_rows`）**：文件按起始时间排序后逐个流式重放。bronze/silver 行进 `_ChunkedWriter`——每攒 `INGEST_WORKER_CHUNK_ROWS`（默认 5 万）行就 flush 一个 row group 到本地 parquet，内存里永远只有一个 chunk；清洗策略（`clean_default` 等）也按 chunk 调用。切片走 `_WindowSlicer` 的**水位线 flush**：处理完一个文件，下一个文件起始时间之前结束的窗口即可关闭（写 Lance + 产出 sample/gold 行），不会把整个 episode 的 silver 行都攒在内存里等最后统一切。

窗口锚点仍是 episode 起始时间（correct 模式由 run pod 从 Iceberg 读好塞进 input.json），"距锚点第 N 个 2s 窗"的序号与切片调用的时间范围无关，确定性 `sample_id = {episode_id}-w{N:04d}` 在 correct 重切时命中原样本、upsert 覆盖而不是新增。

**最关键的一行语义**在 `main` 的异常处理：业务失败时 worker 把 `error_code` + `error` 写进 manifest 后 `sys.exit(0)` **正常退出**（pod 显示 Completed）。这样两类失败的判定简单可靠：

| 失败类型 | 表现 | run pod 的判定依据 |
| --- | --- | --- |
| 业务失败（数据的问题） | pod Completed，manifest 里 `status=error` + **error_code** | 读 manifest（worker 自报，最精确） |
| pod 级失败（OOM/超时/镜像拉不下/调度不上） | pod Failed 或一直 Pending，**没有 manifest** | 监视线程抓到的 pod 终态快照（见 2.3） |

### 2.3 `common/worker_pods.py`：Pipes 拉起 + 监视线程

替代此前手写的 `common/k8s_jobs.py`（K8s Job 轮询版）。`launch_wave` 并发跑一波 worker：

- **`PipesK8sClient.run` 每次调用同步等一个 pod**，所以一波内用线程池并发（每 upload 一个线程）；`common/db.py` 每次调用新建 PG 连接，线程安全。pod spec 里带 `activeDeadlineSeconds`（硬超时，kubelet 到点杀 pod）、内存 limit（按 attempt 升档，见第四节）、`edp-env` ConfigMap 和 Lance PVC。
- **`_WaveMonitor` 监视线程**每 5s 干两件事：
  1. 回调刷 saga 心跳（`advance_many("PARSE", ...)`）——没有这个，等 worker 的几分钟里心跳不动，stuck sensor 会把 run pod 误判为"owner 已死"并抢走会话；
  2. 按 label（`app=edp-ingest-worker, dagster-run-id=...`）抓 pod 状态快照，记录 `terminated.reason` / `exit_code` / pod `reason`。**这是 OOM/超时分类的唯一来源**：`PipesK8sClient` 在 pod 结束后会删掉 pod，事后查不到；而且被 OOMKill 的进程根本来不及自报，只能在它死前后由旁观者记下来。
- **失败隔离**：`client.run` 的任何异常（worker 非零退出、pod 被杀、API 抖动）都收进该 upload 自己的 `PodOutcome`，绝不打断同波其他线程。
- **超时兜底**：`future.result(timeout=deadline+120s)` 兜底"pod 一直 Pending 调度不上、activeDeadlineSeconds 从未开始计时"的情况，超时后主动删 pod。
- pod 由 Pipes 在结束后**自动删除**——排查现场不靠 pod 留存，靠三样东西：流回 run compute log 的 worker 日志、staging 里的 manifest、`saga_log.error_code`。

### 2.4 `engines/spark/ingest_append.py`：run 侧（控制面 + 单写者）

`run_batch(upload_ids, context)` 是 Dagster asset 直接调用的入口。**以 PG 状态为起点**：先查 `upload_session`，只处理 `status ≠ done` 的——这让 Dagster UI 的 Re-execute（`run_config` 里的 upload_ids 原样重放）天然安全，已成功的 upload 廉价跳过，只重跑失败/悬置的部分。然后 `SagaBatch.claim_many()` 批量 CAS 抢占（没抢到的说明别的 run 正持有，跳过不算错）→ 干活 → 整批级异常 `classify_exception` 定码后 `fail_many` 收尾上抛。

`_execute_batch` 的阶段推进，每个阶段先 `advance_many(step, ...)` 刷步骤 + 心跳 + fencing（被接管的 upload 从返回值里消失，后续所有写入自动剔除它）：

1. **PARSE** → `_fan_out_parse`：分波起 worker（每波 ≤ `INGEST_WORKER_MAX_PARALLEL`）。真相判定：有 manifest 看 manifest（`status=error` 用 worker 自报的码 `fail_one`），无 manifest 用 `PodOutcome.classify()` 推断码后 `fail_one`。**同批其他 upload 照常**。
2. **INDEX** → 薄表：所有 manifest 的 `thin_rows` 按表合并，`raw_file`/`episode`/`episode_file` 各一次 upsert commit。
3. **BRONZE / SILVER** → 厚表：`replace_where_chunked`——同一个 Iceberg 事务里，先 delete 本批 episode 旧行（重跑幂等），再**逐 worker、逐 row group** 追加。任何时刻内存里只有一个 row group（≈ chunk_rows 行），不再 `concat_tables` 整批合并；事务内多次 append 只是多写几个数据文件，**快照提交仍然只有一次**，读者看到的原子性与之前完全相同。
4. **SAMPLES** → `sample`/`gold_sample_index` 薄表 upsert。
5. **终态** → `succeed_many` + session done。

commit 次数不变：一批 200 个 upload，7 张表 = **7 次 Iceberg commit**，与 upload 数无关。

### 2.5 `engines/spark/ingest_correct.py`：复用 + 三处差异

复用 `_fan_out_parse`/`_upsert_thin`/`_fail_upload`，差异：

- **锚点准备**（`_load_episode_anchors`）：worker 不碰 catalog，run pod 先从 Iceberg `episode` 表批量读出目标 episode 的 `robot_id`/`start_ts` 塞进 input.json；episode 不存在的 upload 以 `DATA_PARSE_ERROR` 逐条隔离。
- **范围覆盖**：bronze/silver 的删除条件是每个 upload 声明的受影响时间窗，本批所有时间窗 `reduce(Or, ...)` 起来 + 分块追加修正数据，每表一次事务式 commit。
- **RESET_DOWNSTREAM**：受影响 sample 的 `annotation`/`qc_result` 置回 pending，重新进标注流程。

### 2.6 运行时参数与清理

都在 PG `runtime_config` 表，`UPDATE` 后下一个批次生效，不用重启任何组件：

| key | 默认 | 作用 |
| --- | --- | --- |
| `INGEST_WORKER_TIMEOUT_SECONDS` | 600 | worker pod 的 `activeDeadlineSeconds`；run 侧等待兜底用它 +120s |
| `INGEST_WORKER_MAX_PARALLEL` | 20 | 每波同时在跑的 worker 数上限 |
| `INGEST_WORKER_MEMORY_TIERS` | 1Gi,2Gi,4Gi | worker 内存 limit 按 saga attempt 升档（OOM 自动重试用更高档） |
| `INGEST_WORKER_CHUNK_ROWS` | 50000 | worker 流式解析的分块行数（每 N 行 flush 一个 row group） |
| `INGEST_RETRY_BACKOFF_MINUTES` | 5 | failed 会话按码自动重试前的退避 |
| `STAGING_RETENTION_DAYS` | 7 | staging 残留按对象 mtime 清理的保留天数 |

staging **有意不在 run 结束时删**——保留现场方便排查（失败 upload 的 input.json / 半截输出都在）。反正对读者不可见，由每日 `retention_job` 按 mtime 统一清。

---

## 三、错误码体系与重试（`common/errors.py`）

### 3.1 状态码有三个来源，Pipes 只覆盖其中一个

| 来源 | 码 | 谁产生、怎么传回 |
| --- | --- | --- |
| worker 自报（进程活着，能说清楚） | `DATA_PARSE_ERROR` `DATA_EMPTY` `INPUT_MISSING` `STORAGE_IO_ERROR` | 写进 manifest.json（契约真相）；同时经 pipes 消息上报（UI 可见） |
| run 侧从 pod 终态推断（进程死了，自报不了） | `WORKER_OOM` `WORKER_TIMEOUT` `WORKER_LOST` | 监视线程抓的 pod 快照 → `PodOutcome.classify()` |
| run 侧自身 | `COMMIT_CONFLICT` `PG_ERROR` `STUCK_EXHAUSTED` `INTERNAL` | `classify_exception` 兜底归类 |

码落进 `saga_log.error_code`（`fail_one`/`fail_many` 带码），`error` 字段统一 `[CODE] message` 格式；同一份码也进 `alerts.context`。排查任何失败，第一步都是看码。

### 3.2 按码决定重试策略（`RETRY_POLICY`）

| 策略 | 码 | 行为 |
| --- | --- | --- |
| RETRYABLE | `STORAGE_IO_ERROR` `INPUT_MISSING` `WORKER_LOST` `COMMIT_CONFLICT` `PG_ERROR` `INTERNAL` | 瞬时环境问题：stuck sensor 退避 `INGEST_RETRY_BACKOFF_MINUTES` 后自动重置 ready（attempt 上限内），幂等重写保证重跑安全 |
| NEEDS_ANALYSIS | `WORKER_OOM` `WORKER_TIMEOUT` | 可能是资源也可能是数据：同样自动重试，但 claim 的 attempt 递增让 worker 内存按 `INGEST_WORKER_MEMORY_TIERS` **自动升档**（1Gi→2Gi→4Gi）；到 attempt 上限仍失败停手等人工 |
| NOT_RETRYABLE | `DATA_PARSE_ERROR` `DATA_EMPTY` `STUCK_EXHAUSTED` | 数据自身的问题，重试一万次结果一样：落终态 + alert，**自动重试永远不碰**，等人工修数据 |

自动重试的执行者是 `ingest_stuck_sensor`（orchestration/sensors.py）第 3 类修复：查 `status=failed` 且 saga `FAILED` 的会话，按码过滤后重置 ready + 补发 Kafka 触发事件，新批次的 `claim_many` 让 attempt +1。

### 3.3 人工重试的两个入口

| 入口 | 粒度 | 适用场景 |
| --- | --- | --- |
| Dagster UI **Re-execute** | 整批（原 run_config 重放） | run 整体失败/中断后重跑。安全性来自"以 PG 状态为起点"：`run_batch` 跳过 `status=done` 的 upload，只有失败/悬置的会被重新 claim |
| **`POST /sessions/{upload_id}/retry`**（gateway） | 单 upload | 数据/环境修好后精确重跑一个。只允许 `failed → ready`，响应里带上一次的 `error_code`/`attempt`；重置后补发 Kafka 事件，进下一个微批 |

两个入口殊途同归：最终都收敛到"session 回 ready → sensor 组批 → saga claim（attempt+1）→ 幂等重写"。NOT_RETRYABLE 的码不拦人工重试——人比码知道得多（比如已经重传了好数据）。

---

## 四、K8s 权限（RBAC）完整说明

### 4.1 谁在用什么身份调 K8s API

三个"调 K8s API 的时刻"，用的都是**同一个 ServiceAccount `data:dagster`**（`deploy/k8s/00-base.yaml`）：

| 调用方 | 什么时候调 API | 干什么 |
| --- | --- | --- |
| dagster-daemon（K8sRunLauncher） | sensor/schedule 发出 RunRequest 时 | 创建 run pod（batch Job） |
| dagster-webserver | UI 查看 run 日志/终止 run 时 | 读 pod 日志、删 Job |
| **run pod 里的引擎代码**（`common/worker_pods.py`） | `_fan_out_parse` 阶段 | **Pipes 创建/删除 worker pod、follow pod 日志、监视线程 list pod** |

run pod 是 K8sRunLauncher 用 `service_account_name: dagster` 创建的，pod 里挂着这个 SA 的 token；`PipesK8sClient` 检测到 `KUBERNETES_SERVICE_HOST` 自动走 in-cluster 配置，本地调试退回 kubeconfig。

**worker pod 自己不需要任何 API 权限**：pod spec 没指定 `service_account_name`，用 namespace 的 `default` SA（没绑任何 Role）。worker 只碰对象存储，最小权限原则的自然结果。

### 4.2 Role 规则逐条解释

```yaml
rules:
  # K8sRunLauncher 起 run pod 走的是 batch Job（dagster-k8s 默认形态）
  - apiGroups: ["batch"]
    resources: ["jobs", "jobs/status"]
    verbs: ["create", "get", "list", "watch", "delete"]
  # 解析 worker 走 PipesK8sClient 直接建 pod
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["create", "get", "list", "watch", "delete"]
  - apiGroups: [""]
    resources: ["pods/log", "events", "configmaps"]
    verbs: ["get", "list", "watch"]
```

| 规则 | 谁需要 | 为什么 |
| --- | --- | --- |
| `jobs`(+`/status`) 全套 | daemon、webserver | run pod 本身仍是 K8s Job（launcher 形态没变） |
| `pods` create/delete | **run pod** | Pipes 起/收 worker pod |
| `pods` get/list/watch | run pod | `wait_for_pod` 轮询 + 监视线程按 label 抓终态快照 |
| `pods/log` get | run pod、webserver | Pipes follow worker 日志流回 compute log；UI 看 run 日志 |
| `events` get/list/watch | 排查 | 看调度失败/OOMKill 事件 |
| `configmaps` get/list/watch | daemon/run pod | 读 `dagster-instance`/`edp-env` |

### 4.3 事故复盘：`jobs/status` 的 403（子资源要单独授权）

旧版（K8s Job 轮询）第一次回归时踩过：worker Job 7 秒 Complete，run pod 却卡在 PARSE——

```text
kubernetes.client.exceptions.ApiException: (403) Forbidden
"jobs.batch ... cannot get resource \"jobs/status\" in API group \"batch\""
```

**原因**：K8s RBAC 里**子资源（subresource）需要独立授权**。`read_namespaced_job_status()` 调的是 `jobs/{name}/status`，Role 只写 `resources: ["jobs"]` 时 `get jobs` 没问题、`get jobs/status` 就是 403。同类常见坑：`pods/log`、`pods/exec`、`deployments/scale`。**当前 Pipes 形态同样受此约束**——follow worker 日志用的 `pods/log` 就是子资源，Role 里单独列了。

**值得记录的行为**：修复 apply 后（RBAC 变更实时生效、不用重启），卡住的 run 免重启自愈——因为等待循环把 API 异常当瞬时故障重试。代价是权限没修好之前会一直重试到超时，所以 run 卡住时**第一时间看 run pod 日志里有没有 403**。

提前验证权限（比事后看 403 便宜）：

```bash
kubectl auth can-i create pods    --as=system:serviceaccount:data:dagster -n data
kubectl auth can-i get pods/log   --as=system:serviceaccount:data:dagster -n data
kubectl auth can-i create jobs    --as=system:serviceaccount:data:dagster -n data
```

### 4.4 生产化时的权限收紧方向

MVP 里 daemon/webserver/run pod 共用一个 SA 图省事。生产上建议拆：daemon 的 SA 只留 `jobs` create + `configmaps` get；run pod 的 SA 留 `pods` 全套（可靠 label `app=edp-ingest-worker` 配合准入策略限制它只能建 worker 形状的 pod）；webserver 留 `pods/log` get + `jobs` delete；worker 保持 `default` SA 无权限。

---

## 五、失败语义与排查速查

| 场景 | error_code | 系统行为 | 排查入口 |
| --- | --- | --- | --- |
| 某 upload 数据坏（解析失败/全部文件隔离） | `DATA_PARSE_ERROR` / `DATA_EMPTY` | worker 写带码清单正常退出 → `fail_one` + alert，同批其他照常；**不自动重试** | run compute log（worker 日志已流回）、`saga_log.error_code`、`alerts` |
| 存储抖动（MinIO 限流等） | `STORAGE_IO_ERROR` / `INPUT_MISSING` | `fail_one` → stuck sensor 退避后自动重试 | `saga_log.attempt` 看重试轨迹 |
| worker OOM | `WORKER_OOM` | 无清单 → pod 终态分类 → 自动重试且**内存升档** | `saga_log.error_code`、K8s events |
| worker 超时 / 调度不上 | `WORKER_TIMEOUT` / `WORKER_LOST` | 同上（不升档也无妨，升档逻辑按 attempt 走档位表） | 同上 |
| run pod 等待期间 API 抖动/权限问题 | —（重试中） | 重试到超时；权限修好可免重启恢复 | run pod 日志里的 ApiException |
| run pod 自己死了 | — | worker 成孤儿但无害（staging 对读者不可见）；心跳停 → stuck sensor 重置 ready → 新批次重做 | `saga_log` 的 takeover 记录 |
| Iceberg commit 失败（整批级） | `COMMIT_CONFLICT` | `fail_many` 收尾 + 上抛，run 标红；自动重试 + 幂等重写收敛 | Dagster UI run 详情 |
| 自动重试次数耗尽 | `STUCK_EXHAUSTED`（或原码 + attempt 达上限） | 停手 + alert，等人工 | `alerts`、gateway retry API |

验证过的端到端案例（2026-07-14 minikube 回归，旧 Job 形态）：3 个上传里 1 个 worker 撞上 MinIO 限流（`SlowDownRead`）pod 级失败 → 该 upload 被隔离、同批另一个照常入湖；重置 + 重发后 attempt=2 全部 SUCCEEDED。Pipes 形态的回归记录见 README 3.6.5。
