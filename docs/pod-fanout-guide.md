# Pod Fan-out 实现讲解：worker 解析 + run 单写者 commit + K8s 权限

对应 README 3.6.3。本文讲三件事：

1. 这套 fan-out 的代码怎么分工、每个模块干什么；
2. 一次批次从触发到落库的完整时序（含失败路径）；
3. K8s 权限（ServiceAccount / Role / RBAC）怎么配、为什么这么配，以及一次真实的 403 事故复盘。

---

## 一、总体架构：控制面与数据面的切分

一个 ingest 微批（≤200 个 upload）由**一个 Dagster run pod** 处理，但 run pod 只做"控制面"的活；批内每个 upload 最重的计算（下载 MCAP → 解析 → 清洗 → 切片 → 写 Lance）外包给**一个独立的 worker pod**（普通 K8s Job）：

```text
run pod（控制面 + 单写者）                 worker pods（每 upload 一个）
────────────────────────────────         ─────────────────────────────
claim_many（saga 互斥）
写 input.json 到 staging  ─────────────▶  下载 MCAP → 解析 → 清洗 → 切片
分波起 K8s Job、轮询等待（刷 saga 心跳）      → 写 Lance → 厚表写 staging parquet
收 manifest.json（缺失/error → fail_one）◀──  薄表行内联在 manifest 里返回
合并全批行，每表每批一次 Iceberg commit
succeed_many + session done
```

两条不随实现变的硬约束：

- **Iceberg commit 与 Saga 所有权收敛在 run pod 单写者**。worker 各自 commit 等于退回"每上传一次 commit"，200 个写者还会在 catalog 的乐观并发控制上互相冲突重试，saga 的 fencing 也保不住互斥。
- **worker 完全无状态**：只碰对象存储（staging / raw / quarantine 前缀）和 Lance 卷，**不连 PG、不碰 Iceberg catalog、不调 K8s API**。worker 怎么死都不影响一致性——没有清单就等于没干过。

为什么这样切：worker 的资源画像小而稳定（单 upload 纯 Python 解析，几十 MB～几百 MB），有自己的 requests/limits，不和 run pod 里的 Ray/DuckDB 抢内存；实测 worker 只干解析 7 秒完成，而混在 run pod 里跑时是分钟级。

---

## 二、模块讲解

### 2.1 `engines/worker/staging.py`：run ↔ worker 的交接约定

双方唯一的通信通道是 MinIO 上的一个前缀，每个 (run, upload) 一个，天然隔离、可并发：

```text
staging/{run_id}/{upload_id}/input.json      run pod 写：worker 的全部输入
staging/{run_id}/{upload_id}/manifest.json   worker 写：结果清单（薄表行内联）
staging/{run_id}/{upload_id}/bronze_imu.parquet   worker 写：厚表数据文件
staging/{run_id}/{upload_id}/silver_imu.parquet
```

- **厚薄分工**：`bronze_imu`/`silver_imu` 每 upload 成百上千行，走 parquet 文件；`raw_file`/`episode`/`sample` 等索引行每 upload 只有几行，直接内联在 `manifest.json` 的 `thin_rows` 里，省掉几次对象存储往返。
- **可见性**：staging 前缀不属于任何 Iceberg 表。Iceberg 的规则是"不被快照引用的文件不存在"（README 4.5），所以 worker 写到这里的东西对任何读者都不可见，直到 run pod 把行注册进表。
- `try_read_json` 把"文件不存在"和一切读取失败统一返回 `None`——对调用方来说都是"清单缺失 = worker 没干成"。
- JSON 里的 datetime 用 `{"__dt__": iso}` 标记往返（`_encode`/`_decode`），双方不需要知道哪些字段是时间类型。

### 2.2 `engines/worker/ingest_parse.py`：worker 入口

`python -m engines.worker.ingest_parse --upload-id ... --run-id ... --staging-prefix ...`，被 run pod 以 K8s Job 形式拉起。流程：

1. 读 `input.json`。里面有 session 快照（robot_id/manifest 等）、清洗策略入口字符串（如 `engines.spark.ingest_common:clean_default`，worker 用 `importlib` 加载，**不需要连 PG 查策略表**——run pod 已经查好了）、correct 模式下还有 episode 锚点。
2. 按 `mode` 走 `_run_append` 或 `_run_correct`：逐文件解析 MCAP（坏文件写 quarantine 前缀后继续）、清洗、固定窗口切片、写 Lance，然后厚表行写 staging parquet、薄表行连同 `sample_ids`/`episode_id`/`quarantined_files` 一起攒进 manifest。
3. 写 `manifest.json`，正常退出。

**最关键的一行语义**在 `main` 的异常处理：业务失败（解析不出来、全部文件隔离等）时，worker 把 error 写进 manifest 后 `sys.exit(0)` **正常退出**。这样区分出两类失败：

| 失败类型 | 表现 | run pod 的判定依据 |
| --- | --- | --- |
| 业务失败（数据的问题） | pod Completed，manifest 里 `status=error` + 原因 | 读 manifest 看 status |
| pod 级失败（环境的问题：OOM/超时/镜像拉不下/调度不上） | pod Failed 或一直 Pending，**没有 manifest** | manifest 缺失 |

两类都收敛到 upload 粒度的 `fail_one`，但错误信息的精确度不同——业务失败有 worker 亲口说的原因，pod 级失败只有"无清单"加 Job 名（拿 Job 名去 `kubectl logs` 查现场）。

另外两个 correct 模式的细节：

- 切片锚点用 episode **原始的** `start_ts`（从 input.json 拿），这样"距 episode 起点第 N 个窗口"的序号不变，确定性 `sample_id = {episode_id}-w{N:04d}` 才能命中原样本、upsert 覆盖而不是新增；
- correct 的输入文件坏了就整个 upload 失败，不做"隔离坏文件继续"——修正已有数据只写一半时间窗比不写更糟。

### 2.3 `common/k8s_jobs.py`：worker Job 的创建与等待

`launch_parse_worker` 创建一个 K8s Job，几个刻意的参数选择：

- **`backoff_limit=0`**：不用 K8s 自带的失败重试。重试语义归 saga 的 attempt 计数 + stuck sensor 管（重置 ready → 重进新批次），如果 K8s 自己也重试，等于两套重试机制打架，且绕过 saga 的次数上限。
- **`active_deadline_seconds`**：worker 的硬超时（`runtime_config.INGEST_WORKER_TIMEOUT_SECONDS`，默认 600s）。卡死的 worker 被 K8s 直接杀掉、Job 置 Failed，run pod 不会无限等。
- **`ttl_seconds_after_finished=3600`**：完成的 Job/Pod 保留 1 小时供 `kubectl logs` 查现场，之后 K8s 自动删，不留垃圾。
- **409 Conflict 视为复用**：同名 Job 已存在说明上一轮循环创建过（比如 run pod 中途重启后重入），直接等它的结果，天然幂等。
- 镜像用 `settings.edp_image`（`edp-env` 的 `EDP_IMAGE`，默认回退 `DAGSTER_CURRENT_IMAGE`），与 run pod / code location 同源——**worker 跑的代码 == 编排看到的代码**。
- 环境变量整个注入 `edp-env` ConfigMap（和 run pod 同一份），挂 `edp-lance` PVC（Lance 文件要和 run pod 看到同一份）。

`wait_for_jobs` 轮询一组 Job 到终态，三个要点：

- **`on_tick` 回调**：每轮轮询回调一次，run pod 用它批量刷 saga 心跳（`advance_many("PARSE", ...)`）。没有这个，等 worker 的几分钟里 saga 心跳不动，stuck sensor 会把 run pod 误判为"owner 已死"并把会话抢走。
- **总超时**：兜底 `activeDeadlineSeconds` 覆盖不到的情况——pod 一直 Pending 调度不上去时 Job 永远不会到终态。
- **API 错误重试而非失败**：读 Job 状态抛异常只记日志、下一轮再试。这是"把 API 抖动当瞬时故障"的选择，后面 RBAC 事故复盘里会看到它意外发挥了作用。

### 2.4 `engines/spark/ingest_append.py`：run 侧（控制面 + 单写者）

`run_batch(upload_ids, run_id)` 是 Dagster asset 直接调用的入口，外壳与批处理版一致：查 session → `SagaBatch.claim_many()` 批量 CAS 抢占（没抢到的说明别的 run 正持有，跳过不算错）→ 干活 → 整批级异常 `fail_many` 收尾后上抛。

`_execute_batch` 的阶段推进，每个阶段先 `advance_many(step, ...)` 刷步骤 + 心跳 + fencing（被接管的 upload 从返回值里消失，后续所有写入自动剔除它）：

1. **PARSE** → `_fan_out_parse`：分波起 worker（每波最多 `INGEST_WORKER_MAX_PARALLEL` 个，默认 20，200 条的批分 10 波），等待，收清单。清单缺失或 `status=error` 的 upload 走 `_fail_upload`（saga `fail_one` + session failed + alert），**同批其他 upload 照常**。
2. **INDEX** → 薄表：把所有 manifest 的 `thin_rows` 按表合并，`raw_file`/`episode`/`episode_file` 各一次 upsert commit。
3. **BRONZE / SILVER** → 厚表：从 staging 读回各 worker 的 parquet，`pa.concat_tables` 合并，`replace_where`（删本批 episode 旧行 + 追加新行）单 commit。删旧行是为了重跑幂等——上一次半途而废的行先清掉。
4. **SAMPLES** → `sample`/`gold_sample_index` 薄表 upsert。
5. **终态** → `succeed_many` + session done。

数一下 commit 次数：一批 200 个 upload，7 张表 = **7 次 Iceberg commit**，与 upload 数无关。这就是"行的粒度与 commit 的粒度解耦"。

### 2.5 `engines/spark/ingest_correct.py`：复用 + 三处差异

复用 `_fan_out_parse`/`_upsert_thin`/`_fail_upload`，差异：

- **锚点准备**（`_load_episode_anchors`）：worker 不碰 catalog，所以 run pod 先从 Iceberg `episode` 表批量读出目标 episode 的 `robot_id`/`start_ts`，塞进各自的 input.json；episode 不存在的 upload 直接逐条隔离。
- **范围覆盖**：bronze/silver 的删除条件不是"整个 episode"，而是每个 upload 声明的受影响时间窗（`episode_id + ts ∈ [start, end]`），本批所有时间窗 `reduce(Or, ...)` 起来，删旧 + 追加合成每表一次事务式 commit——并发读者看不到"旧数据没了、新数据还没来"的空洞。
- **RESET_DOWNSTREAM**：受影响 sample 的 `annotation`/`qc_result` 置回 pending，重新进标注流程。

### 2.6 运行时参数与清理

都在 PG `runtime_config` 表，`UPDATE` 后下一个批次生效，不用重启任何组件：

| key | 默认 | 作用 |
| --- | --- | --- |
| `INGEST_WORKER_TIMEOUT_SECONDS` | 600 | worker 的 `activeDeadlineSeconds`；run 侧等待用它 +60s |
| `INGEST_WORKER_MAX_PARALLEL` | 20 | 每波同时在跑的 worker 数上限 |
| `STAGING_RETENTION_DAYS` | 7 | staging 残留按对象 mtime 清理的保留天数 |

staging **有意不在 run 结束时删**——保留现场方便排查 worker 问题（失败 upload 的 input.json / 半截输出都在）。反正对读者不可见，由每日 `retention_job`（`orchestration/retention.py`）按 mtime 统一清。

---

## 三、K8s 权限（RBAC）完整说明

### 3.1 谁在用什么身份调 K8s API

这套系统里有三个"调 K8s API 的时刻"，用的都是**同一个 ServiceAccount `data:dagster`**（`deploy/k8s/00-base.yaml`）：

| 调用方 | 什么时候调 API | 干什么 |
| --- | --- | --- |
| dagster-daemon（K8sRunLauncher） | sensor/schedule 发出 RunRequest 时 | 创建 run pod（K8s Job） |
| dagster-webserver | UI 查看 run 日志/终止 run 时 | 读 pod 日志、删 Job |
| **run pod 里的引擎代码**（`common/k8s_jobs.py`） | `_fan_out_parse` 阶段 | **创建 worker Job、轮询 Job 状态** |

第三行是 fan-out 新引入的：run pod 是 K8sRunLauncher 用 `service_account_name: dagster`（`deploy/k8s/dagster.yaml`）创建的，所以 pod 里挂载了这个 SA 的 token；`common/k8s_jobs.py` 里 `config.load_incluster_config()` 读的就是这个 token（挂载在 `/var/run/secrets/kubernetes.io/serviceaccount/`），本地调试时退回 `load_kube_config()`（用你自己的 kubeconfig）。

**worker pod 自己不需要任何 API 权限**：pod spec 没指定 `service_account_name`，用的是 namespace 的 `default` SA（没绑任何 Role）。worker 只碰对象存储，最小权限原则的自然结果。

### 3.2 Role 规则逐条解释

权限用 **namespace 级 Role**（不是 ClusterRole）——所有对象都在 `data` namespace 里，不需要跨 namespace 的任何能力：

```yaml
kind: Role
metadata:
  name: dagster
  namespace: data
rules:
  # jobs/status 子资源：run pod 轮询解析 worker Job 终态用（common/k8s_jobs.py）
  - apiGroups: ["batch"]
    resources: ["jobs", "jobs/status"]
    verbs: ["create", "get", "list", "watch", "delete"]
  - apiGroups: [""]
    resources: ["pods", "pods/log", "events", "configmaps"]
    verbs: ["get", "list", "watch"]
```

| 规则 | 谁需要 | 为什么 |
| --- | --- | --- |
| `jobs` create | daemon（发 run）、run pod（发 worker） | run 和 worker 都是 K8s Job |
| `jobs` get/list/watch | run pod、webserver | 查 Job 是否到终态 |
| `jobs/status` get | **run pod** | 见 3.3，子资源要单独授权 |
| `jobs` delete | webserver | UI 上终止 run |
| `pods`/`pods/log` get/list/watch | webserver、排查 | 读 run/worker 的容器日志 |
| `events` get/list/watch | 排查 | 看调度失败/OOMKill 这类事件 |
| `configmaps` get/list/watch | daemon/run pod | 读 `dagster-instance`/`edp-env` |

### 3.3 事故复盘：`jobs/status` 的 403

**现象**：第一次 fan-out 回归时，worker Job 7 秒就 Complete 了，但 run pod 卡在 PARSE 阶段不动，saga 心跳一直在刷。看 run pod 日志：

```text
kubernetes.client.exceptions.ApiException: (403) Forbidden
"jobs.batch \"edp-parse-...\" is forbidden: User \"system:serviceaccount:data:dagster\"
 cannot get resource \"jobs/status\" in API group \"batch\""
```

**原因**：K8s RBAC 里，**子资源（subresource）需要独立授权**。`kubernetes` Python 客户端的 `read_namespaced_job_status()` 调的是 `GET /apis/batch/v1/namespaces/data/jobs/{name}/status`——这是 `jobs/status` 子资源，不是 `jobs` 主资源。Role 里只写 `resources: ["jobs"]` 时，`get jobs` 没问题，`get jobs/status` 就是 403。（同类常见坑：`pods/log`、`pods/exec`、`deployments/scale` 都是子资源，都要单独列。）

**修复**：Role 的 batch 规则加上 `"jobs/status"`，`kubectl apply` 即生效（RBAC 变更不需要重启任何 pod，API server 每次请求实时鉴权）。

**一个值得记录的行为**：修复 apply 之后，卡住的 run **没有重启就自己恢复了**。因为 `wait_for_jobs` 把读状态的异常当瞬时故障处理（记日志 + 下一轮重试），RBAC 放行后下一轮轮询立即看到 Job 已 Complete，流程继续走完，最终 SUCCEEDED。"对外部依赖的调用失败默认重试而不是失败"这个选择在这里换来了免重启恢复；代价是权限没修好之前它会一直重试到总超时（timeout + 60s）才判 pod 级失败——所以卡住时**第一时间看 run pod 日志**里有没有 403/权限类的字样。

**如何提前验证权限**（比事后看 403 便宜）：

```bash
kubectl auth can-i get jobs/status --as=system:serviceaccount:data:dagster -n data
kubectl auth can-i create jobs      --as=system:serviceaccount:data:dagster -n data
```

### 3.4 生产化时的权限收紧方向

MVP 里 daemon/webserver/run pod 共用一个 SA 图省事。生产上建议拆：

- daemon 用的 SA：`jobs` create + `configmaps` get 即可；
- run pod 用的 SA（K8sRunLauncher `service_account_name` 单独指一个）：`jobs`+`jobs/status` create/get/list/watch，可加 `resourceNames` 或靠 label（worker 都带 `app=edp-ingest-worker`）配合准入策略限制它只能建 worker 形状的 Job；
- webserver 的 SA：`pods/log` get + `jobs` delete；
- worker 保持 `default` SA 无权限不变。

---

## 四、失败语义与排查速查

| 场景 | 系统行为 | 排查入口 |
| --- | --- | --- |
| 某 upload 数据坏（解析失败/全部文件隔离） | worker 写 error 清单正常退出 → run pod `fail_one` + alert，同批其他照常 | PG `saga_log.error`、`alerts` 表 |
| worker OOM / 超时 / 调度不上 | 无清单 → `fail_one`（错误信息带 Job 名） | `kubectl -n data logs job/<job名>`（1 小时内） |
| run pod 等待期间 K8s API 抖动/权限问题 | 重试到总超时；权限修好可免重启恢复 | run pod 日志里的 ApiException |
| run pod 自己死了 | worker 成了孤儿但无害（staging 对读者不可见）；saga 心跳停 → stuck sensor 重置 ready → 新批次重做；旧 worker 的 Job 名带旧 run_id，不与新批次冲突 | `saga_log` 的 takeover 记录 |
| Iceberg commit 失败（整批级） | `fail_many` 收尾 + 上抛，Dagster run 标红；重跑靠幂等写（upsert/replace_where）收敛 | Dagster UI run 详情 |
| 失败后重跑 | 重置 session ready + 重发 Kafka 消息（或等 T+1 兜底），新批次 attempt+1 | `saga_log.attempt` |

验证过的端到端案例（2026-07-14 minikube 回归）：3 个上传里 1 个 worker 撞上 MinIO 限流（`SlowDownRead`）pod 级失败 → 该 upload 被隔离、同批另一个照常入湖；重置 + 重发后 attempt=2 全部 SUCCEEDED；期间还实地踩了 3.3 的 `jobs/status` 403 并验证了免重启恢复。
