# Pipes Fan-out 改造：逐行代码审核文档

> **历史归档（不可作为现行设计）**：Pipes、`common/worker_pods.py`、`common/saga.py`
> 与 `saga_log` 均已移除。现行实现见 README 3.3/3.6.3 和
> `docs/argo-workflows-code-review.md`。

> **2026-07-17 更新**：pod 的拉起/监督已从本文讲的 `common/worker_pods.py`
> （PipesK8sClient）切换为 Argo Workflows（`common/argo_workflows.py`），
> 新实现的逐行讲解见 `docs/argo-workflows-code-review.md`。本文中
> **saga、错误码、staging 契约、worker 内部逻辑、Iceberg commit** 的章节仍然有效
> （那些代码没改）；仅"launch_wave / 监视线程 / Pipes 会话"相关小节已过时。

本文面向代码审核，按**一个批次的执行顺序**把这次改造涉及的每个文件、每段代码讲清楚。
配套阅读：`docs/pod-fanout-guide.md`（架构讲解）、README 3.6.3（设计定位）。

改动清单（按执行顺序排列，也是本文的章节顺序）：

| 文件 | 性质 | 内容 |
| --- | --- | --- |
| `common/errors.py` | 新增 | 状态码枚举 + 重试策略 + 异常分类器 |
| `common/saga.py` | 修改 | `saga_log.error_code` 列；`fail_one/fail_many` 带码；claim 清残留码 |
| `orchestration/sensors.py` | 修改 | Kafka cursor 越界防御；stuck sensor 增加按码自动重试 |
| `engines/spark/ingest_append.py` | 重写 | run 侧控制面：以 PG 状态为起点、Pipes fan-out、分块 commit |
| `engines/spark/ingest_correct.py` | 重写 | 同上，correct 三处差异 |
| `common/worker_pods.py` | 新增 | PipesK8sClient 拉起 + 线程池并发 + 监视线程（替代已删除的 `common/k8s_jobs.py`） |
| `engines/worker/ingest_parse.py` | 重写 | worker：流式解析 + 分块写 + 水位线切片 + Pipes + 带码清单 |
| `engines/worker/staging.py` | 修改 | 新增 `iter_parquet_batches`（逐 row group 读） |
| `common/iceberg.py` | 修改 | 新增 `replace_where_chunked`（事务内分块追加，单次 commit） |
| `gateway/main.py` | 修改 | `POST /sessions/{id}/retry` 人工重试 API |
| `common/runtime_config.py` + `schemas/postgres_platform.sql` | 修改 | 3 个新参数 + `error_code` 列 |
| `deploy/k8s/00-base.yaml` | 修改 | RBAC：`pods` 加 create/delete |

---

## 〇、端到端验证记录（2026-07-15 minikube）

先给结论：五条路径全部实测通过，期间发现并修复 3 个 bug（见第九节踩坑）。

| 验证项 | 操作 | 证据 |
| --- | --- | --- |
| 正向链路（Pipes 形态） | 上传 `upload-67ae68cdae` → sensor 组批 → run pod → Pipes worker → 7 表 commit | run 日志 `RUN_SUCCESS`；`[pipes] external process successfully opened dagster pipes`；worker 的 `worker done: {'num_samples': 10, 'num_messages': 200}` 出现在 **run pod 日志**里（日志流回验证）；PG `status=done`、saga `SUCCEEDED/COMMIT/attempt=1` |
| staging 契约 | 检查 MinIO `staging/{run_id}/{upload_id}/` | 四个文件齐全：`input.json`、`manifest.json`、`bronze_imu.parquet`、`silver_imu.parquet` |
| 失败隔离 + worker 自报码 | 同一批混入一个好上传 + 一个坏文件（`NOT-A-MCAP` 文本冒充 mcap） | 好的 `upload-ab4cfa0a3f` → done；坏的 `upload-72550f146f` → failed，saga `error_code=DATA_EMPTY`、error=`[DATA_EMPTY] WorkerError: 全部 1 个文件不可用并已隔离`；坏文件进 `quarantine/upload-72550f146f/broken.mcap`；alerts 落一条；**整个 run 仍 RUN_SUCCESS**（隔离不拖垮批次） |
| NOT_RETRYABLE 不自动重试 | 坏上传放置 40+ 分钟（stuck sensor 每 60s 在跑，退避 5 分钟早已到期） | `upload-72550f146f` 仍是 failed / attempt=1——按码拦截生效 |
| gateway 人工重试 | `POST /sessions/upload-ffb7fe8e88/retry`（failed 状态） | 返回 200，带 `previous_error_code`/`previous_attempt`；会话回 ready → 新批次 → attempt=2 → **SUCCEEDED**、session done |
| retry 状态校验 | 对 done 的会话调 retry | 409：`只有 failed 的会话可以重试，当前状态是 'done'` |
| RBAC | `kubectl auth can-i` | `create pods` / `delete pods` / `get pods/log` 均 yes |

未实测：OOM 内存升档路径（需把 tier 调成 64Mi 故意打爆 worker，你叫停了该测试；分类逻辑 `exit_code==137/OOMKilled → WORKER_OOM` 与升档逻辑 `_memory_tier` 见下文第四、五节，纯代码路径已审）。

---

## 一、`common/errors.py`：状态码地基

### 1.1 重试策略枚举

```python
class Retry(str, Enum):
    RETRYABLE = "retryable"
    NOT_RETRYABLE = "not_retryable"
    NEEDS_ANALYSIS = "needs_analysis"
```

三值而不是布尔：OOM/超时既不能像存储抖动那样"直接再来一次"，也不能像坏数据那样"永远不试"——它需要**换个条件再试**（内存升档），所以单列 `NEEDS_ANALYSIS`。继承 `str` 让枚举值可以直接进 SQL 参数和 JSON。

### 1.2 状态码枚举

```python
class ErrorCode(str, Enum):
    # ---- worker 自报（业务失败，写进 manifest.json）----
    DATA_PARSE_ERROR = "DATA_PARSE_ERROR"      # MCAP 解析失败 / 消息格式非法
    DATA_EMPTY = "DATA_EMPTY"                  # 文件里没有目标 topic 的消息 / 全部文件被隔离
    INPUT_MISSING = "INPUT_MISSING"            # input.json 或原始文件读不到（NoSuchKey）
    STORAGE_IO_ERROR = "STORAGE_IO_ERROR"      # 对象存储限流/超时（SlowDown 等瞬时故障）
    # ---- run 侧从 pod 终态推断（worker 死了，自报不了）----
    WORKER_OOM = "WORKER_OOM"                  # 容器 OOMKilled
    WORKER_TIMEOUT = "WORKER_TIMEOUT"          # activeDeadlineSeconds 到期被杀 / 等待总超时
    WORKER_LOST = "WORKER_LOST"                # 无清单且原因不明（驱逐/镜像拉不下/未调度）
    # ---- run 侧自身 ----
    COMMIT_CONFLICT = "COMMIT_CONFLICT"        # Iceberg commit 冲突/失败（整批级）
    PG_ERROR = "PG_ERROR"                      # platform 库不可达（整批级）
    STUCK_EXHAUSTED = "STUCK_EXHAUSTED"        # 心跳超时且自动重试次数耗尽（stuck sensor 落的）
    INTERNAL = "INTERNAL"                      # 兜底：未分类异常
```

三个分组对应三个**产生位置**（这是整个设计的核心分界）：

1. worker 活着 → 自己最清楚原因 → 写进 manifest（真相）+ Pipes 消息（观测）；
2. worker 死了（OOM 是内核杀、超时是 kubelet 杀）→ Pipes 通道一起死 → 只能由 run 侧事后从 pod 终态推断；
3. run 自己的错误（commit 冲突、PG 断连）→ 根本不经过 worker。

### 1.3 策略表与查询

```python
RETRY_POLICY: dict[ErrorCode, Retry] = {
    ErrorCode.DATA_PARSE_ERROR: Retry.NOT_RETRYABLE,
    ErrorCode.DATA_EMPTY: Retry.NOT_RETRYABLE,
    ErrorCode.INPUT_MISSING: Retry.RETRYABLE,
    ErrorCode.STORAGE_IO_ERROR: Retry.RETRYABLE,
    ErrorCode.WORKER_OOM: Retry.NEEDS_ANALYSIS,
    ErrorCode.WORKER_TIMEOUT: Retry.NEEDS_ANALYSIS,
    ErrorCode.WORKER_LOST: Retry.RETRYABLE,
    ErrorCode.COMMIT_CONFLICT: Retry.RETRYABLE,
    ErrorCode.PG_ERROR: Retry.RETRYABLE,
    ErrorCode.STUCK_EXHAUSTED: Retry.NOT_RETRYABLE,
    ErrorCode.INTERNAL: Retry.RETRYABLE,
}

def retry_policy(code: str | None) -> Retry:
    try:
        return RETRY_POLICY[ErrorCode(code)] if code else Retry.RETRYABLE
    except ValueError:
        return Retry.RETRYABLE
```

`retry_policy` 接收字符串（来自 SQL 查询结果），两个兜底都偏向 RETRYABLE：`code is None`（升级前落库的历史行没有码）和 `ValueError`（未来加了新码、老代码不认识）。取向是"宁可多试一次幂等重写，也不把可救的数据判死"——重试的代价是一次幂等重写（结果不变），误判 NOT_RETRYABLE 的代价是数据滞留等人工。

### 1.4 带码异常与分类器

```python
class WorkerError(Exception):
    def __init__(self, code: ErrorCode, message: str):
        super().__init__(message)
        self.code = code
```

worker 内部代码路径上**明确知道原因**的地方抛它（例如"全部文件被隔离"→ `DATA_EMPTY`），顶层翻译成 manifest 字段；不带码的裸异常走下面的兜底分类器。

```python
def classify_exception(exc: BaseException, *, where: str = "worker") -> ErrorCode:
    if isinstance(exc, WorkerError):
        return exc.code
    error_code = getattr(exc, "response", None)          # botocore ClientError 的服务端错误码
    if isinstance(error_code, dict):
        s3_code = error_code.get("Error", {}).get("Code", "")
        if s3_code in ("NoSuchKey", "404", "NotFound"):
            return ErrorCode.INPUT_MISSING
        if s3_code:                                       # SlowDown / RequestTimeout / 503 ...
            return ErrorCode.STORAGE_IO_ERROR
    mod = type(exc).__module__ or ""
    if mod.startswith("botocore") or mod.startswith("boto3"):
        return ErrorCode.STORAGE_IO_ERROR
    if mod.startswith("psycopg"):
        return ErrorCode.PG_ERROR
    if mod.startswith("pyiceberg"):
        return ErrorCode.COMMIT_CONFLICT
    if where == "worker" and isinstance(exc, (ValueError, KeyError, TypeError)):
        return ErrorCode.DATA_PARSE_ERROR
    return ErrorCode.INTERNAL
```

逐行说明：

- `WorkerError` 优先——已经带码的直接透传；
- botocore 的 `ClientError` 有 `response` dict，从里面取 S3 错误码：`NoSuchKey` 是"东西不在"（`INPUT_MISSING`），其他一律当存储瞬时故障；
- 按异常的模块名归类 boto/psycopg/pyiceberg——这三个是"确定性强"的特征；
- **`where` 参数是回归时踩坑后加的**（见第九节 bug 2）：`ValueError/KeyError/TypeError` 在 worker 里大概率是数据问题（解析毒数据），判 `DATA_PARSE_ERROR`（不可重试）；但同样的异常出现在 run 侧多半是代码 bug，判 `INTERNAL`（可重试）——把好数据错判成毒数据的代价远高于多重试一次；
- 其余全落 `INTERNAL`。

```python
def format_error(code, message) -> str:
    return f"[{code_str}] {message}"
```

`saga_log.error` / `alerts` 的统一格式，人查库时先看方括号里的码。

---

## 二、`common/saga.py`：error_code 列

改动共四处，语义都很小：

**(1) DDL + 幂等迁移**：

```python
_DDL = """CREATE TABLE IF NOT EXISTS saga_log ( ... error_code TEXT, error TEXT, ... )"""
_MIGRATE = "ALTER TABLE saga_log ADD COLUMN IF NOT EXISTS error_code TEXT"

def _ensure_table() -> None:
    ...
    execute(_DDL)
    execute(_MIGRATE)
```

`CREATE IF NOT EXISTS` 对已存在的表不加列，所以老部署（表已建）靠 `_MIGRATE` 补列；两条都幂等，每个进程第一次用 saga 时跑一遍。`schemas/postgres_platform.sql` 里的正式 DDL 同步加了列（新部署直接就有）。

**(2)(3) `fail` / `fail_one` / `fail_many` 增加 `error_code` 参数**：

```python
def fail_one(self, business_id: str, error: str, error_code: str | None = None) -> bool:
    row = fetch_one(
        """UPDATE saga_log SET status = 'FAILED', error = %(err)s, error_code = %(code)s, updated_at = now()
           WHERE scope = %(scope)s AND business_id = %(bid)s
             AND run_id = %(rid)s AND status = 'RUNNING'
           RETURNING step""", ...)
    return row is not None
```

注意 `WHERE ... run_id = %(rid)s AND status = 'RUNNING'` 没变——fencing 语义保留：只有仍持有 saga 的 run 才能落 FAILED，被接管的返回 `False`，调用方不得再改任何状态。

**(4) `claim` / `claim_many` 接管时清掉上一轮的码**：

```python
ON CONFLICT (scope, business_id) DO UPDATE SET
    ...
    error = NULL,
    error_code = NULL,   -- 新增
    ...
```

不清的话，重试成功的 saga 会留着上一轮的 `error_code`（验证记录里 `upload-ffb7fe8e88` 第一轮就出现了 `SUCCEEDED` + `DATA_PARSE_ERROR` 并存的脏状态，此处即为修复）。语义上"码描述的是本轮 attempt 的结局"，新 attempt 开始就该清零。

---

## 三、触发与重试层：`orchestration/sensors.py`

### 3.1 Kafka cursor 越界防御（`_consume_ingest_requests`）

```python
tps = [TopicPartition(topic, p) for p in sorted(partitions)]
consumer.assign(tps)
beginnings = consumer.beginning_offsets(tps)
ends = consumer.end_offsets(tps)
for tp in tps:
    stored = cursor.get(str(tp.partition))
    if stored is None:
        consumer.seek_to_beginning(tp)   # 首次启动从头读
    elif stored < beginnings[tp] or stored > ends[tp]:
        consumer.seek(tp, beginnings[tp])  # cursor 越界：回到最早可读处
    else:
        consumer.seek(tp, stored)
```

这是回归时踩到的真实故障（第九节 bug 3）：sensor cursor 存的是 `{"0": 6}`，但 Kafka pod 曾重启过且无持久卷，topic 日志缩水到 end offset = 1。`seek(tp, 6)` 指向不存在的 offset，`poll` 永远拉不到消息；新消息 offset 从 1 开始也小于 6——**触发链路整体失明且无任何报错**。修复：每轮 tick 先拿 `beginning_offsets`/`end_offsets`，cursor 不在 `[lo, hi]` 区间就回到最早可读处。重放的老消息由下游 `status=ready` 校验廉价跳过（本来就是至少一次语义），所以回退安全。

### 3.2 stuck sensor 第 3 类修复：按码自动重试

前两类（ingesting 心跳超时重入队、ready 悬置补触发）逻辑未动，只在"attempt 耗尽转 failed"的 UPDATE 里补了 `error_code = 'STUCK_EXHAUSTED'`。新增的第 3 类：

```python
backoff = get_int("INGEST_RETRY_BACKOFF_MINUTES", 5)
failed_rows = fetch_all(
    """SELECT us.upload_id, us.manifest_op, sl.error_code, sl.attempt
       FROM upload_session us
       JOIN saga_log sl
         ON sl.business_id = us.upload_id AND sl.scope = 'ingest_' || us.manifest_op
       WHERE us.status = 'failed' AND sl.status = 'FAILED'
         AND sl.attempt < %s
         AND sl.updated_at < now() - make_interval(mins => %s)""",
    (settings.saga_max_attempts, backoff),
)
retried = 0
for row in failed_rows:
    if retry_policy(row["error_code"]) == Retry.NOT_RETRYABLE:
        continue
    execute(
        "UPDATE upload_session SET status = 'ready', updated_at = now() WHERE upload_id = %s AND status = 'failed'",
        (row["upload_id"],),
    )
    emit_ingest_request(row["upload_id"], row["manifest_op"])
    retried += 1
```

逐行审：

- JOIN 条件 `scope = 'ingest_' || manifest_op`：saga scope 命名规则是 `ingest_append`/`ingest_correct`，从 session 侧推出来；
- `us.status='failed' AND sl.status='FAILED'`：双侧都是终态才碰——`failed` session + `RUNNING` saga 的组合不存在正常路径，不瞎修；
- `attempt < saga_max_attempts`：重试次数上限在 SQL 里就拦掉，避免无限循环；
- `updated_at < now() - backoff`：退避窗口。`fail_one` 落败时刷了 `updated_at`，所以这就是"距失败至少 backoff 分钟"；
- `retry_policy(...) == NOT_RETRYABLE → continue`：数据问题不自动碰（实测：DATA_EMPTY 的上传放置 40 分钟无人动）；
- 重置用 `WHERE ... AND status='failed'` 条件更新——若人工恰好先一步 retry 了，这条 UPDATE 影响 0 行，无害；
- `emit_ingest_request` 补发 Kafka 触发消息；丢了也没关系，T+1 兜底 schedule 轮询 PG 会捞起 ready 的会话。

**OOM 升档为什么不在这里写**：重试只负责把会话送回 ready；新批次 `claim_many` 时 attempt 自动 +1，run 侧起 worker 时按 attempt 查内存档位表（见 4.4）。升档是 attempt 的函数，不需要重试层知道。

---

## 四、run 侧控制面：`engines/spark/ingest_append.py`

### 4.1 `run_batch`：外壳（以 PG 状态为起点）

```python
def run_batch(upload_ids: list[str], op_context) -> dict:
    run_id = op_context.run_id
    sessions = {
        row["upload_id"]: row
        for row in fetch_all(
            "SELECT * FROM upload_session WHERE upload_id = ANY(%s) AND manifest_op = 'append' AND status <> 'done'",
            (list(upload_ids),),
        )
    }
    not_pending = [uid for uid in upload_ids if uid not in sessions]
```

- 签名从 `(upload_ids, run_id)` 改成 `(upload_ids, op_context)`：Pipes 需要 Dagster 的执行上下文才能把 worker 日志绑回本 run（`orchestration/assets/ingest.py` 里相应改为 `run_batch(config.upload_ids, context)`）；
- **`AND status <> 'done'` 是"以 PG 状态为起点"的全部实现**：UI Re-execute 会原样重放 run_config 里的 upload_ids，这个过滤让已成功的被廉价跳过（不 claim、不起 worker、不写表），只重跑失败/悬置的。

```python
    batch = SagaBatch(SCOPE, list(sessions), run_id)
    claimed = batch.claim_many()
    skipped = [uid for uid in sessions if uid not in claimed]
    if claimed:
        execute("UPDATE upload_session SET status = 'ingesting', ... WHERE upload_id = ANY(%s)", (claimed,))
```

claim 语义未变：批量 CAS，抢到的才处理；没抢到说明另一个 run 正持有（并发触发的预期行为），跳过不算错。

```python
    try:
        result = _execute_batch(sessions, claimed, run_id, batch, op_context)
    except Exception as e:
        code = classify_exception(e, where="run")
        failed = batch.fail_many(claimed, format_error(code, f"{type(e).__name__}: {e}"), error_code=code.value)
        if failed:
            execute("UPDATE upload_session SET status = 'failed', ... AND status = 'ingesting'", (failed,))
        raise
```

整批级兜底：能走到这里的异常是 PARSE 之外阶段的（Iceberg commit、PG），影响整批。`where="run"` 让裸异常判 `INTERNAL`（可重试）而不是数据错。`fail_many` 只对**仍归本 run**的 saga 生效（fencing），session 更新也带 `AND status='ingesting'` 条件。`raise` 让 Dagster run 标红——整批失败需要在 UI 上可见。

### 4.2 `_execute_batch`：阶段推进

```python
    alive = batch.advance_many("PARSE", claimed)
    manifests, failures = _fan_out_parse(sessions, alive, run_id, batch, strategy.entrypoint, op_context)

    def _advance(step: str) -> list[dict]:
        ids = batch.advance_many(step, list(manifests))
        return [manifests[uid] for uid in ids]
```

每个阶段边界 `advance_many` 干三件事：推进 step、刷心跳、fencing——返回**仍归本 run**的子集，被接管的 upload 从此从所有后续写入中消失。`_advance` 闭包把"过滤 + 取清单"合并。

```python
    ms = _advance("INDEX")
    for table in (RAW_FILE, EPISODE, EPISODE_FILE):
        _upsert_thin(table, ms)

    ms = _advance("BRONZE")
    _replace_thick(BRONZE_IMU, ms)
    ms = _advance("SILVER")
    _replace_thick(SILVER_IMU, ms)

    ms = _advance("SAMPLES")
    _upsert_thin(SAMPLE, ms)
    _upsert_thin(GOLD_SAMPLE_INDEX, ms)

    succeeded = batch.succeed_many([m["upload_id"] for m in ms])
    if succeeded:
        execute("UPDATE upload_session SET status = 'done', ...", (succeeded,))
```

commit 结构不变：7 张表 = 每批 7 次 commit，与 upload 数无关。写入顺序（索引→厚表→样本→终态）保证任何一步崩溃时，重跑的幂等重写（upsert / replace_where 先删后写）能收敛。

### 4.3 返回值

`per_upload` 只含 `succeeded` 的（被接管/失败的不进物化元数据），`failures` 是 `{upload_id: "[CODE] message"}` 挂在 asset metadata 上（UI 可见）。

### 4.4 `_memory_tier`：OOM 升档

```python
def _memory_tier(attempt: int, tiers: list[str]) -> str:
    return tiers[min(max(attempt, 1), len(tiers)) - 1]
```

attempt=1 → 第 1 档，attempt=2 → 第 2 档，超出档位数封顶在最后一档。`max(attempt,1)` 防御 attempt 异常为 0/负数。调用处：

```python
memory_limit=_memory_tier(batch.attempts.get(uid, 1), tiers)
```

`batch.attempts` 是 `claim_many` 的 `RETURNING attempt` 填的——所以"OOM 后自动重试用更大内存"不需要任何专门代码，attempt 递增自然带动档位。

### 4.5 `_fan_out_parse`：分波 + 收数

```python
    timeout = get_int("INGEST_WORKER_TIMEOUT_SECONDS", 600)
    max_parallel = max(1, get_int("INGEST_WORKER_MAX_PARALLEL", 20))
    chunk_rows = get_int("INGEST_WORKER_CHUNK_ROWS", 50000)
    tiers = [t.strip() for t in get_str("INGEST_WORKER_MEMORY_TIERS", "1Gi,2Gi,4Gi").split(",") if t.strip()]
```

四个参数全部来自 PG `runtime_config`（UPDATE 后下个批次生效）。`chunk_rows` 通过 input.json 传给 worker——**worker 不连 PG，一切参数由 run pod 喂**。

```python
    for i in range(0, len(upload_ids), max_parallel):
        wave = upload_ids[i : i + max_parallel]
        specs: list[WorkerSpec] = []
        for uid in wave:
            prefix = staging.prefix(run_id, uid)
            payload = {
                "mode": mode,
                "upload_id": uid,
                "run_id": run_id,
                "session": {k: sessions[uid][k] for k in ("upload_id", "robot_id", "task_id", "operator", "manifest")},
                "clean_entrypoint": clean_entrypoint,
                "chunk_rows": chunk_rows,
                **({"episode": extra_input[uid]} if extra_input else {}),
            }
            staging.write_json(f"{prefix}/{staging.INPUT_JSON}", payload)
            specs.append(WorkerSpec(upload_id=uid, staging_prefix=prefix,
                                    memory_limit=_memory_tier(batch.attempts.get(uid, 1), tiers)))
```

- 分波：200 条批按 20 一波切 10 波，不一次打爆节点；
- `input.json` 是 worker 的**全部**输入：session 快照（含 manifest）、清洗策略入口字符串（run pod 已查好策略表，worker 用 importlib 加载即可）、chunk 大小、correct 模式的 episode 锚点（`extra_input`）；
- `staging.prefix(run_id, uid)` = `staging/{run_id}/{upload_id}`，每 (run, upload) 一个前缀，天然隔离。

```python
        outcomes = launch_wave(
            op_context, specs,
            run_id=run_id, timeout_seconds=timeout,
            heartbeat=lambda ids=list(wave): batch.advance_many("PARSE", ids),
        )
```

`heartbeat` 闭包用默认参数 `ids=list(wave)` 绑定当前波（惯用法，避免闭包晚绑定 bug）。心跳只刷 step 仍为 PARSE 的行；返回值不用——fencing 统一在写入阶段的 `advance_many` 边界做，等待期间被接管的会话会在那里被剔除。

```python
        for uid in wave:
            m = staging.try_read_json(f"{staging.prefix(run_id, uid)}/{staging.MANIFEST_JSON}")
            if m is None:
                code, detail = outcomes[uid].classify()
                _fail_upload(batch, uid, code, detail, run_id, failures)
            elif m.get("status") != "ok":
                code = ErrorCode(m.get("error_code") or ErrorCode.INTERNAL.value)
                _fail_upload(batch, uid, code, m.get("error", "worker 报告未知错误"), run_id, failures)
            else:
                manifests[uid] = m
                for file_uri in m.get("quarantined_files", []):
                    execute("INSERT INTO alerts ...", ...)
```

**真相判定的三分支**（整个失败语义的落点）：

1. 无清单 → worker 死了 → `PodOutcome.classify()` 从 pod 终态推码（OOM/超时/丢失）；
2. 有清单但 `status != ok` → worker 自报的业务失败，码直接取 `error_code` 字段（缺失兜 INTERNAL）；
3. 正常清单 → 进 `manifests` 待写；清单里的隔离文件（append 模式坏文件不致命）逐条落 alerts。

### 4.6 `_fail_upload`：单条失败的三连写

```python
def _fail_upload(batch, upload_id, code, message, run_id, failures) -> None:
    error = format_error(code, message)
    failures[upload_id] = error
    if batch.fail_one(upload_id, error, error_code=code.value):
        execute("UPDATE upload_session SET status = 'failed', ... AND status = 'ingesting'", (upload_id,))
        execute("INSERT INTO alerts ...", (..., to_json({"upload_id": ..., "error_code": code.value, "error": message})))
```

`fail_one` 返回 `False`（已被新 owner 接管）时**什么都不做**——世界归新 owner，本 run 连 alerts 都不落（避免双份告警）。session 更新带 `AND status='ingesting'` 条件，防止覆盖新 owner 已推进的状态。

### 4.7 `_upsert_thin` / `_replace_thick`：收数与 commit

```python
def _upsert_thin(table: str, manifests: list[dict]) -> None:
    rows: list[dict] = []
    for m in manifests:
        rows.extend(m.get("thin_rows", {}).get(table, []))
    if rows:
        upsert(table, pa.Table.from_pylist(rows), join_cols=THIN_TABLE_KEYS[table])
```

薄表（raw_file/episode/episode_file/sample/gold_sample_index，每 upload 几行）直接从 manifest 内联行合并，一次 upsert。`THIN_TABLE_KEYS` 是各表的幂等键。

```python
def _replace_thick(table: str, manifests: list[dict]) -> None:
    if not manifests:
        return
    def _batches():
        for m in manifests:
            ref = m.get("thick_files", {}).get(table)
            if ref:
                yield from staging.iter_parquet_batches(ref["key"])
    replace_where_chunked(table, in_filter("episode_id", [m["episode_id"] for m in manifests]), _batches())
```

这是去 `concat_tables` 的关键：`_batches()` 是**生成器**，逐 worker、逐 row group 惰性产出 arrow 表；`replace_where_chunked`（见第七节）在单个事务里边迭代边 append。任何时刻内存 = 一个 parquet 文件字节 + 一个 row group（≈ chunk_rows 行）。删除条件 `episode_id IN (本批全部 episode)` 保证重跑幂等（先清上一次的半成品）。

---

## 五、`common/worker_pods.py`：Pipes 拉起与监视

### 5.1 数据结构

```python
@dataclass
class WorkerSpec:
    upload_id: str
    staging_prefix: str
    memory_limit: str
```

run 侧对一个 worker 的全部"K8s 层"描述——业务输入都在 staging 的 input.json 里，这里只有调度参数。

```python
@dataclass
class PodOutcome:
    upload_id: str
    exception: str | None = None          # client.run 抛出的异常（含 worker 非零退出）
    pod_phase: str | None = None
    pod_reason: str | None = None         # pod.status.reason（DeadlineExceeded 等）
    container_reason: str | None = None   # terminated.reason（OOMKilled 等）
    exit_code: int | None = None
    pod_names: list[str] = field(default_factory=list)

    def classify(self) -> tuple[ErrorCode, str]:
        if self.container_reason == "OOMKilled" or self.exit_code == 137:
            return ErrorCode.WORKER_OOM, f"worker 容器被 OOMKill（exit={self.exit_code}）"
        if self.pod_reason == "DeadlineExceeded" or self.container_reason == "DeadlineExceeded":
            return ErrorCode.WORKER_TIMEOUT, "worker 超过 activeDeadlineSeconds 被杀"
        detail = self.exception or f"phase={self.pod_phase} reason={self.pod_reason}"
        return ErrorCode.WORKER_LOST, f"worker 无清单且原因不明：{detail}"
```

`classify` 只在"无清单"时被调用（4.5 的分支 1）。判定依据：

- OOMKill：cgroup 超限时 containerd 记 `terminated.reason=OOMKilled`，exit code 137（128+SIGKILL）双保险；
- 超时：pod 级 `activeDeadlineSeconds` 到期，kubelet 置 `pod.status.reason=DeadlineExceeded`；
- 其余（驱逐/镜像拉不下/一直 Pending）统一 `WORKER_LOST`（可重试），detail 里带 `client.run` 的异常文本供人查。

### 5.2 `_WaveMonitor`：监视线程

```python
class _WaveMonitor(threading.Thread):
    def __init__(self, run_id, outcomes, heartbeat, poll_seconds=5.0):
        super().__init__(daemon=True, name="worker-wave-monitor")
        ...
        # 注意不能叫 _stop：会遮蔽 threading.Thread 内部的 _stop() 方法
        #（join() 在线程结束时调用它），导致 TypeError: 'Event' object is not callable
        self._halt = threading.Event()

    def run(self) -> None:
        while not self._halt.wait(self.poll_seconds):
            try:
                self.heartbeat()
            except Exception:
                logger.exception("saga heartbeat failed")
            try:
                self._snapshot_pods()
            except Exception:
                logger.exception("pod snapshot failed")
```

- `daemon=True`：run pod 万一异常退出，监视线程不阻止进程结束；
- `_halt.wait(poll_seconds)`：既是节拍器又是停机开关（`stop()` 置位后立即醒来退出）；
- 心跳和快照的异常都**只记日志不中断**——瞬时 API/PG 抖动下一轮再试（这一容错在旧版 RBAC 403 事故里已被证明能免重启自愈）；
- `_stop` → `_halt` 的改名是回归时踩的坑（第九节 bug 1）。

```python
    def _snapshot_pods(self) -> None:
        api = _core_api()
        pods = api.list_namespaced_pod(
            namespace=settings.k8s_namespace,
            label_selector=f"app={WORKER_LABEL},dagster-run-id={self.run_id[:63]}",
        ).items
        for pod in pods:
            upload_id = (pod.metadata.labels or {}).get("upload-id")
            outcome = self.outcomes.get(upload_id)
            if outcome is None:
                continue
            if pod.metadata.name not in outcome.pod_names:
                outcome.pod_names.append(pod.metadata.name)
            outcome.pod_phase = pod.status.phase
            outcome.pod_reason = pod.status.reason
            for cs in pod.status.container_statuses or []:
                if cs.state and cs.state.terminated:
                    outcome.container_reason = cs.state.terminated.reason
                    outcome.exit_code = cs.state.terminated.exit_code
```

为什么必须旁路快照而不是事后查：`PipesK8sClient.run` 的 `finally` 里会 `delete_namespaced_pod`——pod 结束即删，等 `client.run` 返回再查已经查不到了。所以监视线程每 5 秒把**最后一次看到的状态**写进 `PodOutcome`，pod 名从我们自己打的 label（`upload-id`）反查归属。快照覆盖写（后见胜出），terminated 状态一旦出现就是终态。

### 5.3 `_run_one`：单个 worker 的 Pipes 调用

```python
def _run_one(client, op_context, spec, run_id, timeout_seconds, outcome) -> None:
    try:
        client.run(
            context=op_context,
            image=settings.edp_image,
            command=["python", "-m", "engines.worker.ingest_parse",
                     "--upload-id", spec.upload_id, "--run-id", run_id,
                     "--staging-prefix", spec.staging_prefix],
            namespace=settings.k8s_namespace,
            base_pod_meta={"labels": {"app": WORKER_LABEL,
                                      "upload-id": spec.upload_id[:63],
                                      "dagster-run-id": run_id[:63]}},
            base_pod_spec={
                "active_deadline_seconds": timeout_seconds,
                "containers": [{
                    "name": "worker",
                    "image_pull_policy": "IfNotPresent",
                    "env_from": [{"config_map_ref": {"name": "edp-env"}}],
                    "resources": {"requests": {"cpu": "100m", "memory": "256Mi"},
                                   "limits": {"memory": spec.memory_limit}},
                    "volume_mounts": [{"name": "lance", "mount_path": "/data/lance"}],
                }],
                "volumes": [{"name": "lance", "persistent_volume_claim": {"claim_name": "edp-lance"}}],
            },
        )
    except Exception as e:
        outcome.exception = f"{type(e).__name__}: {e}"
        logger.warning("worker pod for upload %s failed: %s", spec.upload_id, outcome.exception)
```

逐项审：

- `context=op_context`：Pipes 会话绑定到当前 Dagster step——worker 的 stdout 被 `PipesK8sPodLogsMessageReader` follow 并转发到本 run 的 compute log；bootstrap 环境变量（`DAGSTER_PIPES_CONTEXT` 等）由 client 自动注入 pod；
- `image=settings.edp_image`：与 run pod 同源镜像，"worker 跑的代码 == 编排看到的代码"；
- labels 三件套：`app` 供监视线程按波筛选，`upload-id` 反查归属，`dagster-run-id` 隔离不同 run 的 worker（[:63] 是 K8s label 值长度上限）；
- `active_deadline_seconds`：pod 级硬超时（kubelet 到点杀）；
- `resources`：requests 很小（100m/256Mi，调度友好），limit 用升档后的内存值——**OOMKill 以 limit 为准**，这正是升档要改的东西；
- `env_from edp-env`：与 run pod 同一份环境（MinIO/Lance 配置等），worker 里 `settings` 直接可用；
- Lance PVC 必须挂：worker 写的 `.lance` 文件要和 run pod / 下游 asset 看到同一份；
- `restart_policy` 不写 → `build_pod_body` 默认 `Never`——**pod 不自我重启**，重试语义归 saga；
- 兜底 `except`：任何异常（`DagsterK8sError`、API 断连、worker 非零退出）都收进 `outcome.exception`，绝不打断同波其他线程——失败隔离的第一道闸。

### 5.4 `launch_wave`：一波的编排

```python
def launch_wave(op_context, specs, *, run_id, timeout_seconds, heartbeat) -> dict[str, PodOutcome]:
    outcomes = {s.upload_id: PodOutcome(upload_id=s.upload_id) for s in specs}
    monitor = _WaveMonitor(run_id, outcomes, heartbeat)
    monitor.start()
    client = _pipes_client()
    grace = timeout_seconds + 120
    try:
        with ThreadPoolExecutor(max_workers=len(specs), thread_name_prefix="worker-pod") as pool:
            futures = {s.upload_id: pool.submit(_run_one, client, op_context, s, run_id,
                                                timeout_seconds, outcomes[s.upload_id])
                       for s in specs}
            for upload_id, future in futures.items():
                try:
                    future.result(timeout=grace)
                except FutureTimeout:
                    outcomes[upload_id].exception = f"等待超过 {grace}s（pod 可能一直未调度）"
                    _delete_pods(outcomes[upload_id].pod_names)
                    future.result()
    finally:
        monitor.stop()
    return outcomes
```

- 每 upload 一个线程（波大小 ≤20，线程数可控）；`client.run` 是同步阻塞的，线程池是并发的唯一来源；
- `grace = timeout + 120`：兜底 `activeDeadlineSeconds` 覆盖不到的洞——deadline 从 pod **启动**才计时，一直 Pending 调度不上的 pod 永远不会超时。等待超过 grace 就主动删 pod（`_delete_pods`），等待中的 `client.run` 看到 pod 消失会抛 `Pod was unexpectedly killed` 类异常返回（被 `_run_one` 收进 outcome）；
- `finally: monitor.stop()`：无论如何停监视线程（置位 + join）；
- `FutureTimeout` 显式从 `concurrent.futures` 导入——Python 3.10 里它还不是内置 `TimeoutError` 的别名（3.11 才合并），裸 `except TimeoutError` 接不住。

### 5.5 辅助函数

```python
def _core_api():
    try:
        kubernetes.config.load_incluster_config()
    except Exception:
        kubernetes.config.load_kube_config()
    return kubernetes.client.CoreV1Api()

def _pipes_client():
    from dagster_k8s import PipesK8sClient
    return PipesK8sClient(poll_interval=3.0)
```

in-cluster 优先、本地调试退回 kubeconfig。`PipesK8sClient` 自身也会做同样的探测（看 `KUBERNETES_SERVICE_HOST`）。`poll_interval=3` 把 client 内部 wait_for_pod 的轮询从默认值调密一点，缩短小任务的等待尾巴。

---

## 六、worker：`engines/worker/ingest_parse.py`

### 6.1 显式 schema

```python
_TS = pa.timestamp("us", tz="UTC")
_AUDIT_FIELDS = [("_batch_id", pa.string()), ("_run_id", pa.string()),
                 ("_ingested_at", _TS), ("_source_uri", pa.string())]
BRONZE_SCHEMA = pa.schema([...])
SILVER_SCHEMA = pa.schema([...])
```

为什么必须显式：`pq.ParquetWriter` 要求所有写入块 schema 一致，而 `pa.Table.from_pylist` 的类型推断会随数据浮动（比如某个 chunk 恰好全是 None）。锁死 schema 后跨 chunk 稳定；run 侧 append 前还有 `_align_to_table_schema` 对齐 Iceberg 表 schema，双保险。

### 6.2 `_ChunkedWriter`

```python
class _ChunkedWriter:
    def __init__(self, local_path, schema, chunk_rows, audit):
        ...
        self._rows: list[dict] = []
        self._writer: pq.ParquetWriter | None = None
        self.total = 0

    def add(self, row: dict) -> None:
        self._rows.append({**row, **self.audit, "_ingested_at": now_utc()})
        self.total += 1
        if len(self._rows) >= self.chunk_rows:
            self._flush()

    def _flush(self) -> None:
        if not self._rows:
            return
        table = pa.Table.from_pylist(self._rows, schema=self.schema)
        if self._writer is None:
            self._writer = pq.ParquetWriter(self.local_path, self.schema)
        self._writer.write_table(table)
        self._rows = []

    def close_and_upload(self, staging_key: str) -> int:
        self._flush()
        if self._writer is not None:
            self._writer.close()
            object_store.put_file(staging_key, self.local_path)
        return self.total
```

- `add` 顺手盖审计戳（`_batch_id`/`_run_id`/`_source_uri` 固定，`_ingested_at` 逐行取当前时间）；
- 攒满 `chunk_rows` 就 `_flush` 一个 row group 到**本地盘**，内存立刻归零重来——这就是"峰值 ≈ 一个 chunk"的实现；
- writer 懒建：0 行的表（如全部消息被清洗掉）不产生文件，`close_and_upload` 返回 0，manifest 里就没有这张表的 `thick_files` 条目，run 侧自然跳过；
- 每个 `write_table` 调用产生一个 parquet row group——run 侧 `iter_parquet_batches` 正是按 row group 迭代的，**两端的分块粒度天然对齐**。

### 6.3 `_WindowSlicer`：水位线切片

```python
    def add(self, silver_row: dict) -> None:
        idx = int((silver_row["ts"] - self.anchor_ts).total_seconds() // WINDOW_SECONDS)
        if idx in self.flushed:
            self.late_rows += 1
            return
        self.buffers.setdefault(idx, []).append(silver_row)
```

窗口序号 = 距锚点第几个 2 秒窗（绝对时间定位，README 2.2 原则 8：correct 重切必须命中原 `sample_id`）。`flushed` 集合防御迟到行：正常流程不会发生（见下），万一发生只计数不写——已写 Lance 的窗口不可追加，静默丢弃并把 `late_rows` 报进 manifest 供人发现。

```python
    def flush_before(self, watermark: datetime) -> None:
        for idx in sorted(self.buffers):
            window_end_offset = (idx + 1) * WINDOW_SECONDS
            if (watermark - self.anchor_ts).total_seconds() >= window_end_offset:
                self._flush(idx)
```

水位线语义：窗口 `idx` 覆盖 `[idx*2s, (idx+1)*2s)`，当确知"后续所有行的 ts ≥ watermark"时，结束时间 ≤ watermark 的窗口不可能再收到行，可安全关闭。调用点在文件边界（见 6.5），watermark = 下一个文件的最小 ts——成立的前提是**文件按起始时间排序处理且各文件内部按时间序**（MCAP 按 log_time 顺序写入；`_run` 里显式 `scans.sort(key=lambda s: s.min_ts)`）。

```python
    def _flush(self, idx: int) -> None:
        window = self.buffers.pop(idx)
        sample_id = f"{self.episode_id}-w{idx:04d}"
        score, tags = compute_quality_score(window)
        lance_uri = write_sample_to_lance(sample_id, window)
        self.sample_ids.append(sample_id)
        self.sample_rows.append({...})   # sample 表薄行
        self.gold_rows.append({...})     # gold_sample_index 表薄行
        self.flushed.add(idx)
```

关窗即落地：质量分、Lance 写入、sample/gold 行一次产出。`pop` 释放窗口内存。sample/gold 行本身是薄的（每窗一行），留在内存里最终进 manifest。

### 6.4 pass 1：`_download_and_scan`

```python
def _download_and_scan(entry: dict, workdir: str) -> _FileScan:
    file_uri = entry["file_uri"]
    bucket, key = split_s3_uri(file_uri)
    local_path = os.path.join(workdir, hashlib.sha256(file_uri.encode()).hexdigest()[:16] + ".mcap")
    object_store.client().download_file(bucket, key, local_path)

    sha = hashlib.sha256()
    with open(local_path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            sha.update(block)

    num_imu, min_ts, max_ts = 0, None, None
    with open(local_path, "rb") as f:
        for _schema, _channel, message in make_reader(f).iter_messages(topics=[IMU_TOPIC]):
            ts = ns_to_datetime(message.log_time)
            num_imu += 1
            min_ts = ts if min_ts is None or ts < min_ts else min_ts
            max_ts = ts if max_ts is None or ts > max_ts else max_ts
    return _FileScan(file_uri, local_path, sha.hexdigest(), num_imu, min_ts, max_ts, entry)
```

- 本地文件名用 uri 的 sha256 前 16 位——不同 uri 不冲突，且避免把用户可控的路径片段拼进文件系统路径；
- `download_file` 是 boto3 的流式落盘（分块下载），文件字节**从不整块进内存**——旧版 `get_bytes` 一次读整个文件是 worker OOM 的元凶之一；
- sha256 按 1 MiB 块流式算；
- MCAP 用文件句柄流式迭代，pass 1 只做计数和 min/max ts（决定锚点、raw_file 行、隔离判定），**不保留任何消息**；
- 损坏文件在 `make_reader`/迭代中抛异常，向上传播给 `_run` 决定隔离或整单失败。

### 6.5 pass 2：`_emit_file_rows`

```python
def _emit_file_rows(scan, *, episode_id, robot_id, clean_fn, bronze, silver, slicer, chunk_rows, seq_start):
    skipped = 0
    seq = seq_start
    payload_chunk: list[dict] = []

    def _drain() -> None:
        nonlocal payload_chunk
        for srow in clean_fn(payload_chunk):
            row = {**srow, "episode_id": episode_id, "robot_id": robot_id}
            silver.add(row)
            slicer.add(row)
        payload_chunk = []

    with open(scan.local_path, "rb") as f:
        for _schema, _channel, message in make_reader(f).iter_messages(topics=[IMU_TOPIC]):
            try:
                payload = _json.loads(message.data.decode("utf-8"))
            except (UnicodeDecodeError, _json.JSONDecodeError):
                skipped += 1
                continue
            ts = ns_to_datetime(message.log_time)
            bronze.add({"robot_id": robot_id, "episode_id": episode_id,
                        "source_file": scan.file_uri, "ts": ts, "seq": seq,
                        "payload_json": to_json(payload)})
            payload_chunk.append({"payload": payload, "ts": ts})
            seq += 1
            if len(payload_chunk) >= chunk_rows:
                _drain()
    _drain()
    return seq - seq_start, skipped
```

- 第二次打开文件重放消息（pass 1 已验证可读，这里理论上不再炸；即使炸也被 `_run` 的外层捕获）；
- 单条消息 JSON 解析失败只 `skipped += 1` 跳过（消息级损坏不值得废整个文件），计数进 manifest；
- 每条消息**同时**喂 bronze（原始 payload_json）和 `payload_chunk`；
- `_drain`：攒满 chunk 才调一次清洗策略 `clean_fn`——策略契约仍是"list 进 list 出"（`clean_default` 逐行过滤，分块调用语义等价），清洗后的行喂 silver writer 和切片器；
- `seq` 跨文件连续递增（`seq_start` 传入），bronze 的行序号全 episode 唯一；
- 结尾 `_drain()` 清残余。

### 6.6 `_run`：append / correct 共用主体

```python
    mode = inp["mode"]
    ...
    if mode == "correct":
        episode = inp["episode"]
        episode_id, robot_id = episode["episode_id"], episode["robot_id"]
        anchor_ts = event_date = episode["start_ts"]
    else:
        episode_id, robot_id = f"ep-{upload_id}", session["robot_id"]
        anchor_ts = event_date = None  # pass 1 之后才知道
```

差异 1：correct 的 episode 身份与切片锚点由 run pod 从 Iceberg 读好、经 input.json 注入（worker 不碰 catalog）；append 的锚点要等 pass 1 统计出全部文件的最小 ts。

```python
    for entry in manifest["files"]:
        try:
            scan = _download_and_scan(entry, workdir)
            if scan.num_imu == 0:
                raise WorkerError(ErrorCode.DATA_EMPTY, f"文件里没有 imu 消息：{entry['file_uri']}")
        except Exception as e:
            if mode == "correct":
                raise WorkerError(classify_exception(e),
                                  f"correct 输入文件不可用（整单失败）：{entry['file_uri']}: {e}") from e
            logger.exception("file failed in pass 1, quarantining: %s", entry["file_uri"])
            local = os.path.join(workdir, hashlib.sha256(entry["file_uri"].encode()).hexdigest()[:16] + ".mcap")
            if os.path.exists(local):
                _quarantine(...)
            quarantined_files.append(entry["file_uri"])
            raw_file_rows.append({..., "status": "quarantined"})
            continue
        scans.append(scan)
        raw_file_rows.append({..., "sha256": scan.sha256, "status": "ok"})
```

差异 2（失败语义）：

- append：坏文件写 quarantine 前缀（本地已下载才拷得动，`os.path.exists` 防御下载本身失败的情况）、raw_file 行标 `quarantined`、继续处理其余文件；
- correct：任何坏文件直接 `WorkerError` 整单失败——修正已有数据只写一半时间窗比不写更糟。

```python
    if not scans:
        raise WorkerError(ErrorCode.DATA_EMPTY, f"upload '{upload_id}' 的全部 {len(quarantined_files)} 个文件不可用并已隔离")

    scans.sort(key=lambda s: s.min_ts)
    start_ts = min(s.min_ts for s in scans)
    end_ts = max(s.max_ts for s in scans)
    if anchor_ts is None:
        anchor_ts = event_date = start_ts
```

全坏 → `DATA_EMPTY`（验证记录里坏文件上传走的就是这条）。**`scans.sort` 是水位线正确性的前提**（6.3）。

```python
    bronze = _ChunkedWriter(os.path.join(workdir, "bronze.parquet"), BRONZE_SCHEMA, chunk_rows, audit)
    silver = _ChunkedWriter(os.path.join(workdir, "silver.parquet"), SILVER_SCHEMA, chunk_rows, audit)
    slicer = _WindowSlicer(episode_id=..., robot_id=..., anchor_ts=anchor_ts, event_date=event_date)
    for i, scan in enumerate(scans):
        n, skipped = _emit_file_rows(scan, ...)
        seq += n
        skipped_messages += skipped
        if i + 1 < len(scans):
            slicer.flush_before(scans[i + 1].min_ts)
    slicer.finish()
```

主循环：逐文件重放；**文件边界**用下一个文件的 min_ts 做水位线关窗；最后一个文件处理完 `finish()` 关掉所有剩余窗口。

```python
    thick_files = {}
    for table, writer in (("bronze_imu", bronze), ("silver_imu", silver)):
        key = f"{prefix}/{table}.parquet"
        n = writer.close_and_upload(key)
        if n:
            thick_files[table] = {"key": key, "rows": n}
```

厚表落 staging，manifest 里只放**文件引用**（key + 行数），不放数据。

之后组装 manifest（`result` dict）：`status="ok"`、`error_code=None`、`sample_ids`、诊断计数（`num_messages`/`skipped_messages`/`late_rows`/`quarantined_files`）、`thin_rows`（append 五张表 / correct 三张表，correct 额外带 `affected_sample_ids` 和 `affected_range`）。薄行统一过 `_stamp`（盖审计戳）。字段与旧版 manifest 完全兼容——run 侧照旧消费。

### 6.7 入口与 Pipes 集成

```python
def _maybe_open_pipes():
    from dagster_pipes import DAGSTER_PIPES_CONTEXT_ENV_VAR
    if os.environ.get(DAGSTER_PIPES_CONTEXT_ENV_VAR):
        from dagster_pipes import open_dagster_pipes
        return open_dagster_pipes()
    return contextlib.nullcontext(None)
```

有 bootstrap 环境变量（PipesK8sClient 注入）→ 打开会话；没有（本地 `python -m` 调试）→ `nullcontext(None)`，后续所有 `if pipes is not None` 分支自动短路。**worker 对 Dagster 无硬依赖**。

```python
@click.command()
...
def main(upload_id, run_id, staging_prefix) -> None:
    logging.basicConfig(level=logging.INFO)
    # 业务失败走 return 正常返回（exit 0）而不是 sys.exit：sys.exit 抛 SystemExit，
    # 会被 pipes 上下文的 __exit__ 当异常上报，在 run 日志里制造
    # "pipes closed with exception" 的假警报。
    with _maybe_open_pipes() as pipes:
        _main_inner(upload_id, staging_prefix, pipes)

def _main_inner(upload_id, staging_prefix, pipes) -> None:
    try:
        inp = staging.read_json(f"{staging_prefix}/{staging.INPUT_JSON}")
    except Exception as e:
        code = classify_exception(e)
        code = ErrorCode.INPUT_MISSING if code == ErrorCode.STORAGE_IO_ERROR else code
        _write_error_manifest(staging_prefix, upload_id, code, f"读 input.json 失败: ...", pipes)
        return

    with tempfile.TemporaryDirectory(prefix=f"ingest-{upload_id[:16]}-") as workdir:
        try:
            result = _run(inp, staging_prefix, workdir)
        except Exception as e:
            logger.exception("worker failed for upload %s", upload_id)
            _write_error_manifest(staging_prefix, upload_id, classify_exception(e), f"{type(e).__name__}: {e}", pipes)
            return

    staging.write_json(f"{staging_prefix}/{staging.MANIFEST_JSON}", result)
    summary = {...}
    logger.info("worker done: %s", summary)
    if pipes is not None:
        pipes.report_custom_message(summary)
```

- **业务失败 → return（exit 0）**是整个 worker 最重要的一行语义：pod 显示 Completed，run 侧靠 manifest 的 `status=error` 判定。`return` 而不是 `sys.exit(0)` 是回归时踩的坑（第九节 bug 2）；
- input.json 读失败单独处理（此时连 `_run` 都进不去）：存储类异常改判 `INPUT_MISSING`（比泛泛的 `STORAGE_IO_ERROR` 更精确，二者都可重试）；
- `TemporaryDirectory`：下载文件、本地 parquet 都在里面，with 退出自动清盘——pod 是一次性的，这主要防 deadline 内的磁盘累积；
- `_write_error_manifest`：错误清单（`status/error_code/error`）写 staging + 同一份经 `report_custom_message` 上报 UI；
- 成功路径：先写 manifest（真相落地），再上报 pipes 小结、打日志（`worker done: {...}`——验证记录里这行出现在 run pod 日志里，证明日志流回通道工作）。

---

## 七、两个基础设施函数

### 7.1 `engines/worker/staging.py::iter_parquet_batches`

```python
def iter_parquet_batches(key: str):
    pf = pq.ParquetFile(io.BytesIO(object_store.get_bytes(key)))
    for i in range(pf.num_row_groups):
        yield pf.read_row_group(i)
```

按 row group 惰性产出。内存 = 一个文件的压缩字节 + 一个解压后的 row group。row group 边界就是 worker `_ChunkedWriter` 每次 flush 的边界（≈ chunk_rows 行），两端粒度对齐。

### 7.2 `common/iceberg.py::replace_where_chunked`

```python
def replace_where(table_name, delete_filter, arrow_table) -> Table:
    return replace_where_chunked(table_name, delete_filter,
                                 [arrow_table] if arrow_table is not None else [])

def replace_where_chunked(table_name, delete_filter, arrow_tables) -> Table:
    tbl = load_table(table_name)
    with tbl.transaction() as txn:
        txn.delete(delete_filter=delete_filter)
        for chunk in arrow_tables:
            if chunk is not None and chunk.num_rows > 0:
                txn.append(_align_to_table_schema(chunk, tbl))
    return tbl
```

- 老接口 `replace_where` 退化为单元素列表的分块版，其他调用方（freeze 等）零改动；
- `tbl.transaction()` 是 pyiceberg 的事务上下文：里面的 delete + N 次 append 都只是**积累变更**（写数据文件 + 记 pending 快照操作），`__exit__` 时才做**一次** commit（新快照原子替换 metadata 指针）。所以"分块"只增加数据文件数量，不增加 commit 次数，读者看到的原子性与单块版完全相同；
- `_align_to_table_schema` 逐块对齐 Iceberg 表 schema（列序/类型 cast），worker 侧显式 schema 已保证块间一致。

---

## 八、人工重试：`gateway/main.py`

```python
@app.post("/sessions/{upload_id}/retry")
def retry_session(upload_id: str) -> dict:
    session = _get_session_or_404(upload_id)
    if session["status"] != "failed":
        raise HTTPException(status_code=409,
                            detail=f"只有 failed 的会话可以重试，当前状态是 '{session['status']}'")
    last_error = fetch_one(
        "SELECT error_code, error, attempt FROM saga_log WHERE business_id = %s AND scope = 'ingest_' || %s",
        (upload_id, session["manifest_op"]),
    )
    execute(
        "UPDATE upload_session SET status = 'ready', updated_at = now() WHERE upload_id = %s AND status = 'failed'",
        (upload_id,),
    )
    emit("upload.retry_requested", key=upload_id, payload={"upload_id": upload_id, "source": "gateway"})
    emit_ingest_request(upload_id, session["manifest_op"])
    return {"upload_id": upload_id, "status": "ready", "manifest_op": ...,
            "previous_attempt": ..., "previous_error_code": ..., "previous_error": ...}
```

- 只允许 `failed → ready`：done 没什么可重试（要改数据走 correct 流程），ready/ingesting 是系统正在处理，人工插手只会制造并发；
- UPDATE 带 `AND status='failed'`：与 stuck sensor 的自动重试撞车时后到者影响 0 行，无害；
- `updated_at = now()` 顺带产生新 run_key（sensor 的 run_key 含 updated_at 摘要）；
- 账本 `upload.retry_requested`（审计"谁在什么时候人工重试过"）+ 触发事件 `ingest.requested` 各发一条；
- 响应带上一轮的 `error_code/error/attempt`——调用方立即知道自己在重试什么（实测返回 `previous_error_code: DATA_PARSE_ERROR`）；
- **不看 NOT_RETRYABLE**：人比码知道得多（数据已重传修好等场景），自动重试的拦截不适用于人工入口。

---

## 九、回归中发现并修复的三个 bug（踩坑记录）

### bug 1：`threading.Thread` 的 `_stop` 遮蔽

**现象**：第一次回归，worker 正常完成（`worker done` 都打出来了），run 侧却把 upload 判为 `[DATA_PARSE_ERROR] TypeError: 'Event' object is not callable`。

**根因**：`_WaveMonitor` 最初把停机开关命名为 `self._stop = threading.Event()`。而 `threading.Thread` 内部有个私有方法 `_stop()`（`join()` 在线程结束时会调用它）。属性赋值遮蔽了方法，`join()` 内部调 `self._stop()` 时变成了调用 Event 对象 → `TypeError`。异常从 `monitor.stop()` 冒出，穿过 `_fan_out_parse` 被 `run_batch` 的整批兜底接住。

**修复**：改名 `_halt`（代码里留了注释）。**连带发现第二个问题**：这个 run 侧的 `TypeError` 被 `classify_exception` 判成了 `DATA_PARSE_ERROR`（不可重试）——好数据被判成毒数据。于是给分类器加了 `where` 参数：`ValueError/KeyError/TypeError` 只在 worker 侧判数据错，run 侧判 `INTERNAL`（可重试）。

**顺带验证**：这个失败的 upload 走 gateway retry API 复活，attempt=2 成功——人工重试路径的实测就来自这次事故。

### bug 2：`sys.exit(0)` 在 pipes 会话里制造假警报

**现象**：坏文件上传被正确判 `DATA_EMPTY`、正确隔离，但 run 日志里多了一行 `[pipes] external process pipes closed with exception`。

**根因**：worker 业务失败路径用 `sys.exit(0)` 退出，`sys.exit` 的实现是抛 `SystemExit`——它穿过 `open_dagster_pipes()` 上下文管理器的 `__exit__` 时被 pipes 协议当作"外部进程异常结束"上报。虽然不影响判定（manifest 已写好），但排查时是噪音。

**修复**：`main` 拆出 `_main_inner`，业务失败用 `return` 正常返回，进程自然以 0 退出，pipes 会话干净关闭。

### bug 3：Kafka cursor 越界导致触发链路失明

**现象**：最后一轮回归，上传后 session 停在 ready，sensor 每 30 秒 tick 都报 "returned an empty result"，无任何报错。

**根因**：sensor cursor 里存着 `{"0": 6}`（历史消费位点），但 Kafka pod 曾重启过且 broker 无持久卷，topic 日志缩水到 end offset=1。`seek(tp, 6)` 指向不存在的 offset，poll 拉不到任何东西；新消息 offset 从 1 递增，也永远小于 6。

**修复**：每轮 tick 用 `beginning_offsets`/`end_offsets` 校验 cursor，越界（低于最早可读或高于末尾）就回到最早可读处。重放老消息由 `status=ready` 校验廉价跳过，本来就是至少一次语义，回退安全。这个防御对生产同样必要——retention 清掉旧段时 `stored < beginning` 是常态。

### 一个已知的环境噪音（非本次改动引入）

回归期间有一个 run 的下游 `qc_result` 步骤（Ray）因 minikube 资源紧张出现 PG 连接抖动而失败——**ingest 部分（本次改造范围）已成功**（session done、saga SUCCEEDED），最终一轮完整回归 `RUN_SUCCESS` 全绿。5.8 GB 单节点跑全套组件的固有限制，此前已记录过。

---

## 十、审核检查清单（建议按此顺序过）

1. `common/errors.py`：码的分组是否覆盖你能想到的失败？`RETRY_POLICY` 的归类是否同意（尤其 `INTERNAL → RETRYABLE` 的取向）？
2. `common/saga.py`：`error_code` 随 claim 清零的语义；fencing 条件未被削弱（所有 UPDATE 都带 `run_id` + `status='RUNNING'`）。
3. `common/worker_pods.py`：监视线程的快照时机（pod 删除前）；`grace` 超时兜底的删 pod 路径；`_run_one` 吞异常是否接受（失败隔离 vs 静默）。
4. `engines/worker/ingest_parse.py`：水位线关窗的前提（文件按 min_ts 排序 + 文件内时间序）在你的真实数据上是否成立；`late_rows` 只计数不修复是否接受。
5. `engines/spark/ingest_append.py`：`status <> 'done'` 过滤对 Re-execute 的影响；真相判定三分支的顺序。
6. `orchestration/sensors.py`：自动重试的 SQL 条件（终态双查 + attempt 上限 + 退避）；cursor 越界回退到 beginning 的选择（另一个选项是回退到 end、丢弃缺口消息，靠 T+1 兜底补——当前选 beginning 是"宁重放不丢"）。
7. `gateway/main.py`：retry 只放行 failed 的限制是否符合运营预期。
