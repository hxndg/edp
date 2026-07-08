# 数据一致性与 Saga 实现指南

本文回答三个问题：

1. **一次 ingest 到底写了哪些表？每张表是什么含义？按什么顺序写？**
2. **哪里可能不一致？**（一次业务操作 = 多次 Iceberg commit，中途崩溃怎么办）
3. **Saga 怎么实现的？**——尤其是并发场景：sensor / 定时兜底 / stuck 重试可能同时
   对同一个 upload 拉起多个 run，如何保证不双写、不互相踩。

关联代码：

| 文件 | 职责 |
|---|---|
| `common/saga.py` | Saga 核心：claim（CAS 互斥）/ advance（心跳 + fencing）/ succeed / fail，以及读侧过滤用的 `uncommitted_episode_ids()` |
| `schemas/postgres_platform.sql` | `saga_log` 表 DDL |
| `common/iceberg.py` | pyiceberg **原生 upsert**（单 commit MERGE）和 `replace_where`（transaction 内 delete+append，单 commit） |
| `engines/spark/ingest_append.py` / `ingest_correct.py` | Saga 外壳接入点 |
| `orchestration/sensors.py` | run_key 规则（含 updated_at）+ `ingest_stuck_sensor` |
| `engines/duckdb/entity_tag.py` / `analytics_summary.py` | 读侧过滤未 COMMIT 批次 |
| `engines/spark/freeze_dataset.py` | 先明细后头行的写入顺序 |

---

## 1. 一次 ingest_append 写了哪些表

按实际写入顺序列出（同时是 Saga 的步骤划分）：

| 步骤 (saga step) | 存储 | 表 / 位置 | 含义 | 写法 |
|---|---|---|---|---|
| CLAIM | Postgres | `saga_log` | 本次执行的"事务外壳"记录，抢占 owner | CAS insert/update |
| CLAIM | Postgres | `upload_session.status → ingesting` | 会话状态机推进 | UPDATE |
| PARSE | MinIO | `quarantine/` 前缀 | 解析失败的文件隔离区 | put object |
| PARSE | Postgres | `alerts` | 隔离告警 | INSERT |
| INDEX | Iceberg | `raw_file` | 原始文件登记：每个上传文件一行（uri、sha256、时间范围、ok/quarantined） | 原生 upsert（主键 `file_uri`） |
| INDEX | Iceberg | `episode` | 一次连续采集的语义单元；append 模式下 `episode_id = ep-{upload_id}`，确定性生成 | 原生 upsert（主键 `episode_id`） |
| INDEX | Iceberg | `episode_file` | episode ↔ 文件的多对多关系（含顺序号） | 原生 upsert（主键 `episode_id + file_uri`） |
| BRONZE | Iceberg | `bronze_imu` | 原始信号逐条落表（payload 原样 JSON），不清洗 | **replace_where**：单事务内 delete 本 episode 旧行 + append 新行 |
| SILVER | Iceberg | `silver_imu` | 清洗后的信号（策略由 `pipeline_step_config` 的 `silver_clean` stage 决定） | **replace_where** 同上 |
| SAMPLES | 本地/MinIO | Lance 文件 | 样本本体（时间序列切片），`sample.lance_uri` 指向它 | 覆盖写（同 sample_id 同路径） |
| SAMPLES | Iceberg | `sample` | 样本索引：确定性 `sample_id = {episode_id}-w{窗口序号}`、质量分、lance_uri | 原生 upsert（主键 `sample_id`） |
| SAMPLES | Iceberg | `gold_sample_index` | 面向训练侧的样本目录（时长、点数、质量分） | 原生 upsert（主键 `sample_id`） |
| COMMIT | Postgres | `saga_log.status → SUCCEEDED` | Saga 终态 | CAS UPDATE |
| COMMIT | Postgres | `upload_session.status → done` | **业务上的 COMMIT 点**：只有到这里，这批数据才算"存在" | UPDATE |

`ingest_correct` 的差别：目标 episode 从 manifest 里读（不新建）；BRONZE/SILVER 的
replace_where 过滤条件是"episode + 受影响时间窗"而不是整个 episode；多一步
RESET_DOWNSTREAM——把受影响 sample 的 `annotation` / `qc_result` 置回 pending。

### 谁是真相源（SoT）

- **数据的 SoT 是 Iceberg**：raw/bronze/silver/sample 等表的快照。
- **"这批数据是否完整可用"的 SoT 是 Postgres 的 `upload_session.status`**：
  Iceberg 里躺着的行不代表可以消费——只有对应 session 到了 `done`，这批数据才算
  业务上 COMMIT。下游读侧一律按这个协议过滤（见第 4 节）。

---

## 2. 不一致从哪来

Iceberg **单表单次 commit 是原子的**（要么整批可见要么都不可见），但上面的流程
有 7~8 次独立 commit + 若干次 Postgres 写。三类风险：

1. **中途崩溃**：写完 `episode` 还没写 `sample` 时进程死掉 → Iceberg 里留下
   "有 episode 没 sample"的半成品，`upload_session` 悬在 `ingesting`。
2. **重复执行**：sensor（15s 轮询）、T+1 定时兜底、stuck 重试是三条独立触发路径，
   同一个 upload 可能被拉起两个并发 run → 双写、互相覆盖、或一个在删另一个在读。
   这是用户明确指出的场景："sensor 如果是定时的，重跑了可能同时读写"。
3. **中间态被读到**：旧版手写 upsert 是"先 delete 一次 commit、再 append 一次
   commit"，两次 commit 之间读者会看到"旧行没了、新行还没来"的空洞。

对应三层防线：**幂等写（前向恢复）+ Saga 互斥与终态 + 读侧过滤**。

---

## 3. Saga 设计

### 3.1 为什么是"前向恢复"而不是补偿回滚

经典 Saga 给每一步配一个补偿动作（写了就删）。这里不需要：所有 Iceberg 写入都以
**确定性主键**（`file_uri` / `ep-{upload_id}` / `{episode_id}-w{idx}`）做 upsert 或
replace_where，**重跑一遍 = 把上一次的半成品原地覆盖成完整结果**。所以失败后的
恢复动作不是"往回擦"，而是"再往前跑一遍"（forward recovery）。半成品在重跑成功前
对下游不可见（读侧过滤），所以留着它没有危害。

Dagster 侧不需要任何"回滚资产"的概念——资产物化失败就是失败，重新物化同一个
partition（= 同一个 upload_id）就是恢复动作本身。Saga 的状态完全落在 Postgres，
与编排器解耦。

### 3.2 saga_log 表

```sql
CREATE TABLE saga_log (
    scope        TEXT NOT NULL,   -- 流程名：ingest_append / ingest_correct
    business_id  TEXT NOT NULL,   -- 业务主键：upload_id
    run_id       TEXT NOT NULL,   -- 当前 owner 的 Dagster run_id（fencing token）
    status       TEXT NOT NULL,   -- RUNNING / SUCCEEDED / FAILED
    step         TEXT NOT NULL,   -- CLAIM → PARSE → INDEX → BRONZE → SILVER → SAMPLES → COMMIT
    attempt      INT  NOT NULL,   -- 第几次尝试（claim 接管时 +1），限制自动重试次数
    error        TEXT,
    started_at   TIMESTAMPTZ NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL,  -- 每次 advance 刷新，兼作心跳
    PRIMARY KEY (scope, business_id)
);
```

一个 `(scope, business_id)` 永远只有一行——它记录的不是历史（历史看 Dagster run
日志），而是"这个业务操作当前的事务状态"。

### 3.3 三个原语

**claim（互斥抢占）**——引擎 `run()` 的第一件事：

```sql
INSERT INTO saga_log (...) VALUES (..., 'RUNNING', 'CLAIM', 1)
ON CONFLICT (scope, business_id) DO UPDATE SET
    run_id = EXCLUDED.run_id, status = 'RUNNING', step = 'CLAIM',
    attempt = saga_log.attempt + 1, ...
WHERE saga_log.status <> 'RUNNING'                     -- 上一次已终结（重跑/重试）
   OR saga_log.updated_at < now() - interval '30 min'  -- 上一个 owner 心跳超时（接管）
RETURNING attempt;
```

Postgres 的单语句是原子的，两个并发 run 同时 claim，只有一个拿到返回行；另一个
抛 `SagaConflictError`，run 直接失败退出，**一行数据都不会写**。这就是并发触发
（sensor × 定时 × stuck 重试）下"同一时刻最多一个写者"的保证。

**advance（步骤推进 + 心跳 + fencing）**——每个阶段开始前调用：

```sql
UPDATE saga_log SET step = %s, updated_at = now()
WHERE scope = %s AND business_id = %s
  AND run_id = %s          -- fencing：必须还是我
  AND status = 'RUNNING';
```

影响 0 行说明自己已经被接管（另一个 run 抢走了 saga）→ 抛
`SagaOwnershipLostError`，本 run 立即中止，不再碰任何状态。`updated_at` 同时是
心跳：只要 run 活着并在推进，就不会被判定为卡死。

**succeed / fail（显式终态）**——同样带 `run_id` fencing。成功路径把
`saga_log → SUCCEEDED`、`upload_session → done`；失败路径 `FAILED` + `failed`。
从此 `ingesting` 悬空只剩一种含义："owner 还活着正在跑"，不再是不可判定状态。

### 3.4 卡死恢复：ingest_stuck_sensor

`orchestration/sensors.py` 里的看护 sensor（每 60s），处理"进程直接死掉、连 fail
都没来得及写"的情况：

1. `status = ingesting` 且 saga 心跳超时（默认 30 分钟，`SAGA_TAKEOVER_MINUTES`）：
   - `attempt < 上限`（默认 3，`SAGA_MAX_ATTEMPTS`）→ 把 session 重置回 `ready`。
     普通 ingest sensor 会按新 run_key 重新拉起 run，新 run 的 claim 会接管
     （attempt +1），重跑一遍把半成品覆盖成完整结果。
   - `attempt` 达上限 → saga/session 都落 failed 终态 + 写 `alerts` 等人工介入
     （避免坏数据无限重试）。
2. `status = ready` 放了超过阈值没被拉起（典型原因：上一个 run 在 claim 之前崩了，
   run_key 已被 Dagster 消费）→ 刷新 `updated_at` 生成新 run_key，让 sensor 重新触发。

注意：**只有"卡死"（进程死亡）才自动重试**。正常抛异常的失败（多半是坏数据，
重试也没用）落 FAILED 后不自动重试，人工排查后把 session 置回 `ready` 即可重新入队。

### 3.5 run_key 规则的配合

run_key 从 `{op}-{upload_id}` 改成 `{op}-{upload_id}-{updated_at 时间戳}`：

- 同一个 ready 行没被动过 → updated_at 不变 → sensor 每 15s 产生的 run_key 相同
  → Dagster 去重，不会重复起 run；定时兜底与 sensor 用同一规则，同样被去重。
- 一旦状态被重置（stuck 重试 / 人工把 failed 改回 ready）→ updated_at 刷新 →
  新 run_key → 能触发新 run。旧写法的 run_key 一旦被崩溃的 run 消费掉，这个
  upload 就永远无法自动重试——这是本次修的一个实际 bug。
- 即使 run_key 去重被绕过（极端时序），claim 的 CAS 仍然兜底：多个 run 起来了，
  也只有一个能写。**run_key 是省资源的第一道闸，claim 才是正确性保证。**

---

## 4. 写侧幂等 + 读侧过滤

### 4.1 pyiceberg 原生 upsert / transaction（本次升级）

依赖从 `pyiceberg 0.7.1` 升到 `0.9.1`，`common/iceberg.py` 两处关键变化：

- **`upsert()`** 直接调 `Table.upsert(df, join_cols=...)`：pyiceberg 在**单个
  Iceberg 事务**里完成 matched-update + not-matched-insert，只产生一次 commit。
  旧版手写 delete+append 是两次 commit，中间读者会看到行消失的空洞——已废弃。
- **`replace_where(table, filter, rows)`**（新增）：`Table.transaction()` 里
  delete + append，一次快照提交。两个用途：
  - `ingest_append` 写 bronze/silver 前先清掉本 episode 的旧行再写：重跑不留
    重复信号行（修复了旧版"bronze/silver 纯 append、重跑会重复"的已知取舍）；
  - `ingest_correct` 的时间窗覆盖：删旧窗 + 写新窗原子完成，并发读者要么看到
    修正前的完整数据、要么看到修正后的完整数据，不会看到空洞。

### 4.2 各表写法一览

| 表 | 写法 | 幂等性来源 |
|---|---|---|
| `raw_file` | 原生 upsert by `file_uri` | 文件 uri 天然唯一 |
| `episode` | 原生 upsert by `episode_id` | `ep-{upload_id}` 确定性生成 |
| `episode_file` | 原生 upsert by `episode_id + file_uri` | 同上 |
| `bronze_imu` / `silver_imu` | replace_where by episode（append）/ 时间窗（correct） | 先清后写，重跑覆盖 |
| Lance 样本文件 | 按 `sample_id` 定路径覆盖写 | 确定性 sample_id |
| `sample` / `gold_sample_index` | 原生 upsert by `sample_id` | `{episode_id}-w{idx}`，窗口号由绝对时间锚点算出 |
| `entity_tag` | 原生 upsert by `target_type + target_id + tag_key` | 同一目标同一 tag_key 只有一行 |
| `annotation` / `qc_result` | 原生 upsert by 主键 | correct 重置时覆盖原行 |
| `analytics_summary` | 原生 upsert by `summary_id = scope:metric` | 永远是"当前值"快照 |
| `dataset` / `dataset_sample` | append（不可变，重跑生成新 version） | 见 4.3 |

### 4.3 读侧协议

- **`uncommitted_episode_ids()`**（`common/saga.py`）：返回业务上尚未 COMMIT 的
  episode——append 类 session 只要没到 `done`，`ep-{upload_id}` 都在列；correct 类
  session 进入 `ingesting`/`failed` 后目标 episode 也在列（`ready` 之前引擎没碰过
  表，旧数据仍完整可用，不隔离）。`entity_tag` 和 `analytics_summary` 都用它过滤，
  半成品既不会被打标签，也不会拉偏统计指标。
- **freeze_dataset 天然安全 + 顺序修正**：冻结的质量门要求 sample 有 `passed` 的
  annotation 和 `pass` 的 qc——半成品样本不可能满足，天然被挡在外面。本次额外
  修正了写入顺序：**先写明细 `dataset_sample`，最后写头行 `dataset`**。头行的
  `state = RELEASED` 相当于本次冻结的 COMMIT 标记，读者按"先查到头行才去读明细"
  消费；中途崩溃只会留下没有头行的孤儿明细（无害，重跑生成新 version），不会出现
  "头行 RELEASED、明细缺失"的脏数据。旧顺序正好相反，是评审时发现的第二个 bug。

---

## 5. 并发场景推演

| 场景 | 发生了什么 | 结局 |
|---|---|---|
| sensor 和 T+1 定时同时看到同一个 ready 会话 | run_key 相同（updated_at 没变） | Dagster 去重，只起一个 run |
| run_key 去重被绕过，两个 run 真的并发起来 | 两个 run 同时 claim | CAS 只放行一个；另一个抛 SagaConflictError，0 写入退出 |
| run A 卡死 30 分钟，stuck sensor 重置 ready，run B 起来接管；随后 A 苏醒 | B 的 claim 覆盖了 run_id | A 下一次 advance 影响 0 行 → SagaOwnershipLostError 自杀；A 在两次 advance 之间已写的行是幂等 upsert，B 重跑原地覆盖 |
| run 在写 sample 前崩溃 | Iceberg 留下 episode 无 sample 的半成品，session 悬在 ingesting | 读侧过滤使半成品不可见；心跳超时后 stuck sensor 重新入队，重跑覆盖 |
| run 在 claim 之前就崩溃 | session 停在 ready，但 run_key 已被消费 | stuck sensor 刷新 updated_at → 新 run_key → 重新触发 |
| 坏数据反复失败 | 每次都走异常路径落 FAILED | 不自动重试（只有"卡死"才重试）；attempt 达上限也会转 failed + alert |
| correct 重写时间窗时有并发读者 | delete+append 在单个 Iceberg 事务里 | 读者看到修正前或修正后的完整快照，无空洞 |

### 已知边界（MVP 取舍）

- **fencing 粒度是步骤边界**：zombie run 在两次 advance 之间仍可能提交一两次
  Iceberg 写入。由于所有写入幂等且新 owner 会整体重跑覆盖，最终状态正确；但
  严格的"每次 commit 都验 owner"需要把 fencing token 写进 Iceberg snapshot
  属性并在 commit 前校验，MVP 不做。
- **Lance 孤儿文件**：样本重切后，被废弃窗口的 Lance 文件不会主动删除（`sample`
  表里没有引用即不可达），留给日常 compaction/GC 清理。
- **单一 Saga 内不跨表原子**：raw_file 和 episode 仍是两次 commit；靠"读侧只认
  done"来掩盖中间态，而不是靠多表原子提交（Iceberg 单表事务模型的固有限制）。
- **其余单 commit 流程不需要完整 Saga**：`entity_tag` / `analytics_summary` /
  `prelabel` 等一次业务动作只有一次 Iceberg commit + 幂等 upsert，失败重跑即可，
  只有"多 commit + 有状态机"的 ingest（以及未来类似的流程）才值得上 saga_log。
  新增此类流程时，直接复用 `common/saga.py` 的 `Saga(scope, business_id, run_id)`。

---

## 6. 配置项

| 环境变量 | 默认 | 含义 |
|---|---|---|
| `SAGA_TAKEOVER_MINUTES` | 30 | RUNNING 心跳超过该时长即可被接管 / 被 stuck sensor 重新入队 |
| `SAGA_MAX_ATTEMPTS` | 3 | 卡死自动重试上限，超过转 failed + alert |

运维速查：

```sql
-- 当前在跑什么、跑到哪一步
SELECT * FROM saga_log WHERE status = 'RUNNING';

-- 失败清单（配合 alerts 看原因）
SELECT * FROM saga_log WHERE status = 'FAILED' ORDER BY updated_at DESC;

-- 人工重试一个 failed 的上传（会生成新 run_key，自动重新入队）
UPDATE upload_session SET status = 'ready', updated_at = now() WHERE upload_id = '...';
```
