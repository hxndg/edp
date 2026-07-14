-- platform 库 DDL —— 业务瞬态状态（README 3.1.2）
-- 这些表只做"当前状态点查"，理论上可从 Iceberg 快照 + Kafka 事件重放重建，不是数据 SoT。
-- 由 docker-compose 的 postgres 初始化脚本在容器首次启动时自动执行一次。

CREATE TABLE IF NOT EXISTS upload_session (
    upload_id           TEXT PRIMARY KEY,
    robot_id            TEXT NOT NULL,
    task_id             TEXT,
    operator            TEXT,
    manifest_op         TEXT NOT NULL CHECK (manifest_op IN ('append', 'correct')),
    pipeline_profile    TEXT NOT NULL CHECK (pipeline_profile IN ('auto_only', 'human_required')),
    status              TEXT NOT NULL DEFAULT 'created'
                        CHECK (status IN ('created', 'uploading', 'ready', 'ingesting', 'done', 'failed')),
    manifest_uri        TEXT,
    manifest            JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ingest_job (
    job_id              TEXT PRIMARY KEY,
    upload_id           TEXT NOT NULL REFERENCES upload_session(upload_id),
    op                  TEXT NOT NULL CHECK (op IN ('append', 'correct')),
    dagster_run_id      TEXT,
    status              TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
    error_message       TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS annotation_batch (
    batch_id            TEXT PRIMARY KEY,
    upload_id           TEXT REFERENCES upload_session(upload_id),
    sample_ids          JSONB NOT NULL DEFAULT '[]',
    prelabel_run_id     TEXT,
    package_uri         TEXT,
    status              TEXT NOT NULL DEFAULT 'PRELABELING'
                        CHECK (status IN ('PRELABELING', 'PACKAGED', 'LABELING', 'RETURNED', 'QC', 'DONE')),
    dispatch_run_id     TEXT,
    collect_run_id      TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dataset_request (
    request_id          TEXT PRIMARY KEY,
    requested_by        TEXT,
    dataset_name        TEXT NOT NULL,
    filter_expr         JSONB NOT NULL DEFAULT '{}',
    quality_threshold   DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    split               JSONB NOT NULL DEFAULT '{}',
    status              TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'building', 'released', 'failed')),
    dataset_version     TEXT,
    dagster_run_id      TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Saga 执行日志（docs/saga-consistency-guide.md）：跨多次 Iceberg commit 的引擎流程
-- （ingest_append / ingest_correct）的"事务外壳"。一个 (scope, business_id) 同一时刻
-- 只允许一个 RUNNING 的 owner（run_id 即 fencing token），并发触发靠 claim 的 CAS 挡掉。
-- 注意：common/saga.py 启动时也会 CREATE TABLE IF NOT EXISTS 一份同样的 DDL，
-- 保证老部署（postgres 卷已初始化过、不会重跑本脚本）也能拿到这张表。
CREATE TABLE IF NOT EXISTS saga_log (
    scope               TEXT NOT NULL,          -- 业务流程名：ingest_append / ingest_correct
    business_id         TEXT NOT NULL,          -- 业务主键：upload_id
    run_id              TEXT NOT NULL,          -- 当前 owner 的 Dagster run_id（fencing token）
    status              TEXT NOT NULL DEFAULT 'RUNNING'
                        CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED')),
    step                TEXT NOT NULL DEFAULT 'CLAIM',  -- 最近推进到的步骤，advance() 时更新（兼作心跳）
    attempt             INT  NOT NULL DEFAULT 1,        -- 第几次尝试，claim 接管时 +1，用于限制自动重试
    error               TEXT,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (scope, business_id)
);

CREATE TABLE IF NOT EXISTS alerts (
    alert_id            BIGSERIAL PRIMARY KEY,
    severity            TEXT NOT NULL CHECK (severity IN ('info', 'warning', 'error')),
    source              TEXT NOT NULL,
    run_id              TEXT,
    message             TEXT NOT NULL,
    context             JSONB NOT NULL DEFAULT '{}',
    acked               BOOLEAN NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 运行时配置（README 3.6.2）：微批/背压/保留策略参数。UPDATE 后 sensor 下个 tick
-- 生效，不用重启任何组件。common/runtime_config.py 启动时也会幂等地建表 + 播种默认值。
CREATE TABLE IF NOT EXISTS runtime_config (
    key                 TEXT PRIMARY KEY,
    value               TEXT NOT NULL,
    description         TEXT,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO runtime_config (key, value, description) VALUES
    ('INGEST_BATCH_MAX', '200', '单个微批最多合并多少条 ingest.requested 消息（README 3.6.2）'),
    ('INGEST_MAX_INFLIGHT_BATCHES', '3', '同时在跑（排队+执行中）的 ingest 批次上限，达到即暂停消费 Kafka'),
    ('RETENTION_DAYS', '30', 'Dagster run 记录与 PG 终态行的保留天数（README 3.6.4）'),
    ('INGEST_WORKER_TIMEOUT_SECONDS', '600', '单个解析 worker pod 的硬超时（README 3.6.3，activeDeadlineSeconds）'),
    ('INGEST_WORKER_MAX_PARALLEL', '20', '同时在跑的解析 worker pod 数上限（README 3.6.3，分波调度）'),
    ('STAGING_RETENTION_DAYS', '7', 'MinIO staging/ 交接区残留文件的保留天数（README 3.6.3，retention job 按 mtime 清）')
ON CONFLICT (key) DO NOTHING;

-- 策略注册表（README 3.1.2.2）：每个处理阶段实际执行哪个策略由这张表在运行时解析。
CREATE TABLE IF NOT EXISTS pipeline_step_config (
    stage               TEXT NOT NULL,
    strategy_id         TEXT NOT NULL,
    entrypoint          TEXT NOT NULL,
    owner               TEXT NOT NULL,
    is_default          BOOLEAN NOT NULL DEFAULT FALSE,
    description         TEXT,
    PRIMARY KEY (stage, strategy_id)
);

-- 每个 stage 一个默认策略；entity_tag 额外登记一个备用策略，证明"换算法不改编排图"。
INSERT INTO pipeline_step_config (stage, strategy_id, entrypoint, owner, is_default, description) VALUES
    ('silver_clean', 'default', 'engines.spark.ingest_common:clean_default', 'platform-eng', TRUE, '默认清洗：丢弃缺失时间戳/越界数值的行'),
    ('entity_tag',   'default', 'engines.duckdb.entity_tag:rules_default', 'platform-eng', TRUE, '默认规则打标签：按质量分/时长分档'),
    ('entity_tag',   'strict',  'engines.duckdb.entity_tag:rules_strict', 'research-team-a', FALSE, '备用规则：更严格的质量分阈值，供科研团队 A 试验'),
    ('annotation_promote', 'default', 'engines.ray.annotation_auto:promote_default', 'platform-eng', TRUE, 'auto_only 分支下预标转正阈值策略'),
    ('qc',           'default', 'engines.ray.qc:qc_default', 'platform-eng', TRUE, '默认数据质检：滑动窗口内 imu/pose 频率 >= 8Hz + 位姿相邻位移 <= 0.5m'),
    ('export',       'default', 'engines.spark.export_dataset:export_default', 'platform-eng', TRUE, '默认导出：单一 shard 格式')
ON CONFLICT (stage, strategy_id) DO NOTHING;
