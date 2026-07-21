-- platform 库 DDL —— 业务生命周期 SoT 与运行配置（README 3.1.2）。
-- Iceberg 是已提交数据事实的 SoT；两者职责不同。
-- 由 docker-compose 的 postgres 初始化脚本在容器首次启动时自动执行一次。

CREATE TABLE IF NOT EXISTS upload_session (
    upload_id           TEXT PRIMARY KEY,
    robot_id            TEXT NOT NULL,
    task_id             TEXT,
    operator            TEXT,
    manifest_op         TEXT NOT NULL CHECK (manifest_op IN ('append', 'correct')),
    pipeline_profile    TEXT NOT NULL CHECK (pipeline_profile IN ('auto_only', 'human_required')),
    processing_type     TEXT NOT NULL DEFAULT 'mcap_imu',
    status              TEXT NOT NULL DEFAULT 'created'
                        CHECK (status IN ('created', 'uploading', 'ready', 'ingesting', 'done', 'failed')),
    manifest_uri        TEXT,
    manifest            JSONB,
    last_dagster_run_id TEXT,
    last_execution_profile_id TEXT,
    last_error_code     TEXT,
    last_error          TEXT,
    execution_attempt_count INT NOT NULL DEFAULT 0 CHECK (execution_attempt_count >= 0),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_upload_session_last_run
    ON upload_session (last_dagster_run_id) WHERE last_dagster_run_id IS NOT NULL;

CREATE OR REPLACE FUNCTION freeze_ready_upload_manifest()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.status IN ('ready', 'ingesting', 'done', 'failed')
       AND (
           NEW.manifest IS DISTINCT FROM OLD.manifest
           OR NEW.manifest_uri IS DISTINCT FROM OLD.manifest_uri
           OR NEW.manifest_op IS DISTINCT FROM OLD.manifest_op
           OR NEW.processing_type IS DISTINCT FROM OLD.processing_type
       ) THEN
        RAISE EXCEPTION 'upload % manifest is frozen after ready', OLD.upload_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS trg_freeze_ready_upload_manifest ON upload_session;
CREATE TRIGGER trg_freeze_ready_upload_manifest
BEFORE UPDATE ON upload_session
FOR EACH ROW EXECUTE FUNCTION freeze_ready_upload_manifest();

-- 通用异步任务状态机（README 3.1.2.1 / 3.7.4）：job_type 区分类型（MVP 只有
-- training），payload/result JSONB 装类型专属字段，协议层（common/jobs.py）不解释。
-- upload_session 是同一协议的历史绑定，表结构保持不动；未来新任务类型统一落这张表。
CREATE TABLE IF NOT EXISTS platform_job (
    job_id              TEXT PRIMARY KEY,
    job_type            TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'ready'
                        CHECK (status IN ('ready', 'running', 'done', 'failed')),
    payload             JSONB NOT NULL DEFAULT '{}',
    result              JSONB NOT NULL DEFAULT '{}',
    requested_by        TEXT,
    last_dagster_run_id TEXT,
    last_execution_profile_id TEXT,
    last_error_code     TEXT,
    last_error          TEXT,
    execution_attempt_count INT NOT NULL DEFAULT 0 CHECK (execution_attempt_count >= 0),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_platform_job_type_status ON platform_job (job_type, status);
CREATE INDEX IF NOT EXISTS idx_platform_job_last_run
    ON platform_job (last_dagster_run_id) WHERE last_dagster_run_id IS NOT NULL;

-- 不可变执行 Profile：模板、镜像与资源有任何变化都插入新 profile_id；
-- processing_type_definition 仅移动 active 指针，新旧 run 因而可并存和回滚。
CREATE TABLE IF NOT EXISTS worker_execution_profile (
    profile_id              TEXT PRIMARY KEY,
    workflow_template_name  TEXT NOT NULL,
    contract_version        TEXT NOT NULL,
    image_ref               TEXT NOT NULL,
    memory_tiers            JSONB NOT NULL,
    timeout_seconds         INT NOT NULL CHECK (timeout_seconds > 0),
    parallelism             INT NOT NULL CHECK (parallelism > 0),
    chunk_rows              INT NOT NULL DEFAULT 50000 CHECK (chunk_rows > 0),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS processing_type_definition (
    processing_type             TEXT PRIMARY KEY,
    job_kind                    TEXT NOT NULL CHECK (job_kind IN ('ingest', 'training')),
    worker_module               TEXT NOT NULL,
    strategy_stage              TEXT,
    strategy_id                 TEXT,
    active_execution_profile_id TEXT NOT NULL REFERENCES worker_execution_profile(profile_id),
    enabled                     BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO worker_execution_profile (
    profile_id, workflow_template_name, contract_version, image_ref,
    memory_tiers, timeout_seconds, parallelism, chunk_rows
) VALUES
    ('ingest-mcap-v1', 'edp-worker-batch-v1', 'v1', 'edp:dev',
     '["1Gi","2Gi","4Gi"]', 600, 20, 50000),
    ('training-mock-v1', 'edp-worker-batch-v1', 'v1', 'edp:dev',
     '["1Gi","2Gi","4Gi"]', 1800, 1, 50000)
ON CONFLICT (profile_id) DO NOTHING;

INSERT INTO processing_type_definition (
    processing_type, job_kind, worker_module, strategy_stage, strategy_id,
    active_execution_profile_id
) VALUES
    ('mcap_imu', 'ingest', 'engines.worker.ingest_parse', 'silver_clean', 'default', 'ingest-mcap-v1'),
    ('training_mock', 'training', 'engines.worker.train_mock', NULL, NULL, 'training-mock-v1')
ON CONFLICT (processing_type) DO NOTHING;

CREATE OR REPLACE FUNCTION reject_worker_execution_profile_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'worker_execution_profile is immutable; insert a new profile_id instead';
END;
$$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'worker_execution_profile_immutable') THEN
        CREATE TRIGGER worker_execution_profile_immutable
        BEFORE UPDATE OR DELETE ON worker_execution_profile
        FOR EACH ROW EXECUTE FUNCTION reject_worker_execution_profile_mutation();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_upload_processing_type') THEN
        ALTER TABLE upload_session ADD CONSTRAINT fk_upload_processing_type
        FOREIGN KEY (processing_type) REFERENCES processing_type_definition(processing_type);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_upload_last_execution_profile') THEN
        ALTER TABLE upload_session ADD CONSTRAINT fk_upload_last_execution_profile
        FOREIGN KEY (last_execution_profile_id) REFERENCES worker_execution_profile(profile_id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_job_last_execution_profile') THEN
        ALTER TABLE platform_job ADD CONSTRAINT fk_job_last_execution_profile
        FOREIGN KEY (last_execution_profile_id) REFERENCES worker_execution_profile(profile_id);
    END IF;
END $$;

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

-- 执行租约：只表达当前哪个 Dagster run 有权处理该业务 id。Argo 保存 task 的
-- phase/exit/retry/log，业务终态与最后一次 run 关联写回 upload_session/platform_job。
CREATE TABLE IF NOT EXISTS execution_claim (
    scope               TEXT NOT NULL,
    business_id         TEXT NOT NULL,
    run_id              TEXT NOT NULL,
    heartbeat_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (scope, business_id)
);
CREATE INDEX IF NOT EXISTS idx_execution_claim_heartbeat ON execution_claim (heartbeat_at);

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
    ('STAGING_RETENTION_DAYS', '7', 'MinIO staging/ 交接区残留文件的保留天数（README 3.6.3，retention job 按 mtime 清）'),
    ('TRAIN_MAX_INFLIGHT', '2', '同时在跑（排队+执行中）的训练 run 上限（README 3.7.2 背压）'),
    ('TRAIN_GATE_MIN_ACCURACY', '0.6', 'training_quality_gate asset check 的 val_accuracy 门槛（不挡归档，挡 promote 的手）')
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
