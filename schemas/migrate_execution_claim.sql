-- 已有部署：从 saga_log 运行模型迁移到薄 execution_claim + 业务表最终摘要。
ALTER TABLE upload_session ADD COLUMN IF NOT EXISTS last_dagster_run_id TEXT;
ALTER TABLE upload_session ADD COLUMN IF NOT EXISTS last_error_code TEXT;
ALTER TABLE upload_session ADD COLUMN IF NOT EXISTS last_error TEXT;
ALTER TABLE upload_session ADD COLUMN IF NOT EXISTS execution_attempt_count INT NOT NULL DEFAULT 0;
ALTER TABLE upload_session ADD COLUMN IF NOT EXISTS processing_type TEXT NOT NULL DEFAULT 'mcap_imu';
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
    execution_attempt_count INT NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE platform_job ADD COLUMN IF NOT EXISTS last_dagster_run_id TEXT;
ALTER TABLE platform_job ADD COLUMN IF NOT EXISTS last_error_code TEXT;
ALTER TABLE platform_job ADD COLUMN IF NOT EXISTS last_error TEXT;
ALTER TABLE platform_job ADD COLUMN IF NOT EXISTS execution_attempt_count INT NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS idx_platform_job_type_status ON platform_job (job_type, status);
CREATE INDEX IF NOT EXISTS idx_platform_job_last_run
    ON platform_job (last_dagster_run_id) WHERE last_dagster_run_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS execution_claim (
    scope           TEXT NOT NULL,
    business_id     TEXT NOT NULL,
    run_id          TEXT NOT NULL,
    heartbeat_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (scope, business_id)
);
CREATE INDEX IF NOT EXISTS idx_execution_claim_heartbeat ON execution_claim (heartbeat_at);

-- 若旧 saga_log 仍存在，只搬当前 RUNNING owner；历史 step/attempt/error 不再复制。
DO $$
BEGIN
    IF to_regclass('public.saga_log') IS NOT NULL THEN
        EXECUTE $migrate$
            INSERT INTO execution_claim (scope, business_id, run_id, heartbeat_at)
            SELECT scope, business_id, run_id, updated_at
            FROM saga_log
            WHERE status = 'RUNNING' AND run_id IS NOT NULL
            ON CONFLICT (scope, business_id) DO NOTHING
        $migrate$;
    END IF;
END $$;
