-- 已有部署：增加 processing_type 与不可变 worker execution profile。
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

ALTER TABLE upload_session
    ADD COLUMN IF NOT EXISTS processing_type TEXT NOT NULL DEFAULT 'mcap_imu';
ALTER TABLE upload_session
    ADD COLUMN IF NOT EXISTS last_execution_profile_id TEXT;
ALTER TABLE platform_job
    ADD COLUMN IF NOT EXISTS last_execution_profile_id TEXT;

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
    ('mcap_imu', 'ingest', 'engines.worker.ingest_parse',
     'silver_clean', 'default', 'ingest-mcap-v1'),
    ('training_mock', 'training', 'engines.worker.train_mock',
     NULL, NULL, 'training-mock-v1')
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
