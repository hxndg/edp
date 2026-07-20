from __future__ import annotations

import contextlib
import json
from pathlib import Path

import yaml
from common import execution_claim
from common.argo_workflows import PodOutcome, WorkerSpec, _build_workflow, _collect
from common.errors import ErrorCode
from engines.spark import ingest_append
from engines.worker import exit_policy


class _Result:
    def __init__(self, rows=None):
        self.rows = rows or []

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None


def test_workflow_is_one_cr_with_explicit_business_items():
    specs = [
        WorkerSpec(f"upload-{i}", f"staging/run/upload-{i}", ["1Gi", "2Gi", "4Gi"])
        for i in range(200)
    ]
    workflow = _build_workflow(
        "edp-test",
        specs,
        "run-123",
        600,
        20,
        workflow_template_name="edp-worker-batch-v7",
        image_ref="registry/edp@sha256:abc",
        processing_type="mcap_imu",
        execution_profile_id="ingest-mcap-v7",
    )

    assert workflow["kind"] == "Workflow"
    assert workflow["spec"]["parallelism"] == 20
    assert workflow["spec"]["workflowTemplateRef"]["name"] == "edp-worker-batch-v7"
    assert workflow["spec"]["arguments"]["parameters"][1]["value"] == "registry/edp@sha256:abc"
    assert workflow["metadata"]["annotations"]["edp/processing-type"] == "mcap_imu"
    items = json.loads(workflow["spec"]["arguments"]["parameters"][0]["value"])
    assert len(items) == 200
    assert items[137]["biz_id"] == "upload-137"
    assert "exit_policy --clear" in items[137]["script"]


def test_worker_template_disables_fail_fast_and_retries_non_business_errors():
    template = yaml.safe_load(
        (Path(__file__).parents[1] / "deploy/k8s/31-argo-worker-template.yaml").read_text()
    )
    templates = {item["name"]: item for item in template["spec"]["templates"]}

    assert template["metadata"]["name"] == "edp-worker-batch-v1"
    assert templates["main"]["dag"]["failFast"] is False
    retry = templates["worker"]["retryStrategy"]
    assert retry["limit"] == "2"
    assert retry["expression"] == "lastRetry.exitCode != '10'"
    assert "retries == 0" in templates["worker"]["podSpecPatch"]


def test_collect_uses_final_retry_and_keeps_all_logs():
    specs = [WorkerSpec("full-upload-id", "staging/r/u", ["1Gi", "2Gi", "4Gi"])]

    def node(node_id, phase, exit_code, finished):
        return {
            "id": node_id,
            "type": "Pod",
            "phase": phase,
            "message": "OOMKilled" if exit_code == 137 else "done",
            "finishedAt": finished,
            "inputs": {"parameters": [{"name": "biz-id", "value": "full-upload-id"}]},
            "outputs": {"exitCode": str(exit_code)},
        }

    wf = {
        "status": {
            "nodes": {
                "a": node("pod-a", "Failed", 137, "2026-07-20T01:00:00Z"),
                "b": node("pod-b", "Succeeded", 0, "2026-07-20T01:01:00Z"),
            }
        }
    }
    outcome = _collect(wf, specs, "wf-name")["full-upload-id"]

    assert outcome.phase == "Succeeded"
    assert outcome.exit_code == 0
    assert outcome.retry_count == 1
    assert outcome.pod_names == ["pod-a", "pod-b"]
    assert outcome.log_uris[-1].endswith("/wf-name/pod-b")


def test_outcome_classifies_oom():
    outcome = PodOutcome("u", phase="Failed", exit_code=137, message="OOMKilled")
    assert outcome.classify()[0] == ErrorCode.WORKER_OOM


def test_manifest_exit_policy(monkeypatch):
    monkeypatch.setattr(
        exit_policy.staging,
        "try_read_json",
        lambda _key: {"status": "error", "error_code": ErrorCode.DATA_EMPTY.value},
    )
    assert exit_policy.manifest_exit_code("p") == exit_policy.EXIT_BUSINESS

    monkeypatch.setattr(
        exit_policy.staging,
        "try_read_json",
        lambda _key: {"status": "error", "error_code": ErrorCode.STORAGE_IO_ERROR.value},
    )
    assert exit_policy.manifest_exit_code("p") == exit_policy.EXIT_RETRYABLE


def test_acquire_many_deduplicates_and_contains_takeover_fence(monkeypatch):
    monkeypatch.setattr(execution_claim, "ensure_schema", lambda: None)

    class Conn:
        sql = ""
        params = {}

        def execute(self, sql, params):
            self.sql = sql
            self.params = params
            return _Result([{"business_id": "u1"}])

    conn = Conn()
    claimed = execution_claim.acquire_many("ingest_append", ["u1", "u1"], "run-a", conn=conn)

    assert claimed == ["u1"]
    assert conn.params["ids"] == ["u1"]
    assert conn.params["run_id"] == "run-a"
    assert "execution_claim.run_id = EXCLUDED.run_id" in str(conn.sql)
    assert "heartbeat_at <" in str(conn.sql)


def test_finalize_only_updates_ids_still_owned_by_run(monkeypatch):
    class Conn:
        calls = []

        def execute(self, sql, params):
            self.calls.append((sql, params))
            if "SELECT business_id FROM execution_claim" in sql:
                return _Result([{"business_id": "owned-ok"}, {"business_id": "owned-fail"}])
            return _Result()

    conn = Conn()

    @contextlib.contextmanager
    def fake_transaction():
        yield conn

    monkeypatch.setattr(ingest_append, "transaction", fake_transaction)

    class Batch:
        scope = "ingest_append"
        released = []

        def release_many(self, ids, *, conn):
            self.released = sorted(ids)

    batch = Batch()
    succeeded = ingest_append._finalize_uploads(
        batch,
        ["lost-owner", "owned-ok"],
        {"owned-fail": (ErrorCode.DATA_EMPTY, "empty", None)},
        "run-a",
    )

    assert succeeded == ["owned-ok"]
    assert batch.released == ["owned-fail", "owned-ok"]
    success_update = next(call for call in conn.calls if "SET status = 'done'" in call[0])
    assert success_update[1][0] == ["owned-ok"]
