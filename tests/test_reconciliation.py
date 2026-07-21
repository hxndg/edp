from __future__ import annotations

import contextlib
import logging
from types import SimpleNamespace

from common import jobs


class _Result:
    def __init__(self, rows=None):
        self.rows = rows or []

    def fetchone(self):
        return self.rows[0] if self.rows else None


def _stale_row() -> dict:
    return {
        "upload_id": "upload-1",
        "status": "ingesting",
        "claim_scope": "ingest_append",
        "claim_run_id": "run-1",
        "last_dagster_run_id": "run-1",
        "execution_attempt_count": 2,
    }


def test_reconciliation_keeps_claim_when_dagster_is_active(monkeypatch):
    monkeypatch.setattr(jobs, "_ensure_table", lambda: None)
    monkeypatch.setattr(jobs, "ensure_claim_schema", lambda: None)
    monkeypatch.setattr(jobs, "fetch_all", lambda *_args, **_kwargs: [_stale_row()])

    instance = SimpleNamespace(
        get_run_by_id=lambda _run_id: SimpleNamespace(status=SimpleNamespace(value="STARTED"))
    )
    result = jobs.reconciliation_pass(
        jobs.UPLOAD_KIND,
        instance,
        logging.getLogger(__name__),
        workflow_observer=lambda _run_id: {},
    )

    assert result == {"candidates": 1, "active": 1, "failed": 0, "observation_errors": 0}


def test_reconciliation_marks_missing_execution_failed_without_retry(monkeypatch):
    monkeypatch.setattr(jobs, "_ensure_table", lambda: None)
    monkeypatch.setattr(jobs, "ensure_claim_schema", lambda: None)
    monkeypatch.setattr(jobs, "fetch_all", lambda *_args, **_kwargs: [_stale_row()])
    calls = []

    class Conn:
        def execute(self, sql, params):
            calls.append((sql, params))
            if "DELETE FROM execution_claim" in sql:
                return _Result([{"business_id": "upload-1"}])
            if "UPDATE upload_session" in sql:
                return _Result([{"upload_id": "upload-1"}])
            return _Result()

    @contextlib.contextmanager
    def fake_transaction():
        yield Conn()

    monkeypatch.setattr(jobs, "transaction", fake_transaction)
    instance = SimpleNamespace(get_run_by_id=lambda _run_id: None)

    result = jobs.reconciliation_pass(
        jobs.UPLOAD_KIND,
        instance,
        logging.getLogger(__name__),
        workflow_observer=lambda _run_id: {"old-workflow": "Failed"},
    )

    assert result["failed"] == 1
    update_sql = next(sql for sql, _params in calls if "UPDATE upload_session" in sql)
    assert "EXECUTION_LOST" in update_sql
    assert not any("emit" in sql.lower() or "kafka" in sql.lower() for sql, _params in calls)
