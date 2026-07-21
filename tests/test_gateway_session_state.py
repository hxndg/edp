from __future__ import annotations

import pytest
from fastapi import HTTPException

from gateway import main
from gateway.models import ManifestFileEntry, ManifestSubmitRequest, PresignRequest


def _session(status: str, manifest=None) -> dict:
    return {
        "upload_id": "upload-1",
        "robot_id": "robot-1",
        "manifest_op": "append",
        "processing_type": "mcap_imu",
        "status": status,
        "manifest": manifest,
    }


def test_presign_is_rejected_after_manifest_is_ready(monkeypatch):
    monkeypatch.setattr(main, "_get_session_or_404", lambda _upload_id: _session("ready"))

    with pytest.raises(HTTPException) as error:
        main.presign_upload("upload-1", PresignRequest(file_name="late.mcap"))

    assert error.value.status_code == 409


def test_identical_ready_manifest_is_idempotent_and_reemits_trigger(monkeypatch):
    request = ManifestSubmitRequest(files=[ManifestFileEntry(file_uri="s3://raw/a.mcap")])
    manifest = {
        "files": [entry.model_dump() for entry in request.files],
        "episode_id": None,
        "affected_start_ts": None,
        "affected_end_ts": None,
        "manifest_op": "append",
        "processing_type": "mcap_imu",
    }
    emitted = []
    monkeypatch.setattr(main, "_get_session_or_404", lambda _upload_id: _session("ready", manifest))
    monkeypatch.setattr(main, "emit_ingest_request", lambda *args: emitted.append(args))

    result = main.submit_manifest("upload-1", request)

    assert result["status"] == "ready"
    assert emitted == [("upload-1", "append", "mcap_imu")]


def test_changed_manifest_is_rejected_after_ready(monkeypatch):
    request = ManifestSubmitRequest(files=[ManifestFileEntry(file_uri="s3://raw/new.mcap")])
    monkeypatch.setattr(main, "_get_session_or_404", lambda _upload_id: _session("ready", {"files": []}))

    with pytest.raises(HTTPException) as error:
        main.submit_manifest("upload-1", request)

    assert error.value.status_code == 409
