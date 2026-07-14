"""`ingest_append` job 的 run 侧逻辑（README 3.2.1 / 3.6.3）：新增采集，只新建/追加。

pod fan-out 形态（README 3.6.3）：run pod 是**控制面 + 单写者**，重活外包：

    run pod                                worker pods（每 upload 一个）
    ────────────────────────────────       ─────────────────────────────
    claim_many（saga 互斥）
    写 input.json 到 staging  ──────────▶  下载 MCAP → 解析 → 清洗 → 切片
    分波起 K8s Job、轮询等待（刷心跳）        → 写 Lance → 厚表写 staging parquet
    收 manifest.json（缺失/error→fail_one）◀─  薄表行内联在 manifest 里
    合并全批行，每表每批一次 Iceberg commit
    succeed_many + session done

Iceberg commit 与 Saga 所有权收敛在 run pod 单写者（3.6.3 硬约束）；worker
无状态、不碰 PG/catalog，怎么死都不影响一致性——没有清单就等于没干过。
"""
from __future__ import annotations

import logging

import pyarrow as pa
from common.db import execute, fetch_all, to_json
from common.iceberg import in_filter, replace_where, upsert
from common.k8s_jobs import launch_parse_worker, wait_for_jobs
from common.runtime_config import get_int
from common.saga import SagaBatch
from common.strategy_registry import resolve
from engines.worker import staging
from schemas.iceberg_tables import (
    BRONZE_IMU,
    EPISODE,
    EPISODE_FILE,
    GOLD_SAMPLE_INDEX,
    RAW_FILE,
    SAMPLE,
    SILVER_IMU,
)

logger = logging.getLogger(__name__)

SCOPE = "ingest_append"
THIN_TABLE_KEYS = {
    RAW_FILE: ["file_uri"],
    EPISODE: ["episode_id"],
    EPISODE_FILE: ["episode_id", "file_uri"],
    SAMPLE: ["sample_id"],
    GOLD_SAMPLE_INDEX: ["sample_id"],
}


def run_batch(upload_ids: list[str], run_id: str) -> dict:
    """批量 Saga 外壳（README 3.6.3）：claim_many 抢到的才处理；worker 级失败
    逐条 fail_one 隔离；整批级异常（Iceberg commit 失败等）fail_many 收尾后上抛。
    """
    sessions = {
        row["upload_id"]: row
        for row in fetch_all(
            "SELECT * FROM upload_session WHERE upload_id = ANY(%s) AND manifest_op = 'append'",
            (list(upload_ids),),
        )
    }
    invalid = [uid for uid in upload_ids if uid not in sessions]
    if invalid:
        logger.warning("忽略不存在或非 append 的 upload：%s", invalid)

    batch = SagaBatch(SCOPE, list(sessions), run_id)
    claimed = batch.claim_many()
    skipped = [uid for uid in sessions if uid not in claimed]
    if claimed:
        execute(
            "UPDATE upload_session SET status = 'ingesting', updated_at = now() WHERE upload_id = ANY(%s)",
            (claimed,),
        )

    try:
        result = _execute_batch(sessions, claimed, run_id, batch)
    except Exception as e:  # noqa: BLE001
        failed = batch.fail_many(claimed, f"{type(e).__name__}: {e}")
        if failed:
            execute(
                "UPDATE upload_session SET status = 'failed', updated_at = now() WHERE upload_id = ANY(%s) AND status = 'ingesting'",
                (failed,),
            )
        raise

    result["num_requested"] = len(upload_ids)
    result["skipped_uploads"] = skipped + invalid
    return result


def _execute_batch(sessions: dict[str, dict], claimed: list[str], run_id: str, batch: SagaBatch) -> dict:
    strategy = resolve("silver_clean", None)

    # ---- PARSE：fan-out 到 worker pod（每 upload 一个），失败逐条隔离 ----
    alive = batch.advance_many("PARSE", claimed)
    manifests, failures = _fan_out_parse(sessions, alive, run_id, batch, strategy.entrypoint)

    def _advance(step: str) -> list[dict]:
        ids = batch.advance_many(step, list(manifests))
        return [manifests[uid] for uid in ids]

    # ---- INDEX：薄表（manifest 内联行）合并，每表一次 upsert commit ----
    ms = _advance("INDEX")
    for table in (RAW_FILE, EPISODE, EPISODE_FILE):
        _upsert_thin(table, ms)

    # ---- BRONZE / SILVER：厚表从 staging parquet 收回，事务式 replace_where
    # （删本批 episode 旧行 + 追加新行，每表一次 commit）----
    ms = _advance("BRONZE")
    _replace_thick(BRONZE_IMU, ms)
    ms = _advance("SILVER")
    _replace_thick(SILVER_IMU, ms)

    # ---- SAMPLES：sample / gold_sample_index 薄表 upsert ----
    ms = _advance("SAMPLES")
    _upsert_thin(SAMPLE, ms)
    _upsert_thin(GOLD_SAMPLE_INDEX, ms)

    # ---- 终态 ----
    succeeded = batch.succeed_many([m["upload_id"] for m in ms])
    if succeeded:
        execute(
            "UPDATE upload_session SET status = 'done', updated_at = now() WHERE upload_id = ANY(%s)",
            (succeeded,),
        )

    per_upload = [
        {
            "upload_id": m["upload_id"],
            "episode_id": m["episode_id"],
            "sample_ids": m["sample_ids"],
            "num_files": m["num_files"],
            "quarantined_files": len(m["quarantined_files"]),
        }
        for uid, m in manifests.items()
        if uid in succeeded
    ]
    return {
        "status": "done",
        "num_claimed": len(claimed),
        "num_succeeded": len(succeeded),
        "num_failed": len(failures),
        "failures": failures,
        "per_upload": per_upload,
        "num_samples": sum(len(p["sample_ids"]) for p in per_upload),
        "quarantined_files": sum(p["quarantined_files"] for p in per_upload),
        "silver_clean_strategy_id": strategy.strategy_id,
    }


def _fan_out_parse(
    sessions: dict[str, dict],
    upload_ids: list[str],
    run_id: str,
    batch: SagaBatch,
    clean_entrypoint: str,
    mode: str = "append",
    extra_input: dict[str, dict] | None = None,
) -> tuple[dict[str, dict], dict[str, str]]:
    """给每个 upload 起一个解析 worker（分波，受 INGEST_WORKER_MAX_PARALLEL 限制），
    等待并收清单。返回 (成功的 {upload_id: manifest}, 失败的 {upload_id: error})。
    ingest_correct 复用本函数（mode="correct"，extra_input 注入 episode 锚点）。
    """
    timeout = get_int("INGEST_WORKER_TIMEOUT_SECONDS", 600)
    max_parallel = max(1, get_int("INGEST_WORKER_MAX_PARALLEL", 20))

    manifests: dict[str, dict] = {}
    failures: dict[str, str] = {}

    for i in range(0, len(upload_ids), max_parallel):
        wave = upload_ids[i : i + max_parallel]
        job_by_upload: dict[str, str] = {}
        for uid in wave:
            prefix = staging.prefix(run_id, uid)
            payload = {
                "mode": mode,
                "upload_id": uid,
                "run_id": run_id,
                "session": {
                    k: sessions[uid][k] for k in ("upload_id", "robot_id", "task_id", "operator", "manifest")
                },
                "clean_entrypoint": clean_entrypoint,
                **({"episode": extra_input[uid]} if extra_input else {}),
            }
            staging.write_json(f"{prefix}/{staging.INPUT_JSON}", payload)
            job_name = f"edp-parse-{uid}-{run_id[:8]}".lower()[:63]
            job_by_upload[uid] = launch_parse_worker(
                name=job_name, upload_id=uid, run_id=run_id, staging_prefix=prefix, timeout_seconds=timeout
            )

        # 等待本波结束；on_tick 刷 saga 心跳，防止等 worker 期间被 stuck sensor 接管
        wait_for_jobs(
            list(job_by_upload.values()),
            timeout_seconds=timeout + 60,
            on_tick=lambda: batch.advance_many("PARSE", list(job_by_upload)),
        )

        # 结果以 staging 里的 manifest 为准（Job 状态只是辅助）：
        # - manifest 缺失：pod 级失败（OOM/超时/没调度上）
        # - manifest.status=error：业务失败，worker 已把原因写进来
        for uid, job_name in job_by_upload.items():
            m = staging.try_read_json(f"{staging.prefix(run_id, uid)}/{staging.MANIFEST_JSON}")
            if m is None:
                _fail_upload(batch, uid, f"worker {job_name} 无清单（pod 级失败：超时/OOM/未调度）", run_id, failures)
            elif m.get("status") != "ok":
                _fail_upload(batch, uid, m.get("error", "worker 报告未知错误"), run_id, failures)
            else:
                manifests[uid] = m
                for file_uri in m.get("quarantined_files", []):
                    execute(
                        "INSERT INTO alerts (severity, source, run_id, message, context) VALUES (%s,%s,%s,%s,%s)",
                        ("error", batch.scope, run_id, f"quarantined file {file_uri}", to_json({"upload_id": uid, "file_uri": file_uri})),
                    )
    return manifests, failures


def _fail_upload(batch: SagaBatch, upload_id: str, error: str, run_id: str, failures: dict[str, str]) -> None:
    failures[upload_id] = error
    if batch.fail_one(upload_id, error):
        execute(
            "UPDATE upload_session SET status = 'failed', updated_at = now() WHERE upload_id = %s AND status = 'ingesting'",
            (upload_id,),
        )
        execute(
            "INSERT INTO alerts (severity, source, run_id, message, context) VALUES (%s,%s,%s,%s,%s)",
            (
                "error",
                batch.scope,
                run_id,
                f"upload {upload_id} 解析失败，已逐条隔离（同批其他 upload 不受影响）",
                to_json({"upload_id": upload_id, "error": error}),
            ),
        )


def _upsert_thin(table: str, manifests: list[dict]) -> None:
    rows: list[dict] = []
    for m in manifests:
        rows.extend(m.get("thin_rows", {}).get(table, []))
    if rows:
        upsert(table, pa.Table.from_pylist(rows), join_cols=THIN_TABLE_KEYS[table])


def _replace_thick(table: str, manifests: list[dict]) -> None:
    """本批所有 worker 的 staging parquet 合并 → 删本批 episode 旧行 + 追加，单 commit。"""
    if not manifests:
        return
    episode_ids = [m["episode_id"] for m in manifests]
    tables = []
    for m in manifests:
        ref = m.get("thick_files", {}).get(table)
        if ref:
            tables.append(staging.read_parquet(ref["key"]))
    merged = pa.concat_tables(tables, promote_options="default") if tables else None
    replace_where(table, in_filter("episode_id", episode_ids), merged)
