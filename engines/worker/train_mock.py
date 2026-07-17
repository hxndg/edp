"""训练 worker pod 入口（README 3.7.2）：一次训练任务 = 一个 pod。

契约与 ingest_parse 完全同构（docs/pod-fanout-guide.md）：
- 输入：staging 的 input.json（模型名/数据集版本/超参/seed）+ samples.parquet
  （run pod 预先解析好的样本清单：sample_id / lance_uri / split / quality_score）。
- 输出：manifest.json（终局指标 + 逐 epoch 曲线 + 版本信息 + error_code）+
  eval.parquet（逐样本评测明细）。**manifest 是数据契约的真相**；由 Argo
  Workflow 拉起，stdout 归档 s3://lake/argo/。
- 无状态：只碰 staging / Lance / MinIO models/ / MLflow HTTP，不连 PG、
  不碰 Iceberg catalog。怎么死都不影响一致性——没有清单等于没干过。

训练本身是真的但刻意小：从每个 Lance 样本抽 IMU 统计特征（6 列的均值+方差，
12 维），SGDClassifier 逐 epoch partial_fit 学"质量分是否达标"的二分类——
足够产生真实的 loss 曲线和可评测的预测，又不需要 GPU。将来换 Ray Train/
真模型只改本文件内部，契约不变。

MLflow 打点是 **best-effort**（README 3.7.1）：tracking server 挂了只丢
UI 观测，训练照常、曲线照样进 manifest（Iceberg 归档不依赖 MLflow）。
"""
from __future__ import annotations

import contextlib
import hashlib
import logging
import os
import pickle

import click
import numpy as np

from common import object_store
from common.errors import ErrorCode, WorkerError, classify_exception
from engines.worker import staging

logger = logging.getLogger(__name__)

FEATURE_COLS = ("ax", "ay", "az", "gx", "gy", "gz")


# ---------------------------------------------------------------------------
# MLflow best-effort 外壳：任何一次打点失败只降级不中断，后续调用直接跳过
# ---------------------------------------------------------------------------

class _MlflowSession:
    def __init__(self, experiment: str, run_name: str):
        self.run_id: str | None = None
        self.experiment = experiment
        self._client = None
        try:
            import mlflow

            mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
            mlflow.set_experiment(experiment)
            run = mlflow.start_run(run_name=run_name)
            self.run_id = run.info.run_id
            self._mlflow = mlflow
        except Exception as e:  # noqa: BLE001 - 操作台不可用不是训练失败
            logger.warning("MLflow 不可用，降级为纯本地记录：%s", e)
            self._mlflow = None

    def _call(self, fn, *args, **kwargs) -> None:
        if self._mlflow is None:
            return
        try:
            fn(*args, **kwargs)
        except Exception as e:  # noqa: BLE001 - 单次打点失败整体降级，不再重试
            logger.warning("MLflow 打点失败，本次训练余下打点跳过：%s", e)
            self._mlflow = None

    def log_params(self, params: dict) -> None:
        self._call(lambda: self._mlflow.log_params(params))

    def log_metrics(self, metrics: dict[str, float], step: int) -> None:
        self._call(lambda: self._mlflow.log_metrics(metrics, step=step))

    def register_version(self, model_name: str, artifact_uri: str) -> str | None:
        """在 Model Registry 登记一个版本（source 直接指向 MinIO 的权重 SoT），
        返回 MLflow 侧版本号；失败返回 None（我们自己的版本号 = job_id，不受影响）。"""
        if self._mlflow is None or self.run_id is None:
            return None
        try:
            from mlflow import MlflowClient

            client = MlflowClient()
            with contextlib.suppress(Exception):  # 已存在时报错，忽略
                client.create_registered_model(model_name)
            mv = client.create_model_version(model_name, source=artifact_uri, run_id=self.run_id)
            return str(mv.version)
        except Exception as e:  # noqa: BLE001
            logger.warning("MLflow 模型版本登记失败（不影响 Iceberg 档案）：%s", e)
            return None

    def close(self, status: str) -> None:
        if self._mlflow is None:
            return
        with contextlib.suppress(Exception):
            self._mlflow.end_run(status=status)


# ---------------------------------------------------------------------------
# 特征与训练
# ---------------------------------------------------------------------------

def _sample_features(lance_uri: str) -> np.ndarray | None:
    """一个样本 = 一个 Lance dataset（silver 行的窗口）→ 12 维统计特征。"""
    import lance

    table = lance.dataset(lance_uri).to_table(columns=list(FEATURE_COLS))
    if table.num_rows == 0:
        return None
    arr = np.column_stack([table.column(c).to_numpy(zero_copy_only=False) for c in FEATURE_COLS])
    return np.concatenate([np.nanmean(arr, axis=0), np.nanstd(arr, axis=0)])


def _load_dataset(samples: list[dict], quality_threshold: float) -> tuple[np.ndarray, np.ndarray, list[dict]]:
    """样本清单 → (X, y, 逐样本元信息)。读不出特征的样本跳过（记日志不算失败）。"""
    feats, labels, meta = [], [], []
    for s in samples:
        try:
            f = _sample_features(s["lance_uri"])
        except Exception as e:  # noqa: BLE001 - 个别样本文件损坏不拖垮训练
            logger.warning("样本 %s 特征读取失败，跳过：%s", s["sample_id"], e)
            continue
        if f is None or not np.isfinite(f).all():
            continue
        feats.append(f)
        labels.append(1 if float(s.get("quality_score") or 0.0) >= quality_threshold else 0)
        meta.append(s)
    if not feats:
        raise WorkerError(ErrorCode.DATA_EMPTY, "样本清单里没有任何可读的 Lance 样本")
    return np.array(feats), np.array(labels), meta


def _split_indices(meta: list[dict], seed: int) -> tuple[list[int], list[int]]:
    """优先用 dataset_sample 冻结时的 split（train→训练，其余→验证）；
    数据集只有单一 split 时按 seed 固定切 20% 做验证——保证总有验证集。"""
    train = [i for i, s in enumerate(meta) if (s.get("split") or "train") == "train"]
    val = [i for i in range(len(meta)) if i not in set(train)]
    if not val or not train:
        rng = np.random.RandomState(seed)
        idx = rng.permutation(len(meta))
        cut = max(1, int(len(meta) * 0.2))
        val, train = idx[:cut].tolist(), idx[cut:].tolist()
    return train, val


def _run(inp: dict, staging_prefix: str, pipes) -> dict:
    from sklearn.linear_model import SGDClassifier
    from sklearn.metrics import accuracy_score, log_loss
    from sklearn.preprocessing import StandardScaler

    job_id = inp["job_id"]
    model_name = inp["model_name"]
    params = inp.get("params") or {}
    seed = int(inp["seed"])
    epochs = int(params.get("epochs", 8))
    alpha = float(params.get("alpha", 1e-3))
    quality_threshold = float(params.get("quality_threshold", 0.7))

    samples = staging.read_parquet(inp["samples_key"]).to_pylist()
    if not samples:
        raise WorkerError(ErrorCode.DATA_EMPTY, f"dataset_version {inp['dataset_version']} 的样本清单为空")

    X, y, meta = _load_dataset(samples, quality_threshold)
    train_idx, val_idx = _split_indices(meta, seed)
    scaler = StandardScaler().fit(X[train_idx])
    Xs = scaler.transform(X)

    ml = _MlflowSession(experiment=f"edp/{model_name}", run_name=job_id)
    ml.log_params(
        {
            "job_id": job_id,
            "dataset_version": inp["dataset_version"],
            "seed": seed,
            "epochs": epochs,
            "alpha": alpha,
            "quality_threshold": quality_threshold,
            "num_train": len(train_idx),
            "num_val": len(val_idx),
        }
    )

    model = SGDClassifier(loss="log_loss", alpha=alpha, random_state=seed)
    rng = np.random.RandomState(seed)
    epoch_metrics: list[dict] = []
    classes = np.array([0, 1])
    labels_arg = [0, 1]  # log_loss 需要显式类别：小验证集可能只出现单一类
    final: dict[str, float] = {}
    for epoch in range(1, epochs + 1):
        order = rng.permutation(train_idx)
        model.partial_fit(Xs[order], y[order], classes=classes)
        p_train = model.predict_proba(Xs[train_idx])
        p_val = model.predict_proba(Xs[val_idx])
        final = {
            "train_loss": round(float(log_loss(y[train_idx], p_train, labels=labels_arg)), 6),
            "val_loss": round(float(log_loss(y[val_idx], p_val, labels=labels_arg)), 6),
            "val_accuracy": round(float(accuracy_score(y[val_idx], p_val.argmax(axis=1))), 6),
        }
        for name, value in final.items():
            epoch_metrics.append({"metric_name": name, "epoch": epoch, "value": value})
        ml.log_metrics(final, step=epoch)
        logger.info("epoch %s/%s: %s", epoch, epochs, final)
        if pipes is not None:
            pipes.report_custom_message({"job_id": job_id, "epoch": epoch, **final})

    # ---- 权重：SoT 直传 MinIO models/（不依赖 MLflow artifact 通道）----
    blob = pickle.dumps({"scaler": scaler, "model": model, "feature_cols": FEATURE_COLS})
    sha256 = hashlib.sha256(blob).hexdigest()
    artifact_uri = object_store.put_bytes(
        f"{object_store.PREFIX_MODELS}/{model_name}/{job_id}/model.pkl", blob
    )
    mlflow_version = ml.register_version(model_name, artifact_uri)
    ml.close(status="FINISHED")

    # ---- 逐样本评测明细（全量：train+val 都记，split 字段区分）----
    proba = model.predict_proba(Xs)
    pred = proba.argmax(axis=1)
    train_set = set(train_idx)
    eval_rows = [
        {
            "job_id": job_id,
            "sample_id": meta[i]["sample_id"],
            "model_name": model_name,
            "dataset_version": inp["dataset_version"],
            "split": "train" if i in train_set else "val",
            "y_true": int(y[i]),
            "y_pred": int(pred[i]),
            "prob": round(float(proba[i][pred[i]]), 6),
            "correct": bool(pred[i] == y[i]),
        }
        for i in range(len(meta))
    ]
    eval_key = f"{staging_prefix}/eval.parquet"
    staging.write_parquet(eval_key, eval_rows)

    final_metrics = {**final, "num_train": len(train_idx), "num_val": len(val_idx)}
    return {
        "job_id": job_id,
        "status": "ok",
        "model_name": model_name,
        "dataset_name": inp.get("dataset_name"),
        "dataset_version": inp["dataset_version"],
        "params": params,
        "seed": seed,
        "mlflow_run_id": ml.run_id,
        "mlflow_experiment": ml.experiment,
        "mlflow_version": mlflow_version,
        "artifact_uri": artifact_uri,
        "artifact_sha256": sha256,
        "metrics": final_metrics,
        "epoch_metrics": epoch_metrics,
        "eval_key": eval_key,
    }


# ---------------------------------------------------------------------------
# 顶层：与 ingest_parse 相同的清单契约（业务失败写带码清单后正常退出）
# ---------------------------------------------------------------------------

def _write_error_manifest(prefix: str, job_id: str, code: ErrorCode, message: str, pipes) -> None:
    payload = {"job_id": job_id, "status": "error", "error_code": code.value, "error": message}
    staging.write_json(f"{prefix}/{staging.MANIFEST_JSON}", payload)
    if pipes is not None:
        pipes.report_custom_message(payload)


def _maybe_open_pipes():
    """若由 Dagster Pipes 拉起（有 bootstrap 环境变量）则打开 pipes 会话；
    Argo/本地手工运行时退化为普通进程。"""
    from dagster_pipes import DAGSTER_PIPES_CONTEXT_ENV_VAR

    if os.environ.get(DAGSTER_PIPES_CONTEXT_ENV_VAR):
        from dagster_pipes import open_dagster_pipes

        return open_dagster_pipes()
    return contextlib.nullcontext(None)


def _main_inner(job_id: str, staging_prefix: str, pipes) -> None:
    try:
        inp = staging.read_json(f"{staging_prefix}/{staging.INPUT_JSON}")
    except Exception as e:  # noqa: BLE001 - input.json 都读不到：多半是存储抖动
        code = classify_exception(e)
        code = ErrorCode.INPUT_MISSING if code == ErrorCode.STORAGE_IO_ERROR else code
        _write_error_manifest(staging_prefix, job_id, code, f"读 input.json 失败: {type(e).__name__}: {e}", pipes)
        return

    try:
        result = _run(inp, staging_prefix, pipes)
    except Exception as e:  # noqa: BLE001 - 业务失败：写带码清单后正常退出（return 不是 sys.exit）
        logger.exception("training worker failed for job %s", job_id)
        _write_error_manifest(staging_prefix, job_id, classify_exception(e), f"{type(e).__name__}: {e}", pipes)
        return

    staging.write_json(f"{staging_prefix}/{staging.MANIFEST_JSON}", result)
    summary = {"job_id": job_id, "status": "ok", **result["metrics"]}
    logger.info("training worker done: %s", summary)
    if pipes is not None:
        pipes.report_custom_message(summary)


@click.command()
@click.option("--job-id", required=True)
@click.option("--run-id", required=True)
@click.option("--staging-prefix", required=True)
def main(job_id: str, run_id: str, staging_prefix: str) -> None:  # noqa: ARG001 - run_id 仅用于日志上下文
    logging.basicConfig(level=logging.INFO)
    with _maybe_open_pipes() as pipes:
        _main_inner(job_id, staging_prefix, pipes)


if __name__ == "__main__":
    main()
