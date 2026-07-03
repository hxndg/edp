"""审计列相关的小工具（README 3.1.8）。

真正把四列拼进 pyarrow Table 的逻辑在 `common.iceberg.with_audit_columns`；
这里只负责生成规范的 `_batch_id`，避免每个 job 自己拼字符串格式不一致。
"""
from __future__ import annotations

from datetime import datetime, timezone


def make_batch_id(*, robot_id: str, upload_id: str, when: datetime | None = None) -> str:
    """形如 `20260703-robotA-upload123`（README 3.1.8 示例格式）。"""
    when = when or datetime.now(timezone.utc)
    return f"{when:%Y%m%d}-{robot_id}-{upload_id}"
