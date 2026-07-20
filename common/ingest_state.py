"""入湖业务状态查询；与执行租约、Argo task 状态无关。"""
from __future__ import annotations

from common.db import fetch_all


def uncommitted_episode_ids() -> list[str]:
    """返回业务上尚未完整入湖、下游暂时不应消费的 episode_id。"""
    rows = fetch_all(
        """
        SELECT 'ep-' || upload_id AS episode_id
        FROM upload_session
        WHERE manifest_op = 'append' AND status <> 'done'
        UNION
        SELECT manifest ->> 'episode_id' AS episode_id
        FROM upload_session
        WHERE manifest_op = 'correct'
          AND status IN ('ingesting', 'failed')
          AND manifest ? 'episode_id'
        """
    )
    return [row["episode_id"] for row in rows if row["episode_id"]]
