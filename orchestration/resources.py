"""有意留空：MVP 没有定义自定义 Dagster Resource。

`common/` 下的 `config.py`/`db.py`/`iceberg.py`/`object_store.py`/`kafka_ledger.py`
已经是所有引擎共用的连接层，asset 函数直接 `import` 调用即可；这些连接本身
无状态、懒加载（`functools.lru_cache`），不需要 Dagster Resource 生命周期
管理的额外开销。如果之后要支持"同一份代码测试环境/生产环境切换不同连接"，
再把 `common/config.py` 包一层 `ConfigurableResource` 即可，不影响现有 asset
签名（因为它们本来就不直接依赖 Dagster resource 注入）。
"""
