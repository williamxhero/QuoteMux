# 测试目录审计（2026-08-23）

本目录是 QuoteMux 的正式测试套件。审计前仓库共有 33 个 `test_*.py`，其中 9 个已跟踪、24 个被 `.gitignore` 的 `/tests` 规则意外排除。审计结论是保留并跟踪 23 个仍覆盖现行模块的文件，删除 1 个已经退役的孤儿测试。

标准运行方式：

```powershell
$env:PYTHONPATH='src'
py -3.13 -m pytest tests -q
```

## 原 ignored 文件逐项结论

| 文件 | 结论 | 证据 |
| --- | --- | --- |
| `test_adj_factor_backfill.py` | 保留 | 覆盖现行 `local_daily` 与 factor writer 回填合同。 |
| `test_cache_payload_store.py` | 更新后保留 | payload store 仍在使用；夹具改用现行 `QUOTEMUX_CACHE_PAYLOAD_ROOT`。 |
| `test_capture.py` | 更新后保留 | capture policy、request builder、due job 均为现行核心；删除被专项 catch-up 测试取代的旧“未到本期时间绝不补跑”断言和脆弱的全能力非空断言。 |
| `test_capture_batches.py` | 保留 | 覆盖现行批次拆分与批次执行原语。 |
| `test_concepts.py` | 保留 | 覆盖现行 concept alias/runtime 合同。 |
| `test_db_availability.py` | 保留 | 覆盖现行 fact/ref availability 探测。 |
| `test_db_client_streaming.py` | 保留 | 覆盖现行 server cursor、取消、回滚和连接归还。 |
| `test_futures.py` | 保留 | 覆盖现行 futures catalog、coverage 与更新路径。 |
| `test_import_legacy_datalake_into_store.py` | 删除 | 硬加载从未进入 Git 的 `tools/import_legacy_datalake_into_store.py`；源码、README、runtime 与部署均无引用，collection 必然失败。 |
| `test_local_source_packages.py` | 更新后保留 | loader、manifest、隔离 worker 与 package integration 仍在使用；移除属于外部 provider package 私有实现且已漂移的断言，更新当前 package inventory。 |
| `test_migration_contracts.py` | 保留 | 覆盖现行 P0 cache/live 迁移等价与错误合同。 |
| `test_migration_range_journal.py` | 更新后保留 | migration journal 仍是正式写路径；夹具补上 1m coverage readmodel 刷新。 |
| `test_money_flow_fact_writer.py` | 更新后保留 | 覆盖现行资金流与分钟写入；夹具补上 coverage 汇总维护。 |
| `test_p0_fundamentals.py` | 保留 | 覆盖现行 P0 基本面模型、cache 与 provider fallback。 |
| `test_package_install.py` | 保留 | 覆盖现行 package install、隔离环境与 fingerprint。 |
| `test_postgres_cache_store.py` | 更新后保留 | PostgreSQL cache policy/store 仍是核心；更新默认禁用 realtime policy，并删除会安装真实 provider、且与细粒度 cache 测试重复的 composite smoke test。 |
| `test_query_engine.py` | 保留 | 覆盖现行 query engine 合并与 store 行为。 |
| `test_quotemux_runtime.py` | 更新后保留 | 覆盖公开 runtime、fallback、fact writers 与模型合同；更新当前签名/顺序/汇总维护，删除属于 QuoteMux_Packages 的 provider 私有函数测试和已退役的逐股票封单采集合同。 |
| `test_reference_reads.py` | 保留 | 覆盖现行 reference read SQL。 |
| `test_risk_flags_pagination.py` | 保留 | 覆盖现行 risk flags 分页合同。 |
| `test_shinny_edb.py` | 保留 | 覆盖现行 shinny EDB futures capability 边界。 |
| `test_stock_catalog_contract.py` | 保留 | 覆盖现行股票目录公开合同。 |
| `test_stock_quote_eligibility.py` | 保留 | 覆盖现行股票行情 eligibility 与过滤。 |
| `test_strategy_factor_window.py` | 保留 | 覆盖现行 strategy factor window 聚合合同。 |

## 收集边界

`.gitignore` 不再整体忽略 `/tests` 或 `/tools`；只忽略 Python cache、pytest cache、构建产物和明确的运行时目录。`pyproject.toml` 将 `tests` 固定为 canonical `testpaths`，fresh clone 不会把临时目录或外部 verification tests 混入正式套件。全局测试夹具会阻止 fallback 隐式创建 provider venv；需要验证隔离 worker 的测试显式注入假的 environment，因此 canonical suite 可离线重复运行。
