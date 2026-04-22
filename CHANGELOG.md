# Changelog

## 0.1.0 - 2026-04-22

- 从 `stock_platform/libs/quotemux` 外拆为独立项目 `D:/WILL/STOCK/QuoteMux`
- 内聚 `platform_models`、`platform_provider_clients`、`platform_db`、`providers`
- 新增 `quotemux.models`、`quotemux.results`、`quotemux.runtime_core`、`quotemux.sources`
- 移除 `quotemux` 包内 `runtime_paths` 路径注入依赖
- `stock_platform` 改为消费安装后的外部 `QuoteMux`
