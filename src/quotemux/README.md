# QuoteMux 运行时目录职责

`contracts/` 是唯一 contract registry 入口，集中放 contract 名称、request/result 类型、key fields、默认策略和 contract 能力名。

`runtime_core/` 只负责运行时执行、fallback、report、audit、health、provider gate，不放业务 source 实现。

`source_packages/` 负责发现内建和外部 source package，固定外部目录契约：package 根目录下必须有 `quotemux_package.json`，文件内必须声明 `package_id`、`version`、`source_name`、`display_name`、`contract_names`、`config_schema`、`secret_fields`、`supports_multi_instance`、`handler_targets`。`handler_targets` 的值固定为 `python.module:function`，package 根目录会加入 import path。`version` 使用 `数字.数字.数字` 格式，依赖由 package 自身安装环境提供。

5 个 provider package 的源码已迁移到独立项目。QuoteMux admin 导入 package 目录时，会把源码复制到 QuoteMux 自己的运行时 package 目录，运行时只加载已安装目录。`sources/` 只保留非 provider package 的本地实现。

`config_runtime/` 只负责 source instance、RuntimeProfile、draft policy、active snapshot、publish/rollback 的配置状态。

`infra/` 只放底层通用基础设施，例如 DB、缓存路径、provider runtime gate、日期和代码规范化工具。

## 期货数据口径

`shinny_tqsdk` 提供三个独立的 P0 原始能力：`futures.contracts.catalog`（期货交割合约目录与规格）、`futures.contracts.main_mapping`（当前主力合约映射）和 `futures.quotes.contract.realtime`（指定交割合约实时完整行情）。目录保留 TqSdk 无法统一的原始字段到 `raw_metadata`；主力映射与完整实时行情都由 provider 直接返回，QuoteMux core 不推导合约代码或主力关系。

实时交割合约行情不写入缓存或事实表（TTL=0）。目录按周采集并保留 30 天缓存，主力映射按日采集并保留 1 天缓存；公开 facade 仍会向已配置的 `shinny_tqsdk` instance 直接请求最新值。它们不能与 `shinny_edb` 的 T+1 `futures.quotes.main_continuous.1m`，或 Apex 的回调连续历史序列混用。
