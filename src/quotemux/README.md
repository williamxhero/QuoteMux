# QuoteMux 运行时目录职责

`contracts/` 是唯一 contract registry 入口，集中放 contract 名称、request/result 类型、key fields、默认策略和 contract 能力名。

`runtime_core/` 只负责运行时执行、fallback、report、audit、health、provider gate，不放业务 source 实现。

`source_packages/` 负责发现内建和外部 source package，固定外部目录契约：package 根目录下必须有 `quotemux_package.json`，文件内必须声明 `package_id`、`version`、`source_name`、`display_name`、`contract_names`、`config_schema`、`secret_fields`、`supports_multi_instance`、`handler_targets`。`handler_targets` 的值固定为 `python.module:function`，package 根目录会加入 import path。`version` 使用 `数字.数字.数字` 格式，依赖由 package 自身安装环境提供。

5 个 provider package 的源码已迁移到独立项目。QuoteMux admin 导入 package 目录时，会把源码复制到 QuoteMux 自己的运行时 package 目录，运行时只加载已安装目录。`sources/` 只保留非 provider package 的本地实现。

`config_runtime/` 只负责 source instance、RuntimeProfile、draft policy、active snapshot、publish/rollback 的配置状态。

`infra/` 只放底层通用基础设施，例如 DB、缓存路径、provider runtime gate、日期和代码规范化工具。
