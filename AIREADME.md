# QuoteMux AI 安装与部署指南

本文件面向第一次接触 QuoteMux 的安装 AI。QuoteMux 是核心 Python runtime，不是 HTTP 服务；MarketHub 提供 API/Admin；QuoteMux_Packages 是由 QuoteMux 在线安装的 source package 仓库，不应 clone 到工作区。本文所有路径、主机、端口、数据库和调度器都必须由用户 prompt 指定，缺少时主动询问。不得把安装 AI 自己机器上的路径、主机名、端口、数据库、账号、服务名或调度器配置带到用户目标机器；只能使用用户提供或在目标机器探测确认的配置。

## 0. 参数收集

先确认：OS（Windows/Linux/WSL2）、WORKSPACE_ROOT、Python >= 3.12 解释器路径、是否已有 PostgreSQL + TimescaleDB、DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD、是否启动 MarketHub、是否远程发布、DEPLOY_HOST/DEPLOY_ROOT/SERVICE_NAME、调度器和时区。没有数据库时，询问用户是否授权 AI 在目标机器安装；如果用户没有现成数据库且授权安装，AI 必须自己安装并配置，不得要求用户手工完成。不要把任何示例值当默认值；密码和 provider token 不能写入 Git 或命令历史。

## 1. 工作区与拉取

MarketHub 和 QuoteMux 必须同级：

<WORKSPACE_ROOT>/
  MarketHub/
  QuoteMux/

在用户确认的 WORKSPACE_ROOT 中执行：

git clone --branch main https://github.com/williamxhero/QuoteMux.git QuoteMux
git clone --branch main https://github.com/williamxhero/MarketHub.git MarketHub

已有仓库先检查 git status --short；有未提交修改停止并询问，干净时才允许 git pull --ff-only。

## 2. 安装 QuoteMux

完整系统使用 MarketHub 仓库提供的工作区安装器。把 MarketHub/install_markethub.py 复制到工作区根目录，然后运行：

<PYTHON> install_markethub.py

它创建共享 .venv、editable 安装 QuoteMux、安装 MarketHub 依赖并准备 runtime 目录；随后在线安装全部 Packages，并运行数据库 bootstrap。只验证 QuoteMux 时可执行：

<PYTHON> -m venv .venv
<PYTHON> -m pip install --upgrade pip
<PYTHON> -m pip install -e <WORKSPACE_ROOT>/QuoteMux

默认 runtime 需要 PostgreSQL + TimescaleDB。完整安装器会探测并尝试安装数据库服务，随后创建/确认账号、数据库、TimescaleDB 扩展和项目表；数据库已存在时使用用户提供的 DB_* 参数连接。不要因为数据库尚未准备好就要求用户手工操作；只有目标 OS 包管理器或系统管理员权限确实不可用时才报告阻塞。

安装 AI 必须判断是否需要提前建表：如果 QuoteMux/MarketHub 的启动或迁移入口会自动创建表，就直接调用该入口；如果必须提前创建，安装 AI 自己调用项目已有的迁移/初始化入口，例如 MarketHub/scripts/migrate_market_daily_contracts.py。不要要求用户手工执行 SQL，也不要凭空重写全量建表逻辑。

## 3. 安装全部 Packages

通过 QuoteMux 的在线安装入口安装：

<PYTHON> -c "from quotemux import install_all_packages; print(install_all_packages())"

QuoteMux 使用 git+https://github.com/williamxhero/QuoteMux_Packages.git@main 在线安装。不要 clone QuoteMux_Packages，也不要设置 QUOTEMUX_PACKAGE_REPO_SPEC 指向本地目录。安装会强制刷新 distribution、registry/config，并为带 requirements.txt 的 package 建立隔离环境。必须确认返回值是 PackageInstallResult 且 package id 完整；失败停止。不要手工 pip 安装单个 provider 替代总入口。

MarketHub 的独立 Packages 入口是：

<PYTHON> MarketHub/scripts/install_all_packages.py

## 4. 验证 QuoteMux 与运行 MarketHub

执行：

<PYTHON> -c "import quotemux; from quotemux import install_all_packages; print(quotemux.__file__); print(install_all_packages())"
<PYTHON> -m pytest -q QuoteMux/tests/test_package_install.py QuoteMux/tests/test_local_source_packages.py

QuoteMux 没有常驻服务。若用户要求完整运行，启动：

<PYTHON> MarketHub/scripts/run_api.py

使用用户确认的 MARKETHUB_HOST 和 MARKETHUB_PORT 验证：

curl --fail http://<MARKETHUB_HOST>:<MARKETHUB_PORT>/api/health
curl --fail http://<MARKETHUB_HOST>:<MARKETHUB_PORT>/admin

在 Admin 执行一次“安装或更新全部 Packages”。数据库、manifest、依赖、健康接口任一失败时停止并报告完整错误。

## 5. 持久化和定时任务

先询问用户使用 Windows Task Scheduler、Linux systemd、WSL2 systemd 还是已有 Task Center，并确认时区。到期检查调用为：

POST http://<MARKETHUB_HOST>:<MARKETHUB_PORT>/api/admin/capture/run-due-async

调度器必须使用能保留真实退出码的 shell 执行器；Task Center 通常使用 shell_file。全局数据更新脚本由 MarketHub 安装器复制到用户指定的 MARKETHUB_RUNTIME_ROOT/scripts/，包括 global-data-update.sh、global-data-update-with-health.sh、data-health-check.sh。它们依赖 Linux shell 工具，不能直接交给 Windows 原生任务计划。注册后必须手动运行一次并验证状态、退出码、日志和数据库变化。

## 6. 远程发布

只有用户明确要求远程 Linux 发布时才使用 MarketHub/scripts/deploy_yosef_server.ps1。AI 必须传入用户确认的 DEPLOY_HOST、DEPLOY_ROOT、REMOTE_RUNTIME_ROOT、REMOTE_ENV_PATH、SERVICE_NAME 和 HEALTH_URL，并验证 SSH 与免交互 sudo 权限。脚本负责打包 MarketHub 和 QuoteMux、在线安装 Packages、执行数据库 bootstrap、切换 release、创建 systemd 服务并检查健康接口。若部署假设与目标环境不一致，先修改脚本或询问用户，不能静默跳过验收，也不能改用 Docker。

## 7. 停止条件

缺少用户参数或 secret、TimescaleDB 不可用、PackageInstallResult 失败、API 非 2xx、调度器退出码丢失或数据健康检查失败时，停止并报告需要补充的参数与证据。
