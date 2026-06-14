# QuoteMux

## 这是什么

简单来说，`QuoteMux` 是一个**金融行情数据的超级聚合器**。

它并不是又造了一个类似 `Tushare` 或 `AkShare` 的新轮子，而是把 `Tushare`、`AkShare`、`eFinance`、`OpenTdx` 这些你平时常用的底层数据源**全部整合在了一起**。它不仅具备了这些库所有的数据获取能力，还额外加上了**可配置的本地缓存**功能。

**为什么要用它？主要是为了解决直接对接各种数据源时的一堆破事：**

- **不稳定&数据残缺：** 单一数据源经常报错，或者某些特定数据拿不到。
- **接口不统一：** 换个数据源等于重写一遍对接代码，依赖也容易冲突。
- **没有缓存&限制调用：** 很多底层库不带缓存，稍微多调几次就被封 IP 或限制调用频率。

`QuoteMux` 帮你在这些底层库之上垫了一层。你的业务代码、HTTP API 或者 GUI 界面，只需要和 `QuoteMux` 的**一套稳定接口**打交道就可以了，彻底把系统和特定的数据源解绑。

## 最推荐的“懒人”用法

如果你不想折腾代码，只想用最简单的方式完成安装、启动管理界面、然后一键拉取所有底层数据源，**强烈推荐直接配合 `MarketHub` 使用**：

- [MarketHub](https://github.com/williamxhero/MarketHub)

**操作路径非常傻瓜化：**

1. 在包含 `QuoteMux` 和 `MarketHub` 的工作区根目录执行：`py -3.13 install_markethub.py`
2. 启动 `MarketHub`
3. 浏览器打开管理后台：`http://127.0.0.1:8803/admin`
4. 点击界面上的 **`安装或更新全部 Packages`**

如果后续需要给一批 capability 做人工集中预热，也可以直接在同一个 `/admin` 页面进入 **Warmups** 区域，创建后台预热任务并观察进度，不需要把整批预热挂在单个 HTTP 请求上等待。

*补充：如果你希望服务真正进入可运行状态，而不是只装起 Python 依赖，目标机器还需要预先准备 PostgreSQL + TimescaleDB，并确保目标数据库已启用 `timescaledb` 扩展。*

搞定。你不需要再手动敲命令去装 `QuoteMux` 的 API，也不用自己管那一堆底层数据源的仓库。

## QuoteMux 到底是干嘛的？

作为整个架构的“核心引擎”，`QuoteMux` 专门负责干脏活累活：

- 对外提供统一的、好用的行情数据调用入口。
- 管理底层各种数据源包（Packages）的注册和加载。
- 解决依赖冲突：为自带 `requirements.txt` 的数据源包准备隔离的运行环境。
- 在线一键拉取和更新整个 `QuoteMux_Packages` 仓库里的数据源。

*(注：前面提到的 `MarketHub` 只是包在它外面的 HTTP API 和可视化外壳，真正在底层调数据干活的还是 `QuoteMux`。)*

## 纯代码方式：直接用 QuoteMux 安装所有数据源

如果你不需要 `MarketHub` 的界面，完全可以在 Python 代码里直接一键拉取所有数据源：

Python

```
from quotemux import install_all_packages

result = install_all_packages()
print(result.installed_package_ids)
```

这行代码会固定从官方的数据源仓库（[QuoteMux_Packages](https://github.com/williamxhero/QuoteMux_Packages)）把所需的所有底层库一并安装或更新。

## 安装 QuoteMux 主体

*运行前提：默认运行口径依赖本地 PostgreSQL + TimescaleDB，且目标数据库已启用 `timescaledb` 扩展。*

**Windows:**

PowerShell

```
py -3.13 -m pip install -e D:/path/to/QuoteMux
```

**Linux:**

Bash

```
python3.13 -m pip install -e /path/to/QuoteMux
```

*提示：安装完主体后，再调用上面的 `install_all_packages()` 就可以完成所有底层数据源包的安装了。*

## 整个生态是怎么配合的？（防头晕指南）

- **`QuoteMux`**：核心引擎（负责整合数据、处理缓存、提供统一接口）。
- **`QuoteMux_Packages`**：数据源弹药库（存放所有对接 Tushare/AkShare 等具体提供商的插件包）。
- **`MarketHub`**：外围工具（提供可视化的 Admin Console 和 HTTP API，让你点点鼠标就能调度 QuoteMux）。

## 注意事项

- **全量更新：** 目前的安装粒度是“全量”的，也就是一次性安装或更新整个 `QuoteMux_Packages` 仓库里的所有数据源，**暂时不提供单个包的独立安装入口**。
- **幂等性：** `install_all_packages()` 这个方法你可以随便重复调用，它的实际逻辑是“检测并安装/更新到远程仓库的最新版本”。
