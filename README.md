# QuoteMux 是什么

简单来说，`QuoteMux` 是一个**金融行情数据的超级聚合器**。

它并不是又造了一个类似 `Tushare` 或 `AkShare` 的新轮子，而是把 `Tushare`、`AkShare`、`eFinance`、`OpenTdx` 等等这些你平时常用的底层数据源**全部整合在了一起**。它不仅具备了这些库所有的数据获取能力，还额外加上了**可配置的本地缓存**功能。

**为什么要用它？主要是为了解决直接对接各种数据源时的一堆破事：**

- **不稳定&数据残缺：** 单一数据源经常报错，或者某些特定数据拿不到。
- **接口不统一：** 换个数据源等于重写一遍对接代码，依赖也容易冲突。
- **没有缓存&限制调用：** 很多底层库不带缓存，稍微多调几次就被封 IP 或限制调用频率。

`QuoteMux` 帮你在这些底层库之上垫了一层。你的业务代码、HTTP API  只需要和 `QuoteMux` 的**一套稳定接口**打交道就可以了，彻底把系统和特定的数据源解绑。




## 安装

请使用 AI 安装并跑通本项目（通过 [MarketHub](https://github.com/williamxhero/MarketHub) 仓库安装），提示词示例：“阅读 https://github.com/williamxhero/MarketHub/AIREADME.md 并在本机 D:\MarketHub\ 目录中安装这个项目”
