# QuoteMux

独立市场数据 source runtime。

## 安装

Windows:

```powershell
py -3.13 -m pip install -e D:/WILL/STOCK/QuoteMux[all]
```

WSL:

```bash
python3 -m pip install --user --break-system-packages -e /mnt/d/WILL/STOCK/QuoteMux[all]
```

## 测试

```powershell
py -3.13 -m pytest D:/WILL/STOCK/QuoteMux/tests/test_quotemux_runtime.py -q
```

```bash
python3 -m pytest /mnt/d/WILL/STOCK/QuoteMux/tests/test_quotemux_runtime.py -q
```

## 构建

```powershell
py -3.13 -m pip wheel D:/WILL/STOCK/QuoteMux -w D:/WILL/STOCK/QuoteMux/dist
```

## 发布约定

- 公开入口使用 `quotemux` 包。
- `MarketHub` 以 HTTP 外壳形式消费它。
- `Updater` 以批处理外壳形式消费它。
- 兼容层仅允许做导入过渡和参数适配，不允许承载 source runtime 逻辑。
