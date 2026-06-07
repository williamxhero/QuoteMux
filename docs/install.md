# 安装与回滚

## 本地安装

Windows:

```powershell
py -3.13 -m pip install -e D:/path/to/QuoteMux[all]
```

WSL:

```bash
python3 -m pip install --user --break-system-packages -e /mnt/d/path/to/QuoteMux[all]
```

## wheel 构建

```powershell
py -3.13 -m pip wheel D:/path/to/QuoteMux -w D:/path/to/QuoteMux/dist
```

## 回滚

```powershell
py -3.13 -m pip uninstall -y quotemux
py -3.13 -m pip install <旧版本 wheel>
```
