from __future__ import annotations

from quotemux.store.timeout_policy import sync_default_timeout_policies


def main() -> None:
    ok = sync_default_timeout_policies()
    print("timeout 默认策略同步完成" if ok else "timeout 默认策略同步失败")


if __name__ == "__main__":
    main()
