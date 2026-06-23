from __future__ import annotations

import json

from quotemux.store.timeout_admin import QuoteMuxTimeoutAdmin


def main() -> None:
    admin = QuoteMuxTimeoutAdmin()
    payload = {
        "capability_timeouts": admin.list_effective_capability_timeouts(),
        "provider_timeouts": admin.list_effective_provider_timeouts(),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
