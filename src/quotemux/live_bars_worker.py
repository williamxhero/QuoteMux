from __future__ import annotations

from datetime import datetime
import json
import sys

from quotemux.live_bars import CurrentBarRequest, ingest_current_stock_bars, recover_due_current_stock_bars


def main() -> int:
    try:
        arguments = sys.argv[1:]
        if arguments == ["--recover"]:
            payload: dict[str, object] = {"action": "recover"}
        elif arguments:
            raise ValueError("usage: python -m quotemux.live_bars_worker [--recover]")
        else:
            payload = json.loads(sys.stdin.read())
        action = str(payload.get("action", "ingest"))
        if action == "recover":
            raw_now = payload.get("effective_now")
            effective_now = datetime.fromisoformat(str(raw_now)) if raw_now is not None else None
            result = recover_due_current_stock_bars(effective_now)
            print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
            return 0
        if action != "ingest":
            raise ValueError("action must be ingest or recover")
        effective_now = datetime.fromisoformat(str(payload["effective_now"]))
        codes = payload.get("codes", [])
        if not isinstance(codes, list) or not all(isinstance(code, str) for code in codes):
            raise ValueError("codes must be a list of strings")
        freq = str(payload.get("freq", "1m"))
        result = ingest_current_stock_bars(CurrentBarRequest(codes=tuple(codes), effective_now=effective_now, freq=freq))
    except Exception as exc:
        print(f"live-ingest worker failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result.to_dict(), ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
