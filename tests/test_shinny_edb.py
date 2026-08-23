from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from quotemux_packages.shinny_edb import source


def test_edb_parser_converts_bar_start_to_china_bar_end(monkeypatch) -> None:
    start = datetime(2026, 8, 11, 9, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
    payload = (
        "datetime_nano,open,high,low,close,volume,close_oi\n"
        f"{int(start.timestamp() * 1_000_000_000)},4642,4647.8,4640,4647.4,1351,142736\n"
    )
    monkeypatch.setattr(source, "_fetch_csv", lambda *_args: payload)

    items = source.get_future_main_continuous_1m("IF", "CFFEX", "2026-08-11 09:00:00", "2026-08-11 10:00:00")

    assert len(items) == 1
    assert items[0].bar_time == "2026-08-11 09:31:00"
    assert items[0].series_type == "main_continuous"
    assert items[0].close == 4647.4
