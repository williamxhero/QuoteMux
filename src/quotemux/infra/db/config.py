from __future__ import annotations

import os


def _get_db_port() -> int:
    text = os.getenv("DL_DB_PORT", "55432")
    try:
        return int(text)
    except ValueError:
        return 55432


DL_DB_HOST = os.getenv("DL_DB_HOST", "localhost")
DL_DB_PORT = _get_db_port()
DL_DB_NAME = os.getenv("DL_DB_NAME", "datalake_dev")
DL_DB_USER = os.getenv("DL_DB_USER", "datalake")
DL_DB_PASSWORD = os.getenv("DL_DB_PASSWORD", "datalake_dev_password")
DL_DB_CONNECT_TIMEOUT = 3
