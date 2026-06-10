from __future__ import annotations

import os


def _get_db_port() -> int:
    text = os.getenv("MARKETHUB_DB_PORT", "55432")
    try:
        return int(text)
    except ValueError:
        return 55432


DB_HOST = os.getenv("MARKETHUB_DB_HOST", "localhost")
DB_PORT = _get_db_port()
DB_NAME = os.getenv("MARKETHUB_DB_NAME", "markethub_dev")
DB_USER = os.getenv("MARKETHUB_DB_USER", "markethub")
DB_PASSWORD = os.getenv("MARKETHUB_DB_PASSWORD", "markethub_dev_password")
DB_CONNECT_TIMEOUT = 3
