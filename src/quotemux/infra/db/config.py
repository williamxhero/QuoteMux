from __future__ import annotations

import os


def _get_db_port() -> int:
    text = os.getenv("MARKETHUB_DB_PORT", "5432")
    try:
        return int(text)
    except ValueError:
        return 5432


def _get_db_connect_timeout() -> int:
    text = os.getenv("MHK_DB_CONNECT_TIMEOUT", "3")
    try:
        return int(text)
    except ValueError:
        return 3


DB_HOST = os.getenv("MARKETHUB_DB_HOST", "127.0.0.1")
DB_PORT = _get_db_port()
DB_NAME = os.getenv("MARKETHUB_DB_NAME", "markethub")
DB_USER = os.getenv("MARKETHUB_DB_USER", "markethub")
DB_PASSWORD = os.getenv("MARKETHUB_DB_PASSWORD", "")
DB_CONNECT_TIMEOUT = _get_db_connect_timeout()
