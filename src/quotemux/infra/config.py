from __future__ import annotations

import os
from pathlib import Path


DATE_FORMAT = "%Y%m%d"
DATETIME_FORMAT = "%Y%m%d %H:%M:%S"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_QUOTEMUX_DOCS_ROOT = PROJECT_ROOT / "docs"


def _get_data_root() -> Path:
    path_text = os.getenv("MARKETHUB_DATA_ROOT", "")
    if path_text:
        return Path(path_text)
    if os.name != "nt":
        return Path("/mnt/c/STOCKS/markethub")
    return Path("C:/STOCKS/markethub")


def _get_markethub_docs_root() -> Path:
    path_text = os.getenv("MARKETHUB_DOCS_ROOT", "")
    if path_text:
        return Path(path_text)
    return DEFAULT_QUOTEMUX_DOCS_ROOT


DATA_ROOT = _get_data_root()
DOCS_ROOT = _get_markethub_docs_root()
