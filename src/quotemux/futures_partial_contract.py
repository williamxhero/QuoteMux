"""The one factual admission relation for S000012 partial futures reads.

This module deliberately has no database client.  Both the immutable
publication builder and the public reader embed this CTE verbatim; keeping it
here prevents a metadata plan from accepting rows that a page can later leak
(or vice versa).
"""
from __future__ import annotations

from collections.abc import Iterable

DATASET_ID = "future_1m_partial_s000012_quotemux"
SERIES_TYPE = "apex_l0_adjusted"
PYRAMID_SOURCE_KEY = "pyramid_back_adjusted_20260714"
APEX_SOURCE_KEY = "apex_l0_import"
SHINNY_SOURCE_KEY = "shinny_edb_derived_back_adjusted_20260811"
FACT_NORMALIZATION_VERSION = "quotemux_fact_source_key_v1"

# This is intentionally an exact, closed contract.  In particular TL is not
# an alias for Treasury T and ao is listed under SHFE.
PRODUCT_EXCHANGES: tuple[tuple[str, str], ...] = (
    ("ag", "SHFE"), ("al", "SHFE"), ("AP", "CZCE"), ("CF", "CZCE"),
    ("cu", "SHFE"), ("hc", "SHFE"), ("i", "DCE"), ("j", "DCE"),
    ("m", "DCE"), ("MA", "CZCE"), ("ni", "SHFE"), ("p", "DCE"),
    ("ru", "SHFE"), ("sc", "INE"), ("T", "CFFEX"), ("TA", "CZCE"),
    ("TF", "CFFEX"), ("v", "DCE"), ("y", "DCE"), ("lh", "DCE"),
    ("SA", "CZCE"), ("ao", "SHFE"), ("si", "GFEX"),
)
PRODUCTS = tuple(code for code, _exchange in PRODUCT_EXCHANGES)
PRODUCT_EXCHANGE = dict(PRODUCT_EXCHANGES)

# These were inspected source facts, not inferred missing bars.  They are
# excluded only when the legacy Apex source claims them; Pyramid candidates
# have their own per-candidate qmi admission proof.
INVALID_APEX_KEYS: tuple[tuple[str, str], ...] = (
    ("TA", "2010-11-17 09:05:00"),
    ("y", "2010-11-11 14:01:00"),
    ("y", "2011-02-23 11:22:00"),
)


def canonical_json_bytes(value: object) -> bytes:
    import json
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def candidate_row(row: dict[str, object]) -> dict[str, object]:
    """Return the one canonical fact representation used for qmi hashes."""
    return {
        "product_code": row["product_code"], "exchange": row["exchange"],
        "bar_time": str(row["bar_time"]), "open": row["open"], "high": row["high"],
        "low": row["low"], "close": row["close"], "volume": row["volume"],
        "open_interest": None, "adjustment_offset": row["adjustment_offset"],
        "series_type": SERIES_TYPE, "source_key": PYRAMID_SOURCE_KEY,
    }


def candidate_sha256(row: dict[str, object]) -> str:
    import hashlib
    return hashlib.sha256(canonical_json_bytes(candidate_row(row))).hexdigest()


def admitted_rows_cte(*, qmi_expression: str, cte_name: str = "admitted_rows") -> str:
    """Return the canonical eligible relation.

    ``qmi_expression`` is SQL text yielding one qmi id (usually ``%s`` for a
    planning query or a publication payload subquery for a public page).  It
    is deliberately not a source preference: a Pyramid row is usable only
    after the exact accepted candidate key/hash was persisted in qmi.
    """
    products = ", ".join("(%r::text, %r::text)" % value for value in PRODUCT_EXCHANGES)
    invalid = ", ".join("(%r::text, %r::timestamp)" % value for value in INVALID_APEX_KEYS)
    return f"""
        with partial_products(product_code, exchange) as (values {products}),
        pyramid_admission as materialized (
            select admission.product_code, admission.exchange, admission.series_type,
                   admission.bar_time, admission.candidate_sha256
            from audit.future_bar_1m_import_admission admission
            where admission.qmi_id = ({qmi_expression})
              and admission.disposition in ('inserted', 'already_present_equivalent')
        ),
        {cte_name} as materialized (
            select bars.product_code, bars.exchange, bars.series_type, bars.bar_time,
                   bars.open, bars.high, bars.low, bars.close, bars.volume,
                   bars.open_interest, bars.adjustment_offset, bars.source_key,
                   admission.candidate_sha256 as pyramid_candidate_sha256
            from fact.future_bar_1m bars
            join partial_products products
              on products.product_code = bars.product_code
             and products.exchange = bars.exchange
            left join pyramid_admission admission
              on admission.product_code = bars.product_code
             and admission.exchange = bars.exchange
             and admission.series_type = bars.series_type
             and admission.bar_time = bars.bar_time
            where bars.series_type = '{SERIES_TYPE}'
              and bars.open is not null and bars.high is not null
              and bars.low is not null and bars.close is not null
              and bars.volume is not null and bars.volume >= 0
              and bars.high >= greatest(bars.open, bars.close, bars.low)
              and bars.low <= least(bars.open, bars.close, bars.high)
              and (
                    (bars.source_key = '{PYRAMID_SOURCE_KEY}'
                     and admission.candidate_sha256 is not null)
                 or (bars.source_key in ('{APEX_SOURCE_KEY}', '{SHINNY_SOURCE_KEY}')
                     and (bars.source_key <> '{APEX_SOURCE_KEY}'
                          or (bars.product_code, bars.bar_time) not in (values {invalid})))
              )
        )
    """


def canonical_row_payload(row: Iterable[object]) -> dict[str, object]:
    names = ("product_code", "exchange", "series_type", "source_key", "bar_time", "open", "high", "low", "close", "volume", "open_interest", "adjustment_offset", "pyramid_candidate_sha256")
    return dict(zip(names, row, strict=True))
