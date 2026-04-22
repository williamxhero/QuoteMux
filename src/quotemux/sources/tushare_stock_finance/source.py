from __future__ import annotations

import pandas as pd

from quotemux.infra.cache.store import filter_frame_by_date_range
from platform_models import AuditItem, DisclosureDateItem, DividendItem, ExpressItem, ForecastItem, MainBusinessItem, RepurchaseItem, RightsIssueItem, ShareChangeItem, UnlockScheduleItem
from quotemux.infra.common import normalize_stock_code, stock_code_to_ts
from quotemux.infra.tushare.helpers import normalize_date_range, normalize_period_range, query_frame, read_cached_ranges


def _fetch_dividend_frame(code: str, _: str, __: str) -> pd.DataFrame:
    df = query_frame("dividend", ts_code=stock_code_to_ts(code))
    if df.empty:
        return df
    work = df.copy()
    work["code"] = normalize_stock_code(code)
    work["announce_date"] = work["ann_date"].astype(str) if "ann_date" in work.columns else ""
    work["record_date"] = work["record_date"].astype(str) if "record_date" in work.columns else ""
    work["ex_date"] = work["ex_date"].astype(str) if "ex_date" in work.columns else ""
    work["pay_date"] = work["pay_date"].astype(str) if "pay_date" in work.columns else ""
    work["cash_dividend_per_share"] = work["cash_div"] if "cash_div" in work.columns else work["cash_div_tax"] if "cash_div_tax" in work.columns else None
    work["stock_dividend_per_share"] = work["stk_div"] if "stk_div" in work.columns else None
    work["capital_reserve_per_share"] = work["stk_bo_rate"] if "stk_bo_rate" in work.columns else None
    return work[["code", "announce_date", "record_date", "ex_date", "pay_date", "cash_dividend_per_share", "stock_dividend_per_share", "capital_reserve_per_share"]]


def get_dividends(code: str, start_date: str, end_date: str) -> list[DividendItem]:
    actual_start, actual_end = normalize_date_range("", start_date, end_date, 720)
    cache_df = read_cached_ranges(
        ["stocks", "corporate-actions", "dividends"],
        {"code": normalize_stock_code(code)},
        "announce_date",
        actual_start,
        actual_end,
        "day",
        lambda start_value, end_value: _fetch_dividend_frame(code, start_value, end_value),
    )
    filtered_df = filter_frame_by_date_range(cache_df, "announce_date", actual_start, actual_end)
    return [
        DividendItem(
            code=str(row["code"]),
            announce_date=str(row["announce_date"]),
            record_date=str(row["record_date"]) if pd.notna(row["record_date"]) else "",
            ex_date=str(row["ex_date"]) if pd.notna(row["ex_date"]) else "",
            pay_date=str(row["pay_date"]) if pd.notna(row["pay_date"]) else "",
            cash_dividend_per_share=float(row["cash_dividend_per_share"]) if pd.notna(row["cash_dividend_per_share"]) else None,
            stock_dividend_per_share=float(row["stock_dividend_per_share"]) if pd.notna(row["stock_dividend_per_share"]) else None,
            capital_reserve_per_share=float(row["capital_reserve_per_share"]) if pd.notna(row["capital_reserve_per_share"]) else None,
        )
        for _, row in filtered_df.sort_values("announce_date").iterrows()
    ]


def _fetch_repurchase_frame(code: str, start_value: str, end_value: str) -> pd.DataFrame:
    df = query_frame("repurchase", ts_code=stock_code_to_ts(code), start_date=start_value, end_date=end_value)
    if df.empty:
        return df
    work = df.copy()
    work["code"] = normalize_stock_code(code)
    work["announce_date"] = work["ann_date"].astype(str) if "ann_date" in work.columns else ""
    work["progress"] = work["proc"].astype(str) if "proc" in work.columns else ""
    work["repurchase_volume"] = work["vol"] if "vol" in work.columns else None
    work["repurchase_amount"] = work["amount"] if "amount" in work.columns else None
    work["highest_price"] = work["high_limit"] if "high_limit" in work.columns else None
    work["lowest_price"] = work["low_limit"] if "low_limit" in work.columns else None
    return work[["code", "announce_date", "progress", "repurchase_volume", "repurchase_amount", "highest_price", "lowest_price"]]


def get_repurchases(code: str, start_date: str, end_date: str) -> list[RepurchaseItem]:
    actual_start, actual_end = normalize_date_range("", start_date, end_date, 720)
    cache_df = read_cached_ranges(
        ["stocks", "corporate-actions", "repurchases"],
        {"code": normalize_stock_code(code)},
        "announce_date",
        actual_start,
        actual_end,
        "day",
        lambda start_value, end_value: _fetch_repurchase_frame(code, start_value, end_value),
    )
    filtered_df = filter_frame_by_date_range(cache_df, "announce_date", actual_start, actual_end)
    if filtered_df.empty or "announce_date" not in filtered_df.columns:
        return []
    return [
        RepurchaseItem(
            code=str(row["code"]),
            announce_date=str(row["announce_date"]),
            progress=str(row["progress"]) if pd.notna(row["progress"]) else "",
            repurchase_volume=float(row["repurchase_volume"]) if pd.notna(row["repurchase_volume"]) else None,
            repurchase_amount=float(row["repurchase_amount"]) if pd.notna(row["repurchase_amount"]) else None,
            highest_price=float(row["highest_price"]) if pd.notna(row["highest_price"]) else None,
            lowest_price=float(row["lowest_price"]) if pd.notna(row["lowest_price"]) else None,
        )
        for _, row in filtered_df.sort_values("announce_date").iterrows()
    ]


def get_rights_issues(code: str, start_date: str, end_date: str) -> list[RightsIssueItem]:
    actual_start, actual_end = normalize_date_range("", start_date, end_date, 1440)
    cache_df = read_cached_ranges(
        ["stocks", "corporate-actions", "rights-issues"],
        {"code": normalize_stock_code(code)},
        "announce_date",
        actual_start,
        actual_end,
        "day",
        lambda start_value, end_value: _fetch_rights_issue_frame(code, start_value, end_value),
    )
    filtered_df = filter_frame_by_date_range(cache_df, "announce_date", actual_start, actual_end)
    if filtered_df.empty or "announce_date" not in filtered_df.columns:
        return []
    return [
        RightsIssueItem(
            code=str(row["code"]),
            announce_date=str(row["announce_date"]),
            rights_ratio=float(row["rights_ratio"]) if pd.notna(row["rights_ratio"]) else None,
            rights_price=float(row["rights_price"]) if pd.notna(row["rights_price"]) else None,
            record_date=str(row["record_date"]) if pd.notna(row["record_date"]) else "",
            ex_date=str(row["ex_date"]) if pd.notna(row["ex_date"]) else "",
        )
        for _, row in filtered_df.sort_values("announce_date").iterrows()
    ]


def get_share_changes(code: str, trade_date: str, start_date: str, end_date: str) -> list[ShareChangeItem]:
    actual_start, actual_end = normalize_date_range(trade_date, start_date, end_date, 1440)
    cache_df = read_cached_ranges(
        ["stocks", "corporate-actions", "share-changes"],
        {"code": normalize_stock_code(code)},
        "change_date",
        actual_start,
        actual_end,
        "day",
        lambda start_value, end_value: _fetch_share_change_frame(code, start_value, end_value),
    )
    filtered_df = filter_frame_by_date_range(cache_df, "change_date", actual_start, actual_end)
    if filtered_df.empty or "change_date" not in filtered_df.columns:
        return []
    return [
        ShareChangeItem(
            code=str(row["code"]),
            change_date=str(row["change_date"]),
            reason=str(row["reason"]) if pd.notna(row["reason"]) else "",
            total_share=float(row["total_share"]) if pd.notna(row["total_share"]) else None,
            float_share=float(row["float_share"]) if pd.notna(row["float_share"]) else None,
            restricted_share=float(row["restricted_share"]) if pd.notna(row["restricted_share"]) else None,
        )
        for _, row in filtered_df.sort_values("change_date").iterrows()
    ]


def _fetch_rights_issue_frame(code: str, start_value: str, end_value: str) -> pd.DataFrame:
    df = query_frame("rights_issue", ts_code=stock_code_to_ts(code), start_date=start_value, end_date=end_value)
    if df.empty:
        df = query_frame("stk_ration", ts_code=stock_code_to_ts(code), start_date=start_value, end_date=end_value)
    if df.empty:
        return df
    work = df.copy()
    work["code"] = normalize_stock_code(code)
    if "ann_date" in work.columns:
        work["announce_date"] = work["ann_date"].astype(str)
    elif "report_date" in work.columns:
        work["announce_date"] = work["report_date"].astype(str)
    else:
        work["announce_date"] = ""
    work["rights_ratio"] = work["rights_ratio"] if "rights_ratio" in work.columns else work["ration_ratio"] if "ration_ratio" in work.columns else None
    work["rights_price"] = work["rights_price"] if "rights_price" in work.columns else work["ration_price"] if "ration_price" in work.columns else None
    work["record_date"] = work["record_date"].astype(str) if "record_date" in work.columns else ""
    work["ex_date"] = work["ex_date"].astype(str) if "ex_date" in work.columns else ""
    return work[["code", "announce_date", "rights_ratio", "rights_price", "record_date", "ex_date"]]


def _fetch_share_change_frame(code: str, start_value: str, end_value: str) -> pd.DataFrame:
    df = query_frame("share_change", ts_code=stock_code_to_ts(code), start_date=start_value, end_date=end_value)
    if df.empty:
        df = query_frame("stk_share_change", ts_code=stock_code_to_ts(code), start_date=start_value, end_date=end_value)
    if not df.empty:
        work = df.copy()
        work["code"] = normalize_stock_code(code)
        if "chg_date" in work.columns:
            work["change_date"] = work["chg_date"].astype(str)
        elif "ann_date" in work.columns:
            work["change_date"] = work["ann_date"].astype(str)
        else:
            work["change_date"] = ""
        if "chg_reason" in work.columns:
            work["reason"] = work["chg_reason"].astype(str)
        elif "change_reason" in work.columns:
            work["reason"] = work["change_reason"].astype(str)
        else:
            work["reason"] = ""
        work["total_share"] = work["total_share"] if "total_share" in work.columns else None
        work["float_share"] = work["float_share"] if "float_share" in work.columns else None
        if "restricted_share" in work.columns:
            work["restricted_share"] = work["restricted_share"]
        elif "limit_share" in work.columns:
            work["restricted_share"] = work["limit_share"]
        elif "total_share" in work.columns and "float_share" in work.columns:
            work["restricted_share"] = pd.to_numeric(work["total_share"], errors="coerce") - pd.to_numeric(work["float_share"], errors="coerce")
        else:
            work["restricted_share"] = None
        return work[["code", "change_date", "reason", "total_share", "float_share", "restricted_share"]]
    daily_df = query_frame("daily_basic", ts_code=stock_code_to_ts(code), start_date=start_value, end_date=end_value)
    if daily_df.empty:
        return daily_df
    work = daily_df.copy()
    work["code"] = normalize_stock_code(code)
    work["change_date"] = work["trade_date"].astype(str)
    work["total_share"] = pd.to_numeric(work["total_share"], errors="coerce") if "total_share" in work.columns else None
    work["float_share"] = pd.to_numeric(work["float_share"], errors="coerce") if "float_share" in work.columns else None
    work["restricted_share"] = work["total_share"] - work["float_share"]
    work = work.sort_values("change_date").reset_index(drop=True)
    changed_rows: list[dict[str, object]] = []
    previous_total: float | None = None
    previous_float: float | None = None
    for _, row in work.iterrows():
        total_share = float(row["total_share"]) if pd.notna(row["total_share"]) else None
        float_share = float(row["float_share"]) if pd.notna(row["float_share"]) else None
        if previous_total is None and previous_float is None:
            previous_total = total_share
            previous_float = float_share
            continue
        if total_share == previous_total and float_share == previous_float:
            continue
        reason = "share_structure_change"
        if total_share == previous_total and float_share != previous_float:
            reason = "float_share_change"
        elif total_share != previous_total and float_share == previous_float:
            reason = "total_share_change"
        changed_rows.append(
            {
                "code": normalize_stock_code(code),
                "change_date": str(row["change_date"]),
                "reason": reason,
                "total_share": total_share,
                "float_share": float_share,
                "restricted_share": float(row["restricted_share"]) if pd.notna(row["restricted_share"]) else None,
            }
        )
        previous_total = total_share
        previous_float = float_share
    if not changed_rows:
        return pd.DataFrame()
    return pd.DataFrame(changed_rows)


def _fetch_unlock_frame(code: str, start_value: str, end_value: str) -> pd.DataFrame:
    df = query_frame("share_float", ts_code=stock_code_to_ts(code), start_date=start_value, end_date=end_value)
    if df.empty:
        return df
    work = df.copy()
    work["code"] = normalize_stock_code(code)
    work["unlock_date"] = work["float_date"].astype(str) if "float_date" in work.columns else ""
    work["holder_type"] = work["holder_name"].astype(str) if "holder_name" in work.columns else ""
    work["unlock_volume"] = work["float_share"] if "float_share" in work.columns else None
    work["unlock_ratio"] = work["float_ratio"] if "float_ratio" in work.columns else None
    work["share_type"] = work["share_type"].astype(str) if "share_type" in work.columns else ""
    return work[["code", "unlock_date", "holder_type", "unlock_volume", "unlock_ratio", "share_type"]]


def get_unlock_schedules(code: str, unlock_date: str, start_date: str, end_date: str) -> list[UnlockScheduleItem]:
    actual_start, actual_end = normalize_date_range(unlock_date, start_date, end_date, 720)
    cache_df = read_cached_ranges(
        ["stocks", "corporate-actions", "unlock-schedules"],
        {"code": normalize_stock_code(code)},
        "unlock_date",
        actual_start,
        actual_end,
        "day",
        lambda start_value, end_value: _fetch_unlock_frame(code, start_value, end_value),
    )
    filtered_df = filter_frame_by_date_range(cache_df, "unlock_date", actual_start, actual_end)
    if filtered_df.empty or "unlock_date" not in filtered_df.columns:
        return []
    return [
        UnlockScheduleItem(
            code=str(row["code"]),
            unlock_date=str(row["unlock_date"]),
            holder_type=str(row["holder_type"]) if pd.notna(row["holder_type"]) else "",
            unlock_volume=float(row["unlock_volume"]) if pd.notna(row["unlock_volume"]) else None,
            unlock_ratio=float(row["unlock_ratio"]) if pd.notna(row["unlock_ratio"]) else None,
            share_type=str(row["share_type"]) if pd.notna(row["share_type"]) else "",
        )
        for _, row in filtered_df.sort_values("unlock_date").iterrows()
    ]


def _fetch_audit_frame(code: str, start_value: str, end_value: str) -> pd.DataFrame:
    df = query_frame("fina_audit", ts_code=stock_code_to_ts(code), start_date=start_value, end_date=end_value)
    if df.empty:
        return df
    work = df.copy()
    work["code"] = normalize_stock_code(code)
    work["report_period"] = work["end_date"].astype(str)
    work["announce_date"] = work["ann_date"].astype(str) if "ann_date" in work.columns else ""
    work["audit_result"] = work["audit_result"] if "audit_result" in work.columns else ""
    work["auditor"] = work["audit_agency"] if "audit_agency" in work.columns else ""
    work["sign_accountant"] = work["audit_sign"] if "audit_sign" in work.columns else ""
    return work[["code", "report_period", "audit_result", "auditor", "sign_accountant", "announce_date"]]


def get_audits(code: str, report_period: str, start_period: str, end_period: str) -> list[AuditItem]:
    actual_start, actual_end = normalize_period_range(report_period, start_period, end_period)
    cache_df = read_cached_ranges(
        ["stocks", "finance", "audits"],
        {"code": normalize_stock_code(code)},
        "report_period",
        actual_start,
        actual_end,
        "quarter",
        lambda start_value, end_value: _fetch_audit_frame(code, start_value, end_value),
    )
    filtered_df = filter_frame_by_date_range(cache_df, "report_period", actual_start, actual_end)
    return [
        AuditItem(
            code=str(row["code"]),
            report_period=str(row["report_period"]),
            audit_result=str(row["audit_result"]) if pd.notna(row["audit_result"]) else "",
            auditor=str(row["auditor"]) if pd.notna(row["auditor"]) else "",
            sign_accountant=str(row["sign_accountant"]) if pd.notna(row["sign_accountant"]) else "",
            announce_date=str(row["announce_date"]) if pd.notna(row["announce_date"]) else "",
        )
        for _, row in filtered_df.sort_values("report_period").iterrows()
    ]


def _fetch_disclosure_frame(code: str, _: str, end_value: str) -> pd.DataFrame:
    kwargs: dict[str, object] = {"end_date": end_value}
    if code:
        kwargs["ts_code"] = stock_code_to_ts(code)
    df = query_frame("disclosure_date", **kwargs)
    if df.empty:
        return df
    work = df.copy()
    work["code"] = work["ts_code"].astype(str).str.split(".").str[0] if "ts_code" in work.columns else normalize_stock_code(code)
    work["report_period"] = work["end_date"].astype(str)
    work["plan_date"] = work["pre_date"].astype(str) if "pre_date" in work.columns else ""
    work["actual_date"] = work["actual_date"].astype(str) if "actual_date" in work.columns else ""
    work["change_reason"] = work["modify_reason"] if "modify_reason" in work.columns else ""
    return work[["code", "report_period", "plan_date", "actual_date", "change_reason"]]


def get_disclosure_dates(code: str, report_period: str, start_period: str, end_period: str) -> list[DisclosureDateItem]:
    actual_start, actual_end = normalize_period_range(report_period, start_period, end_period)
    cache_df = read_cached_ranges(
        ["stocks", "finance", "disclosure-dates"],
        {"code": normalize_stock_code(code) or "all"},
        "report_period",
        actual_start,
        actual_end,
        "quarter",
        lambda start_value, end_value: _fetch_disclosure_frame(code, start_value, end_value),
    )
    filtered_df = filter_frame_by_date_range(cache_df, "report_period", actual_start, actual_end)
    return [
        DisclosureDateItem(
            code=str(row["code"]),
            report_period=str(row["report_period"]),
            plan_date=str(row["plan_date"]) if pd.notna(row["plan_date"]) else "",
            actual_date=str(row["actual_date"]) if pd.notna(row["actual_date"]) else "",
            change_reason=str(row["change_reason"]) if pd.notna(row["change_reason"]) else "",
        )
        for _, row in filtered_df.sort_values(["report_period", "code"]).iterrows()
    ]


def _fetch_express_frame(code: str, start_value: str, end_value: str) -> pd.DataFrame:
    df = query_frame("express", ts_code=stock_code_to_ts(code), start_date=start_value, end_date=end_value)
    if df.empty:
        return df
    work = df.copy()
    work["code"] = normalize_stock_code(code)
    work["report_period"] = work["end_date"].astype(str)
    work["announce_date"] = work["ann_date"].astype(str) if "ann_date" in work.columns else ""
    work["operating_profit"] = work["operate_profit"] if "operate_profit" in work.columns else None
    work["net_profit"] = work["n_income"] if "n_income" in work.columns else None
    for column in ("revenue", "total_profit", "eps", "roe"):
        if column not in work.columns:
            work[column] = None
    return work[["code", "report_period", "announce_date", "revenue", "operating_profit", "total_profit", "net_profit", "eps", "roe"]]


def get_express(code: str, report_period: str, start_period: str, end_period: str) -> list[ExpressItem]:
    actual_start, actual_end = normalize_period_range(report_period, start_period, end_period)
    cache_df = read_cached_ranges(
        ["stocks", "finance", "express"],
        {"code": normalize_stock_code(code)},
        "report_period",
        actual_start,
        actual_end,
        "quarter",
        lambda start_value, end_value: _fetch_express_frame(code, start_value, end_value),
    )
    filtered_df = filter_frame_by_date_range(cache_df, "report_period", actual_start, actual_end)
    if filtered_df.empty or "report_period" not in filtered_df.columns:
        return []
    return [
        ExpressItem(
            code=str(row["code"]),
            report_period=str(row["report_period"]),
            announce_date=str(row["announce_date"]),
            revenue=float(row["revenue"]) if pd.notna(row["revenue"]) else None,
            operating_profit=float(row["operating_profit"]) if pd.notna(row["operating_profit"]) else None,
            total_profit=float(row["total_profit"]) if pd.notna(row["total_profit"]) else None,
            net_profit=float(row["net_profit"]) if pd.notna(row["net_profit"]) else None,
            eps=float(row["eps"]) if pd.notna(row["eps"]) else None,
            roe=float(row["roe"]) if pd.notna(row["roe"]) else None,
        )
        for _, row in filtered_df.sort_values("report_period").iterrows()
    ]


def _fetch_forecast_frame(code: str, start_value: str, end_value: str) -> pd.DataFrame:
    df = query_frame("forecast", ts_code=stock_code_to_ts(code), start_date=start_value, end_date=end_value)
    if df.empty:
        return df
    work = df.copy()
    work["code"] = normalize_stock_code(code)
    work["report_period"] = work["end_date"].astype(str)
    work["forecast_type"] = work["type"] if "type" in work.columns else ""
    work["forecast_summary"] = work["summary"] if "summary" in work.columns else ""
    work["pct_chg_min"] = work["p_change_min"] if "p_change_min" in work.columns else None
    work["pct_chg_max"] = work["p_change_max"] if "p_change_max" in work.columns else None
    return work[["code", "report_period", "forecast_type", "forecast_summary", "net_profit_min", "net_profit_max", "pct_chg_min", "pct_chg_max"]]


def get_forecasts(code: str, report_period: str, start_period: str, end_period: str) -> list[ForecastItem]:
    actual_start, actual_end = normalize_period_range(report_period, start_period, end_period)
    cache_df = read_cached_ranges(
        ["stocks", "finance", "forecasts"],
        {"code": normalize_stock_code(code)},
        "report_period",
        actual_start,
        actual_end,
        "quarter",
        lambda start_value, end_value: _fetch_forecast_frame(code, start_value, end_value),
    )
    filtered_df = filter_frame_by_date_range(cache_df, "report_period", actual_start, actual_end)
    if filtered_df.empty or "report_period" not in filtered_df.columns:
        return []
    return [
        ForecastItem(
            code=str(row["code"]),
            report_period=str(row["report_period"]),
            forecast_type=str(row["forecast_type"]) if pd.notna(row["forecast_type"]) else "",
            forecast_summary=str(row["forecast_summary"]) if pd.notna(row["forecast_summary"]) else "",
            net_profit_min=float(row["net_profit_min"]) if pd.notna(row["net_profit_min"]) else None,
            net_profit_max=float(row["net_profit_max"]) if pd.notna(row["net_profit_max"]) else None,
            pct_chg_min=float(row["pct_chg_min"]) if pd.notna(row["pct_chg_min"]) else None,
            pct_chg_max=float(row["pct_chg_max"]) if pd.notna(row["pct_chg_max"]) else None,
        )
        for _, row in filtered_df.sort_values("report_period").iterrows()
    ]


def _fetch_main_business_frame(code: str, start_value: str, end_value: str, classification: str) -> pd.DataFrame:
    type_value = "D" if classification == "product" else "P"
    df = query_frame("fina_mainbz", ts_code=stock_code_to_ts(code), type=type_value, start_date=start_value, end_date=end_value)
    if df.empty:
        return df
    work = df.copy()
    work["code"] = normalize_stock_code(code)
    work["report_period"] = work["end_date"].astype(str)
    work["classification"] = classification
    work["segment_name"] = work["bz_item"] if "bz_item" in work.columns else ""
    work["revenue"] = work["bz_sales"] if "bz_sales" in work.columns else None
    work["cost"] = work["bz_cost"] if "bz_cost" in work.columns else None
    work["profit"] = work["bz_profit"] if "bz_profit" in work.columns else None
    work["revenue_ratio"] = work["bz_sales_ratio"] if "bz_sales_ratio" in work.columns else None
    return work[["code", "report_period", "classification", "segment_name", "revenue", "cost", "profit", "revenue_ratio"]]


def get_main_business(code: str, report_period: str, start_period: str, end_period: str, classification: str) -> list[MainBusinessItem]:
    actual_start, actual_end = normalize_period_range(report_period, start_period, end_period)
    actual_classification = classification or "industry"
    cache_df = read_cached_ranges(
        ["stocks", "finance", "main-business"],
        {"code": normalize_stock_code(code), "classification": actual_classification},
        "report_period",
        actual_start,
        actual_end,
        "quarter",
        lambda start_value, end_value: _fetch_main_business_frame(code, start_value, end_value, actual_classification),
    )
    filtered_df = filter_frame_by_date_range(cache_df, "report_period", actual_start, actual_end)
    return [
        MainBusinessItem(
            code=str(row["code"]),
            report_period=str(row["report_period"]),
            classification=str(row["classification"]),
            segment_name=str(row["segment_name"]),
            revenue=float(row["revenue"]) if pd.notna(row["revenue"]) else None,
            cost=float(row["cost"]) if pd.notna(row["cost"]) else None,
            profit=float(row["profit"]) if pd.notna(row["profit"]) else None,
            revenue_ratio=float(row["revenue_ratio"]) if pd.notna(row["revenue_ratio"]) else None,
        )
        for _, row in filtered_df.sort_values(["report_period", "segment_name"]).iterrows()
    ]


