from __future__ import annotations


P0_REQUIRED_PROVIDER_BY_CAPABILITY = {
    "stocks.profile.company": "eastmoney_official",
    "stocks.corporate_actions.share_changes": "eastmoney_official",
    "stocks.finance.report_disclosures": "cninfo_evidence",
    "stocks.finance.statements": "eastmoney_official",
}

P0_CACHE_TOTAL_BYTES = 64 * 1024 * 1024
P0_CACHE_BYTES_BY_CAPABILITY = {
    "stocks.profile.company": 4 * 1024 * 1024,
    "stocks.corporate_actions.share_changes": 8 * 1024 * 1024,
    "stocks.finance.report_disclosures": 16 * 1024 * 1024,
    "stocks.finance.statements": 32 * 1024 * 1024,
}
P0_CACHE_TTL_SECONDS_BY_CAPABILITY = {
    "stocks.profile.company": 365 * 86400,
    "stocks.corporate_actions.share_changes": 365 * 86400,
    # 正式披露只作短期查询缓存，长期版本由 STS 保存；不能沿用相邻日期能力的 180 天。
    "stocks.finance.report_disclosures": 30 * 86400,
    "stocks.finance.statements": 365 * 86400,
}

P0_SOURCE_BY_CAPABILITY = {
    "stocks.profile.company": "eastmoney_hsf10_company_survey",
    "stocks.corporate_actions.share_changes": "eastmoney_hsf10_capital_structure",
    "stocks.finance.report_disclosures": "news_crawler_cninfo_formal_report_evidence",
    "stocks.finance.statements": "eastmoney_datacenter_financial_statements",
}
