from dataclasses import dataclass
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from .config import PipelineConfig
from .normalization import NormalizedData
from .utils import safe_divide, standardize_company_name

REVENUE_CONCEPT_PRIORITY = [
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "Revenues",
    "SalesRevenueNet",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    "SalesRevenueGoodsNet",
]
EBITDA_CONCEPTS = ["EarningsBeforeInterestTaxesDepreciationAndAmortization"]
OPERATING_INCOME_CONCEPTS = ["OperatingIncomeLoss"]
DEPRECIATION_CONCEPTS = [
    "DepreciationDepletionAndAmortization",
    "DepreciationAndAmortization",
    "Depreciation",
    "DepreciationAmortizationAndAccretionNet",
]
MARKET_CAP_CONCEPTS = ["EntityPublicFloat"]
DEBT_CONCEPTS = ["LongTermDebtNoncurrent", "LongTermDebtCurrent", "ShortTermBorrowings", "LongTermDebt", "DebtCurrent"]
CASH_CONCEPTS = ["CashAndCashEquivalentsAtCarryingValue", "CashCashEquivalentsAndShortTermInvestments"]
SHARES_OUTSTANDING_CONCEPTS = ["EntityCommonStockSharesOutstanding", "CommonStockSharesOutstanding"]
INTEREST_EXPENSE_CONCEPTS = ["InterestExpense", "InterestExpenseDebt"]


@dataclass
class FeatureOutput:
    company_metrics: pd.DataFrame


def _latest_row(df: pd.DataFrame, concept_list: List[str]) -> Optional[pd.Series]:
    subset = df[df["concept"].isin(concept_list)].dropna(subset=["val", "end"])
    if subset.empty:
        return None
    subset = subset.sort_values(["end", "filed"], ascending=[True, True])
    return subset.iloc[-1]


def _previous_year_value(df: pd.DataFrame, concept: str, latest_end: pd.Timestamp) -> Optional[float]:
    if pd.isna(latest_end):
        return None
    threshold = latest_end - pd.Timedelta(days=330)
    subset = df[(df["concept"] == concept) & (df["end"] <= threshold)].dropna(subset=["val", "end"])
    if subset.empty:
        return None
    subset = subset.sort_values("end")
    return float(subset.iloc[-1]["val"])


def _sum_latest_values(df: pd.DataFrame, concepts: List[str]) -> Optional[float]:
    total = 0.0
    found = False
    for concept in concepts:
        row = _latest_row(df, [concept])
        if row is None:
            continue
        val = row.get("val")
        if pd.notna(val):
            total += float(val)
            found = True
    return total if found else None


def _build_sec_metrics_from_companyfacts(companyfacts: pd.DataFrame) -> pd.DataFrame:
    if companyfacts.empty:
        return pd.DataFrame(
            columns=[
                "cik",
                "ticker",
                "company_name",
                "company_name_std",
                "sector",
                "as_of_date",
                "revenue",
                "revenue_prior",
                "revenue_growth_yoy",
                "ebitda",
                "ebitda_margin",
                "market_cap",
                "total_debt",
                "cash",
                "shares_outstanding",
                "interest_expense",
                "enterprise_value",
                "ev_revenue",
                "ev_ebitda",
                "source",
            ]
        )

    rows: List[Dict] = []
    grouped = companyfacts.groupby(["cik", "ticker"], dropna=False)
    for (cik, ticker), group in grouped:
        group = group.sort_values(["end", "filed"])
        company_name = (
            group["entity_name"].dropna().iloc[-1]
            if "entity_name" in group.columns and not group["entity_name"].dropna().empty
            else None
        )
        company_name_std = standardize_company_name(company_name)

        revenue_row = None
        for concept in REVENUE_CONCEPT_PRIORITY:
            revenue_row = _latest_row(group, [concept])
            if revenue_row is not None:
                break
        if revenue_row is None:
            continue

        revenue = float(revenue_row["val"])
        latest_end = revenue_row["end"]
        revenue_prior = _previous_year_value(group, str(revenue_row["concept"]), latest_end)
        revenue_growth_yoy = safe_divide(revenue - revenue_prior, revenue_prior) if revenue_prior else None

        ebitda_row = _latest_row(group, EBITDA_CONCEPTS)
        if ebitda_row is not None:
            ebitda = float(ebitda_row["val"])
        else:
            op_row = _latest_row(group, OPERATING_INCOME_CONCEPTS)
            dep_row = _latest_row(group, DEPRECIATION_CONCEPTS)
            if op_row is not None and dep_row is not None:
                ebitda = float(op_row["val"]) + float(dep_row["val"])
            else:
                ebitda = None

        market_cap_row = _latest_row(group, MARKET_CAP_CONCEPTS)
        market_cap = float(market_cap_row["val"]) if market_cap_row is not None else None

        total_debt = _sum_latest_values(group, DEBT_CONCEPTS)
        cash = _sum_latest_values(group, CASH_CONCEPTS)
        shares_row = _latest_row(group, SHARES_OUTSTANDING_CONCEPTS)
        shares_outstanding = float(shares_row["val"]) if shares_row is not None else None
        interest_row = _latest_row(group, INTEREST_EXPENSE_CONCEPTS)
        interest_expense = float(interest_row["val"]) if interest_row is not None else None

        enterprise_value = None
        if market_cap is not None:
            enterprise_value = market_cap + (total_debt or 0.0) - (cash or 0.0)

        ev_revenue = safe_divide(enterprise_value, revenue)
        ev_ebitda = safe_divide(enterprise_value, ebitda)
        ebitda_margin = safe_divide(ebitda, revenue)

        rows.append(
            {
                "cik": str(cik).zfill(10),
                "ticker": str(ticker).upper().strip(),
                "company_name": company_name,
                "company_name_std": company_name_std,
                "sector": None,
                "as_of_date": latest_end,
                "revenue": revenue,
                "revenue_prior": revenue_prior,
                "revenue_growth_yoy": revenue_growth_yoy,
                "ebitda": ebitda,
                "ebitda_margin": ebitda_margin,
                "market_cap": market_cap,
                "total_debt": total_debt,
                "cash": cash,
                "shares_outstanding": shares_outstanding,
                "interest_expense": interest_expense,
                "enterprise_value": enterprise_value,
                "ev_revenue": ev_revenue,
                "ev_ebitda": ev_ebitda,
                "source": "sec_companyfacts",
            }
        )

    return pd.DataFrame(rows)


def _latest_external_financials(financials: pd.DataFrame) -> pd.DataFrame:
    if financials.empty:
        return pd.DataFrame()
    ext = financials.copy()
    ext["ticker"] = ext["ticker"].astype(str).str.upper().str.strip()
    ext = ext.sort_values(["date", "source_file"], na_position="last")
    ext = ext.drop_duplicates(subset=["ticker", "company_name_std"], keep="last")
    ext["as_of_date"] = ext["date"]
    ext["revenue_prior"] = np.nan
    ext["revenue_growth_yoy"] = np.nan
    ext["enterprise_value"] = ext["enterprise_value"].fillna(
        ext["market_cap"].fillna(0.0) + ext["total_debt"].fillna(0.0) - ext["cash"].fillna(0.0)
    )
    ext["ev_revenue"] = ext["enterprise_value"] / ext["revenue"]
    ext["ev_ebitda"] = ext["enterprise_value"] / ext["ebitda"]
    ext["ebitda_margin"] = ext["ebitda"] / ext["revenue"]
    ext["source"] = "external_financials"
    keep_cols = [
        "cik",
        "ticker",
        "company_name",
        "company_name_std",
        "sector",
        "as_of_date",
        "revenue",
        "revenue_prior",
        "revenue_growth_yoy",
        "ebitda",
        "ebitda_margin",
        "market_cap",
        "total_debt",
        "cash",
        "shares_outstanding",
        "interest_expense",
        "enterprise_value",
        "ev_revenue",
        "ev_ebitda",
        "source",
    ]
    return ext[keep_cols]


def _merge_sec_and_external(sec_metrics: pd.DataFrame, external_metrics: pd.DataFrame) -> pd.DataFrame:
    if sec_metrics.empty and external_metrics.empty:
        return pd.DataFrame()
    if sec_metrics.empty:
        return external_metrics
    if external_metrics.empty:
        return sec_metrics

    sec = sec_metrics.copy()
    ext = external_metrics.copy()
    sec["entity_key"] = sec["ticker"].where(sec["ticker"].ne(""), sec["company_name_std"])
    ext["entity_key"] = ext["ticker"].where(ext["ticker"].ne(""), ext["company_name_std"])

    sec = sec.set_index("entity_key")
    ext = ext.set_index("entity_key")
    all_index = sec.index.union(ext.index)
    merged = pd.DataFrame(index=all_index)

    ordered_cols = [
        "cik",
        "ticker",
        "company_name",
        "company_name_std",
        "sector",
        "as_of_date",
        "revenue",
        "revenue_prior",
        "revenue_growth_yoy",
        "ebitda",
        "ebitda_margin",
        "market_cap",
        "total_debt",
        "cash",
        "shares_outstanding",
        "interest_expense",
        "enterprise_value",
        "ev_revenue",
        "ev_ebitda",
        "source",
    ]
    for col in ordered_cols:
        merged[col] = sec[col].reindex(all_index) if col in sec.columns else np.nan
        if col in ext.columns:
            merged[col] = merged[col].combine_first(ext[col].reindex(all_index))
    merged["source"] = merged["source"].fillna("sec_companyfacts")
    merged = merged.reset_index(drop=True)
    return merged


def _fill_missing_company_labels(metrics: pd.DataFrame, companies: pd.DataFrame) -> pd.DataFrame:
    if metrics.empty or companies.empty:
        return metrics
    filled = metrics.copy()
    ref = companies[["cik", "ticker", "title"]].copy()
    ref = ref.rename(columns={"title": "company_name_ref"})
    filled = filled.merge(ref, on=["cik", "ticker"], how="left")
    filled["company_name"] = filled["company_name"].combine_first(filled["company_name_ref"])
    if "company_name_std" not in filled.columns:
        filled["company_name_std"] = pd.NA
    filled["company_name_std"] = filled["company_name_std"].combine_first(
        filled["company_name_ref"].map(standardize_company_name)
    )
    filled = filled.drop(columns=["company_name_ref"])
    return filled


def select_target_company(company_metrics: pd.DataFrame, config: PipelineConfig) -> pd.Series:
    if company_metrics.empty:
        raise ValueError("No company metrics are available to select a target company.")

    target = company_metrics.copy()
    if config.target_ticker:
        matched = target[target["ticker"].str.upper() == config.target_ticker.upper()]
        if not matched.empty:
            return matched.iloc[0]
    if config.target_cik:
        matched = target[target["cik"] == str(config.target_cik).zfill(10)]
        if not matched.empty:
            return matched.iloc[0]
    if config.target_company:
        needle = standardize_company_name(config.target_company)
        matched = target[target["company_name_std"] == needle]
        if not matched.empty:
            return matched.iloc[0]

    ranked = target.sort_values(
        by=["enterprise_value", "revenue"],
        ascending=[False, False],
        na_position="last",
    )
    return ranked.iloc[0]


def engineer_features(normalized: NormalizedData) -> FeatureOutput:
    sec_metrics = _build_sec_metrics_from_companyfacts(normalized.companyfacts)
    ext_metrics = _latest_external_financials(normalized.financials)
    merged = _merge_sec_and_external(sec_metrics, ext_metrics)
    merged = _fill_missing_company_labels(merged, normalized.companies)

    if merged.empty:
        return FeatureOutput(company_metrics=merged)

    for col in [
        "revenue",
        "ebitda",
        "enterprise_value",
        "market_cap",
        "total_debt",
        "cash",
        "shares_outstanding",
        "interest_expense",
    ]:
        merged[col] = pd.to_numeric(merged[col], errors="coerce")
    merged["ev_revenue"] = merged["ev_revenue"].fillna(merged["enterprise_value"] / merged["revenue"])
    merged["ev_ebitda"] = merged["ev_ebitda"].fillna(merged["enterprise_value"] / merged["ebitda"])
    merged["ebitda_margin"] = merged["ebitda_margin"].fillna(merged["ebitda"] / merged["revenue"])
    merged["net_debt"] = merged["total_debt"].fillna(0.0) - merged["cash"].fillna(0.0)
    merged["implied_share_price_current"] = merged["market_cap"] / merged["shares_outstanding"]
    merged["as_of_date"] = pd.to_datetime(merged["as_of_date"], errors="coerce")
    merged = merged.drop_duplicates(subset=["cik", "ticker"], keep="last")
    return FeatureOutput(company_metrics=merged)
