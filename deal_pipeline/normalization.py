from dataclasses import dataclass
from typing import Dict, List

import pandas as pd

from .ingestion import IngestedData
from .utils import (
    coerce_date_series,
    coerce_numeric_series,
    normalize_key,
    standardize_company_name,
)

FX_TO_USD = {
    "USD": 1.0,
    "US$": 1.0,
    "$": 1.0,
    "EUR": 1.08,
    "GBP": 1.27,
    "CAD": 0.74,
    "JPY": 0.0067,
}

BASE_FINANCIAL_SCHEMA = {
    "company_name": ["company_name", "company", "name", "entity_name", "issuer", "target_company"],
    "ticker": ["ticker", "symbol", "stock_ticker"],
    "cik": ["cik", "cik_id"],
    "sector": ["sector", "industry", "gics_sector", "vertical"],
    "date": ["date", "as_of_date", "period_end", "fiscal_period_end", "filed", "filing_date"],
    "revenue": ["revenue", "revenues", "sales", "total_revenue"],
    "ebitda": ["ebitda", "adj_ebitda", "adjusted_ebitda"],
    "enterprise_value": ["enterprise_value", "ev", "deal_value", "transaction_value"],
    "market_cap": ["market_cap", "market_value", "equity_value", "entity_public_float", "public_float"],
    "total_debt": ["total_debt", "debt", "net_debt", "long_term_debt"],
    "cash": ["cash", "cash_equivalents", "cash_and_equivalents"],
    "currency": ["currency", "ccy"],
    "source_file": ["source_file"],
}

PRECEDENT_SCHEMA = {
    "target_company": ["target_company", "target", "company_name", "company", "issuer"],
    "acquirer": ["acquirer", "buyer", "acquiring_company"],
    "announcement_date": ["announcement_date", "announce_date", "filing_date", "date"],
    "close_date": ["close_date", "closing_date"],
    "sector": ["sector", "industry", "gics_sector"],
    "revenue": ["revenue", "target_revenue", "revenues"],
    "ebitda": ["ebitda", "target_ebitda", "adj_ebitda"],
    "enterprise_value": ["enterprise_value", "ev", "transaction_value", "deal_value"],
    "currency": ["currency", "ccy"],
    "source_file": ["source_file"],
}


@dataclass
class NormalizedData:
    companies: pd.DataFrame
    filings: pd.DataFrame
    companyfacts: pd.DataFrame
    financials: pd.DataFrame
    peers: pd.DataFrame
    precedents: pd.DataFrame
    raw_data_export: pd.DataFrame


def _resolve_schema(df: pd.DataFrame, schema: Dict[str, List[str]]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=list(schema.keys()))

    key_to_column = {normalize_key(col): col for col in df.columns}
    normalized = pd.DataFrame(index=df.index)
    for target_col, aliases in schema.items():
        source_col = None
        for alias in aliases:
            match = key_to_column.get(normalize_key(alias))
            if match:
                source_col = match
                break
        normalized[target_col] = df[source_col] if source_col else None
    return normalized


def _normalize_currency(df: pd.DataFrame, value_columns: List[str]) -> pd.DataFrame:
    if df.empty:
        return df
    normalized = df.copy()
    normalized["currency"] = normalized["currency"].fillna("USD").astype(str).str.upper()
    normalized["fx_to_usd"] = normalized["currency"].map(FX_TO_USD).fillna(1.0)
    for col in value_columns:
        if col in normalized.columns:
            normalized[col] = normalized[col] * normalized["fx_to_usd"]
    normalized["currency"] = "USD"
    return normalized


def _normalize_companies(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["cik", "ticker", "title", "company_name_std"])
    companies = df.copy()
    companies["cik"] = companies.get("cik", "").astype(str).str.zfill(10)
    companies["ticker"] = companies.get("ticker", "").astype(str).str.upper().str.strip()
    title_col = "title" if "title" in companies.columns else "company_name"
    companies["title"] = companies[title_col].astype(str).str.strip()
    companies["company_name_std"] = companies["title"].map(standardize_company_name)
    companies = companies.drop_duplicates(subset=["cik", "ticker"], keep="last")
    return companies


def _normalize_filings(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    filings = df.copy()
    filings["ticker"] = filings.get("ticker", "").astype(str).str.upper().str.strip()
    filings["cik"] = filings.get("cik", "").astype(str).str.zfill(10)
    filings["company_name"] = filings.get("company_name", "").astype(str).str.strip()
    filings["company_name_std"] = filings["company_name"].map(standardize_company_name)
    filings["form"] = filings.get("form", "").astype(str).str.upper().str.strip()
    if "filing_date" in filings.columns:
        filings["filing_date"] = coerce_date_series(filings["filing_date"])
    filings = filings.drop_duplicates(
        subset=["cik", "accession_number", "form", "filing_date", "primary_document"],
        keep="last",
    )
    return filings


def _normalize_companyfacts(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    facts = df.copy()
    facts["cik"] = facts.get("cik", "").astype(str).str.zfill(10)
    facts["ticker"] = facts.get("ticker", "").astype(str).str.upper().str.strip()
    facts["entity_name"] = facts.get("entity_name", "").astype(str).str.strip()
    facts["entity_name_std"] = facts["entity_name"].map(standardize_company_name)
    facts["concept"] = facts.get("concept", "").astype(str).str.strip()
    facts["form"] = facts.get("form", "").astype(str).str.upper().str.strip()
    if "val" in facts.columns:
        facts["val"] = coerce_numeric_series(facts["val"])
    if "end" in facts.columns:
        facts["end"] = coerce_date_series(facts["end"])
    if "filed" in facts.columns:
        facts["filed"] = coerce_date_series(facts["filed"])
    facts = facts.dropna(subset=["concept", "val"], how="any")
    facts = facts.drop_duplicates(
        subset=["cik", "ticker", "concept", "end", "filed", "val", "form"],
        keep="last",
    )
    return facts


def _normalize_external_financial_like(df: pd.DataFrame) -> pd.DataFrame:
    resolved = _resolve_schema(df, BASE_FINANCIAL_SCHEMA)
    if resolved.empty:
        return resolved
    normalized = resolved.copy()
    normalized["company_name"] = normalized["company_name"].astype(str).str.strip()
    normalized["company_name_std"] = normalized["company_name"].map(standardize_company_name)
    normalized["ticker"] = normalized["ticker"].astype(str).str.upper().str.strip()
    normalized["cik"] = normalized["cik"].fillna("").astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(10)
    normalized["sector"] = normalized["sector"].astype(str).str.strip()
    normalized["date"] = coerce_date_series(normalized["date"])
    for col in ["revenue", "ebitda", "enterprise_value", "market_cap", "total_debt", "cash"]:
        normalized[col] = coerce_numeric_series(normalized[col])
    normalized = _normalize_currency(
        normalized,
        ["revenue", "ebitda", "enterprise_value", "market_cap", "total_debt", "cash"],
    )
    normalized = normalized.drop_duplicates(
        subset=["ticker", "company_name_std", "date", "source_file"],
        keep="last",
    )
    return normalized


def _normalize_precedents(df: pd.DataFrame) -> pd.DataFrame:
    resolved = _resolve_schema(df, PRECEDENT_SCHEMA)
    if resolved.empty:
        return resolved
    normalized = resolved.copy()
    normalized["target_company"] = normalized["target_company"].astype(str).str.strip()
    normalized["target_company_std"] = normalized["target_company"].map(standardize_company_name)
    normalized["acquirer"] = normalized["acquirer"].astype(str).str.strip()
    normalized["sector"] = normalized["sector"].astype(str).str.strip()
    normalized["announcement_date"] = coerce_date_series(normalized["announcement_date"])
    normalized["close_date"] = coerce_date_series(normalized["close_date"])
    for col in ["revenue", "ebitda", "enterprise_value"]:
        normalized[col] = coerce_numeric_series(normalized[col])
    normalized = _normalize_currency(normalized, ["revenue", "ebitda", "enterprise_value"])
    normalized["ev_revenue"] = normalized["enterprise_value"] / normalized["revenue"]
    normalized["ev_ebitda"] = normalized["enterprise_value"] / normalized["ebitda"]
    normalized = normalized.drop_duplicates(
        subset=["target_company_std", "acquirer", "announcement_date", "enterprise_value"],
        keep="last",
    )
    return normalized


def _build_raw_data_export(
    companies: pd.DataFrame,
    filings: pd.DataFrame,
    financials: pd.DataFrame,
    precedents: pd.DataFrame,
) -> pd.DataFrame:
    frames = []
    for name, frame in {
        "companies": companies.head(5000),
        "filings": filings.head(15000),
        "financials": financials.head(20000),
        "precedents": precedents.head(20000),
    }.items():
        if frame.empty:
            continue
        temp = frame.copy()
        temp.insert(0, "dataset", name)
        frames.append(temp)
    if not frames:
        return pd.DataFrame(columns=["dataset"])
    return pd.concat(frames, ignore_index=True, sort=False)


def normalize_data(ingested: IngestedData) -> NormalizedData:
    companies = _normalize_companies(ingested.companies)
    filings = _normalize_filings(ingested.filings)
    companyfacts = _normalize_companyfacts(ingested.companyfacts)
    financials = _normalize_external_financial_like(ingested.external_financials)
    peers = _normalize_external_financial_like(ingested.external_peers)
    precedents = _normalize_precedents(ingested.external_precedents)

    raw_data_export = _build_raw_data_export(companies, filings, financials, precedents)

    return NormalizedData(
        companies=companies,
        filings=filings,
        companyfacts=companyfacts,
        financials=financials,
        peers=peers,
        precedents=precedents,
        raw_data_export=raw_data_export,
    )
