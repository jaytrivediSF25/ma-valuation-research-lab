from dataclasses import dataclass
from typing import Dict, Optional

import numpy as np
import pandas as pd

try:
    import duckdb
except Exception:  # pragma: no cover
    duckdb = None


@dataclass
class ComparableAnalysisResult:
    summary: Dict[str, Optional[float]]
    peer_table: pd.DataFrame


@dataclass
class PrecedentAnalysisResult:
    summary: Dict[str, Optional[float]]
    precedent_table: pd.DataFrame


def _percentile_rank(series: pd.Series, value: Optional[float]) -> Optional[float]:
    if value is None or pd.isna(value):
        return None
    clean = series.dropna()
    if clean.empty:
        return None
    return float((clean <= value).mean())


def _median_with_duckdb(df: pd.DataFrame, column: str) -> Optional[float]:
    clean = df[[column]].dropna()
    if clean.empty:
        return None
    if duckdb is None:
        return float(clean[column].median())
    query = f"SELECT median({column}) as m FROM clean"
    result = duckdb.query(query).to_df()
    val = result["m"].iloc[0]
    if pd.isna(val):
        return None
    return float(val)


def run_comparable_analysis(
    target_row: pd.Series,
    company_metrics: pd.DataFrame,
    peers_input: pd.DataFrame,
) -> ComparableAnalysisResult:
    peers = peers_input.copy() if not peers_input.empty else pd.DataFrame()
    if peers.empty:
        peers = company_metrics.copy()
        peers = peers[peers["ticker"] != target_row["ticker"]]
    else:
        peers["ticker"] = peers["ticker"].astype(str).str.upper().str.strip()
        peers = peers.merge(
            company_metrics[
                [
                    "ticker",
                    "company_name",
                    "company_name_std",
                    "sector",
                    "revenue",
                    "ebitda",
                    "enterprise_value",
                    "ev_revenue",
                    "ev_ebitda",
                    "ebitda_margin",
                    "revenue_growth_yoy",
                ]
            ],
            on="ticker",
            how="left",
            suffixes=("_peer", ""),
        )
        peers["company_name"] = peers["company_name"].combine_first(peers.get("company_name_peer"))
        peers["revenue"] = peers["revenue"].combine_first(peers.get("revenue_peer"))
        peers["ebitda"] = peers["ebitda"].combine_first(peers.get("ebitda_peer"))
        peers["enterprise_value"] = peers["enterprise_value"].combine_first(peers.get("enterprise_value_peer"))
        peers["ev_revenue"] = peers["ev_revenue"].combine_first(peers["enterprise_value"] / peers["revenue"])
        peers["ev_ebitda"] = peers["ev_ebitda"].combine_first(peers["enterprise_value"] / peers["ebitda"])
        peers["ebitda_margin"] = peers["ebitda_margin"].combine_first(peers["ebitda"] / peers["revenue"])
        peers = peers[peers["ticker"] != target_row["ticker"]]

    if pd.notna(target_row.get("sector")) and str(target_row.get("sector")).strip():
        sector_filtered = peers[peers["sector"] == target_row["sector"]]
        if len(sector_filtered) >= 5:
            peers = sector_filtered

    target_revenue = target_row.get("revenue")
    if pd.notna(target_revenue) and target_revenue and target_revenue > 0:
        bounded = peers[(peers["revenue"] >= 0.25 * target_revenue) & (peers["revenue"] <= 4.0 * target_revenue)]
        if len(bounded) >= 5:
            peers = bounded

    peers = peers.dropna(subset=["ev_revenue", "ev_ebitda"], how="all")
    if len(peers) > 25:
        peers = peers.sort_values("enterprise_value", ascending=False).head(25)

    median_ev_rev = _median_with_duckdb(peers, "ev_revenue")
    median_ev_ebitda = _median_with_duckdb(peers, "ev_ebitda")

    summary = {
        "peer_count": int(len(peers)),
        "peer_median_ev_revenue": median_ev_rev,
        "peer_median_ev_ebitda": median_ev_ebitda,
        "target_ev_revenue": float(target_row["ev_revenue"]) if pd.notna(target_row.get("ev_revenue")) else None,
        "target_ev_ebitda": float(target_row["ev_ebitda"]) if pd.notna(target_row.get("ev_ebitda")) else None,
        "percentile_ev_revenue": _percentile_rank(peers["ev_revenue"], target_row.get("ev_revenue")),
        "percentile_ev_ebitda": _percentile_rank(peers["ev_ebitda"], target_row.get("ev_ebitda")),
    }

    cols = [
        "ticker",
        "company_name",
        "sector",
        "revenue",
        "ebitda",
        "enterprise_value",
        "ev_revenue",
        "ev_ebitda",
        "revenue_growth_yoy",
        "ebitda_margin",
    ]
    peer_table = peers[[c for c in cols if c in peers.columns]].sort_values("enterprise_value", ascending=False)
    return ComparableAnalysisResult(summary=summary, peer_table=peer_table)


def _prepare_precedents(
    precedents_input: pd.DataFrame,
    filings: pd.DataFrame,
    company_metrics: pd.DataFrame,
) -> pd.DataFrame:
    if not precedents_input.empty:
        precedents = precedents_input.copy()
        precedents["ev_revenue"] = precedents["ev_revenue"].fillna(precedents["enterprise_value"] / precedents["revenue"])
        precedents["ev_ebitda"] = precedents["ev_ebitda"].fillna(precedents["enterprise_value"] / precedents["ebitda"])
        return precedents

    if filings.empty:
        return pd.DataFrame()

    proxy = filings[filings["form"].isin(["S-4", "8-K"])].copy()
    if proxy.empty:
        return pd.DataFrame()

    proxy = proxy.rename(
        columns={
            "company_name": "target_company",
            "filing_date": "announcement_date",
        }
    )
    proxy["target_company_std"] = proxy["target_company"].str.upper()
    proxy = proxy.merge(
        company_metrics[
            [
                "ticker",
                "company_name",
                "company_name_std",
                "sector",
                "revenue",
                "ebitda",
                "enterprise_value",
                "ev_revenue",
                "ev_ebitda",
            ]
        ],
        on="ticker",
        how="left",
        suffixes=("", "_metrics"),
    )
    if "company_name" in proxy.columns:
        proxy["target_company"] = proxy["target_company"].combine_first(proxy["company_name"])
    if "sector_metrics" in proxy.columns:
        proxy["sector"] = proxy["sector"].combine_first(proxy["sector_metrics"])
    proxy["announcement_date"] = pd.to_datetime(proxy["announcement_date"], errors="coerce")
    proxy["close_date"] = pd.NaT
    proxy["acquirer"] = None
    proxy["source_file"] = "proxy_from_sec_filings"
    keep_cols = [
        "target_company",
        "target_company_std",
        "acquirer",
        "announcement_date",
        "close_date",
        "sector",
        "revenue",
        "ebitda",
        "enterprise_value",
        "ev_revenue",
        "ev_ebitda",
        "source_file",
    ]
    proxy = proxy[keep_cols].drop_duplicates(
        subset=["target_company_std", "announcement_date", "enterprise_value"],
        keep="last",
    )
    return proxy


def run_precedent_analysis(
    target_row: pd.Series,
    precedents_input: pd.DataFrame,
    filings: pd.DataFrame,
    company_metrics: pd.DataFrame,
) -> PrecedentAnalysisResult:
    precedents = _prepare_precedents(precedents_input, filings, company_metrics)
    if precedents.empty:
        summary = {
            "transaction_count": 0,
            "median_ev_revenue": None,
            "p25_ev_revenue": None,
            "p75_ev_revenue": None,
            "median_ev_ebitda": None,
            "p25_ev_ebitda": None,
            "p75_ev_ebitda": None,
            "valuation_range_low": None,
            "valuation_range_high": None,
        }
        return PrecedentAnalysisResult(summary=summary, precedent_table=precedents)

    if pd.notna(target_row.get("sector")) and str(target_row.get("sector")).strip():
        sector_filtered = precedents[precedents["sector"] == target_row["sector"]]
        if len(sector_filtered) >= 5:
            precedents = sector_filtered

    target_revenue = target_row.get("revenue")
    if pd.notna(target_revenue) and target_revenue and target_revenue > 0 and "revenue" in precedents.columns:
        bounded = precedents[(precedents["revenue"] >= 0.2 * target_revenue) & (precedents["revenue"] <= 5.0 * target_revenue)]
        if len(bounded) >= 5:
            precedents = bounded

    date_col = "announcement_date" if "announcement_date" in precedents.columns else None
    if date_col:
        dates = pd.to_datetime(precedents[date_col], errors="coerce")
        if dates.notna().any():
            max_date = dates.max()
            recent = precedents[dates >= (max_date - pd.Timedelta(days=3650))]
            if len(recent) >= 5:
                precedents = recent

    ev_rev = precedents["ev_revenue"].dropna()
    ev_ebitda = precedents["ev_ebitda"].dropna()

    median_ev_rev = float(ev_rev.median()) if not ev_rev.empty else None
    p25_ev_rev = float(ev_rev.quantile(0.25)) if not ev_rev.empty else None
    p75_ev_rev = float(ev_rev.quantile(0.75)) if not ev_rev.empty else None

    median_ev_ebitda = float(ev_ebitda.median()) if not ev_ebitda.empty else None
    p25_ev_ebitda = float(ev_ebitda.quantile(0.25)) if not ev_ebitda.empty else None
    p75_ev_ebitda = float(ev_ebitda.quantile(0.75)) if not ev_ebitda.empty else None

    valuation_candidates_low = []
    valuation_candidates_high = []
    if p25_ev_rev is not None and pd.notna(target_row.get("revenue")):
        valuation_candidates_low.append(p25_ev_rev * float(target_row["revenue"]))
        valuation_candidates_high.append((p75_ev_rev or p25_ev_rev) * float(target_row["revenue"]))
    if p25_ev_ebitda is not None and pd.notna(target_row.get("ebitda")):
        valuation_candidates_low.append(p25_ev_ebitda * float(target_row["ebitda"]))
        valuation_candidates_high.append((p75_ev_ebitda or p25_ev_ebitda) * float(target_row["ebitda"]))

    valuation_range_low = float(np.nanmedian(valuation_candidates_low)) if valuation_candidates_low else None
    valuation_range_high = float(np.nanmedian(valuation_candidates_high)) if valuation_candidates_high else None

    summary = {
        "transaction_count": int(len(precedents)),
        "median_ev_revenue": median_ev_rev,
        "p25_ev_revenue": p25_ev_rev,
        "p75_ev_revenue": p75_ev_rev,
        "median_ev_ebitda": median_ev_ebitda,
        "p25_ev_ebitda": p25_ev_ebitda,
        "p75_ev_ebitda": p75_ev_ebitda,
        "valuation_range_low": valuation_range_low,
        "valuation_range_high": valuation_range_high,
    }
    return PrecedentAnalysisResult(summary=summary, precedent_table=precedents)
