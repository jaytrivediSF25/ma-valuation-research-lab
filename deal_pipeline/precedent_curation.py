from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class PrecedentCurationResult:
    summary: Dict[str, Any]
    curated_table: pd.DataFrame


def _f(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        return None


INDUSTRY_KEYWORDS = {
    "healthcare": ["pharma", "med", "health", "diagnostic", "biotech"],
    "software": ["software", "saas", "cloud", "platform", "cyber"],
    "industrials": ["industrial", "aero", "manufactur", "machinery", "supply"],
    "financials": ["bank", "insur", "finance", "asset", "capital"],
    "consumer": ["retail", "consumer", "brand", "food", "beverage"],
    "energy": ["energy", "oil", "gas", "renewable", "utility"],
}


def _industry_tag(text: str) -> str:
    lowered = (text or "").lower()
    for label, keys in INDUSTRY_KEYWORDS.items():
        if any(k in lowered for k in keys):
            return label
    return "other"


def _iqr_bounds(series: pd.Series):
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return None, None
    q1 = float(clean.quantile(0.25))
    q3 = float(clean.quantile(0.75))
    iqr = q3 - q1
    return q1 - 1.5 * iqr, q3 + 1.5 * iqr


def curate_precedent_transactions(target_row: pd.Series, precedents_table: pd.DataFrame) -> PrecedentCurationResult:
    if precedents_table.empty:
        return PrecedentCurationResult(
            summary={
                "raw_transaction_count": 0,
                "curated_transaction_count": 0,
                "outliers_removed": 0,
                "curated_median_ev_revenue": None,
                "curated_median_ev_ebitda": None,
            },
            curated_table=precedents_table,
        )

    curated = precedents_table.copy()
    target_sector = str(target_row.get("sector") or "").strip().lower()
    target_revenue = _f(target_row.get("revenue"))

    sector_scores: List[float] = []
    size_scores: List[float] = []
    industry_tags: List[str] = []
    for _, row in curated.iterrows():
        sector = str(row.get("sector") or "").strip().lower()
        sector_match = 1.0 if (target_sector and sector == target_sector) else (0.6 if target_sector and target_sector in sector else 0.4)
        row_rev = _f(row.get("revenue"))
        if target_revenue and row_rev and row_rev > 0:
            ratio = max(target_revenue, row_rev) / min(target_revenue, row_rev)
            size_score = max(0.0, 1.0 - ((ratio - 1.0) / 5.0))
        else:
            size_score = 0.5
        name_blob = " ".join([str(row.get("target_company") or ""), str(row.get("acquirer") or ""), str(row.get("sector") or "")])
        industry_tags.append(_industry_tag(name_blob))
        sector_scores.append(sector_match)
        size_scores.append(size_score)

    curated["sector_match_score"] = sector_scores
    curated["size_similarity_score"] = size_scores
    curated["relevance_score"] = 0.6 * curated["sector_match_score"] + 0.4 * curated["size_similarity_score"]
    curated["industry_tag"] = industry_tags

    evr_low, evr_high = _iqr_bounds(curated.get("ev_revenue", pd.Series(dtype=float)))
    eve_low, eve_high = _iqr_bounds(curated.get("ev_ebitda", pd.Series(dtype=float)))
    curated["outlier_ev_revenue"] = False
    curated["outlier_ev_ebitda"] = False
    if evr_low is not None and evr_high is not None:
        s = pd.to_numeric(curated["ev_revenue"], errors="coerce")
        curated["outlier_ev_revenue"] = (s < evr_low) | (s > evr_high)
    if eve_low is not None and eve_high is not None:
        s = pd.to_numeric(curated["ev_ebitda"], errors="coerce")
        curated["outlier_ev_ebitda"] = (s < eve_low) | (s > eve_high)
    curated["is_outlier"] = curated["outlier_ev_revenue"] | curated["outlier_ev_ebitda"]

    curated["keep_flag"] = (~curated["is_outlier"]) & (curated["relevance_score"] >= 0.35)
    kept = curated[curated["keep_flag"]].copy()
    if kept.empty:
        kept = curated[~curated["is_outlier"]].copy()

    summary = {
        "raw_transaction_count": int(len(curated)),
        "curated_transaction_count": int(len(kept)),
        "outliers_removed": int(curated["is_outlier"].sum()),
        "curated_median_ev_revenue": _f(pd.to_numeric(kept["ev_revenue"], errors="coerce").median()),
        "curated_median_ev_ebitda": _f(pd.to_numeric(kept["ev_ebitda"], errors="coerce").median()),
    }
    return PrecedentCurationResult(summary=summary, curated_table=kept.sort_values("relevance_score", ascending=False))
