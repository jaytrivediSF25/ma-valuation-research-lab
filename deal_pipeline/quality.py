from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd

from .config import PipelineConfig


@dataclass
class DataQualityResult:
    checks: Dict[str, Any]
    issues: List[str]
    score: float
    check_table: pd.DataFrame


def _pct_non_null(df: pd.DataFrame, column: str) -> float:
    if column not in df.columns or df.empty:
        return 0.0
    return float(df[column].notna().mean())


def _as_int(value: Optional[float]) -> int:
    if value is None or pd.isna(value):
        return 0
    return int(value)


def evaluate_data_quality(
    company_metrics: pd.DataFrame,
    comps_summary: Dict[str, Any],
    precedents_summary: Dict[str, Any],
    config: PipelineConfig,
) -> DataQualityResult:
    checks: Dict[str, Any] = {
        "company_metrics_row_count": int(len(company_metrics)),
        "coverage_revenue": _pct_non_null(company_metrics, "revenue"),
        "coverage_ebitda": _pct_non_null(company_metrics, "ebitda"),
        "coverage_enterprise_value": _pct_non_null(company_metrics, "enterprise_value"),
        "coverage_ev_revenue": _pct_non_null(company_metrics, "ev_revenue"),
        "coverage_ev_ebitda": _pct_non_null(company_metrics, "ev_ebitda"),
        "peer_count": _as_int(comps_summary.get("peer_count")),
        "precedent_count": _as_int(precedents_summary.get("transaction_count")),
    }

    issues: List[str] = []
    if checks["company_metrics_row_count"] < 10:
        issues.append("low_company_metric_coverage")
    if checks["coverage_revenue"] < 0.5:
        issues.append("low_revenue_completeness")
    if checks["coverage_ebitda"] < 0.35:
        issues.append("low_ebitda_completeness")
    if checks["coverage_enterprise_value"] < 0.35:
        issues.append("low_ev_completeness")
    if checks["peer_count"] < config.min_peer_count:
        issues.append("insufficient_peer_set")
    if checks["precedent_count"] < config.min_precedent_count:
        issues.append("insufficient_precedent_set")

    weighted_components = [
        checks["coverage_revenue"] * 0.20,
        checks["coverage_ebitda"] * 0.20,
        checks["coverage_enterprise_value"] * 0.20,
        checks["coverage_ev_revenue"] * 0.10,
        checks["coverage_ev_ebitda"] * 0.10,
        min(1.0, checks["peer_count"] / max(config.min_peer_count, 1)) * 0.10,
        min(1.0, checks["precedent_count"] / max(config.min_precedent_count, 1)) * 0.10,
    ]
    score = round(100.0 * sum(weighted_components), 2)

    check_table = pd.DataFrame(
        [
            {"check": key, "value": value}
            for key, value in checks.items()
        ]
    )
    if issues:
        issue_rows = pd.DataFrame([{"check": "issue", "value": issue} for issue in issues])
        check_table = pd.concat([check_table, issue_rows], ignore_index=True)

    return DataQualityResult(
        checks=checks,
        issues=issues,
        score=score,
        check_table=check_table,
    )
