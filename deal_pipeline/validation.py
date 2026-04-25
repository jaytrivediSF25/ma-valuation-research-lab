from dataclasses import dataclass
from typing import Any, Dict, List

import pandas as pd


@dataclass
class ValidationResult:
    summary: Dict[str, Any]
    validation_table: pd.DataFrame


def run_model_validation_suite(
    target_row: pd.Series,
    comps_summary: Dict[str, Any],
    precedents_summary: Dict[str, Any],
    robustness_summary: Dict[str, Any],
    quality_summary: Dict[str, Any],
    dcf_summary: Dict[str, Any],
) -> ValidationResult:
    checks: List[Dict[str, Any]] = []

    peer_count = int(comps_summary.get("peer_count") or 0)
    checks.append({"check": "peer_sample_sufficiency", "status": "pass" if peer_count >= 5 else "warn", "value": peer_count, "threshold": ">=5"})

    prec_count = int(precedents_summary.get("transaction_count") or 0)
    checks.append({"check": "precedent_sample_sufficiency", "status": "pass" if prec_count >= 5 else "warn", "value": prec_count, "threshold": ">=5"})

    dq = float(quality_summary.get("score") or 0.0)
    checks.append({"check": "data_quality_score", "status": "pass" if dq >= 70 else "warn", "value": dq, "threshold": ">=70"})

    z_rev = robustness_summary.get("target_ev_revenue_zscore")
    if z_rev is None:
        checks.append({"check": "valuation_zscore_ev_revenue", "status": "warn", "value": None, "threshold": "|z|<=2"})
    else:
        checks.append({"check": "valuation_zscore_ev_revenue", "status": "pass" if abs(float(z_rev)) <= 2 else "warn", "value": float(z_rev), "threshold": "|z|<=2"})

    target_ev = target_row.get("enterprise_value")
    dcf_ev = dcf_summary.get("implied_ev_base")
    if target_ev is None or dcf_ev is None:
        checks.append({"check": "dcf_vs_current_ev_gap", "status": "warn", "value": None, "threshold": "abs(gap)<=40%"})
    else:
        gap = abs((float(dcf_ev) / float(target_ev)) - 1.0) if float(target_ev) != 0 else None
        checks.append({"check": "dcf_vs_current_ev_gap", "status": "pass" if (gap is not None and gap <= 0.40) else "warn", "value": gap, "threshold": "abs(gap)<=40%"})

    table = pd.DataFrame(checks)
    warn_count = int((table["status"] == "warn").sum())
    summary = {
        "validation_checks": int(len(table)),
        "validation_warn_count": warn_count,
        "validation_pass_count": int((table["status"] == "pass").sum()),
        "validation_score": float(max(0.0, 100.0 - (warn_count * 12.5))),
    }
    return ValidationResult(summary=summary, validation_table=table)
