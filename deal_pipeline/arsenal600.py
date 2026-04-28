from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class Arsenal600Result:
    summary: Dict[str, Any]
    arsenal_table: pd.DataFrame


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


DOMAINS = [
    "valuation_architecture",
    "deal_thesis_pressure_test",
    "buyer_competition_engineering",
    "synergy_verification",
    "financing_risk_engineering",
    "capital_structure_design",
    "market_dislocation_response",
    "regulatory_diligence",
    "accounting_normalization",
    "integration_execution",
    "board_pack_strength",
    "ic_decision_quality",
    "downside_protection",
    "scenario_containment",
    "term_sheet_optimization",
    "closing_certainty",
    "portfolio_prioritization",
    "resource_allocation",
    "timing_strategy",
    "auction_strategy",
    "precedent_signal_quality",
    "comps_signal_quality",
    "model_risk_defense",
    "auditability_resilience",
    "reproducibility_integrity",
    "automation_depth",
    "coverage_density",
    "execution_velocity",
    "defensibility_quality",
    "governance_maturity",
]


def _status(domain: str, val: Optional[float]) -> str:
    if val is None:
        return "watch"
    if domain in {"coverage_density", "comps_signal_quality", "precedent_signal_quality"}:
        return "pass" if val >= 8 else "watch"
    if domain in {"model_risk_defense", "defensibility_quality", "governance_maturity", "ic_decision_quality", "board_pack_strength"}:
        return "pass" if val >= 75 else "watch"
    if domain in {"downside_protection", "scenario_containment"}:
        return "pass" if val >= -0.45 else "watch"
    if domain in {"valuation_architecture"}:
        return "pass" if abs(val) <= 0.40 else "watch"
    return "pass"


def run_arsenal600(
    target_row: pd.Series,
    comps_summary: Dict[str, Any],
    precedents_summary: Dict[str, Any],
    dcf_summary: Dict[str, Any],
    quality_score: float,
    validation_summary: Dict[str, Any],
    sensitivity_summary: Dict[str, Any],
    buyer_universe_summary: Dict[str, Any],
    negotiation_summary: Dict[str, Any],
    risk_gate_summary: Dict[str, Any],
    arsenal300_summary: Dict[str, Any],
) -> Arsenal600Result:
    p10 = _f(sensitivity_summary.get("probability_band_p10"))
    p50 = _f(sensitivity_summary.get("probability_band_p50"))
    downside = ((p10 / p50) - 1.0) if (p10 is not None and p50 not in {None, 0}) else None

    signals = {
        "valuation_architecture": _f(dcf_summary.get("dcf_gap_to_current")),
        "deal_thesis_pressure_test": _f(validation_summary.get("validation_score")),
        "buyer_competition_engineering": _f(buyer_universe_summary.get("buyer_count")),
        "synergy_verification": _f(target_row.get("ebitda_margin")),
        "financing_risk_engineering": _f(target_row.get("total_debt")),
        "capital_structure_design": _f(target_row.get("cash")),
        "market_dislocation_response": _f(sensitivity_summary.get("probability_band_p50")),
        "regulatory_diligence": _f(quality_score),
        "accounting_normalization": _f(quality_score),
        "integration_execution": _f(target_row.get("revenue_growth_yoy")),
        "board_pack_strength": _f(validation_summary.get("validation_score")),
        "ic_decision_quality": _f(validation_summary.get("validation_score")),
        "downside_protection": downside,
        "scenario_containment": downside,
        "term_sheet_optimization": _f(negotiation_summary.get("walk_away_ev")),
        "closing_certainty": _f(risk_gate_summary.get("warn_count")),
        "portfolio_prioritization": _f(comps_summary.get("peer_count")),
        "resource_allocation": _f(precedents_summary.get("transaction_count")),
        "timing_strategy": _f(precedents_summary.get("transaction_count")),
        "auction_strategy": _f(buyer_universe_summary.get("top_buyer_score")),
        "precedent_signal_quality": _f(precedents_summary.get("transaction_count")),
        "comps_signal_quality": _f(comps_summary.get("peer_count")),
        "model_risk_defense": _f(validation_summary.get("validation_score")),
        "auditability_resilience": _f(arsenal300_summary.get("arsenal300_pass_count")),
        "reproducibility_integrity": _f(arsenal300_summary.get("arsenal300_pass_count")),
        "automation_depth": _f(buyer_universe_summary.get("buyer_count")),
        "coverage_density": _f(comps_summary.get("peer_count")),
        "execution_velocity": _f(arsenal300_summary.get("arsenal300_readiness_pct")),
        "defensibility_quality": _f(validation_summary.get("validation_score")),
        "governance_maturity": _f(validation_summary.get("validation_score")),
    }

    rows: List[Dict[str, Any]] = []
    n = 1
    for domain in DOMAINS:
        metric = signals.get(domain)
        for pack in range(1, 21):  # 30 domains x 20 = 600
            idea_id = f"C{n:03d}"
            status = _status(domain, metric)
            critical = (pack <= 4 and status == "watch")
            rows.append(
                {
                    "idea_id": idea_id,
                    "domain": domain,
                    "initiative": f"{domain}_major_{pack:02d}",
                    "metric_value": metric,
                    "status": status,
                    "severity": "critical" if critical else ("elevated" if status == "watch" else "normal"),
                    "execution_stream": f"stream_{(pack - 1) // 5 + 1}",
                }
            )
            n += 1

    table = pd.DataFrame(rows)
    pass_count = int((table["status"] == "pass").sum())
    watch_count = int((table["status"] == "watch").sum())
    critical = int((table["severity"] == "critical").sum())

    theme = table.groupby("domain", as_index=False).agg(watch_count=("status", lambda s: int((s == "watch").sum())))
    theme = theme.sort_values("watch_count", ascending=False)

    summary = {
        "arsenal600_idea_count": int(len(table)),
        "arsenal600_pass_count": pass_count,
        "arsenal600_watch_count": watch_count,
        "arsenal600_readiness_pct": float(pass_count / max(1, len(table))),
        "arsenal600_critical_count": critical,
        "arsenal600_top_risk_domain": theme.iloc[0]["domain"] if len(theme) else None,
    }
    return Arsenal600Result(summary=summary, arsenal_table=table)
