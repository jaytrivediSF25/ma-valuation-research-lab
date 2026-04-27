from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class Arsenal300Result:
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


THEME_TO_METRIC = {
    "comps_depth": "peer_count",
    "precedent_depth": "transaction_count",
    "valuation_stability": "dcf_gap_to_current",
    "quality_controls": "quality_score",
    "validation_controls": "validation_score",
    "downside_controls": "downside_gap",
    "buyer_coverage": "buyer_count",
    "buyer_quality": "top_buyer_score",
    "pricing_anchors": "walk_away_ev",
    "deal_breakers": "risk_gate_warns",
    "capital_structure": "target_total_debt",
    "financing_strategy": "target_cash",
    "synergy_logic": "target_ebitda_margin",
    "integration_risks": "target_growth",
    "market_regime": "sensitivity_p50",
    "macro_resilience": "sensitivity_p10",
    "precedent_relevance": "precedent_range_high",
    "precedent_governance": "precedent_range_low",
    "execution_readiness": "validation_score",
    "ic_pack_quality": "arsenal50_readiness",
    "audit_traceability": "arsenal50_pass_count",
    "reproducibility": "arsenal50_pass_count",
    "automation_coverage": "buyer_count",
    "portfolio_scaling": "peer_count",
    "risk_escalation": "risk_gate_warns",
    "negotiation_positioning": "opening_bid_ev",
    "term_sheet_design": "stretch_ev",
    "regulatory_defensibility": "quality_score",
    "board_readiness": "validation_score",
    "closing_certainty": "risk_gate_warns",
}

THEMES = list(THEME_TO_METRIC.keys())  # 30 themes


def _status_for(theme: str, metric: Optional[float]) -> str:
    if metric is None:
        return "watch"
    if theme in {"comps_depth", "precedent_depth", "buyer_coverage", "portfolio_scaling"}:
        return "pass" if metric >= 8 else "watch"
    if theme in {"valuation_stability", "downside_controls"}:
        if theme == "valuation_stability":
            return "pass" if abs(metric) <= 0.40 else "watch"
        return "pass" if metric >= -0.45 else "watch"
    if theme in {"quality_controls", "validation_controls", "execution_readiness", "board_readiness", "regulatory_defensibility"}:
        return "pass" if metric >= 75 else "watch"
    if theme in {"risk_escalation", "closing_certainty", "deal_breakers"}:
        return "pass" if metric <= 1 else "watch"
    return "pass"


def run_arsenal300(
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
    arsenal50_summary: Dict[str, Any],
) -> Arsenal300Result:
    metrics = {
        "peer_count": _f(comps_summary.get("peer_count")),
        "transaction_count": _f(precedents_summary.get("transaction_count")),
        "dcf_gap_to_current": _f(dcf_summary.get("dcf_gap_to_current")),
        "quality_score": _f(quality_score),
        "validation_score": _f(validation_summary.get("validation_score")),
        "downside_gap": ((_f(sensitivity_summary.get("probability_band_p10")) / _f(sensitivity_summary.get("probability_band_p50"))) - 1.0)
        if (_f(sensitivity_summary.get("probability_band_p10")) is not None and _f(sensitivity_summary.get("probability_band_p50")) not in {None, 0})
        else None,
        "buyer_count": _f(buyer_universe_summary.get("buyer_count")),
        "top_buyer_score": _f(buyer_universe_summary.get("top_buyer_score")),
        "walk_away_ev": _f(negotiation_summary.get("walk_away_ev")),
        "opening_bid_ev": _f(negotiation_summary.get("opening_bid_ev")),
        "stretch_ev": _f(negotiation_summary.get("stretch_ev")),
        "risk_gate_warns": _f(risk_gate_summary.get("warn_count")),
        "target_total_debt": _f(target_row.get("total_debt")),
        "target_cash": _f(target_row.get("cash")),
        "target_ebitda_margin": _f(target_row.get("ebitda_margin")),
        "target_growth": _f(target_row.get("revenue_growth_yoy")),
        "sensitivity_p50": _f(sensitivity_summary.get("probability_band_p50")),
        "sensitivity_p10": _f(sensitivity_summary.get("probability_band_p10")),
        "precedent_range_high": _f(precedents_summary.get("valuation_range_high")),
        "precedent_range_low": _f(precedents_summary.get("valuation_range_low")),
        "arsenal50_readiness": _f(arsenal50_summary.get("arsenal_readiness_pct")),
        "arsenal50_pass_count": _f(arsenal50_summary.get("arsenal_pass_count")),
    }

    rows: List[Dict[str, Any]] = []
    idx = 1
    for theme in THEMES:
        base_metric = metrics.get(THEME_TO_METRIC[theme])
        for sub in range(1, 11):  # 30 * 10 = 300
            idea_id = f"B{idx:03d}"
            idea = f"{theme}_initiative_{sub:02d}"
            status = _status_for(theme, base_metric)
            priority = "high" if status == "watch" and sub <= 3 else ("medium" if sub <= 6 else "normal")
            rows.append(
                {
                    "idea_id": idea_id,
                    "theme": theme,
                    "idea": idea,
                    "metric_key": THEME_TO_METRIC[theme],
                    "metric_value": base_metric,
                    "status": status,
                    "priority": priority,
                    "execution_package": f"{theme}_pkg_{(sub - 1) // 2 + 1}",
                }
            )
            idx += 1

    table = pd.DataFrame(rows)
    pass_count = int((table["status"] == "pass").sum())
    watch_count = int((table["status"] == "watch").sum())
    high_priority = int((table["priority"] == "high").sum())

    by_theme = table.groupby("theme", as_index=False).agg(
        initiatives=("idea_id", "count"),
        pass_count=("status", lambda x: int((x == "pass").sum())),
        watch_count=("status", lambda x: int((x == "watch").sum())),
    )
    by_theme = by_theme.sort_values(["watch_count", "theme"], ascending=[False, True]).reset_index(drop=True)

    summary = {
        "arsenal300_idea_count": int(len(table)),
        "arsenal300_pass_count": pass_count,
        "arsenal300_watch_count": watch_count,
        "arsenal300_readiness_pct": float(pass_count / max(1, len(table))),
        "arsenal300_high_priority_count": high_priority,
        "arsenal300_top_risk_theme": by_theme.iloc[0]["theme"] if len(by_theme) else None,
    }
    return Arsenal300Result(summary=summary, arsenal_table=table)
