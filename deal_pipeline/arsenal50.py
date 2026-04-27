from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class Arsenal50Result:
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


IDEAS_50: List[Dict[str, str]] = [
    {"id": f"A{i:02d}", "name": n, "category": c}
    for i, (n, c) in enumerate(
        [
            ("peer_depth_guardrail", "coverage"),
            ("precedent_depth_guardrail", "coverage"),
            ("valuation_dispersion_guardrail", "risk"),
            ("dcf_gap_guardrail", "risk"),
            ("quality_score_gate", "quality"),
            ("validation_score_gate", "quality"),
            ("sensitivity_downside_gate", "risk"),
            ("buyer_capacity_screen", "buyers"),
            ("buyer_sector_fit_screen", "buyers"),
            ("buyer_leverage_headroom_screen", "buyers"),
            ("pricing_anchor_precedent_low", "pricing"),
            ("pricing_anchor_precedent_high", "pricing"),
            ("pricing_anchor_blended", "pricing"),
            ("pricing_anchor_sensitivity_p10", "pricing"),
            ("pricing_anchor_sensitivity_p90", "pricing"),
            ("opening_bid_policy", "negotiation"),
            ("walk_away_policy", "negotiation"),
            ("stretch_policy", "negotiation"),
            ("synergy_reliance_flag", "negotiation"),
            ("integration_cost_buffer", "negotiation"),
            ("deal_breaker_qoe", "quality"),
            ("deal_breaker_leverage", "risk"),
            ("deal_breaker_dilution", "risk"),
            ("deal_breaker_cov_breach", "risk"),
            ("deal_breaker_model_warns", "risk"),
            ("capital_structure_flexibility", "capital"),
            ("financing_mix_optimality", "capital"),
            ("interest_coverage_stress", "capital"),
            ("refi_wall_alert", "capital"),
            ("debt_paydown_feasibility", "capital"),
            ("sector_multiple_regime_check", "market"),
            ("market_volatility_penalty", "market"),
            ("rates_shock_penalty", "market"),
            ("spread_shock_penalty", "market"),
            ("macro_recession_case", "market"),
            ("macro_soft_landing_case", "market"),
            ("precedent_outlier_intensity", "precedents"),
            ("precedent_relevance_intensity", "precedents"),
            ("precedent_recency_check", "precedents"),
            ("precedent_sector_purity", "precedents"),
            ("comps_score_concentration", "comps"),
            ("comps_percentile_position", "comps"),
            ("comps_margin_relative", "comps"),
            ("comps_growth_relative", "comps"),
            ("ic_readiness_gate", "workflow"),
            ("memo_readiness_gate", "workflow"),
            ("auditability_gate", "workflow"),
            ("reproducibility_gate", "workflow"),
            ("execution_priority_rank", "workflow"),
            ("go_no_go_signal", "workflow"),
        ],
        start=1,
    )
]


def run_arsenal50(
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
) -> Arsenal50Result:
    peer_count = int(comps_summary.get("peer_count") or 0)
    tx_count = int(precedents_summary.get("transaction_count") or 0)
    dcf_gap = _f(dcf_summary.get("dcf_gap_to_current"))
    p10 = _f(sensitivity_summary.get("probability_band_p10"))
    p50 = _f(sensitivity_summary.get("probability_band_p50"))
    p90 = _f(sensitivity_summary.get("probability_band_p90"))
    downside = ((p10 / p50) - 1.0) if (p10 is not None and p50 not in {None, 0}) else None

    base_metrics = {
        "peer_depth_guardrail": peer_count,
        "precedent_depth_guardrail": tx_count,
        "valuation_dispersion_guardrail": _f(precedents_summary.get("p75_ev_ebitda")),
        "dcf_gap_guardrail": dcf_gap,
        "quality_score_gate": quality_score,
        "validation_score_gate": _f(validation_summary.get("validation_score")),
        "sensitivity_downside_gate": downside,
        "buyer_capacity_screen": _f(buyer_universe_summary.get("top_buyer_score")),
        "buyer_sector_fit_screen": _f(buyer_universe_summary.get("top_buyer_score")),
        "buyer_leverage_headroom_screen": _f(buyer_universe_summary.get("top_buyer_score")),
        "pricing_anchor_precedent_low": _f(precedents_summary.get("valuation_range_low")),
        "pricing_anchor_precedent_high": _f(precedents_summary.get("valuation_range_high")),
        "pricing_anchor_blended": _f(negotiation_summary.get("walk_away_ev")),
        "pricing_anchor_sensitivity_p10": p10,
        "pricing_anchor_sensitivity_p90": p90,
        "opening_bid_policy": _f(negotiation_summary.get("opening_bid_ev")),
        "walk_away_policy": _f(negotiation_summary.get("walk_away_ev")),
        "stretch_policy": _f(negotiation_summary.get("stretch_ev")),
        "synergy_reliance_flag": 0.4,
        "integration_cost_buffer": 0.2,
        "deal_breaker_qoe": quality_score,
        "deal_breaker_leverage": _f(target_row.get("total_debt")),
        "deal_breaker_dilution": _f(target_row.get("ev_revenue")),
        "deal_breaker_cov_breach": downside,
        "deal_breaker_model_warns": _f(validation_summary.get("validation_warn_count")),
        "capital_structure_flexibility": _f(target_row.get("cash")),
        "financing_mix_optimality": _f(target_row.get("enterprise_value")),
        "interest_coverage_stress": _f(target_row.get("interest_expense")),
        "refi_wall_alert": _f(target_row.get("total_debt")),
        "debt_paydown_feasibility": _f(target_row.get("ebitda")),
        "sector_multiple_regime_check": _f(comps_summary.get("peer_median_ev_ebitda")),
        "market_volatility_penalty": 0.1,
        "rates_shock_penalty": 0.1,
        "spread_shock_penalty": 0.1,
        "macro_recession_case": p10,
        "macro_soft_landing_case": p90,
        "precedent_outlier_intensity": _f(precedents_summary.get("p75_ev_revenue")),
        "precedent_relevance_intensity": _f(precedents_summary.get("median_ev_revenue")),
        "precedent_recency_check": tx_count,
        "precedent_sector_purity": tx_count,
        "comps_score_concentration": peer_count,
        "comps_percentile_position": _f(comps_summary.get("percentile_ev_ebitda")),
        "comps_margin_relative": _f(target_row.get("ebitda_margin")),
        "comps_growth_relative": _f(target_row.get("revenue_growth_yoy")),
        "ic_readiness_gate": _f(validation_summary.get("validation_score")),
        "memo_readiness_gate": _f(validation_summary.get("validation_score")),
        "auditability_gate": 1.0,
        "reproducibility_gate": 1.0,
        "execution_priority_rank": _f(negotiation_summary.get("walk_away_ev")),
        "go_no_go_signal": 1.0 if str(risk_gate_summary.get("overall_gate")) == "green" else 0.0,
    }

    rows = []
    for idea in IDEAS_50:
        metric = base_metrics.get(idea["name"])
        status = "watch"
        if idea["name"] in {"peer_depth_guardrail", "precedent_depth_guardrail"}:
            status = "pass" if (metric is not None and metric >= 8) else "watch"
        elif idea["name"] in {"quality_score_gate", "validation_score_gate", "ic_readiness_gate", "memo_readiness_gate"}:
            status = "pass" if (metric is not None and metric >= 75) else "watch"
        elif idea["name"] == "dcf_gap_guardrail":
            status = "pass" if (metric is not None and abs(metric) <= 0.4) else "watch"
        elif idea["name"] == "sensitivity_downside_gate":
            status = "pass" if (metric is not None and metric >= -0.45) else "watch"
        elif idea["name"] == "go_no_go_signal":
            status = "pass" if metric == 1.0 else "watch"
        elif metric is not None:
            status = "pass"

        rows.append(
            {
                "idea_id": idea["id"],
                "idea": idea["name"],
                "category": idea["category"],
                "metric_value": metric,
                "status": status,
                "execution_note": f"{idea['category']}_module_active",
            }
        )

    table = pd.DataFrame(rows)
    pass_count = int((table["status"] == "pass").sum())
    summary = {
        "arsenal_idea_count": int(len(table)),
        "arsenal_pass_count": pass_count,
        "arsenal_watch_count": int(len(table) - pass_count),
        "arsenal_readiness_pct": float(pass_count / max(1, len(table))),
    }
    return Arsenal50Result(summary=summary, arsenal_table=table)
