from dataclasses import dataclass
from typing import Any, Dict, Optional

import pandas as pd

from .config import PipelineConfig


@dataclass
class BlendedValuationResult:
    summary: Dict[str, Any]
    blend_table: pd.DataFrame


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return float(value)
    except Exception:
        return None


def _weighted_average(weights_and_values):
    total_w = 0.0
    total_v = 0.0
    for w, v in weights_and_values:
        if v is None:
            continue
        total_w += w
        total_v += (w * v)
    if total_w == 0:
        return None
    return total_v / total_w


def build_blended_valuation(
    target_row: pd.Series,
    comps_summary: Dict[str, Any],
    precedents_summary: Dict[str, Any],
    scenarios_summary: Dict[str, Any],
    dcf_summary: Dict[str, Any],
    config: PipelineConfig,
) -> BlendedValuationResult:
    revenue = _as_float(target_row.get("revenue"))
    ebitda = _as_float(target_row.get("ebitda"))
    current_ev = _as_float(target_row.get("enterprise_value"))

    comp_anchor = None
    peer_ev_rev = _as_float(comps_summary.get("peer_median_ev_revenue"))
    peer_ev_ebitda = _as_float(comps_summary.get("peer_median_ev_ebitda"))
    if revenue and peer_ev_rev:
        comp_anchor = peer_ev_rev * revenue
    if ebitda and peer_ev_ebitda:
        comp_anchor_alt = peer_ev_ebitda * ebitda
        comp_anchor = _weighted_average([(0.5, comp_anchor), (0.5, comp_anchor_alt)])

    precedents_anchor = _weighted_average(
        [
            (0.5, _as_float(precedents_summary.get("valuation_range_low"))),
            (0.5, _as_float(precedents_summary.get("valuation_range_high"))),
        ]
    )
    scenarios_anchor = _as_float(scenarios_summary.get("implied_ev_base"))
    dcf_anchor = _as_float(dcf_summary.get("implied_ev_base"))

    blend_table = pd.DataFrame(
        [
            {"method": "comps_anchor", "weight": config.blend_weight_comps, "implied_ev": comp_anchor},
            {"method": "precedents_anchor", "weight": config.blend_weight_precedents, "implied_ev": precedents_anchor},
            {"method": "scenarios_anchor", "weight": config.blend_weight_scenarios, "implied_ev": scenarios_anchor},
            {"method": "dcf_anchor", "weight": config.blend_weight_dcf, "implied_ev": dcf_anchor},
        ]
    )

    blended_ev = _weighted_average(
        [(float(row["weight"]), _as_float(row["implied_ev"])) for _, row in blend_table.iterrows()]
    )
    gap = ((blended_ev / current_ev) - 1.0) if (blended_ev is not None and current_ev not in {None, 0}) else None
    stance = "neutral"
    if gap is not None:
        if gap >= 0.15:
            stance = "upside"
        elif gap <= -0.15:
            stance = "downside"

    summary = {
        "blended_implied_ev": blended_ev,
        "current_ev": current_ev,
        "blend_gap_to_current": gap,
        "blend_stance": stance,
    }
    return BlendedValuationResult(summary=summary, blend_table=blend_table)
