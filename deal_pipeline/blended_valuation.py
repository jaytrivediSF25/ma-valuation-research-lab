from dataclasses import dataclass
from typing import Any, Dict, Optional

import numpy as np
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


def _optimize_weights(
    anchor_values: Dict[str, Optional[float]],
    base_weights: Dict[str, float],
    current_ev: Optional[float],
    enabled: bool,
) -> Dict[str, Any]:
    if not enabled or current_ev in {None, 0}:
        return {
            "weights": base_weights,
            "optimizer_used": False,
            "optimizer_status": "disabled_or_missing_current_ev",
        }

    methods = list(anchor_values.keys())
    values = np.array([anchor_values[m] if anchor_values[m] is not None else np.nan for m in methods], dtype=float)
    valid_mask = np.isfinite(values)
    if valid_mask.sum() == 0:
        return {
            "weights": base_weights,
            "optimizer_used": False,
            "optimizer_status": "no_valid_anchors",
        }

    valid_methods = [m for i, m in enumerate(methods) if valid_mask[i]]
    valid_values = values[valid_mask]
    valid_base = np.array([base_weights[m] for m in valid_methods], dtype=float)
    valid_base = valid_base / valid_base.sum() if valid_base.sum() > 0 else np.full_like(valid_base, 1.0 / len(valid_base))

    try:
        import cvxpy as cp
    except Exception:
        return {
            "weights": base_weights,
            "optimizer_used": False,
            "optimizer_status": "cvxpy_not_installed",
        }

    w = cp.Variable(len(valid_methods))
    target = float(current_ev)
    objective = cp.Minimize(cp.square(valid_values @ w - target) + 0.05 * cp.sum_squares(w - valid_base))
    constraints = [cp.sum(w) == 1.0, w >= 0.0, w <= 0.75]
    problem = cp.Problem(objective, constraints)
    try:
        problem.solve(solver=cp.ECOS, verbose=False)
    except Exception:
        try:
            problem.solve(verbose=False)
        except Exception:
            return {
                "weights": base_weights,
                "optimizer_used": False,
                "optimizer_status": "solver_failure",
            }

    if w.value is None:
        return {
            "weights": base_weights,
            "optimizer_used": False,
            "optimizer_status": "no_solution",
        }

    solved = np.maximum(np.array(w.value, dtype=float), 0.0)
    solved = solved / solved.sum() if solved.sum() > 0 else valid_base
    optimized = {m: 0.0 for m in methods}
    for i, m in enumerate(valid_methods):
        optimized[m] = float(solved[i])
    return {
        "weights": optimized,
        "optimizer_used": True,
        "optimizer_status": "success",
    }


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

    base_weights = {
        "comps_anchor": float(config.blend_weight_comps),
        "precedents_anchor": float(config.blend_weight_precedents),
        "scenarios_anchor": float(config.blend_weight_scenarios),
        "dcf_anchor": float(config.blend_weight_dcf),
    }
    weights_out = _optimize_weights(
        anchor_values={
            "comps_anchor": comp_anchor,
            "precedents_anchor": precedents_anchor,
            "scenarios_anchor": scenarios_anchor,
            "dcf_anchor": dcf_anchor,
        },
        base_weights=base_weights,
        current_ev=current_ev,
        enabled=config.enable_blend_optimizer,
    )
    weights = weights_out["weights"]

    blend_table = pd.DataFrame(
        [
            {"method": "comps_anchor", "weight": weights["comps_anchor"], "implied_ev": comp_anchor},
            {"method": "precedents_anchor", "weight": weights["precedents_anchor"], "implied_ev": precedents_anchor},
            {"method": "scenarios_anchor", "weight": weights["scenarios_anchor"], "implied_ev": scenarios_anchor},
            {"method": "dcf_anchor", "weight": weights["dcf_anchor"], "implied_ev": dcf_anchor},
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
        "blend_optimizer_used": bool(weights_out["optimizer_used"]),
        "blend_optimizer_status": str(weights_out["optimizer_status"]),
        "optimized_weights": {k: float(v) for k, v in weights.items()},
    }
    return BlendedValuationResult(summary=summary, blend_table=blend_table)
