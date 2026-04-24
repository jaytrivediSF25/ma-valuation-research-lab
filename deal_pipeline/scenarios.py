from dataclasses import dataclass
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd


@dataclass
class ScenarioResult:
    summary: Dict[str, Any]
    scenario_table: pd.DataFrame


def _safe_float(value: Any) -> Optional[float]:
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


def build_valuation_scenarios(
    target_row: pd.Series,
    comps_summary: Dict[str, Any],
    precedents_summary: Dict[str, Any],
) -> ScenarioResult:
    revenue = _safe_float(target_row.get("revenue"))
    ebitda = _safe_float(target_row.get("ebitda"))
    ev_current = _safe_float(target_row.get("enterprise_value"))

    peer_median_ev_rev = _safe_float(comps_summary.get("peer_median_ev_revenue"))
    peer_median_ev_ebitda = _safe_float(comps_summary.get("peer_median_ev_ebitda"))
    prec_p25_ev_rev = _safe_float(precedents_summary.get("p25_ev_revenue"))
    prec_p75_ev_rev = _safe_float(precedents_summary.get("p75_ev_revenue"))
    prec_p25_ev_ebitda = _safe_float(precedents_summary.get("p25_ev_ebitda"))
    prec_p75_ev_ebitda = _safe_float(precedents_summary.get("p75_ev_ebitda"))

    scenarios = []
    if revenue and peer_median_ev_rev:
        scenarios.append(("peer_base_ev_rev", peer_median_ev_rev * revenue))
        scenarios.append(("peer_downside_ev_rev", peer_median_ev_rev * 0.90 * revenue))
        scenarios.append(("peer_upside_ev_rev", peer_median_ev_rev * 1.10 * revenue))
    if ebitda and peer_median_ev_ebitda:
        scenarios.append(("peer_base_ev_ebitda", peer_median_ev_ebitda * ebitda))
        scenarios.append(("peer_downside_ev_ebitda", peer_median_ev_ebitda * 0.90 * ebitda))
        scenarios.append(("peer_upside_ev_ebitda", peer_median_ev_ebitda * 1.10 * ebitda))
    if revenue and prec_p25_ev_rev:
        scenarios.append(("precedent_low_ev_rev", prec_p25_ev_rev * revenue))
    if revenue and prec_p75_ev_rev:
        scenarios.append(("precedent_high_ev_rev", prec_p75_ev_rev * revenue))
    if ebitda and prec_p25_ev_ebitda:
        scenarios.append(("precedent_low_ev_ebitda", prec_p25_ev_ebitda * ebitda))
    if ebitda and prec_p75_ev_ebitda:
        scenarios.append(("precedent_high_ev_ebitda", prec_p75_ev_ebitda * ebitda))

    if not scenarios:
        scenario_table = pd.DataFrame(columns=["scenario", "implied_enterprise_value", "premium_discount_to_current"])
        summary = {
            "scenario_count": 0,
            "implied_ev_low": None,
            "implied_ev_base": None,
            "implied_ev_high": None,
            "current_ev": ev_current,
            "gap_to_base": None,
        }
        return ScenarioResult(summary=summary, scenario_table=scenario_table)

    scenario_table = pd.DataFrame(scenarios, columns=["scenario", "implied_enterprise_value"])
    if ev_current:
        scenario_table["premium_discount_to_current"] = (
            scenario_table["implied_enterprise_value"] / ev_current
        ) - 1.0
    else:
        scenario_table["premium_discount_to_current"] = np.nan

    implied_values = scenario_table["implied_enterprise_value"].dropna()
    low = float(implied_values.quantile(0.2)) if not implied_values.empty else None
    base = float(implied_values.median()) if not implied_values.empty else None
    high = float(implied_values.quantile(0.8)) if not implied_values.empty else None
    gap_to_base = ((base / ev_current) - 1.0) if (base is not None and ev_current not in {None, 0}) else None

    summary = {
        "scenario_count": int(len(scenario_table)),
        "implied_ev_low": low,
        "implied_ev_base": base,
        "implied_ev_high": high,
        "current_ev": ev_current,
        "gap_to_base": gap_to_base,
    }
    scenario_table = scenario_table.sort_values("implied_enterprise_value").reset_index(drop=True)
    return ScenarioResult(summary=summary, scenario_table=scenario_table)
