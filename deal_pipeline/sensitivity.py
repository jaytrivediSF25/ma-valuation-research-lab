from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd


@dataclass
class SensitivityResult:
    summary: Dict[str, Any]
    grid_table: pd.DataFrame
    tornado_table: pd.DataFrame


def _f(v: Any) -> Optional[float]:
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        return None


def _implied_ev(revenue: float, margin: float, wacc: float, growth: float, leverage_turns: float, exit_multiple: float) -> float:
    ebitda = revenue * margin
    fcff_proxy = ebitda * 0.72
    terminal = (fcff_proxy * (1.0 + growth)) / max(0.03, (wacc - growth))
    ops = fcff_proxy / max(0.03, wacc) + terminal / ((1 + wacc) ** 5)
    leverage_adjust = 1.0 - 0.03 * max(0.0, leverage_turns - 2.0)
    multiple_adjust = max(0.5, exit_multiple / 10.0)
    return float(max(0.0, ops * leverage_adjust * multiple_adjust))


def run_full_sensitivity(target_row: pd.Series) -> SensitivityResult:
    revenue = _f(target_row.get("revenue")) or 0.0
    base_margin = _f(target_row.get("ebitda_margin")) or 0.20
    base_wacc = 0.10
    base_growth = 0.025
    base_leverage = ((_f(target_row.get("total_debt")) or 0.0) / ((_f(target_row.get("ebitda")) or 1.0)))
    base_exit = _f(target_row.get("ev_ebitda")) or 10.0

    wacc_vals = [0.08, 0.10, 0.12]
    growth_vals = [0.01, 0.025, 0.04]
    margin_vals = [max(0.05, base_margin - 0.03), base_margin, min(0.50, base_margin + 0.03)]
    leverage_vals = [max(0.5, base_leverage - 1.0), base_leverage, base_leverage + 1.0]
    exit_vals = [max(6.0, base_exit - 1.5), base_exit, base_exit + 1.5]

    rows: List[Dict[str, Any]] = []
    for w in wacc_vals:
        for g in growth_vals:
            for m in margin_vals:
                for l in leverage_vals:
                    for ex in exit_vals:
                        rows.append(
                            {
                                "wacc": w,
                                "terminal_growth": g,
                                "ebitda_margin": m,
                                "net_leverage": l,
                                "exit_multiple": ex,
                                "implied_ev": _implied_ev(revenue, m, w, g, l, ex),
                            }
                        )
    grid = pd.DataFrame(rows)

    base_ev = _implied_ev(revenue, base_margin, base_wacc, base_growth, base_leverage, base_exit)
    tornado_rows: List[Dict[str, Any]] = []
    levers = {
        "wacc": wacc_vals,
        "terminal_growth": growth_vals,
        "ebitda_margin": margin_vals,
        "net_leverage": leverage_vals,
        "exit_multiple": exit_vals,
    }
    for lever, vals in levers.items():
        lo = float(min(vals))
        hi = float(max(vals))
        args_lo = {
            "wacc": base_wacc,
            "growth": base_growth,
            "margin": base_margin,
            "leverage_turns": base_leverage,
            "exit_multiple": base_exit,
        }
        args_hi = args_lo.copy()
        if lever == "terminal_growth":
            args_lo["growth"] = lo
            args_hi["growth"] = hi
        elif lever == "net_leverage":
            args_lo["leverage_turns"] = lo
            args_hi["leverage_turns"] = hi
        elif lever == "ebitda_margin":
            args_lo["margin"] = lo
            args_hi["margin"] = hi
        elif lever == "exit_multiple":
            args_lo["exit_multiple"] = lo
            args_hi["exit_multiple"] = hi
        else:
            args_lo["wacc"] = lo
            args_hi["wacc"] = hi
        ev_lo = _implied_ev(revenue, args_lo["margin"], args_lo["wacc"], args_lo["growth"], args_lo["leverage_turns"], args_lo["exit_multiple"])
        ev_hi = _implied_ev(revenue, args_hi["margin"], args_hi["wacc"], args_hi["growth"], args_hi["leverage_turns"], args_hi["exit_multiple"])
        tornado_rows.append({"lever": lever, "ev_low": ev_lo, "ev_high": ev_hi, "swing": abs(ev_hi - ev_lo)})
    tornado = pd.DataFrame(tornado_rows).sort_values("swing", ascending=False)

    p10 = float(grid["implied_ev"].quantile(0.10))
    p50 = float(grid["implied_ev"].quantile(0.50))
    p90 = float(grid["implied_ev"].quantile(0.90))
    summary = {
        "scenario_count": int(len(grid)),
        "base_ev": float(base_ev),
        "probability_band_p10": p10,
        "probability_band_p50": p50,
        "probability_band_p90": p90,
    }
    return SensitivityResult(summary=summary, grid_table=grid, tornado_table=tornado)
