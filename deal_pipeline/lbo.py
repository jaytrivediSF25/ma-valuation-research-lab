from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd

from .config import PipelineConfig


@dataclass
class LBOResult:
    summary: Dict[str, Any]
    lbo_table: pd.DataFrame


def _f(value: Any) -> Optional[float]:
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


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def run_lbo_underwriting(target_row: pd.Series, config: PipelineConfig) -> LBOResult:
    ebitda_0 = _f(target_row.get("ebitda"))
    if ebitda_0 is None or ebitda_0 <= 0:
        return LBOResult(
            summary={
                "entry_ev": None,
                "exit_ev": None,
                "entry_equity": None,
                "exit_equity": None,
                "moic": None,
                "irr": None,
                "exit_net_leverage": None,
            },
            lbo_table=pd.DataFrame(),
        )

    years = max(1, int(config.lbo_holding_years))
    entry_mult = _clamp(config.lbo_entry_multiple, 4.0, 25.0)
    exit_mult = _clamp(config.lbo_exit_multiple, 4.0, 25.0)
    growth = _clamp(config.lbo_ebitda_growth, -0.05, 0.25)
    capex_pct = _clamp(config.lbo_capex_pct_ebitda, 0.0, 0.6)
    cash_tax_pct = _clamp(config.lbo_cash_tax_pct_ebitda, 0.0, 0.6)
    senior_mult = _clamp(config.lbo_senior_debt_multiple, 0.0, 8.0)
    mezz_mult = _clamp(config.lbo_mezz_debt_multiple, 0.0, 4.0)
    senior_rate = _clamp(config.lbo_senior_interest_rate, 0.02, 0.20)
    mezz_rate = _clamp(config.lbo_mezz_interest_rate, 0.04, 0.25)

    entry_ev = ebitda_0 * entry_mult
    senior_open = ebitda_0 * senior_mult
    mezz_open = ebitda_0 * mezz_mult
    entry_debt = senior_open + mezz_open
    entry_equity = max(0.0, entry_ev - entry_debt)

    rows: List[Dict[str, Any]] = []
    ebitda = ebitda_0
    senior = senior_open
    mezz = mezz_open

    for year in range(1, years + 1):
        ebitda *= (1.0 + growth)
        interest_senior = senior * senior_rate
        interest_mezz = mezz * mezz_rate
        interest_total = interest_senior + interest_mezz
        capex = ebitda * capex_pct
        cash_taxes = ebitda * cash_tax_pct
        cfads = ebitda - capex - cash_taxes - interest_total

        senior_paydown = min(max(0.0, cfads), senior)
        senior = max(0.0, senior - senior_paydown)
        remaining_cfads = max(0.0, cfads - senior_paydown)
        mezz_paydown = min(remaining_cfads, mezz)
        mezz = max(0.0, mezz - mezz_paydown)

        net_debt = senior + mezz
        leverage = (net_debt / ebitda) if ebitda > 0 else None
        rows.append(
            {
                "year": year,
                "ebitda": ebitda,
                "senior_debt": senior,
                "mezz_debt": mezz,
                "net_debt": net_debt,
                "interest_total": interest_total,
                "capex": capex,
                "cash_taxes": cash_taxes,
                "cfads": cfads,
                "senior_paydown": senior_paydown,
                "mezz_paydown": mezz_paydown,
                "net_leverage": leverage,
            }
        )

    exit_ev = ebitda * exit_mult
    exit_net_debt = senior + mezz
    exit_equity = max(0.0, exit_ev - exit_net_debt)
    moic = (exit_equity / entry_equity) if entry_equity > 0 else None
    irr = ((moic ** (1.0 / years)) - 1.0) if (moic is not None and moic > 0) else None
    exit_leverage = (exit_net_debt / ebitda) if ebitda > 0 else None

    summary = {
        "entry_ev": entry_ev,
        "exit_ev": exit_ev,
        "entry_equity": entry_equity,
        "exit_equity": exit_equity,
        "moic": moic,
        "irr": irr,
        "exit_net_leverage": exit_leverage,
        "entry_leverage": (entry_debt / ebitda_0) if ebitda_0 > 0 else None,
    }
    return LBOResult(summary=summary, lbo_table=pd.DataFrame(rows))
