from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd

from .config import PipelineConfig


@dataclass
class DCFResult:
    summary: Dict[str, Any]
    dcf_table: pd.DataFrame
    sensitivity_table: pd.DataFrame


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


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _build_dcf_case(
    label: str,
    revenue_base: float,
    growth: float,
    margin: float,
    wacc: float,
    terminal_growth: float,
    projection_years: int,
    tax_rate: float,
    dep_pct: float,
    capex_pct: float,
    nwc_pct: float,
) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    revenue = revenue_base
    pv_sum = 0.0
    for year in range(1, projection_years + 1):
        revenue = revenue * (1.0 + growth)
        ebitda = revenue * margin
        depreciation = revenue * dep_pct
        ebit = ebitda - depreciation
        nopat = ebit * (1.0 - tax_rate)
        capex = revenue * capex_pct
        nwc = revenue * nwc_pct
        # Approximate delta NWC as level for simple model stability.
        fcf = nopat + depreciation - capex - nwc
        discount = (1.0 + wacc) ** year
        pv_fcf = fcf / discount
        pv_sum += pv_fcf
        rows.append(
            {
                "case": label,
                "year": year,
                "revenue": revenue,
                "ebitda": ebitda,
                "ebit": ebit,
                "nopat": nopat,
                "depreciation": depreciation,
                "capex": capex,
                "nwc_investment": nwc,
                "fcf": fcf,
                "discount_factor": discount,
                "pv_fcf": pv_fcf,
            }
        )

    terminal_fcf = rows[-1]["fcf"] * (1.0 + terminal_growth)
    denom = max(wacc - terminal_growth, 1e-6)
    terminal_value = terminal_fcf / denom
    pv_terminal = terminal_value / ((1.0 + wacc) ** projection_years)
    implied_ev = pv_sum + pv_terminal

    return {
        "table_rows": rows,
        "summary": {
            "case": label,
            "wacc": wacc,
            "terminal_growth": terminal_growth,
            "revenue_growth_assumption": growth,
            "ebitda_margin_assumption": margin,
            "implied_enterprise_value": implied_ev,
            "pv_fcf_sum": pv_sum,
            "pv_terminal_value": pv_terminal,
            "terminal_value_undiscounted": terminal_value,
        },
    }


def run_dcf_analysis(target_row: pd.Series, config: PipelineConfig) -> DCFResult:
    revenue = _safe_float(target_row.get("revenue"))
    margin = _safe_float(target_row.get("ebitda_margin"))
    growth = _safe_float(target_row.get("revenue_growth_yoy"))
    current_ev = _safe_float(target_row.get("enterprise_value"))
    if revenue is None or revenue <= 0:
        empty = pd.DataFrame()
        return DCFResult(
            summary={
                "case_count": 0,
                "implied_ev_low": None,
                "implied_ev_base": None,
                "implied_ev_high": None,
                "current_ev": current_ev,
                "dcf_gap_to_current": None,
            },
            dcf_table=empty,
            sensitivity_table=empty,
        )

    growth_assumption = _clamp(
        growth if growth is not None else 0.06,
        config.dcf_growth_floor,
        config.dcf_growth_cap,
    )
    margin_assumption = _clamp(margin if margin is not None else 0.18, 0.05, 0.55)

    cases = [
        ("downside", growth_assumption * 0.75, margin_assumption * 0.90, config.dcf_wacc_base + 0.01, config.dcf_terminal_growth_base - 0.005),
        ("base", growth_assumption, margin_assumption, config.dcf_wacc_base, config.dcf_terminal_growth_base),
        ("upside", growth_assumption * 1.20, margin_assumption * 1.08, config.dcf_wacc_base - 0.01, config.dcf_terminal_growth_base + 0.005),
    ]

    case_summaries: List[Dict[str, Any]] = []
    tables: List[pd.DataFrame] = []
    for label, case_growth, case_margin, case_wacc, case_tg in cases:
        built = _build_dcf_case(
            label=label,
            revenue_base=revenue,
            growth=_clamp(case_growth, config.dcf_growth_floor, config.dcf_growth_cap),
            margin=_clamp(case_margin, 0.05, 0.60),
            wacc=_clamp(case_wacc, 0.06, 0.18),
            terminal_growth=_clamp(case_tg, 0.005, 0.04),
            projection_years=config.dcf_projection_years,
            tax_rate=_clamp(config.dcf_tax_rate, 0.0, 0.5),
            dep_pct=_clamp(config.dcf_depreciation_pct_revenue, 0.0, 0.15),
            capex_pct=_clamp(config.dcf_capex_pct_revenue, 0.0, 0.20),
            nwc_pct=_clamp(config.dcf_nwc_pct_revenue, 0.0, 0.10),
        )
        case_summaries.append(built["summary"])
        tables.append(pd.DataFrame(built["table_rows"]))

    case_summary_df = pd.DataFrame(case_summaries)
    dcf_table = pd.concat(tables, ignore_index=True)

    # WACC / terminal growth sensitivity (base margin/growth)
    sensitivity_rows: List[Dict[str, Any]] = []
    for wacc in [config.dcf_wacc_base - 0.01, config.dcf_wacc_base, config.dcf_wacc_base + 0.01]:
        for tg in [config.dcf_terminal_growth_base - 0.005, config.dcf_terminal_growth_base, config.dcf_terminal_growth_base + 0.005]:
            built = _build_dcf_case(
                label=f"sens_wacc_{wacc:.3f}_tg_{tg:.3f}",
                revenue_base=revenue,
                growth=growth_assumption,
                margin=margin_assumption,
                wacc=_clamp(wacc, 0.06, 0.18),
                terminal_growth=_clamp(tg, 0.005, 0.04),
                projection_years=config.dcf_projection_years,
                tax_rate=_clamp(config.dcf_tax_rate, 0.0, 0.5),
                dep_pct=_clamp(config.dcf_depreciation_pct_revenue, 0.0, 0.15),
                capex_pct=_clamp(config.dcf_capex_pct_revenue, 0.0, 0.20),
                nwc_pct=_clamp(config.dcf_nwc_pct_revenue, 0.0, 0.10),
            )["summary"]
            sensitivity_rows.append(
                {
                    "wacc": _clamp(wacc, 0.06, 0.18),
                    "terminal_growth": _clamp(tg, 0.005, 0.04),
                    "implied_enterprise_value": built["implied_enterprise_value"],
                }
            )
    sensitivity_table = pd.DataFrame(sensitivity_rows).sort_values(["wacc", "terminal_growth"]).reset_index(drop=True)

    low = _safe_float(case_summary_df["implied_enterprise_value"].min())
    base = _safe_float(case_summary_df[case_summary_df["case"] == "base"]["implied_enterprise_value"].iloc[0])
    high = _safe_float(case_summary_df["implied_enterprise_value"].max())
    gap = ((base / current_ev) - 1.0) if (base is not None and current_ev not in {None, 0}) else None

    summary = {
        "case_count": int(len(case_summary_df)),
        "implied_ev_low": low,
        "implied_ev_base": base,
        "implied_ev_high": high,
        "current_ev": current_ev,
        "dcf_gap_to_current": gap,
    }

    case_summary_df.insert(0, "row_type", "case_summary")
    dcf_table = pd.concat([case_summary_df, dcf_table], ignore_index=True, sort=False)
    return DCFResult(summary=summary, dcf_table=dcf_table, sensitivity_table=sensitivity_table)
