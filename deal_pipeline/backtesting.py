from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd


@dataclass
class BacktestResult:
    summary: Dict[str, Any]
    backtest_table: pd.DataFrame


def _f(value: Any) -> Optional[float]:
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return float(value)
    except Exception:
        return None


def run_historical_backtest(precedents_table: pd.DataFrame) -> BacktestResult:
    if precedents_table.empty:
        return BacktestResult(summary={"rows": 0}, backtest_table=pd.DataFrame())

    frame = precedents_table.copy()
    if "announcement_date" in frame.columns:
        frame["announcement_date"] = pd.to_datetime(frame["announcement_date"], errors="coerce")
    else:
        frame["announcement_date"] = pd.NaT

    rows: List[Dict[str, Any]] = []
    for idx, row in frame.iterrows():
        actual_ev = _f(row.get("enterprise_value"))
        rev = _f(row.get("revenue"))
        ebitda = _f(row.get("ebitda"))
        sector = str(row.get("sector") or "")
        dt = row.get("announcement_date")

        hist = frame.drop(index=idx)
        if sector:
            sec = hist[hist.get("sector", "") == sector]
            if len(sec) >= 5:
                hist = sec
        if pd.notna(dt):
            hist = hist[pd.to_datetime(hist["announcement_date"], errors="coerce") <= dt]
            if len(hist) < 5:
                hist = frame.drop(index=idx)

        med_ev_rev = pd.to_numeric(hist.get("ev_revenue", pd.Series(dtype=float)), errors="coerce").median()
        med_ev_ebitda = pd.to_numeric(hist.get("ev_ebitda", pd.Series(dtype=float)), errors="coerce").median()

        forecasts: List[float] = []
        if pd.notna(med_ev_rev) and rev and rev > 0:
            forecasts.append(float(med_ev_rev) * rev)
        if pd.notna(med_ev_ebitda) and ebitda and ebitda > 0:
            forecasts.append(float(med_ev_ebitda) * ebitda)

        forecast_ev = float(np.mean(forecasts)) if forecasts else None
        err_pct = (forecast_ev / actual_ev - 1.0) if (forecast_ev is not None and actual_ev not in {None, 0.0}) else None
        rows.append(
            {
                "target_company": row.get("target_company"),
                "announcement_date": dt,
                "actual_ev": actual_ev,
                "forecast_ev": forecast_ev,
                "forecast_error_pct": err_pct,
                "ev_rev_anchor": _f(med_ev_rev),
                "ev_ebitda_anchor": _f(med_ev_ebitda),
                "sample_size": int(len(hist)),
            }
        )

    out = pd.DataFrame(rows)
    clean_err = pd.to_numeric(out["forecast_error_pct"], errors="coerce").dropna()
    summary = {
        "rows": int(len(out)),
        "mae_forecast_error_pct": float(clean_err.abs().mean()) if not clean_err.empty else None,
        "median_forecast_error_pct": float(clean_err.median()) if not clean_err.empty else None,
        "hit_rate_within_20pct": float((clean_err.abs() <= 0.20).mean()) if not clean_err.empty else None,
    }
    return BacktestResult(summary=summary, backtest_table=out)
