from dataclasses import dataclass
from typing import Any, Dict

import numpy as np
import pandas as pd


@dataclass
class RobustnessResult:
    summary: Dict[str, Any]
    robustness_table: pd.DataFrame


def _safe_series(values: pd.Series) -> pd.Series:
    return pd.to_numeric(values, errors="coerce").dropna()


def _bootstrap_ci(series: pd.Series, n_boot: int = 400, alpha: float = 0.10) -> Dict[str, float]:
    if series.empty:
        return {"mean": np.nan, "median": np.nan, "ci_low": np.nan, "ci_high": np.nan, "std": np.nan}
    arr = series.to_numpy(dtype=float)
    if len(arr) == 1:
        return {"mean": float(arr[0]), "median": float(arr[0]), "ci_low": float(arr[0]), "ci_high": float(arr[0]), "std": 0.0}
    samples = np.random.default_rng(42).choice(arr, size=(n_boot, len(arr)), replace=True)
    medians = np.median(samples, axis=1)
    low = float(np.quantile(medians, alpha / 2))
    high = float(np.quantile(medians, 1 - alpha / 2))
    return {
        "mean": float(np.mean(arr)),
        "median": float(np.median(arr)),
        "ci_low": low,
        "ci_high": high,
        "std": float(np.std(arr, ddof=1)),
    }


def compute_robustness_metrics(
    comps_table: pd.DataFrame,
    precedents_table: pd.DataFrame,
    target_row: pd.Series,
) -> RobustnessResult:
    comp_ev_rev = _safe_series(comps_table["ev_revenue"]) if "ev_revenue" in comps_table.columns else pd.Series(dtype=float)
    comp_ev_ebitda = _safe_series(comps_table["ev_ebitda"]) if "ev_ebitda" in comps_table.columns else pd.Series(dtype=float)
    prec_ev_rev = _safe_series(precedents_table["ev_revenue"]) if "ev_revenue" in precedents_table.columns else pd.Series(dtype=float)
    prec_ev_ebitda = _safe_series(precedents_table["ev_ebitda"]) if "ev_ebitda" in precedents_table.columns else pd.Series(dtype=float)

    target_ev_rev = pd.to_numeric(pd.Series([target_row.get("ev_revenue")]), errors="coerce").iloc[0]
    target_ev_ebitda = pd.to_numeric(pd.Series([target_row.get("ev_ebitda")]), errors="coerce").iloc[0]

    rows = []
    for label, series in [
        ("comps_ev_revenue", comp_ev_rev),
        ("comps_ev_ebitda", comp_ev_ebitda),
        ("precedents_ev_revenue", prec_ev_rev),
        ("precedents_ev_ebitda", prec_ev_ebitda),
    ]:
        stats = _bootstrap_ci(series)
        rows.append(
            {
                "distribution": label,
                "count": int(len(series)),
                "mean": stats["mean"],
                "median": stats["median"],
                "std": stats["std"],
                "bootstrap_ci_low": stats["ci_low"],
                "bootstrap_ci_high": stats["ci_high"],
                "coef_variation": (stats["std"] / stats["mean"]) if stats["mean"] not in {0, np.nan} else np.nan,
            }
        )

    robustness_table = pd.DataFrame(rows)

    # Z-scores for target valuation relative to comps distributions.
    z_ev_rev = None
    if len(comp_ev_rev) > 1 and pd.notna(target_ev_rev):
        std = float(comp_ev_rev.std(ddof=1))
        if std > 0:
            z_ev_rev = float((target_ev_rev - float(comp_ev_rev.mean())) / std)

    z_ev_ebitda = None
    if len(comp_ev_ebitda) > 1 and pd.notna(target_ev_ebitda):
        std = float(comp_ev_ebitda.std(ddof=1))
        if std > 0:
            z_ev_ebitda = float((target_ev_ebitda - float(comp_ev_ebitda.mean())) / std)

    summary = {
        "comps_ev_revenue_ci_low": float(robustness_table.loc[robustness_table["distribution"] == "comps_ev_revenue", "bootstrap_ci_low"].iloc[0]) if not robustness_table.empty else None,
        "comps_ev_revenue_ci_high": float(robustness_table.loc[robustness_table["distribution"] == "comps_ev_revenue", "bootstrap_ci_high"].iloc[0]) if not robustness_table.empty else None,
        "comps_ev_ebitda_ci_low": float(robustness_table.loc[robustness_table["distribution"] == "comps_ev_ebitda", "bootstrap_ci_low"].iloc[0]) if not robustness_table.empty else None,
        "comps_ev_ebitda_ci_high": float(robustness_table.loc[robustness_table["distribution"] == "comps_ev_ebitda", "bootstrap_ci_high"].iloc[0]) if not robustness_table.empty else None,
        "target_ev_revenue_zscore": z_ev_rev,
        "target_ev_ebitda_zscore": z_ev_ebitda,
    }
    return RobustnessResult(summary=summary, robustness_table=robustness_table)
