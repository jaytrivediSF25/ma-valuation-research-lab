from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd


@dataclass
class DriftResult:
    feature: str
    score: float
    threshold: float
    status: str


def precedent_similarity_graph(precedents: pd.DataFrame, feature_cols: List[str], k: int = 5) -> pd.DataFrame:
    if precedents.empty:
        return pd.DataFrame(columns=["node", "neighbor", "distance"])
    frame = precedents.copy().reset_index(drop=True)
    x = frame[feature_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0).values
    nodes = []
    for i in range(len(frame)):
        d = np.sqrt(((x - x[i]) ** 2).sum(axis=1))
        order = np.argsort(d)
        neigh = [j for j in order if j != i][:k]
        for j in neigh:
            nodes.append({"node": int(i), "neighbor": int(j), "distance": float(d[j])})
    return pd.DataFrame(nodes)


def causal_importance_proxy(features: pd.DataFrame, target: pd.Series) -> pd.DataFrame:
    out = []
    y = pd.to_numeric(target, errors="coerce")
    for col in features.columns:
        x = pd.to_numeric(features[col], errors="coerce")
        if x.notna().sum() < 5 or y.notna().sum() < 5:
            out.append({"feature": col, "importance": 0.0})
            continue
        corr = x.corr(y)
        out.append({"feature": col, "importance": float(abs(corr)) if pd.notna(corr) else 0.0})
    return pd.DataFrame(out).sort_values("importance", ascending=False)


def outlier_governance(frame: pd.DataFrame, col: str) -> pd.DataFrame:
    out = frame.copy()
    s = pd.to_numeric(out[col], errors="coerce")
    q1 = s.quantile(0.25)
    q3 = s.quantile(0.75)
    iqr = q3 - q1
    lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    out["outlier_flag"] = (s < lo) | (s > hi)
    out["outlier_reason"] = np.where(out["outlier_flag"], "iqr_rule", "in_band")
    out["winsorized_value"] = s.clip(lower=lo, upper=hi)
    return out


def model_risk_tiering(component: str, impact: str, complexity: str) -> str:
    high_impact = impact.lower() in {"high", "critical"}
    high_complexity = complexity.lower() in {"high", "nonlinear", "opaque"}
    if high_impact and high_complexity:
        return "tier_1"
    if high_impact or high_complexity:
        return "tier_2"
    return "tier_3"


def population_stability_index(expected: pd.Series, actual: pd.Series, bins: int = 10) -> float:
    e = pd.to_numeric(expected, errors="coerce").dropna()
    a = pd.to_numeric(actual, errors="coerce").dropna()
    if e.empty or a.empty:
        return 0.0
    cuts = np.quantile(e, np.linspace(0, 1, bins + 1))
    cuts[0] -= 1e-9
    cuts[-1] += 1e-9
    e_hist, _ = np.histogram(e, bins=cuts)
    a_hist, _ = np.histogram(a, bins=cuts)
    e_pct = np.clip(e_hist / max(1, e_hist.sum()), 1e-6, 1.0)
    a_pct = np.clip(a_hist / max(1, a_hist.sum()), 1e-6, 1.0)
    psi = np.sum((a_pct - e_pct) * np.log(a_pct / e_pct))
    return float(psi)


def drift_monitor(expected_frame: pd.DataFrame, actual_frame: pd.DataFrame, threshold: float = 0.2) -> pd.DataFrame:
    common = [c for c in expected_frame.columns if c in actual_frame.columns]
    rows = []
    for col in common:
        score = population_stability_index(expected_frame[col], actual_frame[col])
        rows.append(
            DriftResult(feature=col, score=score, threshold=threshold, status="alert" if score > threshold else "ok").__dict__
        )
    return pd.DataFrame(rows)


def champion_challenger(champion_errors: pd.Series, challenger_errors: pd.Series) -> Dict[str, Any]:
    c = pd.to_numeric(champion_errors, errors="coerce").dropna()
    x = pd.to_numeric(challenger_errors, errors="coerce").dropna()
    if c.empty or x.empty:
        return {"winner": "insufficient_data", "delta_mae": None}
    mae_c = float(c.abs().mean())
    mae_x = float(x.abs().mean())
    return {"winner": "challenger" if mae_x < mae_c else "champion", "delta_mae": mae_c - mae_x}


def temporal_cross_validation(frame: pd.DataFrame, date_col: str, target_col: str, pred_col: str) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    f = frame.copy()
    f[date_col] = pd.to_datetime(f[date_col], errors="coerce")
    f = f.sort_values(date_col)
    f["year"] = f[date_col].dt.year
    rows = []
    for y, g in f.groupby("year"):
        t = pd.to_numeric(g[target_col], errors="coerce")
        p = pd.to_numeric(g[pred_col], errors="coerce")
        err = (p / t - 1.0).replace([np.inf, -np.inf], np.nan).dropna()
        rows.append({"year": int(y), "rows": int(len(g)), "mae": float(err.abs().mean()) if not err.empty else None})
    return pd.DataFrame(rows)


def benchmark_parity(current: Dict[str, float], benchmark: Dict[str, float], tolerance: float = 1e-6) -> Dict[str, Any]:
    deltas: Dict[str, float] = {}
    for k, b in benchmark.items():
        c = current.get(k)
        if c is None:
            deltas[k] = float("inf")
        else:
            deltas[k] = abs(float(c) - float(b))
    failed = {k: v for k, v in deltas.items() if v > tolerance}
    return {"passed": len(failed) == 0, "failed": failed, "deltas": deltas}


def synthetic_deal_generator(n: int = 100, seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    revenue = rng.lognormal(mean=6.5, sigma=0.7, size=n)
    margin = np.clip(rng.normal(0.18, 0.08, size=n), -0.2, 0.5)
    ebitda = revenue * margin
    mult = np.clip(rng.normal(10, 3, size=n), 2, 25)
    ev = np.maximum(0.0, ebitda * mult)
    return pd.DataFrame({"revenue": revenue, "ebitda_margin": margin, "ebitda": ebitda, "ev_ebitda": mult, "enterprise_value": ev})
