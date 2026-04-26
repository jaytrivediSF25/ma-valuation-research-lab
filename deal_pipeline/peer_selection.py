from dataclasses import dataclass
from typing import Dict, List, Optional

import numpy as np
import pandas as pd


@dataclass
class PeerSelectionResult:
    peer_table: pd.DataFrame
    feature_weights: Dict[str, float]


def _safe_float(v: object) -> Optional[float]:
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        return None


def select_peers_with_factor_model(target_row: pd.Series, peers: pd.DataFrame, max_peers: int = 25) -> PeerSelectionResult:
    if peers.empty:
        return PeerSelectionResult(peer_table=peers, feature_weights={})

    frame = peers.copy()
    frame["factor_sector"] = (frame.get("sector", "").astype(str).str.lower() == str(target_row.get("sector") or "").lower()).astype(float)

    target_revenue = _safe_float(target_row.get("revenue"))
    target_margin = _safe_float(target_row.get("ebitda_margin"))
    target_growth = _safe_float(target_row.get("revenue_growth_yoy"))
    target_leverage = None
    td = _safe_float(target_row.get("total_debt"))
    eb = _safe_float(target_row.get("ebitda"))
    if td is not None and eb not in {None, 0.0}:
        target_leverage = td / eb

    frame["factor_size"] = 0.5
    if target_revenue and target_revenue > 0 and "revenue" in frame.columns:
        ratios = pd.to_numeric(frame["revenue"], errors="coerce") / float(target_revenue)
        ratios = ratios.replace([np.inf, -np.inf], np.nan)
        frame["factor_size"] = (1.0 / (1.0 + (ratios.subtract(1.0).abs()))).fillna(0.3)

    frame["factor_margin"] = 0.5
    if target_margin is not None and "ebitda_margin" in frame.columns:
        m = pd.to_numeric(frame["ebitda_margin"], errors="coerce")
        frame["factor_margin"] = (1.0 - (m - target_margin).abs().clip(lower=0.0, upper=1.0)).fillna(0.3)

    frame["factor_growth"] = 0.5
    if target_growth is not None and "revenue_growth_yoy" in frame.columns:
        g = pd.to_numeric(frame["revenue_growth_yoy"], errors="coerce")
        frame["factor_growth"] = (1.0 - (g - target_growth).abs().clip(lower=0.0, upper=1.0)).fillna(0.3)

    frame["factor_leverage"] = 0.5
    if target_leverage is not None and "total_debt" in frame.columns and "ebitda" in frame.columns:
        lev = pd.to_numeric(frame["total_debt"], errors="coerce") / pd.to_numeric(frame["ebitda"], errors="coerce")
        lev = lev.replace([np.inf, -np.inf], np.nan)
        frame["factor_leverage"] = (1.0 / (1.0 + (lev - target_leverage).abs())).fillna(0.3)

    weights = {
        "factor_sector": 0.30,
        "factor_size": 0.25,
        "factor_growth": 0.20,
        "factor_margin": 0.15,
        "factor_leverage": 0.10,
    }
    frame["peer_score"] = sum(frame[k] * w for k, w in weights.items())

    def _explain(r: pd.Series) -> str:
        reasons: List[str] = []
        if float(r.get("factor_sector", 0.0)) >= 1.0:
            reasons.append("same_sector")
        if float(r.get("factor_size", 0.0)) >= 0.7:
            reasons.append("size_match")
        if float(r.get("factor_growth", 0.0)) >= 0.7:
            reasons.append("growth_match")
        if float(r.get("factor_margin", 0.0)) >= 0.7:
            reasons.append("margin_match")
        if float(r.get("factor_leverage", 0.0)) >= 0.7:
            reasons.append("leverage_match")
        return ",".join(reasons) if reasons else "limited_match"

    frame["peer_score_explain"] = frame.apply(_explain, axis=1)
    frame = frame.sort_values(["peer_score", "enterprise_value"], ascending=[False, False], na_position="last")
    frame = frame.head(max(1, int(max_peers))).reset_index(drop=True)
    return PeerSelectionResult(peer_table=frame, feature_weights=weights)
