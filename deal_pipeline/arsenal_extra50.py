from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class ArsenalExtra50Result:
    summary: Dict[str, Any]
    arsenal_table: pd.DataFrame


def _f(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        return None


def run_arsenal_extra50(
    comps_summary: Dict[str, Any],
    precedents_summary: Dict[str, Any],
    validation_summary: Dict[str, Any],
    risk_gate_summary: Dict[str, Any],
    arsenal_massive_summary: Dict[str, Any],
) -> ArsenalExtra50Result:
    peer_count = _f(comps_summary.get("peer_count")) or 0.0
    tx_count = _f(precedents_summary.get("transaction_count")) or 0.0
    val_score = _f(validation_summary.get("validation_score")) or 0.0
    warn_count = _f(risk_gate_summary.get("warn_count")) or 0.0
    massive_readiness = _f(arsenal_massive_summary.get("arsenal_massive_readiness_pct")) or 0.0

    tracks = [
        "coverage", "pricing", "risk", "governance", "execution",
        "buyers", "diligence", "capital", "integration", "controls",
    ]

    rows: List[Dict[str, Any]] = []
    for i in range(1, 51):
        track = tracks[(i - 1) % len(tracks)]
        metric = {
            "coverage": min(peer_count, tx_count),
            "pricing": tx_count,
            "risk": max(0.0, 5.0 - warn_count),
            "governance": val_score,
            "execution": massive_readiness,
            "buyers": peer_count,
            "diligence": tx_count,
            "capital": val_score,
            "integration": massive_readiness,
            "controls": val_score,
        }[track]
        status = "pass"
        if track in {"coverage", "buyers", "diligence"}:
            status = "pass" if metric >= 8 else "watch"
        elif track in {"governance", "capital", "controls"}:
            status = "pass" if metric >= 75 else "watch"
        elif track in {"execution", "integration"}:
            status = "pass" if metric >= 0.65 else "watch"
        elif track == "risk":
            status = "pass" if metric >= 3 else "watch"

        rows.append(
            {
                "idea_id": f"X{i:03d}",
                "track": track,
                "initiative": f"{track}_extra_major_{i:02d}",
                "metric_value": metric,
                "status": status,
                "owner_lane": f"lane_{((i-1)//10)+1}",
            }
        )

    table = pd.DataFrame(rows)
    pass_count = int((table["status"] == "pass").sum())
    summary = {
        "arsenal_extra50_idea_count": int(len(table)),
        "arsenal_extra50_pass_count": pass_count,
        "arsenal_extra50_watch_count": int(len(table) - pass_count),
        "arsenal_extra50_readiness_pct": float(pass_count / 50.0),
    }
    return ArsenalExtra50Result(summary=summary, arsenal_table=table)
