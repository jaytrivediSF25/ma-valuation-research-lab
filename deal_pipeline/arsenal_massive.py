from dataclasses import dataclass
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd


@dataclass
class ArsenalMassiveResult:
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


def run_arsenal_massive(
    idea_count: int,
    comps_summary: Dict[str, Any],
    precedents_summary: Dict[str, Any],
    validation_summary: Dict[str, Any],
    sensitivity_summary: Dict[str, Any],
    risk_gate_summary: Dict[str, Any],
) -> ArsenalMassiveResult:
    n = max(1000, int(idea_count))

    peer_count = _f(comps_summary.get("peer_count")) or 0.0
    tx_count = _f(precedents_summary.get("transaction_count")) or 0.0
    val_score = _f(validation_summary.get("validation_score")) or 0.0
    p10 = _f(sensitivity_summary.get("probability_band_p10"))
    p50 = _f(sensitivity_summary.get("probability_band_p50"))
    downside = ((p10 / p50) - 1.0) if (p10 is not None and p50 not in {None, 0}) else -0.6
    warns = _f(risk_gate_summary.get("warn_count")) or 0.0

    domains = np.array([
        "valuation", "diligence", "buyers", "pricing", "risk", "capital", "synergy", "integration", "governance", "execution"
    ])
    levers = np.array([
        "coverage", "quality", "stability", "discipline", "certainty", "speed", "defense", "control", "readiness", "contingency"
    ])

    idx = np.arange(1, n + 1)
    dom = domains[(idx - 1) % len(domains)]
    lev = levers[((idx - 1) // len(domains)) % len(levers)]

    # Deterministic scoring backbone from live model state
    base = (
        0.22 * min(peer_count / 10.0, 1.0)
        + 0.22 * min(tx_count / 10.0, 1.0)
        + 0.26 * min(val_score / 100.0, 1.0)
        + 0.20 * (1.0 if downside >= -0.45 else 0.4)
        + 0.10 * (1.0 if warns <= 1 else 0.5)
    )

    jitter = ((idx * 37) % 1000) / 1000.0
    readiness = np.clip(base + 0.15 * (jitter - 0.5), 0.0, 1.0)

    status = np.where(readiness >= 0.72, "pass", np.where(readiness >= 0.52, "watch", "critical"))
    priority = np.where(status == "critical", "critical", np.where(status == "watch", "high", "normal"))

    table = pd.DataFrame(
        {
            "initiative_id": [f"M{i:06d}" for i in idx],
            "domain": dom,
            "lever": lev,
            "initiative": [f"{d}_{l}_major_play_{i:06d}" for i, d, l in zip(idx, dom, lev)],
            "readiness_score": readiness,
            "status": status,
            "priority": priority,
            "execution_wave": ((idx - 1) // 5000) + 1,
        }
    )

    pass_count = int((table["status"] == "pass").sum())
    watch_count = int((table["status"] == "watch").sum())
    critical_count = int((table["status"] == "critical").sum())

    risk_domain = (
        table.loc[table["status"] != "pass"].groupby("domain").size().sort_values(ascending=False)
    )
    top_risk_domain = str(risk_domain.index[0]) if len(risk_domain) else "none"

    summary = {
        "arsenal_massive_idea_count": int(n),
        "arsenal_massive_pass_count": pass_count,
        "arsenal_massive_watch_count": watch_count,
        "arsenal_massive_critical_count": critical_count,
        "arsenal_massive_readiness_pct": float(pass_count / max(1, n)),
        "arsenal_massive_top_risk_domain": top_risk_domain,
    }
    return ArsenalMassiveResult(summary=summary, arsenal_table=table)
