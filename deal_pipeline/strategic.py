from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class BuyerUniverseResult:
    summary: Dict[str, Any]
    buyer_table: pd.DataFrame


@dataclass
class RiskGateResult:
    summary: Dict[str, Any]
    gate_table: pd.DataFrame


@dataclass
class NegotiationPlaybookResult:
    summary: Dict[str, Any]
    playbook_table: pd.DataFrame


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


def build_buyer_universe(target_row: pd.Series, company_metrics: pd.DataFrame, peer_table: pd.DataFrame, max_rows: int = 20) -> BuyerUniverseResult:
    target_ticker = str(target_row.get("ticker") or "").upper()
    target_sector = str(target_row.get("sector") or "").strip().lower()
    target_ev = _f(target_row.get("enterprise_value")) or 0.0
    target_rev = _f(target_row.get("revenue")) or 0.0

    universe = company_metrics.copy()
    if universe.empty:
        return BuyerUniverseResult(summary={"buyer_count": 0, "top_buyer": None}, buyer_table=universe)

    universe = universe[universe.get("ticker", "").astype(str).str.upper() != target_ticker].copy()

    if not peer_table.empty and "ticker" in peer_table.columns:
        peer_tickers = set(peer_table["ticker"].astype(str).str.upper().tolist())
        universe["peer_affinity"] = universe.get("ticker", "").astype(str).str.upper().isin(peer_tickers).astype(float)
    else:
        universe["peer_affinity"] = 0.0

    sector_match = universe.get("sector", "").astype(str).str.lower().eq(target_sector).astype(float)
    universe["sector_fit"] = sector_match

    buyer_ev = pd.to_numeric(universe.get("enterprise_value"), errors="coerce")
    buyer_rev = pd.to_numeric(universe.get("revenue"), errors="coerce")
    buyer_cash = pd.to_numeric(universe.get("cash"), errors="coerce").fillna(0.0)
    buyer_debt = pd.to_numeric(universe.get("total_debt"), errors="coerce").fillna(0.0)
    buyer_ebitda = pd.to_numeric(universe.get("ebitda"), errors="coerce")

    size_fit = (buyer_ev / max(1.0, target_ev)).clip(lower=0.0, upper=10.0)
    size_fit = (1.0 / (1.0 + (size_fit - 3.0).abs())).fillna(0.2)

    financing_capacity = ((buyer_cash + 0.3 * buyer_ev) / max(1.0, target_ev)).clip(lower=0.0, upper=2.0)
    financing_capacity = (financing_capacity / 2.0).fillna(0.0)

    leverage = (buyer_debt / buyer_ebitda).replace([pd.NA, float("inf"), float("-inf")], pd.NA)
    leverage = pd.to_numeric(leverage, errors="coerce")
    leverage_headroom = (1.0 - (leverage.fillna(3.5) / 6.0)).clip(lower=0.0, upper=1.0)

    revenue_overlap = (buyer_rev / max(1.0, target_rev)).clip(lower=0.0, upper=8.0)
    revenue_overlap = (1.0 / (1.0 + (revenue_overlap - 2.0).abs())).fillna(0.2)

    universe["buyer_score"] = (
        0.25 * universe["sector_fit"]
        + 0.20 * universe["peer_affinity"]
        + 0.20 * size_fit
        + 0.20 * financing_capacity
        + 0.10 * leverage_headroom
        + 0.05 * revenue_overlap
    )

    universe["buyer_type"] = universe["buyer_score"].apply(lambda x: "strategic" if x >= 0.55 else "financial")
    universe["score_explain"] = universe.apply(
        lambda r: ",".join(
            [
                "sector" if r.get("sector_fit", 0) >= 1 else "",
                "peer" if r.get("peer_affinity", 0) >= 1 else "",
                "capacity" if financing_capacity.loc[r.name] >= 0.4 else "",
            ]
        ).strip(",")
        or "broad_fit",
        axis=1,
    )

    cols = [
        c
        for c in [
            "ticker",
            "company_name",
            "sector",
            "enterprise_value",
            "revenue",
            "ebitda",
            "buyer_type",
            "buyer_score",
            "score_explain",
        ]
        if c in universe.columns
    ]
    out = universe[cols].sort_values("buyer_score", ascending=False).head(max_rows).reset_index(drop=True)

    summary = {
        "buyer_count": int(len(out)),
        "top_buyer": out.iloc[0]["ticker"] if len(out) else None,
        "top_buyer_score": _f(out.iloc[0]["buyer_score"]) if len(out) else None,
    }
    return BuyerUniverseResult(summary=summary, buyer_table=out)


def run_deal_risk_gate(
    target_row: pd.Series,
    comps_summary: Dict[str, Any],
    precedents_summary: Dict[str, Any],
    dcf_summary: Dict[str, Any],
    quality_score: float,
    validation_summary: Dict[str, Any],
    sensitivity_summary: Dict[str, Any],
) -> RiskGateResult:
    checks: List[Dict[str, Any]] = []

    peer_count = int(comps_summary.get("peer_count") or 0)
    checks.append({"gate": "peer_coverage", "value": peer_count, "threshold": ">=8", "status": "pass" if peer_count >= 8 else "warn", "severity": "medium"})

    tx_count = int(precedents_summary.get("transaction_count") or 0)
    checks.append({"gate": "precedent_coverage", "value": tx_count, "threshold": ">=8", "status": "pass" if tx_count >= 8 else "warn", "severity": "medium"})

    dcf_gap = _f(dcf_summary.get("dcf_gap_to_current"))
    dcf_ok = dcf_gap is not None and abs(dcf_gap) <= 0.40
    checks.append({"gate": "dcf_gap_guardrail", "value": dcf_gap, "threshold": "abs<=0.40", "status": "pass" if dcf_ok else "warn", "severity": "high"})

    checks.append({"gate": "data_quality", "value": quality_score, "threshold": ">=75", "status": "pass" if quality_score >= 75 else "warn", "severity": "high"})

    val_score = _f(validation_summary.get("validation_score")) or 0.0
    checks.append({"gate": "model_validation", "value": val_score, "threshold": ">=70", "status": "pass" if val_score >= 70 else "warn", "severity": "high"})

    p10 = _f(sensitivity_summary.get("probability_band_p10"))
    p50 = _f(sensitivity_summary.get("probability_band_p50"))
    downside_gap = ((p10 / p50) - 1.0) if (p10 is not None and p50 not in {None, 0}) else None
    checks.append({"gate": "sensitivity_downside", "value": downside_gap, "threshold": ">=-0.45", "status": "pass" if (downside_gap is not None and downside_gap >= -0.45) else "warn", "severity": "critical"})

    frame = pd.DataFrame(checks)
    warn_count = int((frame["status"] == "warn").sum())
    critical_warn = int(((frame["status"] == "warn") & (frame["severity"] == "critical")).sum())

    if critical_warn > 0:
        overall = "red"
    elif warn_count >= 3:
        overall = "amber"
    else:
        overall = "green"

    summary = {
        "overall_gate": overall,
        "warn_count": warn_count,
        "critical_warn_count": critical_warn,
    }
    return RiskGateResult(summary=summary, gate_table=frame)


def build_negotiation_playbook(
    target_row: pd.Series,
    blended_summary: Dict[str, Any],
    precedents_summary: Dict[str, Any],
    sensitivity_summary: Dict[str, Any],
) -> NegotiationPlaybookResult:
    current_ev = _f(target_row.get("enterprise_value")) or 0.0
    blended_ev = _f(blended_summary.get("blended_implied_ev")) or current_ev
    p25_range = _f(precedents_summary.get("valuation_range_low"))
    p75_range = _f(precedents_summary.get("valuation_range_high"))
    sens_p10 = _f(sensitivity_summary.get("probability_band_p10"))
    sens_p50 = _f(sensitivity_summary.get("probability_band_p50"))
    sens_p90 = _f(sensitivity_summary.get("probability_band_p90"))

    opening_bid = min(v for v in [current_ev * 1.10, blended_ev * 0.92, p25_range or float("inf")] if v is not None)
    walk_away = min(v for v in [blended_ev * 1.03, p75_range or float("inf"), sens_p50 or float("inf")] if v is not None)
    stretch = max(v for v in [blended_ev * 1.12, p75_range or 0.0, sens_p90 or 0.0])
    downside_anchor = sens_p10 if sens_p10 is not None else current_ev * 0.75

    rows = [
        {"term": "opening_bid_ev", "value": opening_bid, "rationale": "anchor at disciplined premium vs current EV"},
        {"term": "walk_away_ev", "value": walk_away, "rationale": "max price before risk adjusted return degrades"},
        {"term": "stretch_ev", "value": stretch, "rationale": "only with higher certainty on synergies"},
        {"term": "downside_anchor_ev", "value": downside_anchor, "rationale": "defensive floor from sensitivity band"},
        {"term": "preferred_structure", "value": "cash+debt with capped leverage", "rationale": "protect dilution and maintain financing flexibility"},
        {"term": "key_condition", "value": "confirmatory diligence on margin durability", "rationale": "largest model sensitivity driver"},
    ]
    table = pd.DataFrame(rows)

    summary = {
        "opening_bid_ev": opening_bid,
        "walk_away_ev": walk_away,
        "stretch_ev": stretch,
        "downside_anchor_ev": downside_anchor,
    }
    return NegotiationPlaybookResult(summary=summary, playbook_table=table)
