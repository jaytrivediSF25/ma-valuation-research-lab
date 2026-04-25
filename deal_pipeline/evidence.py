from dataclasses import dataclass
from typing import Any, Dict, List

import pandas as pd


@dataclass
class EvidenceResult:
    insights: Dict[str, Any]
    summary: Dict[str, Any]
    evidence_table: pd.DataFrame


def _choose_citations(insight: str) -> List[str]:
    t = insight.lower()
    cites: List[str] = []
    if "revenue growth" in t or "growth" in t:
        cites.extend(["financials.revenue_growth_yoy", "signals.growth_profile"])
    if "ev/revenue" in t or "ev / revenue" in t:
        cites.extend(["financials.ev_revenue", "comparable_analysis.peer_median_ev_revenue"])
    if "ev/ebitda" in t or "ev / ebitda" in t:
        cites.extend(["financials.ev_ebitda", "comparable_analysis.peer_median_ev_ebitda"])
    if "precedent" in t or "transaction" in t:
        cites.extend(["precedent_transactions.transaction_count", "precedent_transactions.median_ev_ebitda"])
    if "margin" in t:
        cites.extend(["financials.ebitda_margin", "signals.margin_profile"])
    if "dcf" in t:
        cites.extend(["dcf_analysis.implied_ev_base", "dcf_analysis.dcf_gap_to_current"])
    if "blend" in t:
        cites.extend(["blended_valuation.blended_implied_ev", "blended_valuation.blend_gap_to_current"])
    if not cites:
        cites = ["signals.valuation_position", "financials.enterprise_value"]
    # Deduplicate while preserving order.
    seen = set()
    uniq = []
    for c in cites:
        if c not in seen:
            uniq.append(c)
            seen.add(c)
    return uniq


def apply_evidence_citations(insights: Dict[str, Any]) -> EvidenceResult:
    key_insights = insights.get("key_insights", [])
    enriched = []
    rows = []
    with_citation = 0
    total_citations = 0
    for i, text in enumerate(key_insights, start=1):
        cites = _choose_citations(text)
        cite_text = ", ".join(cites)
        enriched_text = f"{text} [refs: {cite_text}]"
        enriched.append(enriched_text)
        rows.append(
            {
                "insight_index": i,
                "insight_text": text,
                "citations": cite_text,
                "citation_count": len(cites),
            }
        )
        if cites:
            with_citation += 1
            total_citations += len(cites)

    out = dict(insights)
    out["key_insights"] = enriched
    summary = {
        "total_insights": len(key_insights),
        "insights_with_citations": with_citation,
        "total_citations": total_citations,
        "citation_coverage_pct": (with_citation / len(key_insights)) if key_insights else 0.0,
    }
    return EvidenceResult(insights=out, summary=summary, evidence_table=pd.DataFrame(rows))
