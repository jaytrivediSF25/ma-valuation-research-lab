import json
import os
from typing import Any, Dict, List, Optional

import pandas as pd

from .config import PipelineConfig
from .schemas import InsightSet


def generate_signals(
    target_row: pd.Series,
    comps_summary: Dict[str, Optional[float]],
    precedents_summary: Dict[str, Optional[float]],
    config: Optional[PipelineConfig] = None,
) -> Dict[str, Any]:
    low_growth_threshold = config.low_growth_threshold if config else 0.03
    high_growth_threshold = config.high_growth_threshold if config else 0.15
    weak_margin_threshold = config.weak_margin_threshold if config else 0.12
    strong_margin_threshold = config.strong_margin_threshold if config else 0.25
    premium_multiple_buffer = config.premium_multiple_buffer if config else 0.15
    discounted_multiple_buffer = config.discounted_multiple_buffer if config else 0.15
    min_peer_count = config.min_peer_count if config else 5
    min_precedent_count = config.min_precedent_count if config else 5

    growth = target_row.get("revenue_growth_yoy")
    margin = target_row.get("ebitda_margin")
    target_ev_rev = target_row.get("ev_revenue")
    peer_median_ev_rev = comps_summary.get("peer_median_ev_revenue")
    valuation_low = precedents_summary.get("valuation_range_low")
    valuation_high = precedents_summary.get("valuation_range_high")
    target_ev = target_row.get("enterprise_value")

    if growth is None or pd.isna(growth):
        growth_profile = "medium"
    elif growth >= high_growth_threshold:
        growth_profile = "high"
    elif growth >= low_growth_threshold:
        growth_profile = "medium"
    else:
        growth_profile = "low"

    if margin is None or pd.isna(margin):
        margin_profile = "average"
    elif margin >= strong_margin_threshold:
        margin_profile = "strong"
    elif margin >= weak_margin_threshold:
        margin_profile = "average"
    else:
        margin_profile = "weak"

    if target_ev_rev is None or pd.isna(target_ev_rev) or peer_median_ev_rev is None or pd.isna(peer_median_ev_rev):
        valuation_position = "fair"
    elif target_ev_rev >= peer_median_ev_rev * (1.0 + premium_multiple_buffer):
        valuation_position = "premium"
    elif target_ev_rev <= peer_median_ev_rev * (1.0 - discounted_multiple_buffer):
        valuation_position = "discounted"
    else:
        valuation_position = "fair"

    if target_ev is None or pd.isna(target_ev) or valuation_low is None or valuation_high is None:
        precedent_comparison = "within"
    elif target_ev > valuation_high:
        precedent_comparison = "above"
    elif target_ev < valuation_low:
        precedent_comparison = "below_range"
    else:
        precedent_comparison = "within"

    risk_flags: List[str] = []
    if target_row.get("enterprise_value") is None or pd.isna(target_row.get("enterprise_value")):
        risk_flags.append("missing_enterprise_value")
    if target_row.get("ebitda") is not None and pd.notna(target_row.get("ebitda")) and float(target_row["ebitda"]) < 0:
        risk_flags.append("negative_ebitda")
    if growth_profile == "low":
        risk_flags.append("subscale_growth")
    if comps_summary.get("peer_count", 0) < min_peer_count:
        risk_flags.append("thin_peer_set")
    if precedents_summary.get("transaction_count", 0) < min_precedent_count:
        risk_flags.append("thin_precedent_set")

    return {
        "growth_profile": growth_profile,
        "margin_profile": margin_profile,
        "valuation_position": valuation_position,
        "precedent_comparison": precedent_comparison,
        "risk_flags": risk_flags,
    }


def _rule_based_insights(payload: Dict[str, Any]) -> Dict[str, Any]:
    financials = payload["financials"]
    comps = payload["comparable_analysis"]
    precedents = payload["precedent_transactions"]
    signals = payload["signals"]
    dcf = payload.get("dcf_analysis", {})
    capital_structure = payload.get("capital_structure", {})
    blend = payload.get("blended_valuation", {})
    robustness = payload.get("robustness", {})

    insights = []
    revenue_growth = financials.get("revenue_growth_yoy")
    if revenue_growth is not None:
        insights.append(f"Revenue growth YoY is {revenue_growth:.1%}, classified as {signals['growth_profile']} growth.")
    ev_rev = financials.get("ev_revenue")
    peer_ev_rev = comps.get("peer_median_ev_revenue")
    if ev_rev is not None and peer_ev_rev is not None:
        premium = (ev_rev / peer_ev_rev) - 1.0
        insights.append(
            f"EV/Revenue is {ev_rev:.2f}x versus peer median {peer_ev_rev:.2f}x ({premium:+.1%} relative spread)."
        )
    transaction_count = precedents.get("transaction_count")
    median_prec = precedents.get("median_ev_ebitda")
    if transaction_count and median_prec is not None:
        insights.append(
            f"Precedent sample includes {transaction_count} transactions with median EV/EBITDA of {median_prec:.2f}x."
        )
    elif transaction_count:
        insights.append(f"Precedent sample includes {transaction_count} transactions, but EBITDA-based multiples are sparse.")
    ebitda_margin = financials.get("ebitda_margin")
    if ebitda_margin is not None:
        insights.append(f"EBITDA margin is {ebitda_margin:.1%}, indicating a {signals['margin_profile']} margin profile.")
    dcf_gap = dcf.get("dcf_gap_to_current")
    if dcf_gap is not None:
        insights.append(f"DCF base case implies a {dcf_gap:+.1%} spread versus current enterprise value.")
    implied_share_price = dcf.get("implied_share_price_base")
    if implied_share_price is not None:
        insights.append(f"EV-to-equity bridge implies base share value of ${implied_share_price:,.2f}.")
    tax_shield = capital_structure.get("tax_shield_pv_base")
    if tax_shield is not None:
        insights.append(f"Projected debt tax shield contributes approximately ${tax_shield:,.0f} to valuation support.")
    blend_gap = blend.get("blend_gap_to_current")
    if blend_gap is not None:
        insights.append(f"Blended valuation synthesis indicates {blend_gap:+.1%} relative value versus current EV.")
    zscore = robustness.get("target_ev_revenue_zscore")
    if zscore is not None:
        insights.append(f"Target EV/Revenue z-score versus comps is {zscore:+.2f}, informing statistical valuation stretch.")

    insights = [line for line in insights if line][:4]
    if len(insights) < 2:
        insights.append("Core valuation metrics are available but partially incomplete; decisions should be made with supplemental disclosures.")
        insights.append("Peer and precedent context is directionally useful, with confidence scaling to sample quality.")

    primary_risk = "Limited precedent or peer coverage reduces valuation confidence."
    if signals["risk_flags"]:
        primary_risk = signals["risk_flags"][0].replace("_", " ").capitalize() + "."

    conclusion = (
        f"Overall profile is {signals['valuation_position']} versus comps and {signals['precedent_comparison']} precedent valuation range."
    )
    return {
        "key_insights": insights[:4],
        "primary_risk": primary_risk,
        "conclusion": conclusion,
    }


def _validate_insights(data: Dict[str, Any]) -> Dict[str, Any]:
    insight_set = InsightSet(**data)
    return insight_set.model_dump()


def generate_ai_insights(structured_payload: Dict[str, Any], model: str) -> Dict[str, Any]:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return _validate_insights(_rule_based_insights(structured_payload))

    try:
        from openai import OpenAI
    except Exception:
        return _validate_insights(_rule_based_insights(structured_payload))

    system_prompt = (
        "You are a finance analyst. Respond with valid JSON only, no markdown. "
        "Use this exact schema: {\"key_insights\": [2-4 concise strings], \"primary_risk\": string, \"conclusion\": string}. "
        "Ground every statement in provided data."
    )
    user_prompt = json.dumps(structured_payload, default=str)

    try:
        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model=model,
            temperature=0.1,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        content = response.choices[0].message.content or "{}"
        parsed = json.loads(content)
        return _validate_insights(parsed)
    except Exception:
        return _validate_insights(_rule_based_insights(structured_payload))
