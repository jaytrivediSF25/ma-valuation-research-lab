from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from .config import PipelineConfig


def _fmt_money(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        if pd.isna(value):
            return "n/a"
    except Exception:
        pass
    try:
        return f"${float(value):,.0f}"
    except Exception:
        return "n/a"


def _fmt_pct(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        if pd.isna(value):
            return "n/a"
    except Exception:
        pass
    try:
        return f"{100.0 * float(value):.1f}%"
    except Exception:
        return "n/a"


def _fmt_mult(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        if pd.isna(value):
            return "n/a"
    except Exception:
        pass
    try:
        return f"{float(value):.2f}x"
    except Exception:
        return "n/a"


def build_markdown_memo(
    config: PipelineConfig,
    structured_report: Dict[str, Any],
    diagnostics: Dict[str, Any],
) -> Path:
    company = structured_report["company"]
    financials = structured_report["financials"]
    comps = structured_report["comparable_analysis"]
    precedents = structured_report["precedent_transactions"]
    signals = structured_report["signals"]
    insights = structured_report["insights"]
    quality = structured_report.get("data_quality", {})
    scenarios = structured_report.get("valuation_scenarios", {})
    dcf = structured_report.get("dcf_analysis", {})
    capital_structure = structured_report.get("capital_structure", {})
    robustness = structured_report.get("robustness", {})
    blend = structured_report.get("blended_valuation", {})
    acc_dil = structured_report.get("accretion_dilution", {})
    lbo = structured_report.get("lbo_underwriting", {})
    market_data = structured_report.get("market_data", {})

    lines: List[str] = []
    lines.append(f"# Deal Analysis Memo: {company.get('name') or company.get('ticker')}")
    lines.append("")
    lines.append(
        f"_Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')} | "
        f"Target: {company.get('ticker')} | CIK: {company.get('cik')}_"
    )
    lines.append("")
    lines.append("## Executive Conclusion")
    lines.append("")
    lines.append(f"- **Conclusion**: {insights.get('conclusion')}")
    lines.append(f"- **Primary Risk**: {insights.get('primary_risk')}")
    lines.append(f"- **Valuation Signal**: {signals.get('valuation_position')}")
    lines.append(f"- **Precedent Positioning**: {signals.get('precedent_comparison')}")
    lines.append("")
    lines.append("## Financial Snapshot")
    lines.append("")
    lines.append(f"- Revenue: {_fmt_money(financials.get('revenue'))}")
    lines.append(f"- Revenue Growth (YoY): {_fmt_pct(financials.get('revenue_growth_yoy'))}")
    lines.append(f"- EBITDA: {_fmt_money(financials.get('ebitda'))}")
    lines.append(f"- EBITDA Margin: {_fmt_pct(financials.get('ebitda_margin'))}")
    lines.append(f"- Enterprise Value: {_fmt_money(financials.get('enterprise_value'))}")
    lines.append(f"- EV/Revenue: {_fmt_mult(financials.get('ev_revenue'))}")
    lines.append(f"- EV/EBITDA: {_fmt_mult(financials.get('ev_ebitda'))}")
    lines.append("")
    lines.append("## Comparable Company Analysis")
    lines.append("")
    lines.append(f"- Peer count: {comps.get('peer_count')}")
    lines.append(f"- Peer median EV/Revenue: {_fmt_mult(comps.get('peer_median_ev_revenue'))}")
    lines.append(f"- Peer median EV/EBITDA: {_fmt_mult(comps.get('peer_median_ev_ebitda'))}")
    lines.append(f"- Target EV/Revenue percentile: {_fmt_pct(comps.get('percentile_ev_revenue'))}")
    lines.append(f"- Target EV/EBITDA percentile: {_fmt_pct(comps.get('percentile_ev_ebitda'))}")
    lines.append("")
    lines.append("## Precedent Transaction Analysis")
    lines.append("")
    lines.append(f"- Transaction count: {precedents.get('transaction_count')}")
    lines.append(f"- Median EV/Revenue: {_fmt_mult(precedents.get('median_ev_revenue'))}")
    lines.append(f"- Median EV/EBITDA: {_fmt_mult(precedents.get('median_ev_ebitda'))}")
    lines.append(f"- Implied valuation range: {_fmt_money(precedents.get('valuation_range_low'))} to {_fmt_money(precedents.get('valuation_range_high'))}")
    lines.append("")
    lines.append("## Valuation Scenarios")
    lines.append("")
    lines.append(f"- Scenario count: {scenarios.get('scenario_count')}")
    lines.append(f"- Implied EV low/base/high: {_fmt_money(scenarios.get('implied_ev_low'))} / {_fmt_money(scenarios.get('implied_ev_base'))} / {_fmt_money(scenarios.get('implied_ev_high'))}")
    lines.append(f"- Gap to base case vs current EV: {_fmt_pct(scenarios.get('gap_to_base'))}")
    lines.append("")
    lines.append("## DCF Analysis")
    lines.append("")
    lines.append(f"- DCF implied EV low/base/high: {_fmt_money(dcf.get('implied_ev_low'))} / {_fmt_money(dcf.get('implied_ev_base'))} / {_fmt_money(dcf.get('implied_ev_high'))}")
    lines.append(f"- DCF gap to current EV: {_fmt_pct(dcf.get('dcf_gap_to_current'))}")
    lines.append(f"- DCF implied equity value (base): {_fmt_money(dcf.get('implied_equity_value_base'))}")
    lines.append(f"- DCF implied share price (base): {_fmt_money(dcf.get('implied_share_price_base'))}")
    lines.append("")
    lines.append("## Capital Structure Bridge")
    lines.append("")
    lines.append(f"- Net debt (base): {_fmt_money(capital_structure.get('net_debt_base'))}")
    lines.append(f"- Debt years modeled: {capital_structure.get('debt_years_modeled', 0)}")
    lines.append(f"- Tax shield PV (base): {_fmt_money(capital_structure.get('tax_shield_pv_base'))}")
    lines.append("")
    lines.append("## Robustness Diagnostics")
    lines.append("")
    lines.append(f"- Comps EV/Revenue CI: {_fmt_mult(robustness.get('comps_ev_revenue_ci_low'))} to {_fmt_mult(robustness.get('comps_ev_revenue_ci_high'))}")
    lines.append(f"- Target EV/Revenue z-score: {_fmt_mult(robustness.get('target_ev_revenue_zscore'))}")
    lines.append(f"- Target EV/EBITDA z-score: {_fmt_mult(robustness.get('target_ev_ebitda_zscore'))}")
    lines.append("")
    lines.append("## Blended Valuation Synthesis")
    lines.append("")
    lines.append(f"- Blended implied EV: {_fmt_money(blend.get('blended_implied_ev'))}")
    lines.append(f"- Gap vs current EV: {_fmt_pct(blend.get('blend_gap_to_current'))}")
    lines.append(f"- Synthesis stance: {blend.get('blend_stance', 'neutral')}")
    lines.append("")
    lines.append("## Accretion / Dilution")
    lines.append("")
    lines.append(f"- Buyer ticker: {acc_dil.get('buyer_ticker')}")
    lines.append(f"- Deal enterprise value: {_fmt_money(acc_dil.get('deal_enterprise_value'))}")
    lines.append(f"- EPS accretion/dilution: {_fmt_pct(acc_dil.get('eps_accretion_dilution'))}")
    lines.append(f"- Pro forma net leverage: {_fmt_mult(acc_dil.get('proforma_net_leverage'))}")
    lines.append("")
    lines.append("## LBO Underwriting Snapshot")
    lines.append("")
    lines.append(f"- Entry EV / Exit EV: {_fmt_money(lbo.get('entry_ev'))} / {_fmt_money(lbo.get('exit_ev'))}")
    lines.append(f"- Entry equity / Exit equity: {_fmt_money(lbo.get('entry_equity'))} / {_fmt_money(lbo.get('exit_equity'))}")
    lines.append(f"- MOIC / IRR: {_fmt_mult(lbo.get('moic'))} / {_fmt_pct(lbo.get('irr'))}")
    lines.append(f"- Exit net leverage: {_fmt_mult(lbo.get('exit_net_leverage'))}")
    lines.append("")
    lines.append("## Live Market Data")
    lines.append("")
    lines.append(f"- Connector status: {market_data.get('status')}")
    lines.append(f"- Target live price: {_fmt_money(market_data.get('target_price'))}")
    lines.append(f"- Target live market cap: {_fmt_money(market_data.get('target_market_cap_live'))}")
    lines.append("")
    lines.append("## Data Quality")
    lines.append("")
    lines.append(f"- Data quality score: {quality.get('score', 'n/a')}")
    issue_list = quality.get("issues", [])
    if issue_list:
        lines.append("- Quality issues:")
        for issue in issue_list:
            lines.append(f"  - {issue}")
    else:
        lines.append("- Quality issues: none")
    lines.append("")
    lines.append("## Key Insights")
    lines.append("")
    for idx, text in enumerate(insights.get("key_insights", []), start=1):
        lines.append(f"{idx}. {text}")
    lines.append("")
    lines.append("## Pipeline Diagnostics")
    lines.append("")
    for key, value in diagnostics.items():
        lines.append(f"- {key}: {value}")
    lines.append("")

    memo_name = f"deal_analysis_{(company.get('ticker') or 'target').upper()}_memo.md"
    memo_path = config.output_dir / memo_name
    memo_path.write_text("\n".join(lines), encoding="utf-8")
    return memo_path
