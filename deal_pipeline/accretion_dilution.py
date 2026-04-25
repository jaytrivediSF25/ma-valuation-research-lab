from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd

from .config import PipelineConfig


@dataclass
class AccretionDilutionResult:
    summary: Dict[str, Any]
    scenario_table: pd.DataFrame


def _f(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return float(value)
    except Exception:
        return None


def _pick_buyer_row(target_row: pd.Series, company_metrics: pd.DataFrame, config: PipelineConfig) -> pd.Series:
    pool = company_metrics.copy()
    pool = pool[pool["ticker"] != target_row.get("ticker")]
    if config.buyer_ticker:
        picked = pool[pool["ticker"].astype(str).str.upper() == config.buyer_ticker.upper()]
        if not picked.empty:
            return picked.iloc[0]
    ranked = pool.sort_values(by=["enterprise_value", "market_cap", "revenue"], ascending=[False, False, False], na_position="last")
    if ranked.empty:
        return target_row
    return ranked.iloc[0]


def _approx_net_income(row: pd.Series, tax_rate: float) -> float:
    revenue = _f(row.get("revenue")) or 0.0
    ebitda = _f(row.get("ebitda")) or 0.0
    depreciation = 0.03 * revenue
    ebit = ebitda - depreciation
    interest = _f(row.get("interest_expense"))
    if interest is None:
        debt = _f(row.get("total_debt")) or 0.0
        interest = debt * 0.055
    ebt = ebit - interest
    return ebt * (1.0 - tax_rate)


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def run_accretion_dilution_analysis(
    target_row: pd.Series,
    company_metrics: pd.DataFrame,
    config: PipelineConfig,
) -> AccretionDilutionResult:
    buyer = _pick_buyer_row(target_row, company_metrics, config)

    tax_rate = _clamp(config.dcf_tax_rate, 0.0, 0.5)
    premium = _clamp(config.deal_premium_pct, 0.0, 1.0)
    synergy_pct = _clamp(config.synergy_ebitda_pct_target, 0.0, 0.5)
    debt_pct = _clamp(config.financing_debt_pct, 0.0, 1.0)
    cash_pct = _clamp(config.financing_cash_pct, 0.0, 1.0)
    equity_pct = _clamp(config.financing_equity_pct, 0.0, 1.0)
    pct_total = debt_pct + cash_pct + equity_pct
    if pct_total <= 0:
        debt_pct, cash_pct, equity_pct, pct_total = 0.5, 0.2, 0.3, 1.0
    debt_pct, cash_pct, equity_pct = debt_pct / pct_total, cash_pct / pct_total, equity_pct / pct_total

    target_ev = _f(target_row.get("enterprise_value")) or 0.0
    target_revenue = _f(target_row.get("revenue")) or 0.0
    target_ebitda = _f(target_row.get("ebitda")) or 0.0
    buyer_ebitda = _f(buyer.get("ebitda")) or 0.0
    buyer_debt = _f(buyer.get("total_debt")) or 0.0
    buyer_cash = _f(buyer.get("cash")) or 0.0
    target_debt = _f(target_row.get("total_debt")) or 0.0
    target_cash = _f(target_row.get("cash")) or 0.0

    deal_ev = target_ev * (1.0 + premium)
    debt_funding = deal_ev * debt_pct
    cash_funding = deal_ev * cash_pct
    equity_funding = deal_ev * equity_pct

    buyer_market_cap = _f(buyer.get("market_cap")) or 0.0
    buyer_shares = _f(buyer.get("shares_outstanding"))
    buyer_share_price = _f(buyer.get("implied_share_price_current"))
    if buyer_share_price is None or buyer_share_price <= 0:
        if buyer_shares and buyer_shares > 0:
            buyer_share_price = buyer_market_cap / buyer_shares
        else:
            buyer_share_price = (buyer_market_cap / 1_000_000_000.0) if buyer_market_cap > 0 else 100.0
    if buyer_shares is None or buyer_shares <= 0:
        buyer_shares = buyer_market_cap / buyer_share_price if buyer_share_price > 0 else 1.0

    shares_issued = equity_funding / buyer_share_price if buyer_share_price > 0 else 0.0
    proforma_shares = buyer_shares + shares_issued

    buyer_ni = _approx_net_income(buyer, tax_rate)
    target_ni = _approx_net_income(target_row, tax_rate)
    synergy_ebitda = target_ebitda * synergy_pct
    synergy_after_tax = synergy_ebitda * (1.0 - tax_rate)
    integration_cost = target_revenue * _clamp(config.integration_cost_pct_revenue, 0.0, 0.15)
    integration_after_tax = integration_cost * (1.0 - tax_rate)
    incr_interest = debt_funding * _clamp(config.assumed_interest_rate, 0.01, 0.20)
    incr_interest_after_tax = incr_interest * (1.0 - tax_rate)

    proforma_ni = buyer_ni + target_ni + synergy_after_tax - incr_interest_after_tax - integration_after_tax
    buyer_eps = (buyer_ni / buyer_shares) if buyer_shares > 0 else None
    proforma_eps = (proforma_ni / proforma_shares) if proforma_shares > 0 else None
    eps_accretion = ((proforma_eps / buyer_eps) - 1.0) if (buyer_eps not in {None, 0} and proforma_eps is not None) else None

    proforma_net_debt = (buyer_debt + target_debt + debt_funding) - (buyer_cash + target_cash + cash_funding)
    proforma_ebitda = buyer_ebitda + target_ebitda + synergy_ebitda
    net_leverage = (proforma_net_debt / proforma_ebitda) if proforma_ebitda > 0 else None

    scenarios: List[Dict[str, Any]] = []
    scenario_defs = [
        ("downside", -0.20, +0.05),
        ("base", 0.00, 0.00),
        ("upside", +0.20, -0.05),
    ]
    for name, syn_shift, prem_shift in scenario_defs:
        syn = max(0.0, synergy_pct * (1.0 + syn_shift))
        prem_s = max(0.0, premium * (1.0 + prem_shift))
        deal_ev_s = target_ev * (1.0 + prem_s)
        debt_funding_s = deal_ev_s * debt_pct
        equity_funding_s = deal_ev_s * equity_pct
        shares_issued_s = equity_funding_s / buyer_share_price if buyer_share_price > 0 else 0.0
        proforma_shares_s = buyer_shares + shares_issued_s
        synergy_after_tax_s = (target_ebitda * syn) * (1.0 - tax_rate)
        incr_interest_after_tax_s = (debt_funding_s * _clamp(config.assumed_interest_rate, 0.01, 0.20)) * (1.0 - tax_rate)
        proforma_ni_s = buyer_ni + target_ni + synergy_after_tax_s - incr_interest_after_tax_s - integration_after_tax
        proforma_eps_s = (proforma_ni_s / proforma_shares_s) if proforma_shares_s > 0 else None
        eps_accretion_s = ((proforma_eps_s / buyer_eps) - 1.0) if (buyer_eps not in {None, 0} and proforma_eps_s is not None) else None
        scenarios.append(
            {
                "scenario": name,
                "premium_pct": prem_s,
                "synergy_ebitda_pct_target": syn,
                "proforma_eps": proforma_eps_s,
                "eps_accretion_dilution": eps_accretion_s,
            }
        )

    scenario_table = pd.DataFrame(scenarios)
    summary = {
        "buyer_ticker": buyer.get("ticker"),
        "target_ticker": target_row.get("ticker"),
        "deal_enterprise_value": deal_ev,
        "debt_funding": debt_funding,
        "cash_funding": cash_funding,
        "equity_funding": equity_funding,
        "shares_issued": shares_issued,
        "buyer_eps": buyer_eps,
        "proforma_eps": proforma_eps,
        "eps_accretion_dilution": eps_accretion,
        "proforma_net_leverage": net_leverage,
    }
    return AccretionDilutionResult(summary=summary, scenario_table=scenario_table)
