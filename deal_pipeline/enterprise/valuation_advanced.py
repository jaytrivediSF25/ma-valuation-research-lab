from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd


@dataclass
class MonteCarloResult:
    summary: Dict[str, Any]
    samples: pd.DataFrame


@dataclass
class BayesianMultipleResult:
    posterior_mean: float
    posterior_std: float


def monte_carlo_ev(revenue: float, ebitda_margin: float, n: int = 5000, seed: int = 42) -> MonteCarloResult:
    rng = np.random.default_rng(seed)
    growth = rng.normal(0.07, 0.04, n)
    margins = np.clip(rng.normal(ebitda_margin, 0.03, n), 0.05, 0.6)
    wacc = np.clip(rng.normal(0.10, 0.02, n), 0.05, 0.2)
    exit_mult = np.clip(rng.normal(11.0, 2.0, n), 5.0, 20.0)

    ebitda_next = revenue * (1.0 + growth) * margins
    ev = ebitda_next * exit_mult / (1.0 + wacc)
    samples = pd.DataFrame({"growth": growth, "margin": margins, "wacc": wacc, "exit_multiple": exit_mult, "ev": ev})

    summary = {
        "n": n,
        "ev_p10": float(np.quantile(ev, 0.10)),
        "ev_p50": float(np.quantile(ev, 0.50)),
        "ev_p90": float(np.quantile(ev, 0.90)),
        "downside_prob_vs_base": float((ev < np.quantile(ev, 0.5)).mean()),
        "cvar_10": float(ev[ev <= np.quantile(ev, 0.10)].mean()),
    }
    return MonteCarloResult(summary=summary, samples=samples)


def bayesian_multiple_update(prior_mean: float, prior_std: float, observations: List[float], obs_std: float = 1.0) -> BayesianMultipleResult:
    if not observations:
        return BayesianMultipleResult(posterior_mean=prior_mean, posterior_std=prior_std)
    prior_var = prior_std ** 2
    obs_var = obs_std ** 2
    n = len(observations)
    obs_mean = float(np.mean(observations))

    post_var = 1.0 / ((1.0 / prior_var) + (n / obs_var))
    post_mean = post_var * ((prior_mean / prior_var) + (n * obs_mean / obs_var))
    return BayesianMultipleResult(posterior_mean=float(post_mean), posterior_std=float(np.sqrt(post_var)))


def regime_adjusted_multiple(base_multiple: float, vol: float, rates: float, spread: float) -> float:
    vol_adj = -0.4 * max(0.0, vol - 0.25)
    rate_adj = -0.2 * max(0.0, rates - 0.04)
    spread_adj = -0.3 * max(0.0, spread - 0.02)
    return float(max(2.0, base_multiple * (1.0 + vol_adj + rate_adj + spread_adj)))


def macro_shock_scenarios(base_revenue: float, base_margin: float) -> pd.DataFrame:
    rows = [
        {"scenario": "base", "rev_shock": 0.0, "margin_shock": 0.0, "rate_shock": 0.0},
        {"scenario": "recession", "rev_shock": -0.10, "margin_shock": -0.03, "rate_shock": 0.01},
        {"scenario": "stagflation", "rev_shock": -0.06, "margin_shock": -0.04, "rate_shock": 0.02},
        {"scenario": "soft_landing", "rev_shock": 0.04, "margin_shock": 0.01, "rate_shock": -0.01},
    ]
    frame = pd.DataFrame(rows)
    frame["revenue"] = base_revenue * (1.0 + frame["rev_shock"])
    frame["ebitda_margin"] = (base_margin + frame["margin_shock"]).clip(lower=0.02)
    frame["ebitda"] = frame["revenue"] * frame["ebitda_margin"]
    frame["implied_ev"] = frame["ebitda"] * (10.0 - 5.0 * frame["rate_shock"])
    return frame


def segment_valuation_bridge(segments: pd.DataFrame) -> pd.DataFrame:
    frame = segments.copy()
    if frame.empty:
        return frame
    frame["segment_ev"] = pd.to_numeric(frame["segment_ebitda"], errors="coerce") * pd.to_numeric(frame["segment_multiple"], errors="coerce")
    frame["segment_weight"] = frame["segment_ev"] / frame["segment_ev"].sum()
    return frame


def synergy_decomposition(target_revenue: float, target_ebitda: float) -> pd.DataFrame:
    blocks = pd.DataFrame(
        [
            {"block": "cost_synergy", "value": 0.04 * target_ebitda, "probability": 0.85},
            {"block": "revenue_synergy", "value": 0.02 * target_revenue, "probability": 0.60},
            {"block": "dis_synergy", "value": -0.01 * target_revenue, "probability": 0.35},
            {"block": "integration_cost", "value": -0.015 * target_revenue, "probability": 1.00},
        ]
    )
    blocks["risk_weighted_value"] = blocks["value"] * blocks["probability"]
    return blocks


def normalize_reported_ebitda(reported_ebitda: float, adjustments: List[float]) -> float:
    return float(reported_ebitda + sum(adjustments))


def real_usd_conversion(nominal: pd.Series, fx_to_usd: pd.Series, cpi_index: pd.Series, base_cpi: float) -> pd.Series:
    usd = nominal * fx_to_usd
    real = usd * (base_cpi / cpi_index)
    return real


def debt_tranche_metrics(tranches: pd.DataFrame) -> Dict[str, float]:
    frame = tranches.copy()
    frame["balance"] = pd.to_numeric(frame["balance"], errors="coerce").fillna(0.0)
    frame["coupon"] = pd.to_numeric(frame["coupon"], errors="coerce").fillna(0.0)
    total = float(frame["balance"].sum())
    if total == 0:
        return {"weighted_coupon": 0.0, "total_debt": 0.0}
    weighted_coupon = float((frame["balance"] * frame["coupon"]).sum() / total)
    return {"weighted_coupon": weighted_coupon, "total_debt": total}


def covenant_breach_probability(cases: pd.DataFrame, max_net_leverage: float, min_interest_cov: float) -> float:
    frame = cases.copy()
    bad = (pd.to_numeric(frame["net_leverage"], errors="coerce") > max_net_leverage) | (
        pd.to_numeric(frame["interest_coverage"], errors="coerce") < min_interest_cov
    )
    return float(bad.mean()) if len(frame) else 0.0


def lbo_waterfall(exit_equity: float, pref_return: float, mgmt_pool: float) -> pd.DataFrame:
    pref_paid = min(exit_equity, pref_return)
    residual = max(0.0, exit_equity - pref_paid)
    mgmt_paid = residual * mgmt_pool
    sponsor_paid = residual - mgmt_paid
    return pd.DataFrame(
        [
            {"bucket": "preferred", "value": pref_paid},
            {"bucket": "management", "value": mgmt_paid},
            {"bucket": "sponsor", "value": sponsor_paid},
        ]
    )


def optimize_transaction_mix(target_ev: float, leverage_limit: float = 4.5) -> Dict[str, float]:
    debt = min(target_ev * 0.6, leverage_limit * (target_ev / 10.0))
    equity = max(0.0, target_ev - debt)
    cash = min(target_ev * 0.15, equity * 0.5)
    equity = max(0.0, equity - cash)
    return {
        "debt": float(debt),
        "cash": float(cash),
        "equity": float(equity),
    }


def qoe_score(metrics: Dict[str, float]) -> float:
    accruals = metrics.get("accruals_ratio", 0.1)
    cash_conv = metrics.get("cash_conversion", 0.8)
    margin_vol = metrics.get("margin_volatility", 0.1)
    adj_intensity = metrics.get("adjustment_intensity", 0.15)
    raw = 100.0 - 120.0 * accruals + 25.0 * cash_conv - 70.0 * margin_vol - 60.0 * adj_intensity
    return float(min(100.0, max(0.0, raw)))
