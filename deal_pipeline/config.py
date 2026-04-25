from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class PipelineConfig:
    data_dir: Path
    output_dir: Path
    target_ticker: Optional[str] = None
    target_cik: Optional[str] = None
    target_company: Optional[str] = None
    openai_model: str = "gpt-4o-mini"
    max_raw_rows_for_excel: int = 200000
    min_peer_count: int = 5
    min_precedent_count: int = 5
    low_growth_threshold: float = 0.03
    high_growth_threshold: float = 0.15
    weak_margin_threshold: float = 0.12
    strong_margin_threshold: float = 0.25
    premium_multiple_buffer: float = 0.15
    discounted_multiple_buffer: float = 0.15
    enable_markdown_memo: bool = True
    dcf_projection_years: int = 5
    dcf_wacc_base: float = 0.10
    dcf_terminal_growth_base: float = 0.025
    dcf_tax_rate: float = 0.24
    dcf_depreciation_pct_revenue: float = 0.03
    dcf_capex_pct_revenue: float = 0.035
    dcf_nwc_pct_revenue: float = 0.015
    dcf_growth_floor: float = 0.01
    dcf_growth_cap: float = 0.20
    debt_amortization_rate: float = 0.08
    fallback_interest_rate: float = 0.055
    interest_rate_floor: float = 0.02
    interest_rate_cap: float = 0.14
    blend_weight_comps: float = 0.35
    blend_weight_precedents: float = 0.25
    blend_weight_scenarios: float = 0.20
    blend_weight_dcf: float = 0.20
    buyer_ticker: Optional[str] = None
    deal_premium_pct: float = 0.25
    synergy_ebitda_pct_target: float = 0.05
    financing_debt_pct: float = 0.50
    financing_cash_pct: float = 0.20
    financing_equity_pct: float = 0.30
    assumed_interest_rate: float = 0.06
    assumed_buyer_pe: float = 18.0
    integration_cost_pct_revenue: float = 0.01
    lbo_holding_years: int = 5
    lbo_entry_multiple: float = 10.0
    lbo_exit_multiple: float = 11.0
    lbo_ebitda_growth: float = 0.06
    lbo_capex_pct_ebitda: float = 0.18
    lbo_cash_tax_pct_ebitda: float = 0.20
    lbo_senior_debt_multiple: float = 3.0
    lbo_mezz_debt_multiple: float = 1.0
    lbo_senior_interest_rate: float = 0.075
    lbo_mezz_interest_rate: float = 0.11
    enable_market_data: bool = False
    market_data_lookback_days: int = 180
    batch_top_n: int = 25

    def ensure_directories(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
