#!/usr/bin/env python3
import argparse
from pathlib import Path

from deal_pipeline import PipelineConfig, run_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run local M&A analytics pipeline.")
    parser.add_argument("--data-dir", default="./data", help="Input data directory")
    parser.add_argument("--output-dir", default="./output", help="Output directory")
    parser.add_argument("--target-ticker", default=None, help="Optional target ticker")
    parser.add_argument("--target-cik", default=None, help="Optional target CIK")
    parser.add_argument("--target-company", default=None, help="Optional target company name")
    parser.add_argument("--openai-model", default="gpt-4o-mini", help="OpenAI model for insights")
    parser.add_argument(
        "--max-raw-rows-for-excel",
        type=int,
        default=200000,
        help="Row cap for raw_data Excel sheet",
    )
    parser.add_argument("--min-peer-count", type=int, default=5, help="Minimum peer count quality threshold")
    parser.add_argument("--min-precedent-count", type=int, default=5, help="Minimum precedent count quality threshold")
    parser.add_argument("--low-growth-threshold", type=float, default=0.03, help="Growth threshold for low/medium")
    parser.add_argument("--high-growth-threshold", type=float, default=0.15, help="Growth threshold for medium/high")
    parser.add_argument("--weak-margin-threshold", type=float, default=0.12, help="Margin threshold for weak/average")
    parser.add_argument("--strong-margin-threshold", type=float, default=0.25, help="Margin threshold for average/strong")
    parser.add_argument("--premium-multiple-buffer", type=float, default=0.15, help="Buffer above peer multiple median for premium")
    parser.add_argument("--discounted-multiple-buffer", type=float, default=0.15, help="Buffer below peer multiple median for discounted")
    parser.add_argument("--dcf-projection-years", type=int, default=5, help="Number of years in DCF projection model")
    parser.add_argument("--dcf-wacc-base", type=float, default=0.10, help="Base WACC used in DCF")
    parser.add_argument("--dcf-terminal-growth-base", type=float, default=0.025, help="Base terminal growth rate for DCF")
    parser.add_argument("--dcf-tax-rate", type=float, default=0.24, help="Tax rate used in DCF NOPAT")
    parser.add_argument("--dcf-depreciation-pct-revenue", type=float, default=0.03, help="Depreciation as %% of revenue for DCF")
    parser.add_argument("--dcf-capex-pct-revenue", type=float, default=0.035, help="Capex as %% of revenue for DCF")
    parser.add_argument("--dcf-nwc-pct-revenue", type=float, default=0.015, help="NWC investment as %% of revenue for DCF")
    parser.add_argument("--debt-amortization-rate", type=float, default=0.08, help="Annual debt amortization rate for debt schedule")
    parser.add_argument("--fallback-interest-rate", type=float, default=0.055, help="Fallback interest rate when interest expense is missing")
    parser.add_argument("--interest-rate-floor", type=float, default=0.02, help="Lower bound on inferred interest rate")
    parser.add_argument("--interest-rate-cap", type=float, default=0.14, help="Upper bound on inferred interest rate")
    parser.add_argument("--blend-weight-comps", type=float, default=0.35, help="Blend weight assigned to comps anchor")
    parser.add_argument("--blend-weight-precedents", type=float, default=0.25, help="Blend weight assigned to precedents anchor")
    parser.add_argument("--blend-weight-scenarios", type=float, default=0.20, help="Blend weight assigned to scenarios anchor")
    parser.add_argument("--blend-weight-dcf", type=float, default=0.20, help="Blend weight assigned to DCF anchor")
    parser.add_argument("--buyer-ticker", default=None, help="Optional buyer ticker for accretion/dilution model")
    parser.add_argument("--deal-premium-pct", type=float, default=0.25, help="Premium percentage applied to target EV in deal model")
    parser.add_argument("--synergy-ebitda-pct-target", type=float, default=0.05, help="Synergy EBITDA as %% of target EBITDA")
    parser.add_argument("--financing-debt-pct", type=float, default=0.50, help="Debt financing share of deal funds")
    parser.add_argument("--financing-cash-pct", type=float, default=0.20, help="Cash financing share of deal funds")
    parser.add_argument("--financing-equity-pct", type=float, default=0.30, help="Equity financing share of deal funds")
    parser.add_argument("--assumed-interest-rate", type=float, default=0.06, help="Interest rate used for incremental acquisition debt")
    parser.add_argument("--integration-cost-pct-revenue", type=float, default=0.01, help="One-time integration cost as %% of target revenue")
    parser.add_argument("--lbo-holding-years", type=int, default=5, help="LBO holding period in years")
    parser.add_argument("--lbo-entry-multiple", type=float, default=10.0, help="Entry EV/EBITDA multiple")
    parser.add_argument("--lbo-exit-multiple", type=float, default=11.0, help="Exit EV/EBITDA multiple")
    parser.add_argument("--lbo-ebitda-growth", type=float, default=0.06, help="Annual EBITDA growth in LBO model")
    parser.add_argument("--lbo-capex-pct-ebitda", type=float, default=0.18, help="Capex as %% of EBITDA in LBO")
    parser.add_argument("--lbo-cash-tax-pct-ebitda", type=float, default=0.20, help="Cash taxes as %% of EBITDA in LBO")
    parser.add_argument("--lbo-senior-debt-multiple", type=float, default=3.0, help="Senior debt at entry as EBITDA multiple")
    parser.add_argument("--lbo-mezz-debt-multiple", type=float, default=1.0, help="Mezz debt at entry as EBITDA multiple")
    parser.add_argument("--lbo-senior-interest-rate", type=float, default=0.075, help="Senior debt interest rate in LBO")
    parser.add_argument("--lbo-mezz-interest-rate", type=float, default=0.11, help="Mezz debt interest rate in LBO")
    parser.add_argument("--enable-market-data", action="store_true", help="Enable live market data connectors")
    parser.add_argument("--market-data-lookback-days", type=int, default=180, help="Lookback window for market data context")
    parser.add_argument(
        "--disable-markdown-memo",
        action="store_true",
        help="Disable generation of markdown investment memo output",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = PipelineConfig(
        data_dir=Path(args.data_dir).resolve(),
        output_dir=Path(args.output_dir).resolve(),
        target_ticker=args.target_ticker,
        target_cik=args.target_cik,
        target_company=args.target_company,
        openai_model=args.openai_model,
        max_raw_rows_for_excel=args.max_raw_rows_for_excel,
        min_peer_count=args.min_peer_count,
        min_precedent_count=args.min_precedent_count,
        low_growth_threshold=args.low_growth_threshold,
        high_growth_threshold=args.high_growth_threshold,
        weak_margin_threshold=args.weak_margin_threshold,
        strong_margin_threshold=args.strong_margin_threshold,
        premium_multiple_buffer=args.premium_multiple_buffer,
        discounted_multiple_buffer=args.discounted_multiple_buffer,
        dcf_projection_years=args.dcf_projection_years,
        dcf_wacc_base=args.dcf_wacc_base,
        dcf_terminal_growth_base=args.dcf_terminal_growth_base,
        dcf_tax_rate=args.dcf_tax_rate,
        dcf_depreciation_pct_revenue=args.dcf_depreciation_pct_revenue,
        dcf_capex_pct_revenue=args.dcf_capex_pct_revenue,
        dcf_nwc_pct_revenue=args.dcf_nwc_pct_revenue,
        debt_amortization_rate=args.debt_amortization_rate,
        fallback_interest_rate=args.fallback_interest_rate,
        interest_rate_floor=args.interest_rate_floor,
        interest_rate_cap=args.interest_rate_cap,
        blend_weight_comps=args.blend_weight_comps,
        blend_weight_precedents=args.blend_weight_precedents,
        blend_weight_scenarios=args.blend_weight_scenarios,
        blend_weight_dcf=args.blend_weight_dcf,
        buyer_ticker=args.buyer_ticker,
        deal_premium_pct=args.deal_premium_pct,
        synergy_ebitda_pct_target=args.synergy_ebitda_pct_target,
        financing_debt_pct=args.financing_debt_pct,
        financing_cash_pct=args.financing_cash_pct,
        financing_equity_pct=args.financing_equity_pct,
        assumed_interest_rate=args.assumed_interest_rate,
        integration_cost_pct_revenue=args.integration_cost_pct_revenue,
        lbo_holding_years=args.lbo_holding_years,
        lbo_entry_multiple=args.lbo_entry_multiple,
        lbo_exit_multiple=args.lbo_exit_multiple,
        lbo_ebitda_growth=args.lbo_ebitda_growth,
        lbo_capex_pct_ebitda=args.lbo_capex_pct_ebitda,
        lbo_cash_tax_pct_ebitda=args.lbo_cash_tax_pct_ebitda,
        lbo_senior_debt_multiple=args.lbo_senior_debt_multiple,
        lbo_mezz_debt_multiple=args.lbo_mezz_debt_multiple,
        lbo_senior_interest_rate=args.lbo_senior_interest_rate,
        lbo_mezz_interest_rate=args.lbo_mezz_interest_rate,
        enable_market_data=args.enable_market_data,
        market_data_lookback_days=args.market_data_lookback_days,
        enable_markdown_memo=not args.disable_markdown_memo,
    )

    result = run_pipeline(config)
    artifacts = result.export_artifacts

    print("Pipeline complete.")
    print(f"JSON report: {artifacts.report_json_path}")
    print(f"Excel workbook: {artifacts.workbook_path}")
    print("Diagnostics:")
    for key, value in result.diagnostic.items():
        print(f"  - {key}: {value}")


if __name__ == "__main__":
    main()
