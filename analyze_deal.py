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
    parser.add_argument("--blend-weight-comps", type=float, default=0.35, help="Blend weight assigned to comps anchor")
    parser.add_argument("--blend-weight-precedents", type=float, default=0.25, help="Blend weight assigned to precedents anchor")
    parser.add_argument("--blend-weight-scenarios", type=float, default=0.20, help="Blend weight assigned to scenarios anchor")
    parser.add_argument("--blend-weight-dcf", type=float, default=0.20, help="Blend weight assigned to DCF anchor")
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
        blend_weight_comps=args.blend_weight_comps,
        blend_weight_precedents=args.blend_weight_precedents,
        blend_weight_scenarios=args.blend_weight_scenarios,
        blend_weight_dcf=args.blend_weight_dcf,
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
