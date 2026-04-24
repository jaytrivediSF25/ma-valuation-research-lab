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
