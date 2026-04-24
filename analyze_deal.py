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
