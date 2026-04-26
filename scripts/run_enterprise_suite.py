#!/usr/bin/env python3
import argparse
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from deal_pipeline import PipelineConfig, run_pipeline


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run full pipeline including enterprise suite")
    p.add_argument("--data-dir", default="./data")
    p.add_argument("--output-dir", default="./output")
    p.add_argument("--target-ticker", default="ABT")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    cfg = PipelineConfig(
        data_dir=Path(args.data_dir).resolve(),
        output_dir=Path(args.output_dir).resolve(),
        target_ticker=args.target_ticker,
        enable_enterprise_suite=True,
    )
    out = run_pipeline(cfg)
    print(f"JSON: {out.export_artifacts.report_json_path}")
    print(f"XLSX: {out.export_artifacts.workbook_path}")
    print(f"Enterprise dir: {out.diagnostic.get('enterprise_output_dir')}")


if __name__ == "__main__":
    main()
