#!/usr/bin/env python3
import argparse
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from deal_pipeline import PipelineConfig, run_pipeline
from deal_pipeline.role_packs import generate_role_packs


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
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
    )
    res = run_pipeline(cfg)
    report = res.export_artifacts.final_report.model_dump(mode="json")
    packs = generate_role_packs(cfg.output_dir, report)
    print(f"Role pack dir: {packs.pack_dir}")
    for k, v in packs.files.items():
        print(f"- {k}: {v}")


if __name__ == "__main__":
    main()
