#!/usr/bin/env python3
import argparse
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from deal_pipeline.feature_engineering import engineer_features
from deal_pipeline.ingestion import ingest_data
from deal_pipeline.normalization import normalize_data


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--data-dir", default="./data")
    p.add_argument("--output-dir", default="./output")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    data_dir = Path(args.data_dir).resolve()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    ing = ingest_data(data_dir)
    norm = normalize_data(ing)
    feats = engineer_features(norm).company_metrics

    out_csv = output_dir / "feature_snapshot.csv"
    feats.to_csv(out_csv, index=False)
    print(f"Feature rows: {len(feats)}")
    print(f"Feature snapshot: {out_csv}")


if __name__ == "__main__":
    main()
