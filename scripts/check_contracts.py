#!/usr/bin/env python3
import argparse
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from deal_pipeline.analysis import run_comparable_analysis, run_precedent_analysis
from deal_pipeline.data_contracts import assert_contract_suite, run_contract_suite
from deal_pipeline.feature_engineering import engineer_features, select_target_company
from deal_pipeline.ingestion import ingest_data
from deal_pipeline.normalization import normalize_data
from deal_pipeline.config import PipelineConfig


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run strict input/output contract checks")
    p.add_argument("--data-dir", default="./data")
    p.add_argument("--output-dir", default="./output")
    p.add_argument("--target-ticker", default="ABT")
    p.add_argument("--strict", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    cfg = PipelineConfig(
        data_dir=Path(args.data_dir).resolve(),
        output_dir=Path(args.output_dir).resolve(),
        target_ticker=args.target_ticker,
    )

    ing = ingest_data(cfg.data_dir)
    norm = normalize_data(ing)
    feats = engineer_features(norm).company_metrics
    target = select_target_company(feats, cfg)
    comps = run_comparable_analysis(target, feats, norm.peers)
    precs = run_precedent_analysis(target, norm.precedents, norm.filings, feats)

    frames = {
        "companies": norm.companies,
        "filings": norm.filings,
        "companyfacts": norm.companyfacts,
        "financials": norm.financials,
        "peers": norm.peers,
        "precedents": norm.precedents,
        "company_metrics": feats,
        "comps_output": comps.peer_table,
        "precedents_output": precs.precedent_table,
    }

    if args.strict:
        table = assert_contract_suite(frames)
    else:
        table = run_contract_suite(frames)

    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    out_path = cfg.output_dir / "contract_check_report.csv"
    table.to_csv(out_path, index=False)
    failed = int((table["status"] == "fail").sum()) if not table.empty else 0
    print(f"Contracts checked: {len(table)}")
    print(f"Failures: {failed}")
    print(f"Report: {out_path}")
    if args.strict and failed > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
