import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from .config import PipelineConfig


@dataclass
class ICPackResult:
    summary: Dict[str, Any]
    pack_dir: Path
    manifest_path: Path


def create_ic_pack(
    config: PipelineConfig,
    report_payload: Dict[str, Any],
    comps_table: pd.DataFrame,
    precedents_table: pd.DataFrame,
    scenarios_table: pd.DataFrame,
    dcf_table: pd.DataFrame,
) -> ICPackResult:
    ticker = str(report_payload.get("company", {}).get("ticker") or "TARGET").upper()
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    pack_dir = config.output_dir / f"ic_pack_{ticker}_{ts}"
    tables_dir = pack_dir / "tables"
    pack_dir.mkdir(parents=True, exist_ok=True)
    tables_dir.mkdir(parents=True, exist_ok=True)

    executive_text = [
        f"Company: {report_payload.get('company', {}).get('name')}",
        f"Ticker: {ticker}",
        f"Conclusion: {report_payload.get('conclusion')}",
        f"Primary risk: {report_payload.get('insights', {}).get('primary_risk')}",
        f"Valuation stance: {report_payload.get('signals', {}).get('valuation_position')}",
    ]
    (pack_dir / "executive_summary.txt").write_text("\n".join(executive_text), encoding="utf-8")
    (pack_dir / "report.json").write_text(json.dumps(report_payload, indent=2), encoding="utf-8")

    comps_path = tables_dir / "comps.csv"
    precedents_path = tables_dir / "precedents.csv"
    scenarios_path = tables_dir / "scenarios.csv"
    dcf_path = tables_dir / "dcf.csv"
    comps_table.to_csv(comps_path, index=False)
    precedents_table.to_csv(precedents_path, index=False)
    scenarios_table.to_csv(scenarios_path, index=False)
    dcf_table.to_csv(dcf_path, index=False)

    manifest = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "ticker": ticker,
        "files": {
            "executive_summary": str(pack_dir / "executive_summary.txt"),
            "report_json": str(pack_dir / "report.json"),
            "comps_csv": str(comps_path),
            "precedents_csv": str(precedents_path),
            "scenarios_csv": str(scenarios_path),
            "dcf_csv": str(dcf_path),
        },
    }
    manifest_path = pack_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return ICPackResult(
        summary={"generated": True, "pack_dir": str(pack_dir), "manifest_path": str(manifest_path)},
        pack_dir=pack_dir,
        manifest_path=manifest_path,
    )
