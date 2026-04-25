from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from .config import PipelineConfig
from .feature_engineering import engineer_features
from .ingestion import ingest_data
from .normalization import normalize_data
from .pipeline import run_pipeline


@dataclass
class BatchScreenResult:
    csv_path: Path
    json_path: Path
    rows: int


def _score_row(row: Dict[str, Any]) -> float:
    gap = row.get("blend_gap_to_current")
    quality = row.get("data_quality_score")
    risk_count = row.get("risk_flag_count", 0)
    val = 0.0
    if gap is not None:
        val += float(gap) * 100.0
    if quality is not None:
        val += float(quality) * 0.15
    val -= float(risk_count) * 5.0
    return float(val)


def run_portfolio_batch_screen(config: PipelineConfig) -> BatchScreenResult:
    config.ensure_directories()
    ingested = ingest_data(config.data_dir)
    normalized = normalize_data(ingested)
    features = engineer_features(normalized).company_metrics
    if features.empty:
        raise RuntimeError("No company metrics available for batch screening.")

    universe = features.sort_values(["enterprise_value", "revenue"], ascending=[False, False], na_position="last")
    universe = universe.head(max(1, int(config.batch_top_n)))

    rows: List[Dict[str, Any]] = []
    for _, candidate in universe.iterrows():
        ticker = str(candidate.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        cfg = replace(
            config,
            target_ticker=ticker,
            enable_markdown_memo=False,
        )
        result = run_pipeline(cfg)
        report = result.export_artifacts.final_report.model_dump(mode="json")
        rows.append(
            {
                "ticker": report["company"].get("ticker"),
                "company_name": report["company"].get("name"),
                "sector": report["company"].get("sector"),
                "blend_gap_to_current": report["blended_valuation"].get("blend_gap_to_current"),
                "blend_stance": report["blended_valuation"].get("blend_stance"),
                "valuation_position": report["signals"].get("valuation_position"),
                "data_quality_score": report["data_quality"].get("score"),
                "risk_flag_count": len(report["signals"].get("risk_flags", [])),
                "primary_risk": report["insights"].get("primary_risk"),
                "conclusion": report.get("conclusion"),
            }
        )

    screen = pd.DataFrame(rows)
    if screen.empty:
        raise RuntimeError("Batch screen produced no rows.")
    screen["screen_score"] = screen.apply(lambda r: _score_row(r.to_dict()), axis=1)
    screen = screen.sort_values("screen_score", ascending=False).reset_index(drop=True)
    screen.insert(0, "rank", range(1, len(screen) + 1))

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    csv_path = config.output_dir / f"portfolio_screen_{ts}.csv"
    json_path = config.output_dir / f"portfolio_screen_{ts}.json"
    screen.to_csv(csv_path, index=False)
    json_path.write_text(screen.to_json(orient="records", indent=2), encoding="utf-8")
    return BatchScreenResult(csv_path=csv_path, json_path=json_path, rows=int(len(screen)))
