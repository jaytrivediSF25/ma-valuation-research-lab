from pathlib import Path
from typing import Optional

from deal_pipeline import PipelineConfig, run_pipeline

try:
    from prefect import flow
except Exception as exc:  # pragma: no cover
    raise RuntimeError("prefect is required for flows/pipeline_flow.py") from exc


@flow(name="ma-valuation-pipeline")
def run_pipeline_flow(
    data_dir: str = "./data",
    output_dir: str = "./output",
    target_ticker: Optional[str] = None,
    target_cik: Optional[str] = None,
    target_company: Optional[str] = None,
    openai_model: str = "gpt-4o-mini",
    enable_market_data: bool = False,
    enable_duckdb_store: bool = True,
    enable_pandera_validation: bool = True,
    enable_blend_optimizer: bool = True,
    duckdb_path: Optional[str] = None,
) -> dict:
    config = PipelineConfig(
        data_dir=Path(data_dir).resolve(),
        output_dir=Path(output_dir).resolve(),
        target_ticker=target_ticker,
        target_cik=target_cik,
        target_company=target_company,
        openai_model=openai_model,
        enable_market_data=enable_market_data,
        enable_duckdb_store=enable_duckdb_store,
        enable_pandera_validation=enable_pandera_validation,
        enable_blend_optimizer=enable_blend_optimizer,
        duckdb_path=Path(duckdb_path).resolve() if duckdb_path else None,
    )
    result = run_pipeline(config)
    return {
        "report_json_path": str(result.export_artifacts.report_json_path),
        "workbook_path": str(result.export_artifacts.workbook_path),
        "diagnostics": result.diagnostic,
    }


if __name__ == "__main__":
    run_pipeline_flow()
