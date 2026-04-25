from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field

from deal_pipeline import PipelineConfig, run_pipeline

try:
    from fastapi import FastAPI, HTTPException
except Exception as exc:  # pragma: no cover
    raise RuntimeError("fastapi is required for app/main.py") from exc


class DealRunRequest(BaseModel):
    data_dir: str = "./data"
    output_dir: str = "./output"
    target_ticker: Optional[str] = None
    target_cik: Optional[str] = None
    target_company: Optional[str] = None
    openai_model: str = "gpt-4o-mini"
    enable_market_data: bool = False
    enable_duckdb_store: bool = True
    enable_pandera_validation: bool = True
    enable_blend_optimizer: bool = True
    duckdb_path: Optional[str] = None


class DealRunResponse(BaseModel):
    report_json_path: str
    workbook_path: str
    diagnostics: dict = Field(default_factory=dict)


app = FastAPI(title="M&A Valuation Research Lab API", version="3.1.0")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/run", response_model=DealRunResponse)
def run_deal_pipeline(payload: DealRunRequest) -> DealRunResponse:
    config = PipelineConfig(
        data_dir=Path(payload.data_dir).resolve(),
        output_dir=Path(payload.output_dir).resolve(),
        target_ticker=payload.target_ticker,
        target_cik=payload.target_cik,
        target_company=payload.target_company,
        openai_model=payload.openai_model,
        enable_market_data=payload.enable_market_data,
        enable_duckdb_store=payload.enable_duckdb_store,
        enable_pandera_validation=payload.enable_pandera_validation,
        enable_blend_optimizer=payload.enable_blend_optimizer,
        duckdb_path=Path(payload.duckdb_path).resolve() if payload.duckdb_path else None,
    )
    try:
        result = run_pipeline(config)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return DealRunResponse(
        report_json_path=str(result.export_artifacts.report_json_path),
        workbook_path=str(result.export_artifacts.workbook_path),
        diagnostics=result.diagnostic,
    )
