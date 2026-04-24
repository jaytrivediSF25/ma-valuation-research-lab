from dataclasses import dataclass
from typing import Any, Dict

from .analysis import run_comparable_analysis, run_precedent_analysis
from .config import PipelineConfig
from .export import ExportArtifacts, export_outputs
from .feature_engineering import engineer_features, select_target_company
from .ingestion import ingest_data
from .insights import generate_ai_insights, generate_signals
from .normalization import normalize_data


@dataclass
class PipelineRunResult:
    export_artifacts: ExportArtifacts
    diagnostic: Dict[str, Any]


def run_pipeline(config: PipelineConfig) -> PipelineRunResult:
    ingested = ingest_data(config.data_dir)
    normalized = normalize_data(ingested)
    feature_output = engineer_features(normalized)
    company_metrics = feature_output.company_metrics
    if company_metrics.empty:
        raise RuntimeError("No engineered company metrics available. Check input datasets in ./data.")

    target_row = select_target_company(company_metrics, config)
    comps = run_comparable_analysis(target_row, company_metrics, normalized.peers)
    precedents = run_precedent_analysis(target_row, normalized.precedents, normalized.filings, company_metrics)
    signals = generate_signals(target_row, comps.summary, precedents.summary)

    structured_payload = {
        "company": {
            "name": target_row.get("company_name"),
            "ticker": target_row.get("ticker"),
            "cik": target_row.get("cik"),
            "sector": target_row.get("sector"),
        },
        "financials": {
            "revenue": target_row.get("revenue"),
            "revenue_growth_yoy": target_row.get("revenue_growth_yoy"),
            "ebitda": target_row.get("ebitda"),
            "ebitda_margin": target_row.get("ebitda_margin"),
            "enterprise_value": target_row.get("enterprise_value"),
            "ev_revenue": target_row.get("ev_revenue"),
            "ev_ebitda": target_row.get("ev_ebitda"),
        },
        "comparable_analysis": comps.summary,
        "precedent_transactions": precedents.summary,
        "signals": signals,
    }
    insights = generate_ai_insights(structured_payload, config.openai_model)

    exports = export_outputs(
        config=config,
        target_row=target_row,
        comps_summary=comps.summary,
        precedents_summary=precedents.summary,
        signals=signals,
        insights=insights,
        comps_table=comps.peer_table,
        precedents_table=precedents.precedent_table,
        raw_data_table=normalized.raw_data_export,
    )

    diagnostic = {
        "companies_loaded": int(len(normalized.companies)),
        "filings_loaded": int(len(normalized.filings)),
        "companyfacts_loaded": int(len(normalized.companyfacts)),
        "external_financial_rows": int(len(normalized.financials)),
        "external_peer_rows": int(len(normalized.peers)),
        "external_precedent_rows": int(len(normalized.precedents)),
        "peer_count_used": int(comps.summary.get("peer_count", 0)),
        "precedent_count_used": int(precedents.summary.get("transaction_count", 0)),
    }
    return PipelineRunResult(export_artifacts=exports, diagnostic=diagnostic)
