from dataclasses import dataclass
from typing import Any, Dict

from .accretion_dilution import run_accretion_dilution_analysis
from .analysis import run_comparable_analysis, run_precedent_analysis
from .blended_valuation import build_blended_valuation
from .config import PipelineConfig
from .dcf import run_dcf_analysis
from .export import ExportArtifacts, export_outputs
from .feature_engineering import engineer_features, select_target_company
from .ingestion import ingest_data
from .insights import generate_ai_insights, generate_signals
from .ic_pack import create_ic_pack
from .lbo import run_lbo_underwriting
from .lineage import build_lineage_report
from .market_data import fetch_market_data_context
from .memo import build_markdown_memo
from .normalization import normalize_data
from .precedent_curation import curate_precedent_transactions
from .quality import evaluate_data_quality
from .robustness import compute_robustness_metrics
from .scenarios import build_valuation_scenarios
from .sector_packs import apply_sector_pack
from .validation import run_model_validation_suite


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
    runtime_config, sector_pack_summary, sector_pack_table = apply_sector_pack(config, str(target_row.get("sector") or ""))
    comps = run_comparable_analysis(target_row, company_metrics, normalized.peers)
    precedents = run_precedent_analysis(target_row, normalized.precedents, normalized.filings, company_metrics)
    precedent_curation = curate_precedent_transactions(target_row, precedents.precedent_table)
    signals = generate_signals(target_row, comps.summary, precedents.summary, config=runtime_config)
    quality = evaluate_data_quality(company_metrics, comps.summary, precedents.summary, config=runtime_config)
    scenarios = build_valuation_scenarios(target_row, comps.summary, precedents.summary)
    dcf = run_dcf_analysis(target_row, config=runtime_config)
    robustness = compute_robustness_metrics(comps.peer_table, precedents.precedent_table, target_row)
    acc_dil = run_accretion_dilution_analysis(target_row, company_metrics, config=runtime_config)
    lbo = run_lbo_underwriting(target_row, config=runtime_config)
    market_data = fetch_market_data_context(target_row, comps.peer_table, config=runtime_config)
    blended = build_blended_valuation(
        target_row=target_row,
        comps_summary=comps.summary,
        precedents_summary=precedents.summary,
        scenarios_summary=scenarios.summary,
        dcf_summary=dcf.summary,
        config=runtime_config,
    )
    validation = run_model_validation_suite(
        target_row=target_row,
        comps_summary=comps.summary,
        precedents_summary=precedents.summary,
        robustness_summary=robustness.summary,
        quality_summary={"score": quality.score},
        dcf_summary=dcf.summary,
    )
    lineage = build_lineage_report(
        target_row=target_row,
        additional_sections={
            "dcf_analysis": dcf.summary,
            "capital_structure": dcf.capital_structure_summary,
            "blended_valuation": blended.summary,
            "accretion_dilution": acc_dil.summary,
            "lbo_underwriting": lbo.summary,
            "validation": validation.summary,
        },
    )

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
            "market_cap": target_row.get("market_cap"),
            "total_debt": target_row.get("total_debt"),
            "cash": target_row.get("cash"),
            "net_debt": target_row.get("net_debt"),
            "shares_outstanding": target_row.get("shares_outstanding"),
            "interest_expense": target_row.get("interest_expense"),
            "implied_share_price_current": target_row.get("implied_share_price_current"),
        },
        "comparable_analysis": comps.summary,
        "precedent_transactions": precedents.summary,
        "signals": signals,
        "data_quality": {
            "score": quality.score,
            "checks": quality.checks,
            "issues": quality.issues,
        },
        "valuation_scenarios": scenarios.summary,
        "dcf_analysis": dcf.summary,
        "capital_structure": dcf.capital_structure_summary,
        "robustness": robustness.summary,
        "blended_valuation": blended.summary,
        "accretion_dilution": acc_dil.summary,
        "lbo_underwriting": lbo.summary,
        "market_data": market_data.summary,
        "precedent_curation": precedent_curation.summary,
        "sector_pack": sector_pack_summary,
        "lineage": lineage.summary,
        "validation": validation.summary,
    }
    insights = generate_ai_insights(structured_payload, config.openai_model)
    report_for_pack = {**structured_payload, "insights": insights, "conclusion": insights.get("conclusion")}
    ic_pack = create_ic_pack(
        config=config,
        report_payload=report_for_pack,
        comps_table=comps.peer_table,
        precedents_table=precedents.precedent_table,
        scenarios_table=scenarios.scenario_table,
        dcf_table=dcf.dcf_table,
    )
    structured_payload["ic_pack"] = ic_pack.summary

    diagnostic = {
        "companies_loaded": int(len(normalized.companies)),
        "filings_loaded": int(len(normalized.filings)),
        "companyfacts_loaded": int(len(normalized.companyfacts)),
        "external_financial_rows": int(len(normalized.financials)),
        "external_peer_rows": int(len(normalized.peers)),
        "external_precedent_rows": int(len(normalized.precedents)),
        "peer_count_used": int(comps.summary.get("peer_count", 0)),
        "precedent_count_used": int(precedents.summary.get("transaction_count", 0)),
        "data_quality_score": quality.score,
        "valuation_scenario_count": int(scenarios.summary.get("scenario_count", 0)),
        "dcf_case_count": int(dcf.summary.get("case_count", 0)),
        "dcf_implied_share_price_base": dcf.summary.get("implied_share_price_base"),
        "eps_accretion_dilution": acc_dil.summary.get("eps_accretion_dilution"),
        "lbo_irr": lbo.summary.get("irr"),
        "market_data_status": market_data.summary.get("status"),
        "precedent_curated_count": precedent_curation.summary.get("curated_transaction_count"),
        "sector_pack": sector_pack_summary.get("sector_pack"),
        "lineage_row_count": lineage.summary.get("lineage_row_count"),
        "validation_score": validation.summary.get("validation_score"),
        "ic_pack_dir": ic_pack.summary.get("pack_dir"),
        "blend_stance": blended.summary.get("blend_stance"),
    }

    exports = export_outputs(
        config=config,
        target_row=target_row,
        comps_summary=comps.summary,
        precedents_summary=precedents.summary,
        signals=signals,
        data_quality={
            "score": quality.score,
            "checks": quality.checks,
            "issues": quality.issues,
        },
        valuation_scenarios=scenarios.summary,
        dcf_summary=dcf.summary,
        capital_structure_summary=dcf.capital_structure_summary,
        robustness_summary=robustness.summary,
        blended_valuation_summary=blended.summary,
        accretion_dilution_summary=acc_dil.summary,
        lbo_summary=lbo.summary,
        market_data_summary=market_data.summary,
        precedent_curation_summary=precedent_curation.summary,
        sector_pack_summary=sector_pack_summary,
        lineage_summary=lineage.summary,
        validation_summary=validation.summary,
        ic_pack_summary=ic_pack.summary,
        insights=insights,
        comps_table=comps.peer_table,
        precedents_table=precedents.precedent_table,
        scenario_table=scenarios.scenario_table,
        dcf_table=dcf.dcf_table,
        dcf_sensitivity_table=dcf.sensitivity_table,
        debt_schedule_table=dcf.debt_schedule_table,
        capital_bridge_table=dcf.capital_bridge_table,
        robustness_table=robustness.robustness_table,
        blend_table=blended.blend_table,
        accretion_dilution_table=acc_dil.scenario_table,
        lbo_table=lbo.lbo_table,
        market_data_table=market_data.quotes_table,
        precedent_curation_table=precedent_curation.curated_table,
        sector_pack_table=sector_pack_table,
        lineage_table=lineage.lineage_table,
        validation_table=validation.validation_table,
        quality_table=quality.check_table,
        raw_data_table=normalized.raw_data_export,
        diagnostics=diagnostic,
    )

    if config.enable_markdown_memo:
        memo_path = build_markdown_memo(
            config=config,
            structured_report={**structured_payload, "insights": insights},
            diagnostics=diagnostic,
        )
        diagnostic["memo_path"] = str(memo_path)

    return PipelineRunResult(export_artifacts=exports, diagnostic=diagnostic)
