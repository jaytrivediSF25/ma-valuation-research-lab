from dataclasses import dataclass
from typing import Any, Dict

import pandas as pd

from .accretion_dilution import run_accretion_dilution_analysis
from .analysis import run_comparable_analysis, run_precedent_analysis
from .blended_valuation import build_blended_valuation
from .backtesting import run_historical_backtest
from .config import PipelineConfig
from .contracts import ContractValidationResult, validate_data_contracts
from .dcf import run_dcf_analysis
from .duckdb_store import persist_to_duckdb
from .export import ExportArtifacts, export_outputs
from .feature_engineering import engineer_features, select_target_company
from .ingestion import ingest_data
from .insights import generate_ai_insights, generate_signals
from .evidence import apply_evidence_citations
from .ic_pack import create_ic_pack
from .lbo import run_lbo_underwriting
from .lineage import build_lineage_report
from .market_data import fetch_market_data_context
from .memo import build_markdown_memo
from .normalization import normalize_data
from .observability import RunLogger
from .enterprise import run_enterprise_suite
from .precedent_curation import curate_precedent_transactions
from .quality import evaluate_data_quality
from .role_packs import generate_role_packs
from .robustness import compute_robustness_metrics
from .scenarios import build_valuation_scenarios
from .sector_packs import apply_sector_pack
from .sensitivity import run_full_sensitivity
from .strategic import build_buyer_universe, build_negotiation_playbook, run_deal_risk_gate
from .validation import run_model_validation_suite


@dataclass
class PipelineRunResult:
    export_artifacts: ExportArtifacts
    diagnostic: Dict[str, Any]


def run_pipeline(config: PipelineConfig) -> PipelineRunResult:
    obs = RunLogger(config.output_dir)
    with obs.timed("ingestion"):
        ingested = ingest_data(config.data_dir)
    with obs.timed("normalization"):
        normalized = normalize_data(ingested)
    with obs.timed("feature_engineering"):
        feature_output = engineer_features(normalized)
    company_metrics = feature_output.company_metrics
    if company_metrics.empty:
        raise RuntimeError("No engineered company metrics available. Check input datasets in ./data.")

    with obs.timed("target_selection"):
        target_row = select_target_company(company_metrics, config)
    with obs.timed("sector_pack"):
        runtime_config, sector_pack_summary, sector_pack_table = apply_sector_pack(config, str(target_row.get("sector") or ""))
    with obs.timed("comps_analysis"):
        comps = run_comparable_analysis(target_row, company_metrics, normalized.peers)
    with obs.timed("precedent_analysis"):
        precedents = run_precedent_analysis(target_row, normalized.precedents, normalized.filings, company_metrics)
    with obs.timed("precedent_curation"):
        precedent_curation = curate_precedent_transactions(target_row, precedents.precedent_table)
    if config.enable_pandera_validation:
        with obs.timed("pandera_contracts"):
            contract_validation = validate_data_contracts(
                company_metrics=company_metrics,
                precedents_table=precedent_curation.curated_table,
            )
    else:
        contract_validation = ContractValidationResult(
            summary={"contracts_checked": 0, "contracts_failed": 0, "contracts_skipped": 1},
            table=pd.DataFrame([{"contract": "pipeline_contracts", "status": "skipped", "detail": "validation_disabled"}]),
        )
    with obs.timed("signals"):
        signals = generate_signals(target_row, comps.summary, precedents.summary, config=runtime_config)
    with obs.timed("quality"):
        quality = evaluate_data_quality(company_metrics, comps.summary, precedents.summary, config=runtime_config)
    with obs.timed("scenarios"):
        scenarios = build_valuation_scenarios(target_row, comps.summary, precedents.summary)
    with obs.timed("dcf"):
        dcf = run_dcf_analysis(target_row, config=runtime_config)
    with obs.timed("robustness"):
        robustness = compute_robustness_metrics(comps.peer_table, precedents.precedent_table, target_row)
    with obs.timed("accretion_dilution"):
        acc_dil = run_accretion_dilution_analysis(target_row, company_metrics, config=runtime_config)
    with obs.timed("lbo"):
        lbo = run_lbo_underwriting(target_row, config=runtime_config)
    with obs.timed("market_data"):
        market_data = fetch_market_data_context(target_row, comps.peer_table, config=runtime_config)
    with obs.timed("blended"):
        blended = build_blended_valuation(
            target_row=target_row,
            comps_summary=comps.summary,
            precedents_summary=precedents.summary,
            scenarios_summary=scenarios.summary,
            dcf_summary=dcf.summary,
            config=runtime_config,
        )
    with obs.timed("validation"):
        validation = run_model_validation_suite(
            target_row=target_row,
            comps_summary=comps.summary,
            precedents_summary=precedents.summary,
            robustness_summary=robustness.summary,
            quality_summary={"score": quality.score},
            dcf_summary=dcf.summary,
        )
    with obs.timed("sensitivity"):
        sensitivity = run_full_sensitivity(target_row)
    with obs.timed("backtesting"):
        backtest = run_historical_backtest(precedent_curation.curated_table)
    with obs.timed("buyer_universe"):
        buyer_universe = build_buyer_universe(target_row=target_row, company_metrics=company_metrics, peer_table=comps.peer_table)
    with obs.timed("risk_gate"):
        risk_gate = run_deal_risk_gate(
            target_row=target_row,
            comps_summary=comps.summary,
            precedents_summary=precedents.summary,
            dcf_summary=dcf.summary,
            quality_score=quality.score,
            validation_summary=validation.summary,
            sensitivity_summary=sensitivity.summary,
        )
    with obs.timed("negotiation_playbook"):
        negotiation = build_negotiation_playbook(
            target_row=target_row,
            blended_summary=blended.summary,
            precedents_summary=precedents.summary,
            sensitivity_summary=sensitivity.summary,
        )
    with obs.timed("lineage"):
        lineage = build_lineage_report(
            target_row=target_row,
            additional_sections={
                "dcf_analysis": dcf.summary,
                "capital_structure": dcf.capital_structure_summary,
                "blended_valuation": blended.summary,
                "accretion_dilution": acc_dil.summary,
                "lbo_underwriting": lbo.summary,
                "validation": validation.summary,
                "sensitivity": sensitivity.summary,
                "backtest": backtest.summary,
                "buyer_universe": buyer_universe.summary,
                "risk_gate": risk_gate.summary,
                "negotiation_playbook": negotiation.summary,
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
        "contract_validation": contract_validation.summary,
        "sensitivity": sensitivity.summary,
        "backtest": backtest.summary,
        "buyer_universe": buyer_universe.summary,
        "risk_gate": risk_gate.summary,
        "negotiation_playbook": negotiation.summary,
    }
    insights_raw = generate_ai_insights(structured_payload, config.openai_model)
    evidence = apply_evidence_citations(insights_raw)
    insights = evidence.insights
    report_for_pack = {**structured_payload, "insights": insights, "evidence_citations": evidence.summary, "conclusion": insights.get("conclusion")}
    ic_pack = create_ic_pack(
        config=config,
        report_payload=report_for_pack,
        comps_table=comps.peer_table,
        precedents_table=precedents.precedent_table,
        scenarios_table=scenarios.scenario_table,
        dcf_table=dcf.dcf_table,
    )
    structured_payload["ic_pack"] = ic_pack.summary
    structured_payload["evidence_citations"] = evidence.summary

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
        "citation_coverage_pct": evidence.summary.get("citation_coverage_pct"),
        "blend_stance": blended.summary.get("blend_stance"),
        "blend_optimizer_status": blended.summary.get("blend_optimizer_status"),
        "contracts_checked": contract_validation.summary.get("contracts_checked"),
        "contracts_failed": contract_validation.summary.get("contracts_failed"),
        "contracts_skipped": contract_validation.summary.get("contracts_skipped"),
        "sensitivity_scenario_count": sensitivity.summary.get("scenario_count"),
        "backtest_rows": backtest.summary.get("rows"),
        "backtest_mae_forecast_error_pct": backtest.summary.get("mae_forecast_error_pct"),
        "buyer_universe_count": buyer_universe.summary.get("buyer_count"),
        "top_buyer": buyer_universe.summary.get("top_buyer"),
        "risk_gate_overall": risk_gate.summary.get("overall_gate"),
        "negotiation_walk_away_ev": negotiation.summary.get("walk_away_ev"),
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
        evidence_summary=evidence.summary,
        contract_validation_summary=contract_validation.summary,
        sensitivity_summary=sensitivity.summary,
        backtest_summary=backtest.summary,
        buyer_universe_summary=buyer_universe.summary,
        risk_gate_summary=risk_gate.summary,
        negotiation_summary=negotiation.summary,
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
        evidence_table=evidence.evidence_table,
        quality_table=quality.check_table,
        contract_validation_table=contract_validation.table,
        sensitivity_grid_table=sensitivity.grid_table,
        sensitivity_tornado_table=sensitivity.tornado_table,
        backtest_table=backtest.backtest_table,
        buyer_universe_table=buyer_universe.buyer_table,
        risk_gate_table=risk_gate.gate_table,
        negotiation_table=negotiation.playbook_table,
        raw_data_table=normalized.raw_data_export,
        diagnostics=diagnostic,
    )

    if config.enable_duckdb_store:
        duckdb_path = config.duckdb_path or (config.output_dir / "warehouse" / "deal_pipeline.duckdb")
        duckdb_result = persist_to_duckdb(
            db_path=duckdb_path,
            tables={
                "company_metrics": company_metrics,
                "comps": comps.peer_table,
                "precedents_curated": precedent_curation.curated_table,
                "dcf": dcf.dcf_table,
                "blend": blended.blend_table,
                "validation": validation.validation_table,
                "evidence": evidence.evidence_table,
            },
        )
        diagnostic["duckdb_path"] = str(duckdb_result.db_path)
        diagnostic["duckdb_tables_written"] = len(duckdb_result.tables_written)
        diagnostic["duckdb_rows_written"] = int(sum(duckdb_result.tables_written.values())) if duckdb_result.tables_written else 0

    if config.enable_markdown_memo:
        with obs.timed("memo"):
            memo_path = build_markdown_memo(
                config=config,
                structured_report={**structured_payload, "insights": insights},
                diagnostics=diagnostic,
            )
        diagnostic["memo_path"] = str(memo_path)

    with obs.timed("role_packs"):
        role_packs = generate_role_packs(config.output_dir, exports.final_report.model_dump(mode="json"))
    diagnostic["role_pack_dir"] = str(role_packs.pack_dir)
    diagnostic["role_pack_files"] = len(role_packs.files)

    if config.enable_enterprise_suite:
        with obs.timed("enterprise_suite"):
            enterprise = run_enterprise_suite(
                output_dir=config.output_dir,
                data_dir=config.data_dir,
                report_payload=exports.final_report.model_dump(mode="json"),
                company_metrics=company_metrics,
                precedents=precedent_curation.curated_table,
            )
        diagnostic["enterprise_output_dir"] = str(enterprise.output_dir)
        diagnostic["enterprise_readiness_tier"] = enterprise.summary.get("readiness_tier")
        diagnostic["enterprise_run_key"] = enterprise.summary.get("run_key")

    obs_path = obs.finalize(extra=diagnostic)
    diagnostic["observability_path"] = str(obs_path)

    return PipelineRunResult(export_artifacts=exports, diagnostic=diagnostic)
