import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from .config import PipelineConfig
from .schemas import (
    AccretionDilutionSummary,
    BlendedValuationSummary,
    CapitalStructureSummary,
    ComparableAnalysis,
    ContractValidationSummary,
    DataQuality,
    DCFSummary,
    EvidenceSummary,
    FinalReport,
    FinancialSnapshot,
    ICPackSummary,
    LBOSummary,
    LineageSummary,
    MarketDataSummary,
    PrecedentCurationSummary,
    PrecedentAnalysis,
    RobustnessSummary,
    SectorPackSummary,
    SignalSet,
    ValidationSummary,
    ValuationScenarioSummary,
)


@dataclass
class ExportArtifacts:
    report_json_path: Path
    workbook_path: Path
    final_report: FinalReport


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return float(value)
    except Exception:
        return None


def _build_summary_sheet(
    target_row: pd.Series,
    comps_summary: Dict[str, Any],
    precedents_summary: Dict[str, Any],
    signals: Dict[str, Any],
    data_quality: Dict[str, Any],
    valuation_scenarios: Dict[str, Any],
    dcf_summary: Dict[str, Any],
    capital_structure_summary: Dict[str, Any],
    robustness_summary: Dict[str, Any],
    blended_valuation_summary: Dict[str, Any],
    accretion_dilution_summary: Dict[str, Any],
    lbo_summary: Dict[str, Any],
    market_data_summary: Dict[str, Any],
    precedent_curation_summary: Dict[str, Any],
    sector_pack_summary: Dict[str, Any],
    lineage_summary: Dict[str, Any],
    validation_summary: Dict[str, Any],
    ic_pack_summary: Dict[str, Any],
    evidence_summary: Dict[str, Any],
    contract_validation_summary: Dict[str, Any],
    sensitivity_summary: Optional[Dict[str, Any]],
    backtest_summary: Optional[Dict[str, Any]],
    buyer_universe_summary: Optional[Dict[str, Any]],
    risk_gate_summary: Optional[Dict[str, Any]],
    negotiation_summary: Optional[Dict[str, Any]],
    arsenal_summary: Optional[Dict[str, Any]],
    arsenal300_summary: Optional[Dict[str, Any]],
    arsenal600_summary: Optional[Dict[str, Any]],
    arsenal_massive_summary: Optional[Dict[str, Any]],
    arsenal_extra50_summary: Optional[Dict[str, Any]],
    insights: Dict[str, Any],
) -> pd.DataFrame:
    rows = [
        ("company_name", target_row.get("company_name")),
        ("ticker", target_row.get("ticker")),
        ("cik", target_row.get("cik")),
        ("as_of_date", str(target_row.get("as_of_date"))),
        ("revenue", _as_float(target_row.get("revenue"))),
        ("revenue_growth_yoy", _as_float(target_row.get("revenue_growth_yoy"))),
        ("ebitda", _as_float(target_row.get("ebitda"))),
        ("ebitda_margin", _as_float(target_row.get("ebitda_margin"))),
        ("enterprise_value", _as_float(target_row.get("enterprise_value"))),
        ("ev_revenue", _as_float(target_row.get("ev_revenue"))),
        ("ev_ebitda", _as_float(target_row.get("ev_ebitda"))),
        ("peer_count", comps_summary.get("peer_count")),
        ("peer_median_ev_revenue", comps_summary.get("peer_median_ev_revenue")),
        ("peer_median_ev_ebitda", comps_summary.get("peer_median_ev_ebitda")),
        ("precedent_transaction_count", precedents_summary.get("transaction_count")),
        ("precedent_valuation_range_low", precedents_summary.get("valuation_range_low")),
        ("precedent_valuation_range_high", precedents_summary.get("valuation_range_high")),
        ("growth_profile", signals.get("growth_profile")),
        ("margin_profile", signals.get("margin_profile")),
        ("valuation_position", signals.get("valuation_position")),
        ("precedent_comparison", signals.get("precedent_comparison")),
        ("risk_flags", ", ".join(signals.get("risk_flags", []))),
        ("data_quality_score", data_quality.get("score")),
        ("data_quality_issues", ", ".join(data_quality.get("issues", []))),
        ("scenario_count", valuation_scenarios.get("scenario_count")),
        ("implied_ev_low", valuation_scenarios.get("implied_ev_low")),
        ("implied_ev_base", valuation_scenarios.get("implied_ev_base")),
        ("implied_ev_high", valuation_scenarios.get("implied_ev_high")),
        ("gap_to_base", valuation_scenarios.get("gap_to_base")),
        ("dcf_implied_ev_low", dcf_summary.get("implied_ev_low")),
        ("dcf_implied_ev_base", dcf_summary.get("implied_ev_base")),
        ("dcf_implied_ev_high", dcf_summary.get("implied_ev_high")),
        ("dcf_gap_to_current", dcf_summary.get("dcf_gap_to_current")),
        ("dcf_implied_share_price_base", dcf_summary.get("implied_share_price_base")),
        ("capital_structure_net_debt_base", capital_structure_summary.get("net_debt_base")),
        ("comps_ev_rev_ci_low", robustness_summary.get("comps_ev_revenue_ci_low")),
        ("comps_ev_rev_ci_high", robustness_summary.get("comps_ev_revenue_ci_high")),
        ("target_ev_rev_zscore", robustness_summary.get("target_ev_revenue_zscore")),
        ("blended_implied_ev", blended_valuation_summary.get("blended_implied_ev")),
        ("blend_gap_to_current", blended_valuation_summary.get("blend_gap_to_current")),
        ("blend_stance", blended_valuation_summary.get("blend_stance")),
        ("accretion_dilution_pct", accretion_dilution_summary.get("eps_accretion_dilution")),
        ("proforma_net_leverage", accretion_dilution_summary.get("proforma_net_leverage")),
        ("lbo_moic", lbo_summary.get("moic")),
        ("lbo_irr", lbo_summary.get("irr")),
        ("market_data_status", market_data_summary.get("status")),
        ("precedent_curated_count", precedent_curation_summary.get("curated_transaction_count")),
        ("sector_pack", sector_pack_summary.get("sector_pack")),
        ("lineage_row_count", lineage_summary.get("lineage_row_count")),
        ("validation_score", validation_summary.get("validation_score")),
        ("ic_pack_generated", ic_pack_summary.get("generated")),
        ("citation_coverage_pct", evidence_summary.get("citation_coverage_pct")),
        ("contracts_checked", contract_validation_summary.get("contracts_checked")),
        ("contracts_failed", contract_validation_summary.get("contracts_failed")),
        ("contracts_skipped", contract_validation_summary.get("contracts_skipped")),
        ("sensitivity_scenario_count", (sensitivity_summary or {}).get("scenario_count")),
        ("sensitivity_p50_ev", (sensitivity_summary or {}).get("probability_band_p50")),
        ("backtest_rows", (backtest_summary or {}).get("rows")),
        ("backtest_mae_error_pct", (backtest_summary or {}).get("mae_forecast_error_pct")),
        ("buyer_universe_count", (buyer_universe_summary or {}).get("buyer_count")),
        ("top_buyer", (buyer_universe_summary or {}).get("top_buyer")),
        ("risk_gate_overall", (risk_gate_summary or {}).get("overall_gate")),
        ("risk_gate_warn_count", (risk_gate_summary or {}).get("warn_count")),
        ("negotiation_opening_bid_ev", (negotiation_summary or {}).get("opening_bid_ev")),
        ("negotiation_walk_away_ev", (negotiation_summary or {}).get("walk_away_ev")),
        ("arsenal_idea_count", (arsenal_summary or {}).get("arsenal_idea_count")),
        ("arsenal_pass_count", (arsenal_summary or {}).get("arsenal_pass_count")),
        ("arsenal_readiness_pct", (arsenal_summary or {}).get("arsenal_readiness_pct")),
        ("arsenal300_idea_count", (arsenal300_summary or {}).get("arsenal300_idea_count")),
        ("arsenal300_pass_count", (arsenal300_summary or {}).get("arsenal300_pass_count")),
        ("arsenal300_readiness_pct", (arsenal300_summary or {}).get("arsenal300_readiness_pct")),
        ("arsenal300_top_risk_theme", (arsenal300_summary or {}).get("arsenal300_top_risk_theme")),
        ("arsenal600_idea_count", (arsenal600_summary or {}).get("arsenal600_idea_count")),
        ("arsenal600_pass_count", (arsenal600_summary or {}).get("arsenal600_pass_count")),
        ("arsenal600_readiness_pct", (arsenal600_summary or {}).get("arsenal600_readiness_pct")),
        ("arsenal600_top_risk_domain", (arsenal600_summary or {}).get("arsenal600_top_risk_domain")),
        ("arsenal_massive_idea_count", (arsenal_massive_summary or {}).get("arsenal_massive_idea_count")),
        ("arsenal_massive_pass_count", (arsenal_massive_summary or {}).get("arsenal_massive_pass_count")),
        ("arsenal_massive_readiness_pct", (arsenal_massive_summary or {}).get("arsenal_massive_readiness_pct")),
        ("arsenal_massive_top_risk_domain", (arsenal_massive_summary or {}).get("arsenal_massive_top_risk_domain")),
        ("arsenal_extra50_idea_count", (arsenal_extra50_summary or {}).get("arsenal_extra50_idea_count")),
        ("arsenal_extra50_pass_count", (arsenal_extra50_summary or {}).get("arsenal_extra50_pass_count")),
        ("arsenal_extra50_readiness_pct", (arsenal_extra50_summary or {}).get("arsenal_extra50_readiness_pct")),
        ("primary_risk", insights.get("primary_risk")),
        ("conclusion", insights.get("conclusion")),
    ]
    for i, line in enumerate(insights.get("key_insights", []), start=1):
        rows.append((f"key_insight_{i}", line))
    return pd.DataFrame(rows, columns=["metric", "value"])


def export_outputs(
    config: PipelineConfig,
    target_row: pd.Series,
    comps_summary: Dict[str, Any],
    precedents_summary: Dict[str, Any],
    signals: Dict[str, Any],
    data_quality: Dict[str, Any],
    valuation_scenarios: Dict[str, Any],
    dcf_summary: Dict[str, Any],
    capital_structure_summary: Dict[str, Any],
    robustness_summary: Dict[str, Any],
    blended_valuation_summary: Dict[str, Any],
    accretion_dilution_summary: Dict[str, Any],
    lbo_summary: Dict[str, Any],
    market_data_summary: Dict[str, Any],
    precedent_curation_summary: Dict[str, Any],
    sector_pack_summary: Dict[str, Any],
    lineage_summary: Dict[str, Any],
    validation_summary: Dict[str, Any],
    ic_pack_summary: Dict[str, Any],
    evidence_summary: Dict[str, Any],
    contract_validation_summary: Dict[str, Any],
    sensitivity_summary: Optional[Dict[str, Any]],
    backtest_summary: Optional[Dict[str, Any]],
    buyer_universe_summary: Optional[Dict[str, Any]],
    risk_gate_summary: Optional[Dict[str, Any]],
    negotiation_summary: Optional[Dict[str, Any]],
    arsenal_summary: Optional[Dict[str, Any]],
    arsenal300_summary: Optional[Dict[str, Any]],
    arsenal600_summary: Optional[Dict[str, Any]],
    arsenal_massive_summary: Optional[Dict[str, Any]],
    arsenal_extra50_summary: Optional[Dict[str, Any]],
    insights: Dict[str, Any],
    comps_table: pd.DataFrame,
    precedents_table: pd.DataFrame,
    scenario_table: pd.DataFrame,
    dcf_table: pd.DataFrame,
    dcf_sensitivity_table: pd.DataFrame,
    debt_schedule_table: pd.DataFrame,
    capital_bridge_table: pd.DataFrame,
    robustness_table: pd.DataFrame,
    blend_table: pd.DataFrame,
    accretion_dilution_table: pd.DataFrame,
    lbo_table: pd.DataFrame,
    market_data_table: pd.DataFrame,
    precedent_curation_table: pd.DataFrame,
    sector_pack_table: pd.DataFrame,
    lineage_table: pd.DataFrame,
    validation_table: pd.DataFrame,
    evidence_table: pd.DataFrame,
    quality_table: pd.DataFrame,
    contract_validation_table: pd.DataFrame,
    sensitivity_grid_table: Optional[pd.DataFrame],
    sensitivity_tornado_table: Optional[pd.DataFrame],
    backtest_table: Optional[pd.DataFrame],
    buyer_universe_table: Optional[pd.DataFrame],
    risk_gate_table: Optional[pd.DataFrame],
    negotiation_table: Optional[pd.DataFrame],
    arsenal_table: Optional[pd.DataFrame],
    arsenal300_table: Optional[pd.DataFrame],
    arsenal600_table: Optional[pd.DataFrame],
    arsenal_massive_table: Optional[pd.DataFrame],
    arsenal_extra50_table: Optional[pd.DataFrame],
    raw_data_table: pd.DataFrame,
    diagnostics: Dict[str, Any],
) -> ExportArtifacts:
    config.ensure_directories()
    ticker = str(target_row.get("ticker") or "target").upper()
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    report = FinalReport(
        company={
            "name": target_row.get("company_name"),
            "ticker": target_row.get("ticker"),
            "cik": target_row.get("cik"),
            "sector": target_row.get("sector"),
        },
        financials=FinancialSnapshot(
            as_of_date=str(target_row.get("as_of_date")) if pd.notna(target_row.get("as_of_date")) else None,
            revenue=_as_float(target_row.get("revenue")),
            revenue_growth_yoy=_as_float(target_row.get("revenue_growth_yoy")),
            ebitda=_as_float(target_row.get("ebitda")),
            ebitda_margin=_as_float(target_row.get("ebitda_margin")),
            enterprise_value=_as_float(target_row.get("enterprise_value")),
            ev_revenue=_as_float(target_row.get("ev_revenue")),
            ev_ebitda=_as_float(target_row.get("ev_ebitda")),
            market_cap=_as_float(target_row.get("market_cap")),
            total_debt=_as_float(target_row.get("total_debt")),
            cash=_as_float(target_row.get("cash")),
            net_debt=_as_float(target_row.get("net_debt")),
            shares_outstanding=_as_float(target_row.get("shares_outstanding")),
            interest_expense=_as_float(target_row.get("interest_expense")),
            implied_share_price_current=_as_float(target_row.get("implied_share_price_current")),
        ),
        comparable_analysis=ComparableAnalysis(**comps_summary),
        precedent_transactions=PrecedentAnalysis(**precedents_summary),
        signals=SignalSet(**signals),
        data_quality=DataQuality(**data_quality),
        valuation_scenarios=ValuationScenarioSummary(**valuation_scenarios),
        dcf_analysis=DCFSummary(**dcf_summary),
        capital_structure=CapitalStructureSummary(**capital_structure_summary),
        robustness=RobustnessSummary(**robustness_summary),
        blended_valuation=BlendedValuationSummary(**blended_valuation_summary),
        accretion_dilution=AccretionDilutionSummary(**accretion_dilution_summary),
        lbo_underwriting=LBOSummary(**lbo_summary),
        market_data=MarketDataSummary(**market_data_summary),
        precedent_curation=PrecedentCurationSummary(**precedent_curation_summary),
        sector_pack=SectorPackSummary(**sector_pack_summary),
        lineage=LineageSummary(**lineage_summary),
        validation=ValidationSummary(**validation_summary),
        ic_pack=ICPackSummary(**ic_pack_summary),
        evidence_citations=EvidenceSummary(**evidence_summary),
        contract_validation=ContractValidationSummary(**contract_validation_summary),
        insights=insights,
        diagnostics=diagnostics,
        conclusion=insights["conclusion"],
    )

    report_json_path = config.output_dir / f"deal_analysis_{ticker}_{timestamp}.json"
    workbook_path = config.output_dir / f"deal_analysis_{ticker}_{timestamp}.xlsx"
    report_json_path.write_text(json.dumps(report.model_dump(mode="json"), indent=2), encoding="utf-8")

    summary_df = _build_summary_sheet(
        target_row=target_row,
        comps_summary=comps_summary,
        precedents_summary=precedents_summary,
        signals=signals,
        data_quality=data_quality,
        valuation_scenarios=valuation_scenarios,
        dcf_summary=dcf_summary,
        capital_structure_summary=capital_structure_summary,
        robustness_summary=robustness_summary,
        blended_valuation_summary=blended_valuation_summary,
        accretion_dilution_summary=accretion_dilution_summary,
        lbo_summary=lbo_summary,
        market_data_summary=market_data_summary,
        precedent_curation_summary=precedent_curation_summary,
        sector_pack_summary=sector_pack_summary,
        lineage_summary=lineage_summary,
        validation_summary=validation_summary,
        ic_pack_summary=ic_pack_summary,
        evidence_summary=evidence_summary,
        contract_validation_summary=contract_validation_summary,
        sensitivity_summary=sensitivity_summary,
        backtest_summary=backtest_summary,
        buyer_universe_summary=buyer_universe_summary,
        risk_gate_summary=risk_gate_summary,
        negotiation_summary=negotiation_summary,
        arsenal_summary=arsenal_summary,
        arsenal300_summary=arsenal300_summary,
        arsenal600_summary=arsenal600_summary,
        arsenal_massive_summary=arsenal_massive_summary,
        arsenal_extra50_summary=arsenal_extra50_summary,
        insights=insights,
    )

    raw_for_excel = raw_data_table.head(config.max_raw_rows_for_excel).copy()
    with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
        summary_df.to_excel(writer, index=False, sheet_name="summary")
        comps_table.to_excel(writer, index=False, sheet_name="comps")
        precedents_table.to_excel(writer, index=False, sheet_name="precedents")
        scenario_table.to_excel(writer, index=False, sheet_name="scenarios")
        dcf_table.to_excel(writer, index=False, sheet_name="dcf")
        dcf_sensitivity_table.to_excel(writer, index=False, sheet_name="dcf_sens")
        debt_schedule_table.to_excel(writer, index=False, sheet_name="debt_schedule")
        capital_bridge_table.to_excel(writer, index=False, sheet_name="cap_bridge")
        robustness_table.to_excel(writer, index=False, sheet_name="robustness")
        blend_table.to_excel(writer, index=False, sheet_name="blend")
        accretion_dilution_table.to_excel(writer, index=False, sheet_name="acc_dil")
        lbo_table.to_excel(writer, index=False, sheet_name="lbo")
        market_data_table.to_excel(writer, index=False, sheet_name="market_data")
        precedent_curation_table.to_excel(writer, index=False, sheet_name="precedent_curated")
        sector_pack_table.to_excel(writer, index=False, sheet_name="sector_pack")
        lineage_table.to_excel(writer, index=False, sheet_name="lineage")
        validation_table.to_excel(writer, index=False, sheet_name="validation")
        evidence_table.to_excel(writer, index=False, sheet_name="evidence")
        quality_table.to_excel(writer, index=False, sheet_name="quality")
        contract_validation_table.to_excel(writer, index=False, sheet_name="contracts")
        if sensitivity_grid_table is not None and not sensitivity_grid_table.empty:
            sensitivity_grid_table.to_excel(writer, index=False, sheet_name="sens_grid")
        if sensitivity_tornado_table is not None and not sensitivity_tornado_table.empty:
            sensitivity_tornado_table.to_excel(writer, index=False, sheet_name="sens_tornado")
        if backtest_table is not None and not backtest_table.empty:
            backtest_table.to_excel(writer, index=False, sheet_name="backtest")
        if buyer_universe_table is not None and not buyer_universe_table.empty:
            buyer_universe_table.to_excel(writer, index=False, sheet_name="buyers")
        if risk_gate_table is not None and not risk_gate_table.empty:
            risk_gate_table.to_excel(writer, index=False, sheet_name="risk_gate")
        if negotiation_table is not None and not negotiation_table.empty:
            negotiation_table.to_excel(writer, index=False, sheet_name="negotiation")
        if arsenal_table is not None and not arsenal_table.empty:
            arsenal_table.to_excel(writer, index=False, sheet_name="arsenal50")
        if arsenal300_table is not None and not arsenal300_table.empty:
            arsenal300_table.to_excel(writer, index=False, sheet_name="arsenal300")
        if arsenal600_table is not None and not arsenal600_table.empty:
            arsenal600_table.to_excel(writer, index=False, sheet_name="arsenal600")
        if arsenal_massive_table is not None and not arsenal_massive_table.empty:
            arsenal_massive_table.head(5000).to_excel(writer, index=False, sheet_name="arsenal_massive")
        if arsenal_extra50_table is not None and not arsenal_extra50_table.empty:
            arsenal_extra50_table.to_excel(writer, index=False, sheet_name="arsenal_extra50")
        raw_for_excel.to_excel(writer, index=False, sheet_name="raw_data")

    return ExportArtifacts(
        report_json_path=report_json_path,
        workbook_path=workbook_path,
        final_report=report,
    )
