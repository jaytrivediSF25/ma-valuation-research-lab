import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from .config import PipelineConfig
from .schemas import (
    AccretionDilutionSummary,
    BlendedValuationSummary,
    CapitalStructureSummary,
    ComparableAnalysis,
    DataQuality,
    DCFSummary,
    FinalReport,
    FinancialSnapshot,
    LBOSummary,
    LineageSummary,
    MarketDataSummary,
    PrecedentCurationSummary,
    PrecedentAnalysis,
    RobustnessSummary,
    SectorPackSummary,
    SignalSet,
    ValuationScenarioSummary,
)


@dataclass
class ExportArtifacts:
    report_json_path: Path
    workbook_path: Path
    final_report: FinalReport


def _as_float(value):
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
        ("dcf_implied_equity_value_base", dcf_summary.get("implied_equity_value_base")),
        ("dcf_implied_share_price_base", dcf_summary.get("implied_share_price_base")),
        ("dcf_tax_shield_pv_base", dcf_summary.get("tax_shield_pv_base")),
        ("capital_structure_net_debt_base", capital_structure_summary.get("net_debt_base")),
        ("capital_structure_debt_years_modeled", capital_structure_summary.get("debt_years_modeled")),
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
        ("lbo_exit_net_leverage", lbo_summary.get("exit_net_leverage")),
        ("market_data_status", market_data_summary.get("status")),
        ("market_data_target_price", market_data_summary.get("target_price")),
        ("precedent_curated_count", precedent_curation_summary.get("curated_transaction_count")),
        ("precedent_outliers_removed", precedent_curation_summary.get("outliers_removed")),
        ("sector_pack", sector_pack_summary.get("sector_pack")),
        ("sector_pack_overrides", sector_pack_summary.get("override_count")),
        ("lineage_row_count", lineage_summary.get("lineage_row_count")),
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
    quality_table: pd.DataFrame,
    raw_data_table: pd.DataFrame,
    diagnostics: Dict[str, Any],
) -> ExportArtifacts:
    config.ensure_directories()
    ticker = str(target_row.get("ticker") or "target").upper()
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    financials = FinancialSnapshot(
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
    )
    comparable_analysis = ComparableAnalysis(**comps_summary)
    precedent_analysis = PrecedentAnalysis(**precedents_summary)
    signal_set = SignalSet(**signals)
    data_quality_set = DataQuality(**data_quality)
    valuation_scenario_set = ValuationScenarioSummary(**valuation_scenarios)
    dcf_set = DCFSummary(**dcf_summary)
    capital_structure_set = CapitalStructureSummary(**capital_structure_summary)
    robustness_set = RobustnessSummary(**robustness_summary)
    blend_set = BlendedValuationSummary(**blended_valuation_summary)
    acc_dil_set = AccretionDilutionSummary(**accretion_dilution_summary)
    lbo_set = LBOSummary(**lbo_summary)
    market_data_set = MarketDataSummary(**market_data_summary)
    precedent_curation_set = PrecedentCurationSummary(**precedent_curation_summary)
    sector_pack_set = SectorPackSummary(**sector_pack_summary)
    lineage_set = LineageSummary(**lineage_summary)

    report = FinalReport(
        company={
            "name": target_row.get("company_name"),
            "ticker": target_row.get("ticker"),
            "cik": target_row.get("cik"),
            "sector": target_row.get("sector"),
        },
        financials=financials,
        comparable_analysis=comparable_analysis,
        precedent_transactions=precedent_analysis,
        signals=signal_set,
        data_quality=data_quality_set,
        valuation_scenarios=valuation_scenario_set,
        dcf_analysis=dcf_set,
        capital_structure=capital_structure_set,
        robustness=robustness_set,
        blended_valuation=blend_set,
        accretion_dilution=acc_dil_set,
        lbo_underwriting=lbo_set,
        market_data=market_data_set,
        precedent_curation=precedent_curation_set,
        sector_pack=sector_pack_set,
        lineage=lineage_set,
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
        quality_table.to_excel(writer, index=False, sheet_name="quality")
        raw_for_excel.to_excel(writer, index=False, sheet_name="raw_data")

    return ExportArtifacts(
        report_json_path=report_json_path,
        workbook_path=workbook_path,
        final_report=report,
    )
