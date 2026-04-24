import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from .config import PipelineConfig
from .schemas import ComparableAnalysis, FinalReport, FinancialSnapshot, PrecedentAnalysis, SignalSet


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
    insights: Dict[str, Any],
    comps_table: pd.DataFrame,
    precedents_table: pd.DataFrame,
    raw_data_table: pd.DataFrame,
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
    )
    comparable_analysis = ComparableAnalysis(**comps_summary)
    precedent_analysis = PrecedentAnalysis(**precedents_summary)
    signal_set = SignalSet(**signals)

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
        insights=insights,
        conclusion=insights["conclusion"],
    )

    report_json_path = config.output_dir / f"deal_analysis_{ticker}_{timestamp}.json"
    workbook_path = config.output_dir / f"deal_analysis_{ticker}_{timestamp}.xlsx"

    report_json_path.write_text(json.dumps(report.model_dump(mode="json"), indent=2), encoding="utf-8")

    summary_df = _build_summary_sheet(target_row, comps_summary, precedents_summary, signals, insights)
    raw_for_excel = raw_data_table.head(config.max_raw_rows_for_excel).copy()

    with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
        summary_df.to_excel(writer, index=False, sheet_name="summary")
        comps_table.to_excel(writer, index=False, sheet_name="comps")
        precedents_table.to_excel(writer, index=False, sheet_name="precedents")
        raw_for_excel.to_excel(writer, index=False, sheet_name="raw_data")

    return ExportArtifacts(
        report_json_path=report_json_path,
        workbook_path=workbook_path,
        final_report=report,
    )
