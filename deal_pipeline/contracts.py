from dataclasses import dataclass
from typing import Any, Dict

import pandas as pd


@dataclass
class ContractValidationResult:
    summary: Dict[str, Any]
    table: pd.DataFrame


def validate_data_contracts(company_metrics: pd.DataFrame, precedents_table: pd.DataFrame) -> ContractValidationResult:
    rows = []
    try:
        import pandera as pa
        from pandera import Column, DataFrameSchema
    except Exception:
        rows.append({"contract": "pandera_import", "status": "skipped", "detail": "pandera_not_installed"})
        return ContractValidationResult(
            summary={"contracts_checked": 0, "contracts_failed": 0, "contracts_skipped": 1},
            table=pd.DataFrame(rows),
        )

    metrics_schema = DataFrameSchema(
        {
            "ticker": Column(str, nullable=True, required=False),
            "revenue": Column(float, nullable=True, required=False),
            "ebitda": Column(float, nullable=True, required=False),
            "enterprise_value": Column(float, nullable=True, required=False),
        },
        coerce=True,
        strict=False,
    )
    precedents_schema = DataFrameSchema(
        {
            "ev_revenue": Column(float, nullable=True, required=False),
            "ev_ebitda": Column(float, nullable=True, required=False),
        },
        coerce=True,
        strict=False,
    )

    failed = 0
    for name, schema, frame in [
        ("company_metrics_contract", metrics_schema, company_metrics.copy()),
        ("precedents_contract", precedents_schema, precedents_table.copy()),
    ]:
        try:
            schema.validate(frame, lazy=True)
            rows.append({"contract": name, "status": "pass", "detail": ""})
        except Exception as exc:
            failed += 1
            rows.append({"contract": name, "status": "fail", "detail": str(exc)[:240]})

    summary = {
        "contracts_checked": len(rows),
        "contracts_failed": failed,
        "contracts_skipped": 0,
    }
    return ContractValidationResult(summary=summary, table=pd.DataFrame(rows))
