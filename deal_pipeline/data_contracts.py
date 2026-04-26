from dataclasses import dataclass
from typing import Dict, List

import pandas as pd


@dataclass
class ContractCheck:
    name: str
    status: str
    detail: str


REQUIRED_COLUMNS: Dict[str, List[str]] = {
    "companies": ["cik", "ticker", "title", "company_name_std"],
    "filings": ["cik", "ticker", "form", "filing_date"],
    "companyfacts": ["cik", "ticker", "concept", "val", "end"],
    "financials": ["company_name", "ticker", "revenue", "ebitda", "enterprise_value"],
    "peers": ["company_name", "ticker"],
    "precedents": ["target_company", "enterprise_value"],
    "company_metrics": ["ticker", "revenue", "ebitda", "enterprise_value", "ev_revenue", "ev_ebitda"],
    "comps_output": ["ticker", "ev_revenue", "ev_ebitda"],
    "precedents_output": ["ev_revenue", "ev_ebitda"],
}


NUMERIC_COLUMNS: Dict[str, List[str]] = {
    "company_metrics": ["revenue", "ebitda", "enterprise_value", "ev_revenue", "ev_ebitda"],
    "comps_output": ["ev_revenue", "ev_ebitda"],
    "precedents_output": ["ev_revenue", "ev_ebitda"],
}


DATE_COLUMNS: Dict[str, List[str]] = {
    "filings": ["filing_date"],
    "companyfacts": ["end"],
    "precedents": ["announcement_date"],
}


def _check_required(name: str, frame: pd.DataFrame) -> List[ContractCheck]:
    checks: List[ContractCheck] = []
    required = REQUIRED_COLUMNS.get(name, [])
    missing = [c for c in required if c not in frame.columns]
    if missing:
        checks.append(ContractCheck(name=name, status="fail", detail=f"missing_columns={missing}"))
    else:
        checks.append(ContractCheck(name=name, status="pass", detail="required_columns_present"))
    return checks


def _check_numeric(name: str, frame: pd.DataFrame) -> List[ContractCheck]:
    checks: List[ContractCheck] = []
    numeric = NUMERIC_COLUMNS.get(name, [])
    for col in numeric:
        if col not in frame.columns:
            checks.append(ContractCheck(name=f"{name}.{col}", status="fail", detail="column_missing"))
            continue
        parsed = pd.to_numeric(frame[col], errors="coerce")
        ratio = float(parsed.notna().mean()) if len(parsed) else 1.0
        status = "pass" if ratio >= 0.7 else "fail"
        checks.append(ContractCheck(name=f"{name}.{col}", status=status, detail=f"numeric_coverage={ratio:.3f}"))
    return checks


def _check_dates(name: str, frame: pd.DataFrame) -> List[ContractCheck]:
    checks: List[ContractCheck] = []
    for col in DATE_COLUMNS.get(name, []):
        if col not in frame.columns:
            checks.append(ContractCheck(name=f"{name}.{col}", status="fail", detail="column_missing"))
            continue
        parsed = pd.to_datetime(frame[col], errors="coerce")
        ratio = float(parsed.notna().mean()) if len(parsed) else 1.0
        status = "pass" if ratio >= 0.7 else "fail"
        checks.append(ContractCheck(name=f"{name}.{col}", status=status, detail=f"date_parse_coverage={ratio:.3f}"))
    return checks


def run_contract_suite(frames: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows: List[ContractCheck] = []
    for name, frame in frames.items():
        rows.extend(_check_required(name, frame))
        rows.extend(_check_numeric(name, frame))
        rows.extend(_check_dates(name, frame))
    out = pd.DataFrame([{"contract": r.name, "status": r.status, "detail": r.detail} for r in rows])
    if out.empty:
        return pd.DataFrame(columns=["contract", "status", "detail"])
    return out


def assert_contract_suite(frames: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    out = run_contract_suite(frames)
    failed = out[out["status"] == "fail"]
    if not failed.empty:
        raise ValueError(f"Contract suite failed with {len(failed)} failures")
    return out
