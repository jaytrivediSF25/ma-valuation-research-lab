import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import pandas as pd

from .utils import normalize_key

SUPPORTED_SUFFIXES = {".csv", ".json", ".xlsx", ".xls"}

CORE_COMPANYFACTS_CONCEPTS = {
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
    "Revenues",
    "SalesRevenueNet",
    "SalesRevenueGoodsNet",
    "OperatingIncomeLoss",
    "DepreciationDepletionAndAmortization",
    "DepreciationAndAmortization",
    "Depreciation",
    "DepreciationAmortizationAndAccretionNet",
    "EarningsBeforeInterestTaxesDepreciationAndAmortization",
    "EntityPublicFloat",
    "LongTermDebtNoncurrent",
    "LongTermDebtCurrent",
    "LongTermDebt",
    "DebtCurrent",
    "ShortTermBorrowings",
    "CashAndCashEquivalentsAtCarryingValue",
    "CashCashEquivalentsAndShortTermInvestments",
    "EntityCommonStockSharesOutstanding",
    "CommonStockSharesOutstanding",
    "InterestExpense",
    "InterestExpenseDebt",
}


@dataclass
class IngestedData:
    companies: pd.DataFrame
    filings: pd.DataFrame
    companyfacts: pd.DataFrame
    companyconcept: pd.DataFrame
    external_financials: pd.DataFrame
    external_peers: pd.DataFrame
    external_precedents: pd.DataFrame
    discovered_files: Dict[str, List[Path]]


def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str, low_memory=False)


def _load_companyfacts_filtered(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    usecols = [
        "cik",
        "ticker",
        "entity_name",
        "taxonomy",
        "concept",
        "unit",
        "end",
        "start",
        "val",
        "accn",
        "fy",
        "fp",
        "form",
        "filed",
        "frame",
    ]
    frames: List[pd.DataFrame] = []
    for chunk in pd.read_csv(path, usecols=usecols, dtype=str, chunksize=200000, low_memory=False):
        filtered = chunk[
            chunk["concept"].isin(CORE_COMPANYFACTS_CONCEPTS)
            | chunk["concept"].str.contains(
                "Revenue|Sales|Ebitda|Depreciation|Debt|Cash|PublicFloat|SharesOutstanding|InterestExpense|Interest",
                na=False,
            )
        ]
        filtered = filtered[filtered["form"].isin(["10-K", "10-Q", "10-K/A", "10-Q/A"])]
        if not filtered.empty:
            frames.append(filtered)
    if not frames:
        return pd.DataFrame(columns=usecols)
    return pd.concat(frames, ignore_index=True)


def _read_json_to_df(path: Path) -> pd.DataFrame:
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if isinstance(payload, list):
        return pd.json_normalize(payload)
    if isinstance(payload, dict):
        list_candidates = [v for v in payload.values() if isinstance(v, list)]
        if list_candidates:
            return pd.json_normalize(list_candidates[0])
        return pd.json_normalize(payload)
    return pd.DataFrame()


def _read_table(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path, dtype=str, low_memory=False)
    if suffix == ".json":
        return _read_json_to_df(path)
    if suffix in {".xlsx", ".xls"}:
        sheets = pd.read_excel(path, sheet_name=None, dtype=str)
        if not sheets:
            return pd.DataFrame()
        stamped_frames = []
        for sheet_name, frame in sheets.items():
            temp = frame.copy()
            temp["source_sheet"] = sheet_name
            stamped_frames.append(temp)
        return pd.concat(stamped_frames, ignore_index=True, sort=False)
    raise ValueError(f"Unsupported file type: {path}")


def _load_folder_tables(folder: Path) -> pd.DataFrame:
    if not folder.exists():
        return pd.DataFrame()
    frames: List[pd.DataFrame] = []
    for path in sorted(folder.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in SUPPORTED_SUFFIXES:
            continue
        frame = _read_table(path)
        if frame.empty:
            continue
        frame = frame.copy()
        frame["source_file"] = str(path)
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True, sort=False)


def _discover_generic_input_files(data_dir: Path) -> Dict[str, List[Path]]:
    discovered = {"financials": [], "peers": [], "precedents": []}
    for path in sorted(data_dir.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in SUPPORTED_SUFFIXES:
            continue
        normalized_name = normalize_key(path.name)
        if "financial" in normalized_name:
            discovered["financials"].append(path)
        if "peer" in normalized_name or "comp" in normalized_name:
            discovered["peers"].append(path)
        if "precedent" in normalized_name or "transaction" in normalized_name or "mna" in normalized_name:
            discovered["precedents"].append(path)
    return discovered


def ingest_data(data_dir: Path) -> IngestedData:
    processed_dir = data_dir / "processed"
    companies = _safe_read_csv(processed_dir / "companies.csv")
    filings_target = _safe_read_csv(processed_dir / "filings_target_forms.csv")
    filings_all = _safe_read_csv(processed_dir / "filings_all.csv")
    filings = filings_target if not filings_target.empty else filings_all

    companyfacts = _load_companyfacts_filtered(processed_dir / "companyfacts_all.csv")
    companyconcept = _safe_read_csv(processed_dir / "companyconcept_revenue.csv")

    external_financials = _load_folder_tables(data_dir / "financials")
    external_peers = _load_folder_tables(data_dir / "peers")
    external_precedents = _load_folder_tables(data_dir / "precedents")

    discovered = _discover_generic_input_files(data_dir)
    if external_financials.empty and discovered["financials"]:
        external_financials = pd.concat(
            [
                _read_table(path).assign(source_file=str(path))
                for path in discovered["financials"]
                if path.parent != processed_dir and "companyfacts_all" not in path.name
            ],
            ignore_index=True,
            sort=False,
        )
    if external_peers.empty and discovered["peers"]:
        external_peers = pd.concat(
            [
                _read_table(path).assign(source_file=str(path))
                for path in discovered["peers"]
                if path.parent != processed_dir
            ],
            ignore_index=True,
            sort=False,
        )
    if external_precedents.empty and discovered["precedents"]:
        external_precedents = pd.concat(
            [
                _read_table(path).assign(source_file=str(path))
                for path in discovered["precedents"]
                if path.parent != processed_dir
            ],
            ignore_index=True,
            sort=False,
        )

    return IngestedData(
        companies=companies,
        filings=filings,
        companyfacts=companyfacts,
        companyconcept=companyconcept,
        external_financials=external_financials,
        external_peers=external_peers,
        external_precedents=external_precedents,
        discovered_files=discovered,
    )
