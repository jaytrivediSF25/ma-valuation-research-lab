from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ModelVersion(BaseModel):
    major: int = 1
    minor: int = 0
    patch: int = 0

    def as_semver(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"


class Company(BaseModel):
    ticker: str
    cik: Optional[str] = None
    company_name: str
    sector: Optional[str] = None
    sub_sector: Optional[str] = None


class Filing(BaseModel):
    cik: str
    ticker: Optional[str] = None
    form: str
    filing_date: datetime
    accession_number: Optional[str] = None
    source_file: Optional[str] = None


class Transaction(BaseModel):
    target_company: str
    acquirer: Optional[str] = None
    announcement_date: Optional[datetime] = None
    close_date: Optional[datetime] = None
    sector: Optional[str] = None
    enterprise_value: Optional[float] = None
    revenue: Optional[float] = None
    ebitda: Optional[float] = None


class MarketQuote(BaseModel):
    ticker: str
    as_of: datetime
    price: Optional[float] = None
    market_cap: Optional[float] = None
    volume: Optional[float] = None


class ValuationResult(BaseModel):
    ticker: str
    as_of: datetime
    enterprise_value: Optional[float] = None
    implied_ev_low: Optional[float] = None
    implied_ev_base: Optional[float] = None
    implied_ev_high: Optional[float] = None
    method: str
    confidence: Optional[float] = None


class CanonicalEnvelope(BaseModel):
    version: ModelVersion = Field(default_factory=ModelVersion)
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    company: Optional[Company] = None
    filings: List[Filing] = Field(default_factory=list)
    transactions: List[Transaction] = Field(default_factory=list)
    quotes: List[MarketQuote] = Field(default_factory=list)
    valuations: List[ValuationResult] = Field(default_factory=list)


@dataclass
class MedallionLayout:
    bronze_tables: List[str]
    silver_tables: List[str]
    gold_tables: List[str]


def default_medallion_layout() -> MedallionLayout:
    return MedallionLayout(
        bronze_tables=["bronze_raw_files", "bronze_sec_filings", "bronze_external_sources"],
        silver_tables=["silver_companies", "silver_filings", "silver_companyfacts", "silver_financials", "silver_precedents"],
        gold_tables=["gold_company_metrics", "gold_comps", "gold_precedents", "gold_valuation_summary"],
    )
