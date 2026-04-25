from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


class FinancialSnapshot(BaseModel):
    as_of_date: Optional[str] = None
    revenue: Optional[float] = None
    revenue_growth_yoy: Optional[float] = None
    ebitda: Optional[float] = None
    ebitda_margin: Optional[float] = None
    enterprise_value: Optional[float] = None
    ev_revenue: Optional[float] = None
    ev_ebitda: Optional[float] = None
    market_cap: Optional[float] = None
    total_debt: Optional[float] = None
    cash: Optional[float] = None
    net_debt: Optional[float] = None
    shares_outstanding: Optional[float] = None
    interest_expense: Optional[float] = None
    implied_share_price_current: Optional[float] = None


class ComparableAnalysis(BaseModel):
    peer_count: int
    peer_median_ev_revenue: Optional[float] = None
    peer_median_ev_ebitda: Optional[float] = None
    target_ev_revenue: Optional[float] = None
    target_ev_ebitda: Optional[float] = None
    percentile_ev_revenue: Optional[float] = None
    percentile_ev_ebitda: Optional[float] = None


class PrecedentAnalysis(BaseModel):
    transaction_count: int
    median_ev_revenue: Optional[float] = None
    p25_ev_revenue: Optional[float] = None
    p75_ev_revenue: Optional[float] = None
    median_ev_ebitda: Optional[float] = None
    p25_ev_ebitda: Optional[float] = None
    p75_ev_ebitda: Optional[float] = None
    valuation_range_low: Optional[float] = None
    valuation_range_high: Optional[float] = None


class SignalSet(BaseModel):
    growth_profile: str
    margin_profile: str
    valuation_position: str
    precedent_comparison: str
    risk_flags: List[str]


class DataQuality(BaseModel):
    score: float
    checks: Dict[str, Any]
    issues: List[str]


class ValuationScenarioSummary(BaseModel):
    scenario_count: int
    implied_ev_low: Optional[float] = None
    implied_ev_base: Optional[float] = None
    implied_ev_high: Optional[float] = None
    current_ev: Optional[float] = None
    gap_to_base: Optional[float] = None


class DCFSummary(BaseModel):
    case_count: int
    implied_ev_low: Optional[float] = None
    implied_ev_base: Optional[float] = None
    implied_ev_high: Optional[float] = None
    current_ev: Optional[float] = None
    dcf_gap_to_current: Optional[float] = None
    implied_equity_value_base: Optional[float] = None
    implied_share_price_base: Optional[float] = None
    tax_shield_pv_base: Optional[float] = None


class CapitalStructureSummary(BaseModel):
    net_debt_base: Optional[float] = None
    implied_equity_value_base: Optional[float] = None
    implied_share_price_base: Optional[float] = None
    debt_years_modeled: int = 0
    tax_shield_pv_base: Optional[float] = None


class RobustnessSummary(BaseModel):
    comps_ev_revenue_ci_low: Optional[float] = None
    comps_ev_revenue_ci_high: Optional[float] = None
    comps_ev_ebitda_ci_low: Optional[float] = None
    comps_ev_ebitda_ci_high: Optional[float] = None
    target_ev_revenue_zscore: Optional[float] = None
    target_ev_ebitda_zscore: Optional[float] = None


class BlendedValuationSummary(BaseModel):
    blended_implied_ev: Optional[float] = None
    current_ev: Optional[float] = None
    blend_gap_to_current: Optional[float] = None
    blend_stance: str = "neutral"


class InsightSet(BaseModel):
    key_insights: List[str] = Field(default_factory=list)
    primary_risk: str
    conclusion: str

    @field_validator("key_insights")
    @classmethod
    def validate_key_insights(cls, value: List[str]) -> List[str]:
        cleaned = [v.strip() for v in value if isinstance(v, str) and v.strip()]
        if len(cleaned) < 2:
            raise ValueError("key_insights must contain at least 2 items")
        if len(cleaned) > 4:
            return cleaned[:4]
        return cleaned


class FinalReport(BaseModel):
    company: Dict[str, Any]
    financials: FinancialSnapshot
    comparable_analysis: ComparableAnalysis
    precedent_transactions: PrecedentAnalysis
    signals: SignalSet
    data_quality: DataQuality
    valuation_scenarios: ValuationScenarioSummary
    dcf_analysis: DCFSummary
    capital_structure: CapitalStructureSummary
    robustness: RobustnessSummary
    blended_valuation: BlendedValuationSummary
    insights: InsightSet
    diagnostics: Dict[str, Any]
    conclusion: str
