from dataclasses import replace
from typing import Any, Dict, Tuple

import pandas as pd

from .config import PipelineConfig


SECTOR_PACKS: Dict[str, Dict[str, Any]] = {
    "healthcare": {
        "high_growth_threshold": 0.18,
        "strong_margin_threshold": 0.27,
        "dcf_wacc_base": 0.095,
        "dcf_terminal_growth_base": 0.025,
        "lbo_entry_multiple": 11.0,
        "lbo_exit_multiple": 11.5,
    },
    "software": {
        "high_growth_threshold": 0.22,
        "strong_margin_threshold": 0.30,
        "dcf_wacc_base": 0.105,
        "dcf_terminal_growth_base": 0.03,
        "lbo_entry_multiple": 14.0,
        "lbo_exit_multiple": 13.0,
    },
    "industrials": {
        "high_growth_threshold": 0.12,
        "strong_margin_threshold": 0.22,
        "dcf_wacc_base": 0.10,
        "dcf_terminal_growth_base": 0.022,
        "lbo_entry_multiple": 9.0,
        "lbo_exit_multiple": 9.5,
    },
    "financials": {
        "high_growth_threshold": 0.10,
        "strong_margin_threshold": 0.20,
        "dcf_wacc_base": 0.11,
        "dcf_terminal_growth_base": 0.02,
        "lbo_entry_multiple": 8.0,
        "lbo_exit_multiple": 8.5,
    },
}


def _sector_key(sector: str) -> str:
    s = (sector or "").lower()
    if "health" in s or "pharma" in s or "med" in s:
        return "healthcare"
    if "software" in s or "tech" in s or "saas" in s:
        return "software"
    if "industrial" in s or "aero" in s or "manufact" in s:
        return "industrials"
    if "bank" in s or "insur" in s or "financial" in s:
        return "financials"
    return "default"


def apply_sector_pack(config: PipelineConfig, sector: str) -> Tuple[PipelineConfig, Dict[str, Any], pd.DataFrame]:
    key = _sector_key(sector)
    overrides = SECTOR_PACKS.get(key, {})
    if not overrides:
        return config, {"sector_pack": "default", "override_count": 0}, pd.DataFrame(columns=["parameter", "base_value", "override_value"])

    override_table = []
    for k, v in overrides.items():
        override_table.append({"parameter": k, "base_value": getattr(config, k), "override_value": v})
    updated = replace(config, **overrides)
    summary = {"sector_pack": key, "override_count": len(overrides)}
    return updated, summary, pd.DataFrame(override_table)
