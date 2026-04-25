from dataclasses import dataclass
from typing import Any, Dict, List

import pandas as pd


@dataclass
class LineageResult:
    summary: Dict[str, Any]
    lineage_table: pd.DataFrame


def build_lineage_report(target_row: pd.Series, additional_sections: Dict[str, Dict[str, Any]]) -> LineageResult:
    source = target_row.get("source")
    rows: List[Dict[str, Any]] = [
        {"metric": "revenue", "value": target_row.get("revenue"), "formula": "latest available SEC/external revenue concept", "source": source},
        {"metric": "revenue_growth_yoy", "value": target_row.get("revenue_growth_yoy"), "formula": "(revenue - prior_revenue) / prior_revenue", "source": source},
        {"metric": "ebitda", "value": target_row.get("ebitda"), "formula": "EBITDA concept or Operating Income + Depreciation", "source": source},
        {"metric": "ebitda_margin", "value": target_row.get("ebitda_margin"), "formula": "ebitda / revenue", "source": source},
        {"metric": "enterprise_value", "value": target_row.get("enterprise_value"), "formula": "market_cap + total_debt - cash", "source": source},
        {"metric": "ev_revenue", "value": target_row.get("ev_revenue"), "formula": "enterprise_value / revenue", "source": source},
        {"metric": "ev_ebitda", "value": target_row.get("ev_ebitda"), "formula": "enterprise_value / ebitda", "source": source},
        {"metric": "implied_share_price_current", "value": target_row.get("implied_share_price_current"), "formula": "market_cap / shares_outstanding", "source": source},
    ]

    for section, payload in additional_sections.items():
        for key, value in payload.items():
            rows.append(
                {
                    "metric": f"{section}.{key}",
                    "value": value,
                    "formula": f"derived in {section} module",
                    "source": section,
                }
            )

    lineage = pd.DataFrame(rows)
    summary = {
        "lineage_row_count": int(len(lineage)),
        "lineage_sections": int(lineage["metric"].str.split(".").str[0].nunique()),
    }
    return LineageResult(summary=summary, lineage_table=lineage)
