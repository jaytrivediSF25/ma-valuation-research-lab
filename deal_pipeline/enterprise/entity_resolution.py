from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import pandas as pd

try:
    from rapidfuzz import fuzz
except Exception:  # pragma: no cover
    fuzz = None


@dataclass
class ResolutionResult:
    resolved: pd.DataFrame
    matches: pd.DataFrame


def _name_score(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    if fuzz is None:
        a2 = a.lower().strip()
        b2 = b.lower().strip()
        return 100.0 if a2 == b2 else (70.0 if a2 in b2 or b2 in a2 else 0.0)
    return float(fuzz.token_sort_ratio(a, b))


def resolve_entities(left: pd.DataFrame, right: pd.DataFrame) -> ResolutionResult:
    if left.empty or right.empty:
        return ResolutionResult(resolved=left.copy(), matches=pd.DataFrame())

    left_frame = left.copy()
    right_frame = right.copy()
    records: List[Dict] = []
    matched_right_indices = []

    for idx, row in left_frame.iterrows():
        l_ticker = str(row.get("ticker") or "").upper().strip()
        l_cik = str(row.get("cik") or "").strip()
        l_name = str(row.get("company_name") or "").strip()

        best_idx = None
        best_score = -1.0
        for ridx, r in right_frame.iterrows():
            score = 0.0
            r_ticker = str(r.get("ticker") or "").upper().strip()
            r_cik = str(r.get("cik") or "").strip()
            r_name = str(r.get("company_name") or r.get("title") or "").strip()
            if l_ticker and r_ticker and l_ticker == r_ticker:
                score += 60.0
            if l_cik and r_cik and l_cik == r_cik:
                score += 30.0
            score += 0.1 * _name_score(l_name, r_name)
            if score > best_score:
                best_score = score
                best_idx = ridx

        matched = right_frame.loc[best_idx] if best_idx is not None else None
        matched_right_indices.append(best_idx)
        records.append(
            {
                "left_index": idx,
                "right_index": best_idx,
                "confidence": best_score,
                "ticker_match": str(matched.get("ticker")) if matched is not None else None,
                "name_match": str(matched.get("company_name") or matched.get("title")) if matched is not None else None,
            }
        )

    matches = pd.DataFrame(records)
    resolved = left_frame.copy()
    resolved["entity_resolution_confidence"] = matches["confidence"].values
    return ResolutionResult(resolved=resolved, matches=matches)
