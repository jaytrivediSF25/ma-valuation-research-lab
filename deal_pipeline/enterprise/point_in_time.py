from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pandas as pd


@dataclass
class PITCheckResult:
    passed: bool
    leaked_rows: int
    max_leak_date: str | None


def enforce_point_in_time(frame: pd.DataFrame, cutoff_col: str, feature_date_col: str) -> PITCheckResult:
    if frame.empty:
        return PITCheckResult(passed=True, leaked_rows=0, max_leak_date=None)
    c = pd.to_datetime(frame[cutoff_col], errors="coerce")
    f = pd.to_datetime(frame[feature_date_col], errors="coerce")
    leaked = frame[f > c]
    max_leak = None
    if not leaked.empty:
        max_date = pd.to_datetime(leaked[feature_date_col], errors="coerce").max()
        if pd.notna(max_date):
            max_leak = str(max_date.date())
    return PITCheckResult(passed=leaked.empty, leaked_rows=int(len(leaked)), max_leak_date=max_leak)
