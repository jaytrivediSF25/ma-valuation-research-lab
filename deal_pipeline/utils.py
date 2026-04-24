import math
import re
from typing import Iterable, Optional

import numpy as np
import pandas as pd


COMPANY_SUFFIX_PATTERN = re.compile(
    r"\b(incorporated|inc|corp|corporation|co|company|ltd|limited|plc|llc|holdings|group)\b\.?",
    re.IGNORECASE,
)


def normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").strip().lower())


def standardize_company_name(name: Optional[str]) -> Optional[str]:
    if name is None:
        return None
    cleaned = COMPANY_SUFFIX_PATTERN.sub("", str(name))
    cleaned = re.sub(r"[^A-Za-z0-9 ]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip().upper()
    return cleaned or None


def parse_numeric(value) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float, np.number)):
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None
        return float(value)

    text = str(value).strip()
    if text == "":
        return None

    is_negative = False
    if text.startswith("(") and text.endswith(")"):
        is_negative = True
        text = text[1:-1]

    text = text.replace(",", "").replace("$", "").replace("%", "")
    text = re.sub(r"\s+", "", text)
    if text in {"", "-", "NA", "N/A", "null", "None"}:
        return None

    try:
        parsed = float(text)
    except ValueError:
        return None
    if is_negative:
        parsed *= -1
    return parsed


def coerce_numeric_series(series: pd.Series) -> pd.Series:
    return series.map(parse_numeric).astype("float64")


def coerce_date_series(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce", utc=False)
    return parsed


def safe_divide(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
    if numerator is None or denominator is None:
        return None
    if isinstance(numerator, float) and np.isnan(numerator):
        return None
    if isinstance(denominator, float) and np.isnan(denominator):
        return None
    if denominator == 0:
        return None
    return float(numerator) / float(denominator)


def first_available(row: pd.Series, fields: Iterable[str]) -> Optional[str]:
    for field in fields:
        if field in row and pd.notna(row[field]):
            value = str(row[field]).strip()
            if value:
                return value
    return None
