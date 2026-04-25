from dataclasses import dataclass
from typing import Any, Dict, List

import pandas as pd

from .config import PipelineConfig


@dataclass
class MarketDataResult:
    summary: Dict[str, Any]
    quotes_table: pd.DataFrame


def _safe_float(value: Any):
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return float(value)
    except Exception:
        return None


def fetch_market_data_context(
    target_row: pd.Series,
    comps_table: pd.DataFrame,
    config: PipelineConfig,
) -> MarketDataResult:
    if not config.enable_market_data:
        return MarketDataResult(
            summary={
                "status": "disabled",
                "target_ticker": target_row.get("ticker"),
                "target_price": None,
                "target_market_cap_live": None,
                "peer_price_median": None,
            },
            quotes_table=pd.DataFrame(columns=["symbol", "price", "market_cap", "currency", "source"]),
        )

    try:
        import requests
    except Exception:
        return MarketDataResult(
            summary={"status": "requests_missing", "target_ticker": target_row.get("ticker")},
            quotes_table=pd.DataFrame(columns=["symbol", "price", "market_cap", "currency", "source"]),
        )

    symbols: List[str] = []
    target = str(target_row.get("ticker") or "").upper().strip()
    if target:
        symbols.append(target)
    if "ticker" in comps_table.columns:
        peer_syms = [
            str(s).upper().strip()
            for s in comps_table["ticker"].dropna().astype(str).head(12).tolist()
            if str(s).strip()
        ]
        symbols.extend(peer_syms)
    symbols = sorted(set(symbols))
    if not symbols:
        return MarketDataResult(
            summary={"status": "no_symbols", "target_ticker": target},
            quotes_table=pd.DataFrame(columns=["symbol", "price", "market_cap", "currency", "source"]),
        )

    try:
        url = "https://query1.finance.yahoo.com/v7/finance/quote"
        resp = requests.get(url, params={"symbols": ",".join(symbols)}, timeout=10)
        resp.raise_for_status()
        payload = resp.json()
        results = payload.get("quoteResponse", {}).get("result", [])
        rows = []
        for row in results:
            rows.append(
                {
                    "symbol": row.get("symbol"),
                    "price": _safe_float(row.get("regularMarketPrice")),
                    "market_cap": _safe_float(row.get("marketCap")),
                    "currency": row.get("currency"),
                    "source": "yahoo_quote_api",
                }
            )
        quotes = pd.DataFrame(rows)
        if quotes.empty:
            return MarketDataResult(
                summary={"status": "empty_response", "target_ticker": target},
                quotes_table=pd.DataFrame(columns=["symbol", "price", "market_cap", "currency", "source"]),
            )
        target_row_q = quotes[quotes["symbol"] == target]
        target_price = _safe_float(target_row_q["price"].iloc[0]) if not target_row_q.empty else None
        target_mc = _safe_float(target_row_q["market_cap"].iloc[0]) if not target_row_q.empty else None
        peer_prices = quotes[quotes["symbol"] != target]["price"].dropna()
        peer_price_median = _safe_float(peer_prices.median()) if not peer_prices.empty else None

        return MarketDataResult(
            summary={
                "status": "ok",
                "target_ticker": target,
                "target_price": target_price,
                "target_market_cap_live": target_mc,
                "peer_price_median": peer_price_median,
                "symbols_queried": len(symbols),
            },
            quotes_table=quotes,
        )
    except Exception as exc:
        return MarketDataResult(
            summary={"status": "error", "target_ticker": target, "error": str(exc)},
            quotes_table=pd.DataFrame(columns=["symbol", "price", "market_cap", "currency", "source"]),
        )
