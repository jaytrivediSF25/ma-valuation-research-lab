import json
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from .config import PipelineConfig
from .pipeline import run_pipeline


@dataclass
class ScheduledRefreshResult:
    snapshot_path: Path
    alerts_path: Path
    refreshed: int
    alerts: int


def _load_watchlist(path: Path) -> List[str]:
    if not path.exists():
        return []
    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return [str(x).upper().strip() for x in data if str(x).strip()]
    lines = [line.strip().upper() for line in path.read_text(encoding="utf-8").splitlines()]
    return [x for x in lines if x and not x.startswith("#")]


def _load_previous_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def run_scheduled_refresh(
    config: PipelineConfig,
    watchlist_file: Path,
    state_file: Path,
) -> ScheduledRefreshResult:
    config.ensure_directories()
    tickers = _load_watchlist(watchlist_file)
    if not tickers:
        raise RuntimeError(f"No tickers found in watchlist: {watchlist_file}")

    previous = _load_previous_state(state_file)
    previous_by_ticker = previous.get("by_ticker", {})

    current_by_ticker: Dict[str, Any] = {}
    alerts: List[Dict[str, Any]] = []
    for ticker in tickers:
        cfg = replace(config, target_ticker=ticker, enable_markdown_memo=False)
        result = run_pipeline(cfg)
        report = result.export_artifacts.final_report.model_dump(mode="json")
        now_state = {
            "valuation_position": report["signals"].get("valuation_position"),
            "blend_stance": report["blended_valuation"].get("blend_stance"),
            "conclusion": report.get("conclusion"),
        }
        current_by_ticker[ticker] = now_state

        prev_state = previous_by_ticker.get(ticker, {})
        if prev_state:
            changed = []
            for key in ["valuation_position", "blend_stance"]:
                if prev_state.get(key) != now_state.get(key):
                    changed.append(key)
            if changed:
                alerts.append(
                    {
                        "ticker": ticker,
                        "changed_fields": changed,
                        "previous": prev_state,
                        "current": now_state,
                    }
                )

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    snapshot = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "watchlist": tickers,
        "by_ticker": current_by_ticker,
    }
    state_file.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
    alerts_path = config.output_dir / f"scheduled_alerts_{timestamp}.json"
    alerts_path.write_text(json.dumps(alerts, indent=2), encoding="utf-8")
    return ScheduledRefreshResult(
        snapshot_path=state_file,
        alerts_path=alerts_path,
        refreshed=len(tickers),
        alerts=len(alerts),
    )
