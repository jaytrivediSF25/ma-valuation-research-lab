from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List


@dataclass
class IncrementalState:
    latest_filing_date: str = ""
    file_mtimes: Dict[str, float] = None

    def to_dict(self) -> Dict:
        return {
            "latest_filing_date": self.latest_filing_date,
            "file_mtimes": self.file_mtimes or {},
        }


def load_state(path: Path) -> IncrementalState:
    if not path.exists():
        return IncrementalState(latest_filing_date="", file_mtimes={})
    payload = json.loads(path.read_text(encoding="utf-8"))
    return IncrementalState(
        latest_filing_date=str(payload.get("latest_filing_date", "")),
        file_mtimes=dict(payload.get("file_mtimes", {})),
    )


def save_state(path: Path, state: IncrementalState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state.to_dict(), indent=2), encoding="utf-8")


def changed_files(data_dir: Path, state: IncrementalState) -> List[Path]:
    out: List[Path] = []
    current = state.file_mtimes or {}
    for p in data_dir.rglob("*"):
        if not p.is_file():
            continue
        m = p.stat().st_mtime
        key = str(p)
        if key not in current or float(current[key]) != float(m):
            out.append(p)
    return out


def update_file_mtimes(data_dir: Path, state: IncrementalState) -> IncrementalState:
    mtimes = {}
    for p in data_dir.rglob("*"):
        if p.is_file():
            mtimes[str(p)] = p.stat().st_mtime
    return IncrementalState(latest_filing_date=state.latest_filing_date, file_mtimes=mtimes)
