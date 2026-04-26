from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import yaml


@dataclass
class AssumptionResolution:
    resolved: Dict[str, Any]
    trace: Dict[str, str]


def _deep_merge(base: Dict[str, Any], over: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for k, v in over.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_assumption_tree(path: Path) -> Dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return payload


def resolve_assumptions(tree: Dict[str, Any], sector: str, sub_sector: str = "", overrides: Dict[str, Any] | None = None) -> AssumptionResolution:
    global_cfg = tree.get("global", {})
    sector_cfg = tree.get("sector", {}).get(sector.lower(), {})
    sub_cfg = tree.get("sub_sector", {}).get(sub_sector.lower(), {}) if sub_sector else {}
    user_cfg = overrides or {}

    out = _deep_merge(global_cfg, sector_cfg)
    out = _deep_merge(out, sub_cfg)
    out = _deep_merge(out, user_cfg)

    trace = {}
    for k in out:
        if k in user_cfg:
            trace[k] = "override"
        elif k in sub_cfg:
            trace[k] = "sub_sector"
        elif k in sector_cfg:
            trace[k] = "sector"
        else:
            trace[k] = "global"
    return AssumptionResolution(resolved=out, trace=trace)
