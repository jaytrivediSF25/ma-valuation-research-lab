from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional

import pandas as pd


@dataclass
class CacheResult:
    cache_hit: bool
    cache_key: str
    path: Path


def semantic_cache_key(parts: Iterable[str]) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(str(p).encode("utf-8"))
    return h.hexdigest()


def cache_write(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def cache_read(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def dag_execute(tasks: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    completed: Dict[str, Any] = {}
    pending = dict(tasks)
    while pending:
        progressed = False
        for name in list(pending.keys()):
            deps = pending[name].get("deps", [])
            if any(d not in completed for d in deps):
                continue
            fn: Callable = pending[name]["fn"]
            inputs = {d: completed[d] for d in deps}
            completed[name] = fn(inputs)
            del pending[name]
            progressed = True
        if not progressed:
            raise RuntimeError("DAG dependency cycle detected")
    return completed


def checkpoint_write(path: Path, stage: str, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    blob = {
        "stage": stage,
        "updated_utc": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }
    path.write_text(json.dumps(blob, indent=2, default=str), encoding="utf-8")


def checkpoint_load(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def validate_required_secrets(required: List[str], env: Dict[str, str]) -> Dict[str, Any]:
    missing = [k for k in required if not env.get(k)]
    return {"passed": len(missing) == 0, "missing": missing}


def dependency_governance(requirements_path: Path) -> pd.DataFrame:
    rows = []
    for line in requirements_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        pinned = ">=" in line or "==" in line
        rows.append({"dependency": line, "pinned": pinned, "risk": "low" if pinned else "medium"})
    return pd.DataFrame(rows)


def plugin_connector_registry(path: Path, connectors: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"connectors": connectors}, indent=2), encoding="utf-8")


def feature_store_write(path: Path, features: pd.DataFrame, entity_key: str = "ticker") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        features.to_parquet(path, index=False)
        return path
    except Exception:
        fallback = path.with_suffix(".csv")
        features.to_csv(fallback, index=False)
        return fallback


def sdk_payload(result: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "version": "1.0.0",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "result": result,
    }


def research_pack_write(path: Path, sections: Dict[str, str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["# Research Pack", ""]
    for title, body in sections.items():
        lines.extend([f"## {title}", body, ""])
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def portfolio_allocation(scores: pd.Series, max_weight: float = 0.15) -> pd.Series:
    s = pd.to_numeric(scores, errors="coerce").fillna(0.0)
    if s.sum() <= 0:
        return pd.Series([0.0] * len(s), index=s.index)
    w = s / s.sum()
    w = w.clip(upper=max_weight)
    if w.sum() > 0:
        w = w / w.sum()
    return w


def event_delta_report(previous: Dict[str, Any], current: Dict[str, Any]) -> Dict[str, Any]:
    changes = {}
    keys = set(previous.keys()) | set(current.keys())
    for k in keys:
        if previous.get(k) != current.get(k):
            changes[k] = {"previous": previous.get(k), "current": current.get(k)}
    return {"changed_fields": changes, "changed_count": len(changes)}


def compliance_audit_append(path: Path, actor: str, action: str, payload_hash: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "actor": actor,
        "action": action,
        "payload_hash": payload_hash,
    }
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row) + "\n")


def docs_as_code_check(markdown_paths: List[Path], required_tokens: List[str]) -> Dict[str, Any]:
    missing_by_file = {}
    for p in markdown_paths:
        text = p.read_text(encoding="utf-8") if p.exists() else ""
        missing = [t for t in required_tokens if t not in text]
        if missing:
            missing_by_file[str(p)] = missing
    return {"passed": len(missing_by_file) == 0, "missing_by_file": missing_by_file}


def readiness_scorecard(metrics: Dict[str, float]) -> Dict[str, Any]:
    weights = {
        "reliability": 0.25,
        "data_quality": 0.20,
        "model_governance": 0.20,
        "test_coverage": 0.20,
        "security": 0.15,
    }
    score = 0.0
    for k, w in weights.items():
        score += w * float(metrics.get(k, 0.0))
    tier = "needs_work"
    if score >= 85:
        tier = "production_ready"
    elif score >= 70:
        tier = "pilot_ready"
    return {"score": score, "tier": tier, "weights": weights, "metrics": metrics}


def sql_sanity_check(sql: str) -> Dict[str, Any]:
    lowered = sql.lower()
    checks = {
        "no_select_star": "select *" not in lowered,
        "has_where_or_join": (" where " in lowered) or (" join " in lowered),
        "no_cartesian_join_pattern": " join " not in lowered or " on " in lowered,
    }
    return {"passed": all(checks.values()), "checks": checks}
