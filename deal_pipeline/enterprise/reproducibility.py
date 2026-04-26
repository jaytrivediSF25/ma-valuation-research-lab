from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable


@dataclass
class RunManifest:
    git_commit: str
    python_version: str
    platform: str
    config_hash: str
    dependency_hash: str
    input_hash: str

    def to_dict(self) -> Dict[str, str]:
        return {
            "git_commit": self.git_commit,
            "python_version": self.python_version,
            "platform": self.platform,
            "config_hash": self.config_hash,
            "dependency_hash": self.dependency_hash,
            "input_hash": self.input_hash,
        }


def _sha256_text(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def hash_files(paths: Iterable[Path]) -> str:
    h = hashlib.sha256()
    for p in sorted([x for x in paths if x.exists()]):
        h.update(str(p).encode("utf-8"))
        h.update(p.read_bytes())
    return h.hexdigest()


def build_manifest(config_payload: Dict, data_dir: Path, requirements_path: Path) -> RunManifest:
    try:
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL, text=True).strip()
    except Exception:
        commit = "unknown"

    data_files = [p for p in data_dir.rglob("*") if p.is_file()]
    dep_hash = hash_files([requirements_path])
    input_hash = hash_files(data_files[:2000])
    cfg_hash = _sha256_text(json.dumps(config_payload, sort_keys=True, default=str))

    return RunManifest(
        git_commit=commit,
        python_version=sys.version.split()[0],
        platform=f"{platform.system()}-{platform.machine()}",
        config_hash=cfg_hash,
        dependency_hash=dep_hash,
        input_hash=input_hash,
    )


def write_manifest(path: Path, manifest: RunManifest) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest.to_dict(), indent=2), encoding="utf-8")


def replay_key(manifest: RunManifest) -> str:
    return _sha256_text("|".join([manifest.config_hash, manifest.dependency_hash, manifest.input_hash]))
