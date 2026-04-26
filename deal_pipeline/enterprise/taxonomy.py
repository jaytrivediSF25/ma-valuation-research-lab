from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import yaml


@dataclass
class TaxonomyRule:
    source_field: str
    canonical_field: str
    converter: str = "identity"


def load_taxonomy_rules(path: Path) -> List[TaxonomyRule]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or []
    return [TaxonomyRule(**row) for row in payload]


def apply_taxonomy(row: Dict, rules: List[TaxonomyRule]) -> Dict:
    out: Dict = {}
    for r in rules:
        val = row.get(r.source_field)
        if r.converter == "upper" and val is not None:
            val = str(val).upper().strip()
        elif r.converter == "float" and val is not None:
            try:
                val = float(val)
            except Exception:
                val = None
        out[r.canonical_field] = val
    return out
