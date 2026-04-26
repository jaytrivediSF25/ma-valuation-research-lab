from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import pandas as pd

try:
    import duckdb
except Exception:  # pragma: no cover
    duckdb = None


@dataclass
class MedallionResult:
    db_path: Path
    bronze_rows: int
    silver_rows: int
    gold_rows: int


def build_medallion_layers(db_path: Path, raw_tables: Dict[str, pd.DataFrame], normalized_tables: Dict[str, pd.DataFrame], gold_tables: Dict[str, pd.DataFrame]) -> MedallionResult:
    if duckdb is None:
        return MedallionResult(db_path=db_path, bronze_rows=0, silver_rows=0, gold_rows=0)

    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    bronze_rows = 0
    silver_rows = 0
    gold_rows = 0
    try:
        for name, frame in raw_tables.items():
            con.register("tmp", frame)
            con.execute(f"create or replace table bronze_{name} as select * from tmp")
            bronze_rows += len(frame)
            con.unregister("tmp")
        for name, frame in normalized_tables.items():
            con.register("tmp", frame)
            con.execute(f"create or replace table silver_{name} as select * from tmp")
            silver_rows += len(frame)
            con.unregister("tmp")
        for name, frame in gold_tables.items():
            con.register("tmp", frame)
            con.execute(f"create or replace table gold_{name} as select * from tmp")
            gold_rows += len(frame)
            con.unregister("tmp")
    finally:
        con.close()

    return MedallionResult(db_path=db_path, bronze_rows=bronze_rows, silver_rows=silver_rows, gold_rows=gold_rows)
