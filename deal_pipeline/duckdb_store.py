from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import pandas as pd


@dataclass
class DuckDBStoreResult:
    db_path: Path
    tables_written: Dict[str, int]


def persist_to_duckdb(db_path: Path, tables: Dict[str, pd.DataFrame]) -> DuckDBStoreResult:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        import duckdb
    except Exception:
        return DuckDBStoreResult(db_path=db_path, tables_written={})

    con = duckdb.connect(str(db_path))
    written = {}
    try:
        for name, frame in tables.items():
            if frame is None:
                continue
            if frame.empty:
                con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM (SELECT 1 as _empty) WHERE 1=0")
                written[name] = 0
            else:
                con.register("tmp_df", frame)
                con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM tmp_df")
                written[name] = int(len(frame))
                con.unregister("tmp_df")
    finally:
        con.close()
    return DuckDBStoreResult(db_path=db_path, tables_written=written)
