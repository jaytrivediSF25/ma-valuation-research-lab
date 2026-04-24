from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class PipelineConfig:
    data_dir: Path
    output_dir: Path
    target_ticker: Optional[str] = None
    target_cik: Optional[str] = None
    target_company: Optional[str] = None
    openai_model: str = "gpt-4o-mini"
    max_raw_rows_for_excel: int = 200000

    def ensure_directories(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
