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
    min_peer_count: int = 5
    min_precedent_count: int = 5
    low_growth_threshold: float = 0.03
    high_growth_threshold: float = 0.15
    weak_margin_threshold: float = 0.12
    strong_margin_threshold: float = 0.25
    premium_multiple_buffer: float = 0.15
    discounted_multiple_buffer: float = 0.15
    enable_markdown_memo: bool = True

    def ensure_directories(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
