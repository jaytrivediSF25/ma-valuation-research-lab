from __future__ import annotations

from typing import Any, Dict, Optional

import requests


class DealPipelineClient:
    def __init__(self, base_url: str = "http://localhost:8000") -> None:
        self.base_url = base_url.rstrip("/")

    def health(self) -> Dict[str, Any]:
        r = requests.get(f"{self.base_url}/health", timeout=30)
        r.raise_for_status()
        return r.json()

    def run(self, target_ticker: Optional[str] = None, data_dir: str = "./data", output_dir: str = "./output") -> Dict[str, Any]:
        payload: Dict[str, Any] = {"data_dir": data_dir, "output_dir": output_dir}
        if target_ticker:
            payload["target_ticker"] = target_ticker
        r = requests.post(f"{self.base_url}/api/v1/run", json=payload, timeout=300)
        r.raise_for_status()
        return r.json()
