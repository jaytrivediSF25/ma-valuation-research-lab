#!/usr/bin/env python3
from pathlib import Path
import json
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from deal_pipeline.enterprise.platform_ops import event_delta_report


def main() -> None:
    prev = {"valuation_position": "fair", "blend_stance": "neutral", "risk": "moderate"}
    curr = {"valuation_position": "premium", "blend_stance": "upside", "risk": "moderate"}
    delta = event_delta_report(prev, curr)
    out = Path("output") / "event_delta_report.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(delta, indent=2), encoding="utf-8")
    print(f"Event delta report: {out}")


if __name__ == "__main__":
    main()
