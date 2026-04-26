#!/usr/bin/env python3
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from deal_pipeline.enterprise.platform_ops import docs_as_code_check


def main() -> None:
    docs = list(Path("docs").rglob("*.md")) + [Path("README.md")]
    out = docs_as_code_check(docs, ["valuation", "data", "model"])
    print(out)
    if not out["passed"]:
        print("Documentation token coverage has gaps. Review missing_by_file for follow-up.")


if __name__ == "__main__":
    main()
