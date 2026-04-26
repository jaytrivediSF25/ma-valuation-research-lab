import json
import unittest
from pathlib import Path

import pandas as pd

from deal_pipeline.backtesting import run_historical_backtest
from deal_pipeline.sensitivity import run_full_sensitivity


class SnapshotTests(unittest.TestCase):
    def test_backtest_summary_snapshot(self) -> None:
        frame = pd.read_csv(Path("tests/fixtures/benchmark_precedents.csv"))
        out = run_historical_backtest(frame)
        golden = json.loads(Path("tests/fixtures/golden_backtest_summary.json").read_text(encoding="utf-8"))

        self.assertEqual(out.summary["rows"], golden["rows"])
        self.assertAlmostEqual(out.summary["mae_forecast_error_pct"], golden["mae_forecast_error_pct"], places=6)
        self.assertAlmostEqual(out.summary["median_forecast_error_pct"], golden["median_forecast_error_pct"], places=6)
        self.assertAlmostEqual(out.summary["hit_rate_within_20pct"], golden["hit_rate_within_20pct"], places=6)

    def test_sensitivity_output_shape(self) -> None:
        target = pd.Series({"revenue": 1200.0, "ebitda_margin": 0.22, "total_debt": 900.0, "ebitda": 260.0, "ev_ebitda": 12.0})
        out = run_full_sensitivity(target)
        self.assertEqual(out.summary["scenario_count"], len(out.grid_table))
        self.assertEqual(len(out.tornado_table), 5)
        self.assertIn("probability_band_p50", out.summary)


if __name__ == "__main__":
    unittest.main()
