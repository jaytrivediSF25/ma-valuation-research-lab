import unittest
from pathlib import Path

import pandas as pd

from deal_pipeline.config import PipelineConfig
from deal_pipeline.insights import generate_signals
from deal_pipeline.quality import evaluate_data_quality
from deal_pipeline.scenarios import build_valuation_scenarios


class PipelineUnitTests(unittest.TestCase):
    def test_generate_signals_thresholds(self) -> None:
        config = PipelineConfig(data_dir=Path("."), output_dir=Path("./output"))
        target = pd.Series(
            {
                "revenue_growth_yoy": 0.2,
                "ebitda_margin": 0.3,
                "ev_revenue": 6.0,
                "enterprise_value": 900.0,
                "ebitda": 100.0,
            }
        )
        comps = {"peer_count": 7, "peer_median_ev_revenue": 4.5}
        precedents = {"transaction_count": 12, "valuation_range_low": 800.0, "valuation_range_high": 1000.0}

        signals = generate_signals(target, comps, precedents, config=config)
        self.assertEqual(signals["growth_profile"], "high")
        self.assertEqual(signals["margin_profile"], "strong")
        self.assertEqual(signals["valuation_position"], "premium")
        self.assertEqual(signals["precedent_comparison"], "within")

    def test_quality_score_and_flags(self) -> None:
        config = PipelineConfig(data_dir=Path("."), output_dir=Path("./output"))
        metrics = pd.DataFrame(
            [
                {"revenue": 1.0, "ebitda": 0.2, "enterprise_value": 3.0, "ev_revenue": 3.0, "ev_ebitda": 15.0},
                {"revenue": 2.0, "ebitda": None, "enterprise_value": 6.0, "ev_revenue": 3.0, "ev_ebitda": None},
                {"revenue": None, "ebitda": None, "enterprise_value": None, "ev_revenue": None, "ev_ebitda": None},
            ]
        )
        quality = evaluate_data_quality(
            company_metrics=metrics,
            comps_summary={"peer_count": 2},
            precedents_summary={"transaction_count": 3},
            config=config,
        )
        self.assertTrue(0 <= quality.score <= 100)
        self.assertIn("insufficient_peer_set", quality.issues)
        self.assertIn("insufficient_precedent_set", quality.issues)

    def test_valuation_scenarios(self) -> None:
        target = pd.Series({"revenue": 100.0, "ebitda": 20.0, "enterprise_value": 450.0})
        comps = {"peer_median_ev_revenue": 4.0, "peer_median_ev_ebitda": 18.0}
        precedents = {
            "p25_ev_revenue": 3.0,
            "p75_ev_revenue": 5.5,
            "p25_ev_ebitda": 14.0,
            "p75_ev_ebitda": 22.0,
        }
        scenarios = build_valuation_scenarios(target, comps, precedents)
        self.assertGreaterEqual(scenarios.summary["scenario_count"], 6)
        self.assertIsNotNone(scenarios.summary["implied_ev_base"])
        self.assertFalse(scenarios.scenario_table.empty)


if __name__ == "__main__":
    unittest.main()
