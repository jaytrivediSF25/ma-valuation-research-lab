import json
import unittest
from pathlib import Path

import pandas as pd

from deal_pipeline.enterprise.analytics_ml import benchmark_parity, synthetic_deal_generator
from deal_pipeline.enterprise.assumptions import load_assumption_tree, resolve_assumptions
from deal_pipeline.enterprise.entity_resolution import resolve_entities
from deal_pipeline.enterprise.platform_ops import readiness_scorecard, sql_sanity_check
from deal_pipeline.enterprise.reproducibility import build_manifest, replay_key
from deal_pipeline.enterprise.suite import run_enterprise_suite
from deal_pipeline.enterprise.valuation_advanced import monte_carlo_ev


class EnterpriseModuleTests(unittest.TestCase):
    def test_assumption_resolution(self) -> None:
        tree = load_assumption_tree(Path("deal_pipeline/enterprise/configs/assumptions.yaml"))
        out = resolve_assumptions(tree, sector="healthcare")
        self.assertIn("wacc", out.resolved)
        self.assertIn("wacc", out.trace)

    def test_entity_resolution(self) -> None:
        left = pd.DataFrame([{"ticker": "ABT", "cik": "0000001800", "company_name": "Abbott Laboratories"}])
        right = pd.DataFrame([{"ticker": "ABT", "cik": "0000001800", "company_name": "Abbott Labs"}])
        out = resolve_entities(left, right)
        self.assertEqual(len(out.matches), 1)
        self.assertGreaterEqual(float(out.matches.iloc[0]["confidence"]), 50)

    def test_repro_manifest(self) -> None:
        m = build_manifest(config_payload={"x": 1}, data_dir=Path("data"), requirements_path=Path("requirements.txt"))
        key = replay_key(m)
        self.assertTrue(len(key) > 10)

    def test_monte_carlo(self) -> None:
        out = monte_carlo_ev(1000.0, 0.2, n=500)
        self.assertEqual(out.summary["n"], 500)
        self.assertIn("ev", out.samples.columns)

    def test_benchmark_parity(self) -> None:
        out = benchmark_parity({"a": 1.0}, {"a": 1.0})
        self.assertTrue(out["passed"])

    def test_readiness_scorecard(self) -> None:
        out = readiness_scorecard({"reliability": 80, "data_quality": 90, "model_governance": 85, "test_coverage": 88, "security": 70})
        self.assertIn("tier", out)

    def test_sql_sanity(self) -> None:
        out = sql_sanity_check("select ticker from x where y > 0")
        self.assertTrue(out["passed"])

    def test_synthetic_generator(self) -> None:
        s = synthetic_deal_generator(50)
        self.assertEqual(len(s), 50)

    def test_enterprise_suite(self) -> None:
        report = {
            "company": {"ticker": "ABT", "sector": "Healthcare"},
            "financials": {"revenue": 1000.0, "ebitda_margin": 0.2, "enterprise_value": 5000.0},
            "diagnostics": {"x": 1},
        }
        cm = pd.DataFrame(
            [
                {"ticker": "ABT", "cik": "0000001800", "company_name": "Abbott Laboratories", "revenue": 1000.0, "ebitda": 200.0},
                {"ticker": "MDT", "cik": "0000001613", "company_name": "Medtronic", "revenue": 900.0, "ebitda": 170.0},
            ]
        )
        prec = pd.DataFrame(
            [
                {"target_company": "A", "acquirer": "X", "ev_revenue": 3.2, "ev_ebitda": 12.0, "enterprise_value": 3000.0, "revenue": 900.0, "ebitda": 250.0},
                {"target_company": "B", "acquirer": "Y", "ev_revenue": 3.4, "ev_ebitda": 13.0, "enterprise_value": 3200.0, "revenue": 910.0, "ebitda": 246.0},
            ]
        )
        out = run_enterprise_suite(Path("output"), Path("data"), report, cm, prec)
        self.assertTrue((out.output_dir / "enterprise_summary.csv").exists())
        self.assertIn("run_key", out.summary)


if __name__ == "__main__":
    unittest.main()
