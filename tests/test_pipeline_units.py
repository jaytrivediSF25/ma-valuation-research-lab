import unittest
from pathlib import Path

import pandas as pd

from deal_pipeline.accretion_dilution import run_accretion_dilution_analysis
from deal_pipeline.blended_valuation import build_blended_valuation
from deal_pipeline.config import PipelineConfig
from deal_pipeline.dcf import run_dcf_analysis
from deal_pipeline.insights import generate_signals
from deal_pipeline.lbo import run_lbo_underwriting
from deal_pipeline.lineage import build_lineage_report
from deal_pipeline.market_data import fetch_market_data_context
from deal_pipeline.precedent_curation import curate_precedent_transactions
from deal_pipeline.quality import evaluate_data_quality
from deal_pipeline.scenarios import build_valuation_scenarios
from deal_pipeline.sector_packs import apply_sector_pack


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

    def test_dcf_runs_and_returns_cases(self) -> None:
        config = PipelineConfig(data_dir=Path("."), output_dir=Path("./output"))
        target = pd.Series(
            {
                "revenue": 1000.0,
                "revenue_growth_yoy": 0.08,
                "ebitda_margin": 0.22,
                "enterprise_value": 4800.0,
                "total_debt": 1200.0,
                "cash": 200.0,
                "shares_outstanding": 50.0,
                "interest_expense": 72.0,
            }
        )
        dcf = run_dcf_analysis(target, config)
        self.assertEqual(dcf.summary["case_count"], 3)
        self.assertIsNotNone(dcf.summary["implied_ev_base"])
        self.assertIsNotNone(dcf.summary["implied_equity_value_base"])
        self.assertIsNotNone(dcf.summary["implied_share_price_base"])
        self.assertGreaterEqual(dcf.capital_structure_summary["debt_years_modeled"], 1)
        self.assertFalse(dcf.dcf_table.empty)
        self.assertFalse(dcf.sensitivity_table.empty)

    def test_blended_valuation(self) -> None:
        config = PipelineConfig(data_dir=Path("."), output_dir=Path("./output"))
        target = pd.Series({"revenue": 100.0, "ebitda": 25.0, "enterprise_value": 500.0})
        blend = build_blended_valuation(
            target_row=target,
            comps_summary={"peer_median_ev_revenue": 4.0, "peer_median_ev_ebitda": 16.0},
            precedents_summary={"valuation_range_low": 420.0, "valuation_range_high": 610.0},
            scenarios_summary={"implied_ev_base": 560.0},
            dcf_summary={"implied_ev_base": 540.0},
            config=config,
        )
        self.assertIsNotNone(blend.summary["blended_implied_ev"])
        self.assertIn(blend.summary["blend_stance"], {"upside", "neutral", "downside"})
        self.assertEqual(len(blend.blend_table), 4)

    def test_accretion_dilution(self) -> None:
        config = PipelineConfig(data_dir=Path("."), output_dir=Path("./output"), buyer_ticker="BUYR")
        company_metrics = pd.DataFrame(
            [
                {
                    "ticker": "BUYR",
                    "enterprise_value": 15000.0,
                    "market_cap": 13000.0,
                    "revenue": 4000.0,
                    "ebitda": 900.0,
                    "total_debt": 2500.0,
                    "cash": 600.0,
                    "shares_outstanding": 100.0,
                    "interest_expense": 150.0,
                    "implied_share_price_current": 130.0,
                },
                {
                    "ticker": "TGT",
                    "enterprise_value": 5000.0,
                    "market_cap": 4300.0,
                    "revenue": 1800.0,
                    "ebitda": 360.0,
                    "total_debt": 900.0,
                    "cash": 200.0,
                    "shares_outstanding": 40.0,
                    "interest_expense": 55.0,
                    "implied_share_price_current": 107.5,
                },
            ]
        )
        target = company_metrics[company_metrics["ticker"] == "TGT"].iloc[0]
        out = run_accretion_dilution_analysis(target, company_metrics, config)
        self.assertIsNotNone(out.summary["eps_accretion_dilution"])
        self.assertIsNotNone(out.summary["proforma_net_leverage"])
        self.assertEqual(len(out.scenario_table), 3)

    def test_lbo_underwriting(self) -> None:
        config = PipelineConfig(data_dir=Path("."), output_dir=Path("./output"))
        target = pd.Series({"ebitda": 250.0})
        out = run_lbo_underwriting(target, config)
        self.assertIsNotNone(out.summary["entry_ev"])
        self.assertIsNotNone(out.summary["moic"])
        self.assertIsNotNone(out.summary["irr"])
        self.assertFalse(out.lbo_table.empty)

    def test_market_data_disabled(self) -> None:
        config = PipelineConfig(data_dir=Path("."), output_dir=Path("./output"), enable_market_data=False)
        target = pd.Series({"ticker": "ABT"})
        comps = pd.DataFrame([{"ticker": "MDT"}])
        out = fetch_market_data_context(target, comps, config)
        self.assertEqual(out.summary["status"], "disabled")

    def test_precedent_curation(self) -> None:
        target = pd.Series({"sector": "Healthcare", "revenue": 1000.0})
        precedents = pd.DataFrame(
            [
                {"target_company": "A Med", "acquirer": "X Health", "sector": "Healthcare", "revenue": 900.0, "ev_revenue": 3.2, "ev_ebitda": 12.0},
                {"target_company": "B Med", "acquirer": "Y Health", "sector": "Healthcare", "revenue": 1100.0, "ev_revenue": 3.4, "ev_ebitda": 12.8},
                {"target_company": "Outlier", "acquirer": "Z", "sector": "Healthcare", "revenue": 800.0, "ev_revenue": 25.0, "ev_ebitda": 90.0},
            ]
        )
        out = curate_precedent_transactions(target, precedents)
        self.assertEqual(out.summary["raw_transaction_count"], 3)
        self.assertIn("relevance_score", out.curated_table.columns)
        self.assertGreaterEqual(out.summary["curated_transaction_count"], 1)

    def test_sector_pack_application(self) -> None:
        config = PipelineConfig(data_dir=Path("."), output_dir=Path("./output"))
        new_cfg, summary, table = apply_sector_pack(config, "Healthcare")
        self.assertEqual(summary["sector_pack"], "healthcare")
        self.assertGreaterEqual(summary["override_count"], 1)
        self.assertFalse(table.empty)
        self.assertNotEqual(new_cfg.high_growth_threshold, config.high_growth_threshold)

    def test_lineage_report(self) -> None:
        target = pd.Series(
            {
                "revenue": 100.0,
                "revenue_growth_yoy": 0.1,
                "ebitda": 20.0,
                "ebitda_margin": 0.2,
                "enterprise_value": 300.0,
                "ev_revenue": 3.0,
                "ev_ebitda": 15.0,
                "implied_share_price_current": 25.0,
                "source": "unit_test",
            }
        )
        out = build_lineage_report(target, {"dcf_analysis": {"implied_ev_base": 320.0}})
        self.assertGreaterEqual(out.summary["lineage_row_count"], 9)
        self.assertIn("metric", out.lineage_table.columns)


if __name__ == "__main__":
    unittest.main()
