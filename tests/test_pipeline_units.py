import unittest
from pathlib import Path

import pandas as pd

from deal_pipeline.accretion_dilution import run_accretion_dilution_analysis
from deal_pipeline.automation import _load_watchlist
from deal_pipeline.batch_screen import _score_row
from deal_pipeline.blended_valuation import build_blended_valuation
from deal_pipeline.backtesting import run_historical_backtest
from deal_pipeline.config import PipelineConfig
from deal_pipeline.contracts import validate_data_contracts
from deal_pipeline.dcf import run_dcf_analysis
from deal_pipeline.duckdb_store import persist_to_duckdb
from deal_pipeline.evidence import apply_evidence_citations
from deal_pipeline.insights import generate_signals
from deal_pipeline.ic_pack import create_ic_pack
from deal_pipeline.lbo import run_lbo_underwriting
from deal_pipeline.lineage import build_lineage_report
from deal_pipeline.market_data import fetch_market_data_context
from deal_pipeline.peer_selection import select_peers_with_factor_model
from deal_pipeline.precedent_curation import curate_precedent_transactions
from deal_pipeline.quality import evaluate_data_quality
from deal_pipeline.scenarios import build_valuation_scenarios
from deal_pipeline.sector_packs import apply_sector_pack
from deal_pipeline.sensitivity import run_full_sensitivity
from deal_pipeline.strategic import build_buyer_universe, build_negotiation_playbook, run_deal_risk_gate
from deal_pipeline.arsenal50 import run_arsenal50
from deal_pipeline.arsenal300 import run_arsenal300
from deal_pipeline.arsenal600 import run_arsenal600
from deal_pipeline.arsenal_massive import run_arsenal_massive
from deal_pipeline.arsenal_extra50 import run_arsenal_extra50
from deal_pipeline.validation import run_model_validation_suite


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
        self.assertIn("blend_optimizer_status", blend.summary)

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

    def test_batch_score_row(self) -> None:
        s1 = _score_row({"blend_gap_to_current": 0.2, "data_quality_score": 80, "risk_flag_count": 1})
        s2 = _score_row({"blend_gap_to_current": -0.1, "data_quality_score": 60, "risk_flag_count": 4})
        self.assertGreater(s1, s2)

    def test_validation_suite(self) -> None:
        target = pd.Series({"enterprise_value": 500.0})
        out = run_model_validation_suite(
            target_row=target,
            comps_summary={"peer_count": 8},
            precedents_summary={"transaction_count": 9},
            robustness_summary={"target_ev_revenue_zscore": 1.2},
            quality_summary={"score": 82.0},
            dcf_summary={"implied_ev_base": 560.0},
        )
        self.assertEqual(out.summary["validation_checks"], 5)
        self.assertIn("status", out.validation_table.columns)

    def test_load_watchlist(self) -> None:
        p = Path("./output/test_watchlist.txt")
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("ABT\n# comment\nMDT\n", encoding="utf-8")
        loaded = _load_watchlist(p)
        self.assertEqual(loaded, ["ABT", "MDT"])

    def test_ic_pack_generation(self) -> None:
        config = PipelineConfig(data_dir=Path("."), output_dir=Path("./output"))
        payload = {"company": {"ticker": "TEST", "name": "Test Co"}, "signals": {"valuation_position": "fair"}, "insights": {"primary_risk": "none"}, "conclusion": "ok"}
        result = create_ic_pack(
            config=config,
            report_payload=payload,
            comps_table=pd.DataFrame([{"ticker": "AAA"}]),
            precedents_table=pd.DataFrame([{"target_company": "BBB"}]),
            scenarios_table=pd.DataFrame([{"scenario": "base"}]),
            dcf_table=pd.DataFrame([{"case": "base"}]),
        )
        self.assertTrue(result.summary["generated"])

    def test_evidence_citation_layer(self) -> None:
        insights = {"key_insights": ["EV/Revenue is above peer median.", "Revenue growth is strong."], "primary_risk": "x", "conclusion": "y"}
        out = apply_evidence_citations(insights)
        self.assertEqual(out.summary["total_insights"], 2)
        self.assertEqual(out.summary["insights_with_citations"], 2)
        self.assertGreaterEqual(out.summary["citation_coverage_pct"], 1.0)

    def test_sensitivity_engine(self) -> None:
        target = pd.Series({"revenue": 1000.0, "ebitda_margin": 0.2, "total_debt": 700.0, "ebitda": 200.0, "ev_ebitda": 12.0})
        out = run_full_sensitivity(target)
        self.assertGreater(out.summary["scenario_count"], 10)
        self.assertFalse(out.grid_table.empty)
        self.assertFalse(out.tornado_table.empty)

    def test_backtesting_engine(self) -> None:
        precedents = pd.DataFrame(
            [
                {"target_company": "A", "announcement_date": "2020-01-01", "sector": "Healthcare", "revenue": 100.0, "ebitda": 20.0, "enterprise_value": 320.0, "ev_revenue": 3.2, "ev_ebitda": 16.0},
                {"target_company": "B", "announcement_date": "2020-06-01", "sector": "Healthcare", "revenue": 110.0, "ebitda": 21.0, "enterprise_value": 350.0, "ev_revenue": 3.18, "ev_ebitda": 16.67},
                {"target_company": "C", "announcement_date": "2021-01-01", "sector": "Healthcare", "revenue": 120.0, "ebitda": 25.0, "enterprise_value": 390.0, "ev_revenue": 3.25, "ev_ebitda": 15.6},
                {"target_company": "D", "announcement_date": "2021-05-01", "sector": "Healthcare", "revenue": 90.0, "ebitda": 18.0, "enterprise_value": 280.0, "ev_revenue": 3.11, "ev_ebitda": 15.56},
                {"target_company": "E", "announcement_date": "2021-10-01", "sector": "Healthcare", "revenue": 130.0, "ebitda": 26.0, "enterprise_value": 430.0, "ev_revenue": 3.31, "ev_ebitda": 16.54},
            ]
        )
        out = run_historical_backtest(precedents)
        self.assertEqual(out.summary["rows"], 5)
        self.assertIn("forecast_error_pct", out.backtest_table.columns)

    def test_peer_selection_scoring(self) -> None:
        target = pd.Series({"sector": "Healthcare", "revenue": 1000.0, "ebitda_margin": 0.2, "revenue_growth_yoy": 0.08, "total_debt": 500.0, "ebitda": 200.0})
        peers = pd.DataFrame(
            [
                {"ticker": "A", "sector": "Healthcare", "revenue": 900.0, "ebitda_margin": 0.19, "revenue_growth_yoy": 0.09, "total_debt": 400.0, "ebitda": 190.0, "enterprise_value": 3000.0},
                {"ticker": "B", "sector": "Industrials", "revenue": 5000.0, "ebitda_margin": 0.1, "revenue_growth_yoy": 0.02, "total_debt": 2500.0, "ebitda": 300.0, "enterprise_value": 6000.0},
            ]
        )
        out = select_peers_with_factor_model(target, peers, max_peers=2)
        self.assertIn("peer_score", out.peer_table.columns)
        self.assertIn("peer_score_explain", out.peer_table.columns)

    def test_buyer_universe(self) -> None:
        target = pd.Series({"ticker": "TGT", "sector": "Healthcare", "enterprise_value": 4000.0, "revenue": 1200.0})
        metrics = pd.DataFrame(
            [
                {"ticker": "A", "company_name": "A Co", "sector": "Healthcare", "enterprise_value": 20000.0, "revenue": 5000.0, "cash": 3000.0, "total_debt": 7000.0, "ebitda": 1400.0},
                {"ticker": "B", "company_name": "B Co", "sector": "Industrials", "enterprise_value": 8000.0, "revenue": 2200.0, "cash": 400.0, "total_debt": 3000.0, "ebitda": 450.0},
            ]
        )
        peers = pd.DataFrame([{"ticker": "A"}])
        out = build_buyer_universe(target, metrics, peers)
        self.assertGreaterEqual(out.summary["buyer_count"], 1)
        self.assertIn("buyer_score", out.buyer_table.columns)

    def test_risk_gate(self) -> None:
        target = pd.Series({"enterprise_value": 1000.0})
        out = run_deal_risk_gate(
            target_row=target,
            comps_summary={"peer_count": 4},
            precedents_summary={"transaction_count": 5},
            dcf_summary={"dcf_gap_to_current": 0.5},
            quality_score=60.0,
            validation_summary={"validation_score": 55.0},
            sensitivity_summary={"probability_band_p10": 400.0, "probability_band_p50": 1000.0},
        )
        self.assertIn(out.summary["overall_gate"], {"green", "amber", "red"})
        self.assertIn("gate", out.gate_table.columns)

    def test_negotiation_playbook(self) -> None:
        target = pd.Series({"enterprise_value": 1000.0})
        out = build_negotiation_playbook(
            target_row=target,
            blended_summary={"blended_implied_ev": 1150.0},
            precedents_summary={"valuation_range_low": 980.0, "valuation_range_high": 1280.0},
            sensitivity_summary={"probability_band_p10": 800.0, "probability_band_p50": 1100.0, "probability_band_p90": 1400.0},
        )
        self.assertIsNotNone(out.summary["walk_away_ev"])
        self.assertIn("term", out.playbook_table.columns)

    def test_arsenal50(self) -> None:
        target = pd.Series({"enterprise_value": 1000.0, "ebitda_margin": 0.2, "revenue_growth_yoy": 0.1, "total_debt": 300.0, "cash": 100.0, "ev_revenue": 3.0})
        out = run_arsenal50(
            target_row=target,
            comps_summary={"peer_count": 10, "percentile_ev_ebitda": 0.6},
            precedents_summary={"transaction_count": 12, "valuation_range_low": 900.0, "valuation_range_high": 1250.0, "p75_ev_ebitda": 14.0, "p75_ev_revenue": 4.2, "median_ev_revenue": 3.6},
            dcf_summary={"dcf_gap_to_current": 0.2},
            quality_score=82.0,
            validation_summary={"validation_score": 80.0, "validation_warn_count": 1},
            sensitivity_summary={"probability_band_p10": 800.0, "probability_band_p50": 1000.0, "probability_band_p90": 1300.0},
            buyer_universe_summary={"top_buyer_score": 0.8},
            negotiation_summary={"opening_bid_ev": 980.0, "walk_away_ev": 1120.0, "stretch_ev": 1280.0},
            risk_gate_summary={"overall_gate": "green"},
        )
        self.assertEqual(out.summary["arsenal_idea_count"], 50)
        self.assertIn("idea_id", out.arsenal_table.columns)

    def test_arsenal300(self) -> None:
        target = pd.Series({"enterprise_value": 1000.0, "ebitda_margin": 0.2, "revenue_growth_yoy": 0.1, "total_debt": 300.0, "cash": 100.0, "ev_revenue": 3.0})
        out = run_arsenal300(
            target_row=target,
            comps_summary={"peer_count": 10},
            precedents_summary={"transaction_count": 12, "valuation_range_low": 900.0, "valuation_range_high": 1250.0},
            dcf_summary={"dcf_gap_to_current": 0.2},
            quality_score=82.0,
            validation_summary={"validation_score": 80.0, "validation_warn_count": 1},
            sensitivity_summary={"probability_band_p10": 800.0, "probability_band_p50": 1000.0, "probability_band_p90": 1300.0},
            buyer_universe_summary={"buyer_count": 15, "top_buyer_score": 0.8},
            negotiation_summary={"opening_bid_ev": 980.0, "walk_away_ev": 1120.0, "stretch_ev": 1280.0},
            risk_gate_summary={"warn_count": 1},
            arsenal50_summary={"arsenal_pass_count": 42, "arsenal_readiness_pct": 0.84},
        )
        self.assertEqual(out.summary["arsenal300_idea_count"], 300)
        self.assertIn("theme", out.arsenal_table.columns)

    def test_arsenal600(self) -> None:
        target = pd.Series({"enterprise_value": 1000.0, "ebitda_margin": 0.2, "revenue_growth_yoy": 0.1, "total_debt": 300.0, "cash": 100.0})
        out = run_arsenal600(
            target_row=target,
            comps_summary={"peer_count": 10},
            precedents_summary={"transaction_count": 12, "valuation_range_low": 900.0, "valuation_range_high": 1250.0},
            dcf_summary={"dcf_gap_to_current": 0.2},
            quality_score=82.0,
            validation_summary={"validation_score": 80.0, "validation_warn_count": 1},
            sensitivity_summary={"probability_band_p10": 800.0, "probability_band_p50": 1000.0},
            buyer_universe_summary={"buyer_count": 15, "top_buyer_score": 0.8},
            negotiation_summary={"opening_bid_ev": 980.0, "walk_away_ev": 1120.0, "stretch_ev": 1280.0},
            risk_gate_summary={"warn_count": 1},
            arsenal300_summary={"arsenal300_pass_count": 210, "arsenal300_readiness_pct": 0.70},
        )
        self.assertEqual(out.summary["arsenal600_idea_count"], 600)
        self.assertIn("domain", out.arsenal_table.columns)

    def test_arsenal_massive(self) -> None:
        out = run_arsenal_massive(
            idea_count=5000,
            comps_summary={"peer_count": 10},
            precedents_summary={"transaction_count": 12},
            validation_summary={"validation_score": 80.0},
            sensitivity_summary={"probability_band_p10": 800.0, "probability_band_p50": 1000.0},
            risk_gate_summary={"warn_count": 1},
        )
        self.assertEqual(out.summary["arsenal_massive_idea_count"], 5000)
        self.assertIn("initiative_id", out.arsenal_table.columns)

    def test_arsenal_extra50(self) -> None:
        out = run_arsenal_extra50(
            comps_summary={"peer_count": 10},
            precedents_summary={"transaction_count": 12},
            validation_summary={"validation_score": 80.0},
            risk_gate_summary={"warn_count": 1},
            arsenal_massive_summary={"arsenal_massive_readiness_pct": 0.7},
        )
        self.assertEqual(out.summary["arsenal_extra50_idea_count"], 50)
        self.assertIn("track", out.arsenal_table.columns)

    def test_contract_validation_fallback_or_pass(self) -> None:
        metrics = pd.DataFrame([{"ticker": "AAA", "revenue": 100.0, "ebitda": 20.0, "enterprise_value": 350.0}])
        precedents = pd.DataFrame([{"ev_revenue": 3.5, "ev_ebitda": 14.0}])
        out = validate_data_contracts(metrics, precedents)
        self.assertIn(out.summary["contracts_skipped"], {0, 1})
        self.assertIn("status", out.table.columns)

    def test_duckdb_store_graceful(self) -> None:
        db_path = Path("./output/test_pipeline.duckdb")
        out = persist_to_duckdb(db_path, {"tbl_a": pd.DataFrame([{"x": 1}, {"x": 2}])})
        self.assertEqual(out.db_path, db_path)
        self.assertIsInstance(out.tables_written, dict)
        if db_path.exists():
            db_path.unlink()


if __name__ == "__main__":
    unittest.main()
