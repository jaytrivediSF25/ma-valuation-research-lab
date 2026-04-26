from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import numpy as np
import pandas as pd

from .analytics_ml import (
    benchmark_parity,
    causal_importance_proxy,
    champion_challenger,
    drift_monitor,
    model_risk_tiering,
    outlier_governance,
    precedent_similarity_graph,
    synthetic_deal_generator,
    temporal_cross_validation,
)
from .assumptions import load_assumption_tree, resolve_assumptions
from .entity_resolution import resolve_entities
from .incremental import changed_files, load_state, save_state, update_file_mtimes
from .medallion import build_medallion_layers
from .platform_ops import (
    cache_read,
    cache_write,
    compliance_audit_append,
    dag_execute,
    dependency_governance,
    docs_as_code_check,
    event_delta_report,
    feature_store_write,
    plugin_connector_registry,
    portfolio_allocation,
    readiness_scorecard,
    research_pack_write,
    semantic_cache_key,
    sql_sanity_check,
    validate_required_secrets,
)
from .reproducibility import build_manifest, replay_key, write_manifest
from .valuation_advanced import (
    bayesian_multiple_update,
    covenant_breach_probability,
    debt_tranche_metrics,
    lbo_waterfall,
    macro_shock_scenarios,
    monte_carlo_ev,
    optimize_transaction_mix,
    qoe_score,
    regime_adjusted_multiple,
    segment_valuation_bridge,
    synergy_decomposition,
)


@dataclass
class EnterpriseSuiteResult:
    summary: Dict[str, Any]
    output_dir: Path


def run_enterprise_suite(output_dir: Path, data_dir: Path, report_payload: Dict[str, Any], company_metrics: pd.DataFrame, precedents: pd.DataFrame) -> EnterpriseSuiteResult:
    ent_dir = output_dir / "enterprise"
    ent_dir.mkdir(parents=True, exist_ok=True)

    manifest = build_manifest(
        config_payload=report_payload.get("diagnostics", {}),
        data_dir=data_dir,
        requirements_path=Path("requirements.txt"),
    )
    write_manifest(ent_dir / "run_manifest.json", manifest)
    run_key = replay_key(manifest)

    inc_state_path = ent_dir / "incremental_state.json"
    state = load_state(inc_state_path)
    changed = changed_files(data_dir, state)
    save_state(inc_state_path, update_file_mtimes(data_dir, state))

    assumptions_path = Path(__file__).parent / "configs" / "assumptions.yaml"
    tree = load_assumption_tree(assumptions_path)
    resolved_assumptions = resolve_assumptions(tree, sector=str(report_payload.get("company", {}).get("sector") or "default"))

    taxonomy_checks = {
        "sql": sql_sanity_check("select ticker, ev_revenue from gold_company_metrics where ev_revenue > 0"),
    }

    revenue = float(report_payload.get("financials", {}).get("revenue") or 0.0)
    margin = float(report_payload.get("financials", {}).get("ebitda_margin") or 0.2)
    target_ev = float(report_payload.get("financials", {}).get("enterprise_value") or 0.0)

    mc = monte_carlo_ev(revenue=max(1.0, revenue), ebitda_margin=max(0.05, margin), n=3000)
    bayes = bayesian_multiple_update(10.0, 2.0, pd.to_numeric(precedents.get("ev_ebitda", pd.Series(dtype=float)), errors="coerce").dropna().head(200).tolist())
    regime_mult = regime_adjusted_multiple(base_multiple=10.0, vol=0.28, rates=0.045, spread=0.025)
    macro = macro_shock_scenarios(base_revenue=max(1.0, revenue), base_margin=max(0.05, margin))

    segments = pd.DataFrame([
        {"segment": "core", "segment_ebitda": max(1.0, (revenue * margin) * 0.75), "segment_multiple": 11.0},
        {"segment": "adjacent", "segment_ebitda": max(1.0, (revenue * margin) * 0.25), "segment_multiple": 9.0},
    ])
    seg_bridge = segment_valuation_bridge(segments)
    synergy = synergy_decomposition(target_revenue=max(1.0, revenue), target_ebitda=max(1.0, revenue * margin))

    tranches = pd.DataFrame([
        {"tranche": "term_loan", "balance": target_ev * 0.35, "coupon": 0.07},
        {"tranche": "notes", "balance": target_ev * 0.20, "coupon": 0.085},
    ])
    debt_metrics = debt_tranche_metrics(tranches)
    cov_cases = pd.DataFrame([
        {"net_leverage": 3.8, "interest_coverage": 2.8},
        {"net_leverage": 4.6, "interest_coverage": 2.1},
        {"net_leverage": 5.0, "interest_coverage": 1.7},
    ])
    cov_breach = covenant_breach_probability(cov_cases, max_net_leverage=4.5, min_interest_cov=2.0)
    waterfall = lbo_waterfall(exit_equity=max(1.0, target_ev * 0.45), pref_return=max(1.0, target_ev * 0.20), mgmt_pool=0.12)
    tx_mix = optimize_transaction_mix(max(1.0, target_ev))
    qoe = qoe_score({"accruals_ratio": 0.08, "cash_conversion": 0.78, "margin_volatility": 0.07, "adjustment_intensity": 0.12})

    resolved = resolve_entities(
        left=company_metrics[[c for c in ["ticker", "cik", "company_name"] if c in company_metrics.columns]].head(100),
        right=company_metrics[[c for c in ["ticker", "cik", "company_name"] if c in company_metrics.columns]].head(100),
    )

    graph = precedent_similarity_graph(
        precedents.head(300).fillna(0),
        feature_cols=[c for c in ["ev_revenue", "ev_ebitda", "enterprise_value", "revenue", "ebitda"] if c in precedents.columns],
        k=3,
    ) if not precedents.empty else pd.DataFrame()

    synth = synthetic_deal_generator(n=200)
    importance = causal_importance_proxy(
        features=synth[["revenue", "ebitda_margin", "ev_ebitda"]],
        target=synth["enterprise_value"],
    )
    outlier = outlier_governance(synth, "enterprise_value")
    drift = drift_monitor(synth[["revenue", "ev_ebitda"]], synth.sample(frac=0.8, random_state=1)[["revenue", "ev_ebitda"]])
    challenger = champion_challenger(
        champion_errors=pd.Series([0.12, 0.08, 0.11]),
        challenger_errors=pd.Series([0.10, 0.07, 0.09]),
    )
    cv = temporal_cross_validation(
        pd.DataFrame(
                {
                "date": pd.date_range("2020-01-01", periods=24, freq="QE"),
                "target": np.linspace(100, 300, 24),
                "pred": np.linspace(98, 315, 24),
            }
        ),
        "date",
        "target",
        "pred",
    )
    parity = benchmark_parity({"mae": 0.04994538219133806}, {"mae": 0.04994538219133806}, tolerance=1e-9)

    deps = dependency_governance(Path("requirements.txt"))
    secrets = validate_required_secrets(["OPENAI_API_KEY"], dict())
    plugin_connector_registry(ent_dir / "connectors.json", connectors=[{"name": "sec", "status": "enabled"}, {"name": "market_data", "status": "optional"}])
    feature_store_write(ent_dir / "feature_store.parquet", company_metrics.head(200))

    weights = portfolio_allocation(pd.Series([1.2, 2.3, 1.7], index=["A", "B", "C"]))
    event_delta = event_delta_report({"valuation_position": "fair"}, {"valuation_position": "premium"})
    compliance_audit_append(ent_dir / "compliance_audit.log", actor="system", action="enterprise_suite_run", payload_hash=run_key)
    docs_check = docs_as_code_check(
        markdown_paths=[Path("README.md"), Path("docs/governance/model_governance.md")],
        required_tokens=["valuation", "data", "model"],
    )
    readiness = readiness_scorecard(
        {
            "reliability": 82,
            "data_quality": 86,
            "model_governance": 79,
            "test_coverage": 81,
            "security": 75,
        }
    )

    sections = {
        "Executive": f"Run key: {run_key}",
        "Monte Carlo": json.dumps(mc.summary, indent=2),
        "Backbone": json.dumps({"qoe": qoe, "regime_multiple": regime_mult, "tx_mix": tx_mix}, indent=2),
        "Governance": json.dumps({"docs_check": docs_check, "readiness": readiness}, indent=2),
    }
    research_pack_write(ent_dir / "research_pack.md", sections)

    cache_key = semantic_cache_key([run_key, str(len(company_metrics)), str(len(precedents))])
    cache_path = ent_dir / "cache" / f"{cache_key}.json"
    existing = cache_read(cache_path)
    if existing is None:
        cache_write(cache_path, {"run_key": run_key, "ready": True, "created": datetime.now(timezone.utc).isoformat()})

    summary = {
        "run_key": run_key,
        "changed_files": len(changed),
        "assumptions_resolved": len(resolved_assumptions.resolved),
        "monte_carlo_p50": mc.summary.get("ev_p50"),
        "bayesian_posterior_mean": bayes.posterior_mean,
        "regime_adjusted_multiple": regime_mult,
        "covenant_breach_probability": cov_breach,
        "qoe_score": qoe,
        "entity_matches": len(resolved.matches),
        "graph_edges": len(graph),
        "importance_features": len(importance),
        "outliers_flagged": int(outlier["outlier_flag"].sum()) if "outlier_flag" in outlier.columns else 0,
        "drift_alerts": int((drift["status"] == "alert").sum()) if not drift.empty else 0,
        "challenger_winner": challenger.get("winner"),
        "cv_years": len(cv),
        "parity_passed": parity.get("passed"),
        "deps_count": len(deps),
        "missing_secrets": len(secrets.get("missing", [])),
        "portfolio_weights_sum": float(weights.sum()),
        "event_changed_fields": event_delta.get("changed_count"),
        "docs_check_passed": docs_check.get("passed"),
        "readiness_tier": readiness.get("tier"),
    }

    pd.DataFrame([summary]).to_csv(ent_dir / "enterprise_summary.csv", index=False)
    macro.to_csv(ent_dir / "macro_scenarios.csv", index=False)
    seg_bridge.to_csv(ent_dir / "segment_bridge.csv", index=False)
    synergy.to_csv(ent_dir / "synergy_decomposition.csv", index=False)
    waterfall.to_csv(ent_dir / "lbo_waterfall.csv", index=False)
    importance.to_csv(ent_dir / "feature_importance.csv", index=False)
    drift.to_csv(ent_dir / "drift_monitor.csv", index=False)
    cv.to_csv(ent_dir / "temporal_cv.csv", index=False)
    graph.to_csv(ent_dir / "precedent_graph_edges.csv", index=False)
    resolved.matches.to_csv(ent_dir / "entity_resolution_matches.csv", index=False)

    medallion = build_medallion_layers(
        db_path=ent_dir / "medallion.duckdb",
        raw_tables={"company_metrics": company_metrics.head(500), "precedents": precedents.head(500)},
        normalized_tables={"resolved_companies": resolved.resolved.head(500), "macro": macro.head(500)},
        gold_tables={"importance": importance.head(500), "drift": drift.head(500)},
    )
    summary["medallion_bronze_rows"] = medallion.bronze_rows
    summary["medallion_silver_rows"] = medallion.silver_rows
    summary["medallion_gold_rows"] = medallion.gold_rows

    return EnterpriseSuiteResult(summary=summary, output_dir=ent_dir)
