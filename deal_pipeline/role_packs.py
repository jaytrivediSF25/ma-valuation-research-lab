from dataclasses import dataclass
from pathlib import Path
from typing import Dict


@dataclass
class RolePackResult:
    pack_dir: Path
    files: Dict[str, str]


def generate_role_packs(output_dir: Path, report: Dict) -> RolePackResult:
    ticker = str(report.get("company", {}).get("ticker") or "TARGET")
    pack_dir = output_dir / f"role_packs_{ticker}"
    pack_dir.mkdir(parents=True, exist_ok=True)

    files: Dict[str, str] = {}

    md_text = (
        f"Ticker: {ticker}\n"
        f"Conclusion: {report.get('conclusion')}\n"
        f"Blended stance: {report.get('blended_valuation', {}).get('blend_stance')}\n"
        f"Primary risk: {report.get('insights', {}).get('primary_risk')}\n"
    )
    vp_text = (
        f"Comps median EV/Revenue: {report.get('comparable_analysis', {}).get('peer_median_ev_revenue')}\n"
        f"Precedent median EV/EBITDA: {report.get('precedent_transactions', {}).get('median_ev_ebitda')}\n"
        f"DCF implied EV base: {report.get('dcf_analysis', {}).get('implied_ev_base')}\n"
        f"Validation score: {report.get('validation', {}).get('validation_score')}\n"
    )
    associate_text = (
        "Key insights:\n"
        + "\n".join([f"- {x}" for x in report.get("insights", {}).get("key_insights", [])])
        + "\n"
        + f"Risk flags: {', '.join(report.get('signals', {}).get('risk_flags', []))}\n"
    )
    de_text = (
        f"Run diagnostics: {report.get('diagnostics', {})}\n"
        f"Contract checks: {report.get('contract_validation', {})}\n"
        f"Lineage: {report.get('lineage', {})}\n"
    )

    payloads = {
        "md_pack.txt": md_text,
        "vp_pack.txt": vp_text,
        "associate_pack.txt": associate_text,
        "data_engineering_pack.txt": de_text,
    }
    for fname, text in payloads.items():
        p = pack_dir / fname
        p.write_text(text, encoding="utf-8")
        files[fname] = str(p)

    return RolePackResult(pack_dir=pack_dir, files=files)
