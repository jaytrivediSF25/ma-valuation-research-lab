# M&A Valuation Research Lab

A production-grade, local-first **finance data science pipeline** for transforming fragmented SEC, market, and transaction data into valuation-ready M&A intelligence.

This project is designed for analyst workflows that need:

- disciplined data engineering,
- transparent valuation mechanics,
- reproducible outputs,
- and structured AI-assisted interpretation.

No frontend. No dashboard. Pure analytical backend.

---

## Investment Use Case

The pipeline answers a core transaction question:

**"Given available fundamentals, peers, and precedents, is the target trading/valued at a premium, fair value, or discount, and what are the key risks to conviction?"**

The system produces decision-support artifacts suitable for:

- internal IC prep,
- banker valuation framing,
- corp-dev screening,
- PE diligence triage,
- and repeatable model refreshes.

---

## Analytical Scope

### Inputs

- SEC filing data (`S-4`, `8-K`, `10-K`, `10-Q`)
- SEC company facts (XBRL concepts)
- Optional external financial datasets
- Optional peer datasets
- Optional precedent transaction datasets

Supported file formats:

- `CSV`
- `JSON`
- `XLSX/XLS`

### Core Outputs

1. Canonical, cleaned analytical dataset.
2. Comparable company analysis (peer medians, target percentile position).
3. Precedent transaction analysis (distributional multiples, implied range).
4. Structured signal layer (growth/margin/valuation/risk flags).
5. AI reasoning layer over engineered metrics (strict JSON response contract).
6. Export package:
   - machine-readable JSON,
   - finance-friendly Excel workbook,
   - markdown investment memo.

---

## Architecture

```text
analyze_deal.py                     # CLI entry point

└── deal_pipeline/
    ├── ingestion.py                # multi-format file ingestion + discovery
    ├── normalization.py            # schema harmonization + cleaning + FX normalization
    ├── feature_engineering.py      # metric derivation (Revenue/EBITDA/EV/multiples)
    ├── analysis.py                 # comps + precedents analytical engine
    ├── quality.py                  # data quality checks and confidence score
    ├── scenarios.py                # implied valuation scenario engine
    ├── insights.py                 # signal detection + AI reasoning
    ├── memo.py                     # narrative memo generation
    ├── export.py                   # JSON/XLSX artifacts
    ├── schemas.py                  # strict Pydantic contracts
    ├── config.py                   # pipeline runtime configuration
    └── pipeline.py                 # orchestration layer
```

---

## Methodology

## 1) Ingestion

- Reads curated directories (`data/processed`, `data/financials`, `data/peers`, `data/precedents`).
- Auto-discovers compatible files by naming signals when folder conventions are incomplete.
- Preserves source-file lineage in loaded frames.

## 2) Normalization

- Removes duplicates.
- Standardizes company names and identifiers.
- Parses dates to ISO-compatible datetime representation.
- Coerces numeric text to numeric types.
- Converts supported non-USD currencies to USD via rule-based FX mapping.
- Aligns heterogenous fields into canonical schemas.

## 3) Feature Engineering

- Revenue (latest available from prioritized SEC concepts or external sources)
- Revenue growth (YoY where historical reference exists)
- EBITDA (direct concept or operating-income + depreciation proxy)
- EBITDA margin
- Enterprise value (derived when direct EV unavailable)
- EV/Revenue and EV/EBITDA

## 4) Comps Analysis

- Builds peer set from external peers, otherwise from available company universe.
- Applies sector and scale filters with minimum sample guardrails.
- Computes peer medians and target percentile ranking on EV multiples.

## 5) Precedent Analysis

- Uses explicit precedents when provided.
- Falls back to filing-derived transaction proxy set when needed.
- Filters by sector, size, and recency.
- Computes median/P25/P75 multiple distributions and implied valuation range.

## 6) Signal Layer

Outputs discrete analytical labels:

- `growth_profile`: `high | medium | low`
- `margin_profile`: `strong | average | weak`
- `valuation_position`: `premium | fair | discounted`
- `precedent_comparison`: `above | within | below_range`
- `risk_flags`: machine-detected quality/coverage risks

## 7) AI Reasoning Layer

- Input: engineered/aggregated metrics only (never raw filings).
- Output contract (strict JSON):
  - `key_insights` (2–4),
  - `primary_risk` (1),
  - `conclusion` (1 concise line).
- Deterministic fallback logic if model/API unavailable.

---

## Data Quality and Confidence Controls

`quality.py` computes a weighted score (0–100) from:

- revenue completeness,
- EBITDA completeness,
- EV completeness,
- multiple completeness,
- peer sufficiency,
- precedent sufficiency.

Issue flags include:

- `low_revenue_completeness`
- `low_ebitda_completeness`
- `low_ev_completeness`
- `insufficient_peer_set`
- `insufficient_precedent_set`

This score is included in JSON, Excel, and memo output.

---

## Valuation Scenario Engine

`scenarios.py` generates implied EV cases from:

- peer EV/Revenue and EV/EBITDA anchors,
- precedent P25/P75 distribution anchors,
- target scale metrics (Revenue/EBITDA).

Produces:

- scenario-level implied enterprise values,
- low/base/high aggregate implied EV,
- gap-to-base relative to current EV.

---

## Outputs

Each run writes artifacts to `output/`:

1. `deal_analysis_<TICKER>_<TIMESTAMP>.json`
2. `deal_analysis_<TICKER>_<TIMESTAMP>.xlsx`
3. `deal_analysis_<TICKER>_memo.md`

Excel workbook sheets:

- `summary`
- `comps`
- `precedents`
- `scenarios`
- `quality`
- `raw_data`

JSON top-level contract:

- `company`
- `financials`
- `comparable_analysis`
- `precedent_transactions`
- `signals`
- `data_quality`
- `valuation_scenarios`
- `insights`
- `diagnostics`
- `conclusion`

---

## Runbook

## Setup

```bash
cd /Users/jaytrivedi/Documents/Codex/2026-04-23-files-mentioned-by-the-user-sec-2
python3 -m pip install --user -r requirements.txt
```

Optional AI reasoning:

```bash
export OPENAI_API_KEY="<your_key>"
```

## Execute

```bash
python3 analyze_deal.py
```

Example with explicit controls:

```bash
python3 analyze_deal.py \
  --target-ticker ABT \
  --openai-model gpt-4o-mini \
  --min-peer-count 7 \
  --min-precedent-count 10 \
  --low-growth-threshold 0.04 \
  --high-growth-threshold 0.18 \
  --weak-margin-threshold 0.10 \
  --strong-margin-threshold 0.24 \
  --premium-multiple-buffer 0.20 \
  --discounted-multiple-buffer 0.20
```

---

## CLI Reference

- `--data-dir`: root input directory (default `./data`)
- `--output-dir`: artifact output directory (default `./output`)
- `--target-ticker`: target ticker override
- `--target-cik`: target CIK override
- `--target-company`: target name override
- `--openai-model`: insight model name
- `--max-raw-rows-for-excel`: cap row volume for `raw_data` tab
- `--min-peer-count`: minimum peer sample confidence threshold
- `--min-precedent-count`: minimum precedent sample confidence threshold
- `--low-growth-threshold`: low/medium growth cutoff
- `--high-growth-threshold`: medium/high growth cutoff
- `--weak-margin-threshold`: weak/average margin cutoff
- `--strong-margin-threshold`: average/strong margin cutoff
- `--premium-multiple-buffer`: premium vs peer median buffer
- `--discounted-multiple-buffer`: discounted vs peer median buffer
- `--disable-markdown-memo`: skip markdown memo generation

---

## Reproducibility and Governance

- Deterministic transformations for ingestion/normalization/feature engineering.
- Typed output schemas via Pydantic.
- Source lineage retained on ingested records (`source_file`).
- Test coverage for key logic blocks (`tests/test_pipeline_units.py`).
- Version-controlled code and parameterized thresholds for auditability.

---

## Tests

```bash
python3 -m unittest discover -s tests -p "test_*.py" -v
```

---

## Current Dataset Note

The linked dataset currently contains strong SEC filing/facts coverage and limited external peer/precedent enrichment. Analytical outputs remain valid, but peer-based valuation conviction improves materially when richer `data/peers` and `data/precedents` tables are provided.

---

## Roadmap (Suggested Next Upgrades)

- Add DCF module with WACC, terminal growth, and sensitivity cube.
- Add accretion/dilution merger model for buyer/target scenarios.
- Add geography-aware precedent filters and FX history tables.
- Add confidence-weighted blended valuation framework.

