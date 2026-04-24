# Local M&A Intelligence Pipeline (SEC + Financial Modeling + AI Reasoning)

This repository contains a **production-style backend analytics pipeline** for M&A evaluation.

It ingests SEC/financial datasets, normalizes them, engineers valuation metrics, runs comps + precedent analysis, generates structured risk/signals, calls an LLM for concise investment insights, and exports both machine-readable and analyst-friendly outputs.

No UI. No web app. Pure local CLI pipeline.

---

## 1. What This Project Does

Given local raw files in `./data`, the pipeline produces:

1. Structured analytical dataset for the target company.
2. Comparable company analysis with median multiples and target percentiles.
3. Precedent transaction valuation ranges.
4. Structured signal detection (`growth_profile`, `margin_profile`, valuation positioning, risk flags).
5. AI-generated insights using **structured metrics only** (not raw filings).
6. Exports:
   - JSON report
   - Excel workbook
   - Markdown memo

---

## 2. Repository Structure

```text
.
├── analyze_deal.py
├── requirements.txt
├── data -> /Users/jaytrivedi/Desktop/SEC_EDGAR_BULK_PULL/data
├── output/
└── deal_pipeline/
    ├── __init__.py
    ├── config.py
    ├── ingestion.py
    ├── normalization.py
    ├── feature_engineering.py
    ├── analysis.py
    ├── insights.py
    ├── quality.py
    ├── scenarios.py
    ├── memo.py
    ├── export.py
    ├── pipeline.py
    ├── schemas.py
    └── utils.py
```

---

## 3. End-to-End Flow

1. `ingestion.py`
   - Reads CSV/JSON/XLSX from known folders and auto-discovered files.
   - Loads SEC-processed assets (`companies`, `filings`, `companyfacts`, etc.).

2. `normalization.py`
   - Removes duplicates.
   - Standardizes company names.
   - Converts dates to datetime.
   - Converts numeric text to numeric types.
   - Normalizes currencies to USD (rule-based FX map).
   - Aligns heterogeneous input columns into canonical schemas.

3. `feature_engineering.py`
   - Derives revenue, EBITDA, margins, EV.
   - Computes EV/Revenue and EV/EBITDA.
   - Merges SEC-derived metrics + external financial metrics.
   - Selects target company by ticker/CIK/name or defaults to largest EV.

4. `analysis.py`
   - Comparable analysis:
     - Peer filtering by sector and size bounds.
     - Peer median multiples.
     - Target percentile vs peers.
   - Precedent analysis:
     - Uses explicit precedent data if present, else derives proxy transactions from `S-4`/`8-K`.
     - Filters by sector, size, and recency.
     - Calculates EV multiple distributions and valuation range.

5. `quality.py` (new)
   - Computes data quality coverage checks and issues.
   - Produces a weighted quality score (0–100).

6. `scenarios.py` (new)
   - Builds downside/base/upside implied EV scenarios from comps and precedents.
   - Computes spread vs current EV.

7. `insights.py`
   - Rule-based signals.
   - LLM output in strict JSON schema (`key_insights`, `primary_risk`, `conclusion`).
   - Falls back to deterministic rule-based insight generation if API key/model call fails.

8. `export.py`
   - Validates output with Pydantic schemas.
   - Writes JSON report.
   - Writes multi-sheet Excel:
     - `summary`
     - `comps`
     - `precedents`
     - `scenarios`
     - `quality`
     - `raw_data`

9. `memo.py` (new)
   - Writes a clean markdown investment memo in `output/`.

---

## 4. Data Inputs

The pipeline accepts local files in any of these formats:

- `.csv`
- `.json`
- `.xlsx` / `.xls`

### Expected Data Areas

- `data/processed/` (SEC-preprocessed tables)
- `data/raw/` (SEC raw JSONs)
- `data/financials/` (optional external financial tables)
- `data/peers/` (optional peer dataset)
- `data/precedents/` (optional transaction dataset)

If structured folders are missing, the ingestion logic auto-discovers compatible files by filename keywords:

- financial datasets: names containing `financial`
- peer datasets: names containing `peer` or `comp`
- precedent datasets: names containing `precedent`, `transaction`, or `mna`

---

## 5. Financial Formulas Used

Core metrics:

- `EBITDA Margin = EBITDA / Revenue`
- `Enterprise Value = Market Cap + Total Debt - Cash`
- `EV/Revenue = Enterprise Value / Revenue`
- `EV/EBITDA = Enterprise Value / EBITDA`
- `Revenue Growth YoY = (Revenue - Prior Revenue) / Prior Revenue`

Comparable outputs:

- Peer median EV multiples
- Target percentile ranking in peer distribution

Precedent outputs:

- Median/P25/P75 EV/Revenue and EV/EBITDA
- Valuation range (low/high) from precedent multiple bands applied to target scale

---

## 6. Signal Logic

Generated signals:

- `growth_profile`: `high | medium | low`
- `margin_profile`: `strong | average | weak`
- `valuation_position`: `premium | fair | discounted`
- `precedent_comparison`: `above | within | below_range`
- `risk_flags`: derived issues such as:
  - `thin_peer_set`
  - `thin_precedent_set`
  - `negative_ebitda`
  - `missing_enterprise_value`
  - `subscale_growth`

All thresholds are configurable via CLI flags.

---

## 7. Quick Start

### 7.1 Install

```bash
cd /Users/jaytrivedi/Documents/Codex/2026-04-23-files-mentioned-by-the-user-sec-2
python3 -m pip install --user -r requirements.txt
```

### 7.2 Optional LLM setup

```bash
export OPENAI_API_KEY="your_key_here"
```

### 7.3 Run

```bash
python3 analyze_deal.py
```

---

## 8. CLI Options

```bash
python3 analyze_deal.py \
  --data-dir ./data \
  --output-dir ./output \
  --target-ticker ABT \
  --openai-model gpt-4o-mini \
  --min-peer-count 5 \
  --min-precedent-count 5 \
  --low-growth-threshold 0.03 \
  --high-growth-threshold 0.15 \
  --weak-margin-threshold 0.12 \
  --strong-margin-threshold 0.25 \
  --premium-multiple-buffer 0.15 \
  --discounted-multiple-buffer 0.15
```

### Option Reference

- `--data-dir`: input data root.
- `--output-dir`: artifacts directory.
- `--target-ticker`, `--target-cik`, `--target-company`: explicit target selector.
- `--openai-model`: model for insight layer.
- `--max-raw-rows-for-excel`: cap row volume in `raw_data` sheet.
- `--min-peer-count`, `--min-precedent-count`: quality thresholds.
- `--low-growth-threshold`, `--high-growth-threshold`: growth profile cutoffs.
- `--weak-margin-threshold`, `--strong-margin-threshold`: margin profile cutoffs.
- `--premium-multiple-buffer`, `--discounted-multiple-buffer`: valuation positioning buffers.
- `--disable-markdown-memo`: skips markdown memo generation.

---

## 9. Output Artifacts

Each run writes timestamped files into `output/`:

1. JSON report:
   - `deal_analysis_<TICKER>_<YYYYMMDD_HHMMSS>.json`
2. Excel workbook:
   - `deal_analysis_<TICKER>_<YYYYMMDD_HHMMSS>.xlsx`
3. Markdown memo:
   - `deal_analysis_<TICKER>_memo.md`

### JSON Top-Level Keys

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

## 10. Example JSON Shape

```json
{
  "company": {
    "name": "ABBOTT LABORATORIES",
    "ticker": "ABT",
    "cik": "0000001800",
    "sector": null
  },
  "financials": {
    "revenue": 42873000000.0,
    "ebitda": 12035000000.0,
    "enterprise_value": 170000000000.0
  },
  "comparable_analysis": {
    "peer_count": 12,
    "peer_median_ev_revenue": 4.9
  },
  "precedent_transactions": {
    "transaction_count": 35,
    "valuation_range_low": 148000000000.0,
    "valuation_range_high": 192000000000.0
  },
  "signals": {
    "growth_profile": "medium",
    "margin_profile": "strong",
    "valuation_position": "fair",
    "precedent_comparison": "within",
    "risk_flags": []
  },
  "data_quality": {
    "score": 84.5,
    "checks": {},
    "issues": []
  },
  "valuation_scenarios": {
    "scenario_count": 10,
    "implied_ev_low": 151000000000.0,
    "implied_ev_base": 171500000000.0,
    "implied_ev_high": 198200000000.0
  },
  "insights": {
    "key_insights": [
      "..."
    ],
    "primary_risk": "...",
    "conclusion": "..."
  },
  "diagnostics": {
    "companies_loaded": 50
  },
  "conclusion": "..."
}
```

---

## 11. Data Quality Scoring (new)

Score range: `0 - 100`

Weighted components:

- Revenue completeness: 20%
- EBITDA completeness: 20%
- EV completeness: 20%
- EV/Revenue completeness: 10%
- EV/EBITDA completeness: 10%
- Peer sufficiency: 10%
- Precedent sufficiency: 10%

Issue flags include:

- `low_company_metric_coverage`
- `low_revenue_completeness`
- `low_ebitda_completeness`
- `low_ev_completeness`
- `insufficient_peer_set`
- `insufficient_precedent_set`

---

## 12. Valuation Scenario Modeling (new)

Scenario engine combines:

- Peer median EV/Revenue, EV/EBITDA
- Precedent P25/P75 EV multiples
- Target scale metrics (Revenue, EBITDA)

Outputs:

- Scenario table with implied EV per method
- `implied_ev_low`, `implied_ev_base`, `implied_ev_high`
- `gap_to_base` vs current EV

---

## 13. Running Unit Tests

```bash
python3 -m unittest discover -s tests -p "test_*.py" -v
```

---

## 14. Troubleshooting

### `ModuleNotFoundError`

Install dependencies:

```bash
python3 -m pip install --user -r requirements.txt
```

### `No engineered company metrics available`

- Confirm `data/processed/companyfacts_all.csv` exists and is non-empty.
- Confirm relevant SEC concepts are present for revenue/debt/cash/EBITDA derivations.

### Low peer/precedent count

- Add external `data/peers` and `data/precedents`.
- Relax thresholds:
  - `--min-peer-count`
  - `--min-precedent-count`

---

## 15. Extension Guide

To extend this pipeline:

1. Add new normalized concepts in `normalization.py`.
2. Add additional SEC concept mappings in `feature_engineering.py`.
3. Add advanced filtering logic in `analysis.py`.
4. Add domain-specific signals in `insights.py`.
5. Add custom output sheets in `export.py`.
6. Add additional model-based scenario methods in `scenarios.py`.

---

## 16. Notes on AI Layer

- The LLM receives only structured payloads (not raw filings).
- Output contract is strict JSON.
- If API calls fail or API key is absent, pipeline uses deterministic fallback logic.

---

## 17. Current Production Status

- CLI pipeline: implemented
- SEC ingestion integration: implemented
- Comps analysis: implemented
- Precedent analysis: implemented
- Signal detection: implemented
- AI JSON insight generation: implemented
- JSON + Excel export: implemented
- Markdown memo export: implemented
- Unit tests for key components: implemented

