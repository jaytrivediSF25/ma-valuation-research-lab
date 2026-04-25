# M&A Valuation Research Lab

A local, modular, production-style finance data science system for **deal screening, valuation triangulation, and structured investment reasoning**.

This repository turns fragmented SEC/regulatory and financial datasets into a reproducible M&A decision package with:

- engineered financial metrics,
- comparable company and precedent transaction analysis,
- DCF and blended valuation synthesis,
- statistical robustness diagnostics,
- machine-readable + analyst-readable outputs.
- optional API + orchestration surfaces (FastAPI + Prefect),
- optional local analytical warehouse persistence (DuckDB),
- optional contract validation and optimizer layers (Pandera + CVXPY).

---

## 1. Design Principles

1. **Local-first reproducibility**: deterministic data processing with explicit config.
2. **Finance-native outputs**: EV ranges, multiple distributions, implied valuation anchors.
3. **Typed contracts**: strict schemas for downstream reliability.
4. **Model transparency**: explicit formulas and assumptions (no hidden black-box scoring).
5. **Graceful degradation**: AI reasoning falls back to deterministic logic when unavailable.

---

## 2. Problem Statement

M&A analysis usually suffers from 4 recurring issues:

1. Input fragmentation (SEC filings vs peer tables vs precedents in separate formats).
2. Inconsistent entity naming and schema conventions.
3. Weak confidence controls around sparse samples.
4. Narrative outputs disconnected from numerical evidence.

This project addresses all 4 by structuring the workflow into formal pipeline stages and quality controls.

---

## 3. Repository Layout

```text
.
├── app/
│   └── main.py
├── flows/
│   └── pipeline_flow.py
├── analyze_deal.py
├── requirements.txt
├── tests/
│   └── test_pipeline_units.py
├── deal_pipeline/
│   ├── __init__.py
│   ├── config.py
│   ├── ingestion.py
│   ├── normalization.py
│   ├── feature_engineering.py
│   ├── analysis.py
│   ├── quality.py
│   ├── scenarios.py
│   ├── dcf.py
│   ├── robustness.py
│   ├── blended_valuation.py
│   ├── insights.py
│   ├── memo.py
│   ├── export.py
│   ├── schemas.py
│   └── pipeline.py
├── data/      # symlink or local directory
└── output/
```

---

## 4. Data Universe and Input Contracts

### 4.1 Supported file formats

- CSV
- JSON
- XLSX / XLS

### 4.2 Expected data zones

- `data/processed/`:
  - `companies.csv`
  - `filings_target_forms.csv`
  - `filings_all.csv`
  - `companyfacts_all.csv`
  - `companyconcept_revenue.csv`
- `data/raw/`: SEC source JSON payloads
- `data/financials/`: optional external fundamentals
- `data/peers/`: optional peer datasets
- `data/precedents/`: optional transaction datasets

### 4.3 Auto-discovery fallback

If dedicated folders are missing, ingestion discovers candidate files using normalized filename patterns:

- financial: `financial`
- peer/comps: `peer` or `comp`
- precedents: `precedent`, `transaction`, `mna`

---

## 5. Pipeline Stages

## Stage A: Ingestion (`ingestion.py`)

- Loads SEC processed tables.
- Streams large `companyfacts_all.csv` in chunks and filters to core valuation concepts.
- Reads mixed-format external datasets and stamps source lineage.

## Stage B: Normalization (`normalization.py`)

- Entity normalization (ticker/CIK/name standardization).
- Date coercion to datetime.
- Numeric coercion from string-formatted financial fields.
- Currency normalization to USD via explicit FX map.
- Schema alignment from heterogeneous aliases to canonical fields.

## Stage C: Feature Engineering (`feature_engineering.py`)

Primary engineered metrics:

- Revenue
- Revenue growth YoY
- EBITDA
- EBITDA margin
- Enterprise value
- EV/Revenue
- EV/EBITDA

Derivation behavior:

- Prefers SEC concept hierarchy for revenue and EBITDA.
- Derives EV from market cap + debt - cash where EV is unavailable.
- Merges SEC-derived and external fundamentals with precedence rules.

## Stage D: Comps and Precedents (`analysis.py`)

### Comparable analysis

- Peer universe creation (external peers if available; else fallback universe).
- Sector and size bounding.
- Medians and target percentile positions.

### Precedent analysis

- Uses explicit precedents if available.
- Otherwise synthesizes proxy precedent dataset from `S-4` / `8-K` context.
- Sector/size/recency filters.
- Distributional stats and valuation range construction.

## Stage E: Quality / Scenario / DCF / Robustness / Blend

### Quality (`quality.py`)

Weighted confidence score from coverage + sample sufficiency:

- revenue completeness
- EBITDA completeness
- EV completeness
- EV multiple completeness
- peer sufficiency
- precedent sufficiency

### Scenario engine (`scenarios.py`)

Builds implied EV distributions from:

- peer multiple anchors
- precedent quantile anchors

### DCF engine (`dcf.py`)

Computes downside/base/upside DCF cases using configurable assumptions:

- projection years
- WACC
- terminal growth
- tax, capex, depreciation, and NWC ratios

Includes WACC/terminal-growth sensitivity grid.

### Robustness diagnostics (`robustness.py`)

- Bootstrap confidence intervals for multiple distributions.
- Statistical spread metrics.
- Target z-scores vs comps distributions.

### Blended valuation synthesis (`blended_valuation.py`)

Combines 4 anchors with configurable weights:

- comps anchor
- precedents anchor
- scenarios anchor
- DCF anchor

Produces blended implied EV and directional stance (`upside|neutral|downside`).

## Stage F: Insight Generation (`insights.py`)

Signals:

- growth profile
- margin profile
- valuation position
- precedent comparison
- risk flags

AI reasoning:

- Input strictly structured engineered payload.
- Output strict JSON with:
  - `key_insights` (2–4)
  - `primary_risk` (1)
  - `conclusion` (1)

## Stage G: Exports (`export.py`, `memo.py`)

### JSON report

Typed contract via Pydantic.

### Excel workbook

Sheets:

- `summary`
- `comps`
- `precedents`
- `scenarios`
- `dcf`
- `dcf_sens`
- `robustness`
- `blend`
- `quality`
- `contracts`
- `raw_data`

### Markdown memo

Narrative investment memo with:

- financial snapshot
- comps/precedent takeaways
- scenario + DCF interpretation
- blend conclusion
- diagnostics and key risks

---

## 6. Formula Reference

### Core valuation formulas

- `EBITDA Margin = EBITDA / Revenue`
- `Enterprise Value = Market Cap + Debt - Cash`
- `EV/Revenue = Enterprise Value / Revenue`
- `EV/EBITDA = Enterprise Value / EBITDA`

### Growth

- `Revenue Growth YoY = (Revenue_t - Revenue_t-1) / Revenue_t-1`

---

## 7. Runtime Interfaces

### 7.1 CLI (primary)

```bash
python analyze_deal.py --data-dir ./data --output-dir ./output --target-ticker ABT
```

Useful runtime toggles:

- `--disable-duckdb-store`
- `--duckdb-path ./output/warehouse/deal_pipeline.duckdb`
- `--disable-pandera-validation`
- `--disable-blend-optimizer`

### 7.2 API (FastAPI)

```bash
uvicorn app.main:app --reload
```

- `GET /health` for service liveness
- `POST /run` to execute the full pipeline and return paths + diagnostics

### 7.3 Orchestration (Prefect)

```bash
python flows/pipeline_flow.py
```

This executes the same production pipeline through a flow wrapper that can be scheduled/deployed in Prefect.

### DCF framework

For each projected year `t`:

- `Revenue_t = Revenue_(t-1) * (1 + g)`
- `EBITDA_t = Revenue_t * EBITDA_margin`
- `EBIT_t = EBITDA_t - Depreciation_t`
- `NOPAT_t = EBIT_t * (1 - tax_rate)`
- `FCF_t = NOPAT_t + Depreciation_t - Capex_t - ΔNWC_t`
- `PV(FCF_t) = FCF_t / (1 + WACC)^t`

Terminal value:

- `TV = FCF_(n+1) / (WACC - g_terminal)`

Enterprise value:

- `EV = Σ PV(FCF_t) + PV(TV)`

### Blended valuation

- `Blended EV = Σ(weight_i * anchor_i)` over available anchors only.

---

## 7. Configuration Surface

`PipelineConfig` exposes analytical controls for:

- quality thresholds
- signal thresholds
- DCF assumptions
- blend weights
- output behavior

CLI (`analyze_deal.py`) maps directly to key config fields.

Example:

```bash
python3 analyze_deal.py \
  --target-ticker ABT \
  --min-peer-count 7 \
  --min-precedent-count 10 \
  --dcf-projection-years 7 \
  --dcf-wacc-base 0.105 \
  --dcf-terminal-growth-base 0.025 \
  --blend-weight-comps 0.30 \
  --blend-weight-precedents 0.25 \
  --blend-weight-scenarios 0.20 \
  --blend-weight-dcf 0.25
```

---

## 8. Running the Project

## 8.1 Install

```bash
python3 -m pip install --user -r requirements.txt
```

## 8.2 Optional AI key

```bash
export OPENAI_API_KEY="<your_key>"
```

## 8.3 Execute

```bash
python3 analyze_deal.py
```

---

## 9. Testing

```bash
python3 -m unittest discover -s tests -p "test_*.py" -v
```

Current test coverage validates:

- signal threshold behavior
- quality scoring and issue flags
- scenario valuation generation
- DCF case generation and sensitivity output
- blended valuation synthesis

---

## 10. Output Contract (Top-Level JSON)

- `company`
- `financials`
- `comparable_analysis`
- `precedent_transactions`
- `signals`
- `data_quality`
- `valuation_scenarios`
- `dcf_analysis`
- `robustness`
- `blended_valuation`
- `insights`
- `diagnostics`
- `conclusion`

---

## 11. Risk and Limitations

1. SEC concept mapping can vary by filer taxonomy quality.
2. Precedent fallback from filings is directional, not equal to curated M&A databases.
3. FX map is static and should be replaced with dated FX curves for production valuation parity.
4. DCF assumptions are intentionally transparent/simple; advanced capital-structure modeling is not yet included.
5. Statistical robustness depends on sample depth.

---

## 12. Recommended Next Expansions

1. Transaction-level synergy and accretion/dilution module.
2. Multi-currency historical FX normalization with date-aware rates.
3. Capital structure scenarios and equity value bridge.
4. Sector-specific multiple rules and outlier treatment policies.
5. Explainability report that links each insight line to exact metric inputs.

---

## 13. Versioning

Current package version:

- `deal_pipeline.__version__ = 3.0.0`
