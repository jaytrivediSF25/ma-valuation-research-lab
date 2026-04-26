# ADR 001 Architecture

Status: accepted

Decision
- Use modular Python package architecture with CLI first execution
- Use pandas for canonical tabular transforms and DuckDB as local analytical store
- Keep AI reasoning downstream of deterministic financial engineering

Consequences
- Transparent formulas and deterministic replay for core valuation logic
- Optional AI layer can fail open without blocking output generation
