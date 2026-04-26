PYTHON ?= python3

.PHONY: setup ingest normalize features analyze export test lint typecheck security contracts smoke clean

setup:
	$(PYTHON) -m pip install -r requirements.txt

ingest:
	$(PYTHON) analyze_deal.py --data-dir ./data --output-dir ./output --target-ticker ABT --disable-markdown-memo --disable-duckdb-store

normalize:
	$(PYTHON) scripts/check_contracts.py --data-dir ./data --output-dir ./output

features:
	$(PYTHON) scripts/build_feature_snapshot.py --data-dir ./data --output-dir ./output

analyze:
	$(PYTHON) analyze_deal.py --data-dir ./data --output-dir ./output --target-ticker ABT

export:
	$(PYTHON) scripts/generate_role_packs.py --data-dir ./data --output-dir ./output --target-ticker ABT

test:
	$(PYTHON) -m unittest discover -s tests -p 'test_*.py' -v

lint:
	ruff check .

format:
	ruff format .

typecheck:
	mypy deal_pipeline app analyze_deal.py scripts

security:
	bandit -q -r deal_pipeline app scripts analyze_deal.py

contracts:
	$(PYTHON) scripts/check_contracts.py --data-dir ./data --output-dir ./output --strict

smoke:
	$(PYTHON) analyze_deal.py --target-ticker ABT --disable-markdown-memo

clean:
	find . -name '__pycache__' -type d -prune -exec rm -rf {} +
	find . -name '*.pyc' -delete
