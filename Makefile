PY := .venv/bin/python
PIP := .venv/bin/pip

.PHONY: venv generate ingest normalize run test up down ais dbt eta

venv:
	python3 -m venv .venv
	$(PIP) install -q -e ".[dev]"

generate:
	$(PY) -m generator.generate --count 200 --seed 42

ingest:
	$(PY) -m pipelines.ingest

normalize:
	$(PY) -m pipelines.normalize

run: generate ingest normalize

test:
	.venv/bin/pytest -q

up:
	docker compose up -d

down:
	docker compose down

ais:  # reads AISSTREAM_API_KEY from .env if present
	@set -a; [ -f .env ] && . ./.env; set +a; $(PY) -m ais.consumer

dbt:  # gold layer build + tests (R9); needs `python3.13 -m venv .venv-dbt && .venv-dbt/bin/pip install dbt-duckdb`
	@[ -f data/silver/dim_vessel.csv ] || echo "mmsi,imo,name,type,observed_at" > data/silver/dim_vessel.csv
	@$(PY) -c "import yaml,csv,sys; vs=yaml.safe_load(open('config/vessels.yml'))['vessels']; w=csv.DictWriter(open('config/vessels_flat.csv','w',newline=''),fieldnames=['mmsi','name']); w.writeheader(); [w.writerow(v) for v in vs]"
	@mkdir -p data/gold
	cd dbt && DBT_PROFILES_DIR=. ../.venv-dbt/bin/dbt build --quiet
	@.venv-dbt/bin/python -c "import duckdb; c=duckdb.connect('data/gold/manifest.duckdb'); [c.sql(f\"copy {m} to 'data/gold/{m}.csv' (header)\") for m in ('carrier_scorecard','eta_baseline')]; print('exported carrier_scorecard.csv, eta_baseline.csv')"

eta:  # R10 predictions (uses gold/eta_baseline.csv when present)
	$(PY) -m eta.predict
