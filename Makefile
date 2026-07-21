PY := .venv/bin/python
PIP := .venv/bin/pip

.PHONY: venv generate ingest normalize run test up down ais

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

ais:
	$(PY) -m ais.consumer
