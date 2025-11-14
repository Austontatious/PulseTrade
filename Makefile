.PHONY: models.fetch build up down logs api test fmt lint type data.fetch data.compile data.validate train.nbeats train-nbeats train-tft train-chronos scenarios.train scenarios.bundle scenarios.up graphx.train graphx.bundle graphx.up llm-up llm-logs llm-restart test-llm migrate-llm eval-llm quiver-backfill quiver-update quiver-backfill-tier1 quiver-update-tier1 ingest-backfill

models.fetch:
	@mkdir -p /mnt/data/models
	@cd /mnt/data/models && \
		(git clone https://github.com/state-spaces/s4.git s4 || true)
	@cd /mnt/data/models && \
		mkdir -p nbeats && \
		(git clone https://github.com/ServiceNow/N-BEATS.git nbeats/servicenow || true) && \
		(git clone https://github.com/philipperemy/n-beats.git nbeats/philipperemy || true)
	@cd /mnt/data/models && \
		mkdir -p nhits && \
		(git clone https://github.com/cchallu/n-hits.git nhits || true)
	@cd /mnt/data/models && \
		mkdir -p tft && cd tft && \
		(git clone https://github.com/sktime/pytorch-forecasting.git pytorch-forecasting || true)
	@cd /mnt/data/models && \
		(git clone https://github.com/Nixtla/neuralforecast.git neuralforecast || true)
	@cd /mnt/data/models && \
		mkdir -p gnn && cd gnn && \
		(git clone https://github.com/benedekrozemberczki/pytorch_geometric_temporal.git pytorch_geometric_temporal || true) && \
		(git clone https://github.com/microsoft/StemGNN.git stemgnn || true)
	@cd /mnt/data/models && \
		mkdir -p diffusion && cd diffusion && \
		(git clone https://github.com/awslabs/gluonts.git gluonts || true) && \
		(git clone https://github.com/ermongroup/CSDI.git csdi || true) && \
		(git clone https://github.com/zalandoresearch/pytorch-ts.git pytorch-ts || true)

build:
	docker compose build --pull

up:
	docker compose --compatibility up -d

down:
	docker compose --compatibility down

logs:
	docker compose --compatibility logs -f kronos-nbeats kronos-graph
api:
	curl -s http://localhost:8001/health && echo

llm-up:
	docker compose up -d llm

llm-logs:
	docker compose logs -f llm

llm-restart:
	docker compose restart llm
fmt:
	ruff check --fix .
lint:
	ruff check .
type:
	mypy services libs
test:
	pytest -q

test-llm:
	pytest -q tests/test_llm_client.py tests/test_llm_policy.py

migrate-llm:
	docker compose exec -T db psql -U pulse -d pulse -f db/migrations/20251027_add_llm_tables.sql

eval-llm:
	docker compose run --rm tools bash -lc 'python tools/llm_eval/run_eval.py'

quiver-backfill:
	docker compose run --rm ingest bash -lc 'python -m ingest.run quiver_backfill'

quiver-update:
	docker compose run --rm ingest bash -lc 'python -m ingest.run quiver_update'

quiver-backfill-tier1:
	docker compose run --rm ingest bash -lc 'python -m ingest.run quiver_backfill'

quiver-update-tier1:
	docker compose run --rm ingest bash -lc 'python -m ingest.run quiver_update'

data.fetch:
	python3 tools/kronos_data/fetch_datasets.py

data.compile:
	python3 tools/kronos_data/compile_daily.py

data.validate:
	python3 tools/kronos_data/validate_surface.py && \
	cat /mnt/data/kronos_data/logs/validate_surface.txt

train.nbeats:
	python3 tools/kronos_data/train_nbeats.py

scenarios.train:
	PYTHONPATH=. python3 tools/kronos_scenarios/train_timegrad_csdi.py

scenarios.bundle:
	PYTHONPATH=. python3 tools/kronos_scenarios/bundle_artifact.py

scenarios.up:
	docker compose up -d --build kronos-scenarios

graphx.train:
	PYTHONPATH=. python3 tools/kronos_graphx/train_graph_transformer.py

graphx.bundle:
	PYTHONPATH=. python3 tools/kronos_graphx/bundle_artifact.py

graphx.up:
	docker compose up -d --build kronos-graphx

.PHONY: ingest-backfill
ingest-backfill:
	docker compose run --rm ingest bash -lc 'PYTHONPATH=/app:/app/libs python -m ingest.alpaca_rest backfill'

train-nbeats: train.nbeats

train-tft: tft.train

.PHONY: train-chronos
train-chronos:
	@echo "Chronos training pipeline not implemented; skip."

.PHONY: fmp.backfill
fmp.backfill:
	PYTHONPATH=. python3 tools/fmp/backfill_fundamentals.py \
		--period $${FMP_FUNDAMENTALS_PERIOD:-annual} \
		--since $${FMP_FUNDAMENTALS_SINCE:-2005-01-01} \
		--universe services/ingest/universe_symbols.txt

.PHONY: universe.build
universe.build:
	PYTHONPATH=. python3 tools/universe/build_universe.py

.PHONY: factors.export
factors.export:
	PYTHONPATH=. python3 tools/factors/export_factors.py

.PHONY: tft.data tft.train tft.up
tft.data:
	PYTHONPATH=. python3 tools/kronos_tft/build_dataset.py

tft.train:
	PYTHONPATH=. python3 tools/kronos_tft/train_tft.py

tft.up:
	docker compose up -d --build kronos-tft
