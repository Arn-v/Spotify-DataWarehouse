.PHONY: install dev db migrate seed ingest analytics dashboard test lint clean

install:
	pip install -e .

dev:
	pip install -e ".[all]"

db:
	docker-compose up -d

db-down:
	docker-compose down

migrate:
	alembic upgrade head

seed:
	python scripts/seed_kaggle.py --file data/raw/spotify_tracks.csv

ingest:
	python scripts/run_ingestion.py

scheduler:
	python scripts/run_scheduler.py

analytics:
	python scripts/run_analytics.py

dashboard:
	streamlit run dashboard/app.py

test:
	pytest tests/ -v --cov=src/spotify_dw --cov-report=term-missing

test-unit:
	pytest tests/unit/ -v

test-integration:
	pytest tests/integration/ -v -m integration

lint:
	ruff check src/ tests/
	ruff format --check src/ tests/

format:
	ruff format src/ tests/

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .coverage htmlcov/ *.egg-info/ dist/ build/
