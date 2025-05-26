.PHONY: help install setup dev api dashboard test format lint clean

# ===========================================
# Development Environment
# ===========================================

install:
	poetry install --no-root

setup: install
	poetry run pre-commit install
	mkdir -p logs data
	cp .env.example .env
	@echo "✅ Setup 완료!"
	@echo "⚠️  .env 파일을 편집하여 API 키를 입력하세요"

format:
	poetry run black --line-length=88 services/ scripts/
	poetry run isort --profile=black --line-length=88 services/ scripts/

lint:
	poetry run flake8 --max-line-length=88 --extend-ignore=E203,W503 services/ scripts/
	poetry run mypy services/mvp-trading-service/src/

test:
	poetry run pytest

test-cov:
	poetry run pytest --cov=services/mvp-trading-service/src --cov-report=html

clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf .coverage htmlcov/ .pytest_cache/ .mypy_cache/

# ===========================================
# Database Management (MongoDB + InfluxDB)
# ===========================================

db-init:
	poetry run python scripts/setup/init_database.py

db-shell-mongo:
	docker-compose exec mongodb mongosh --username admin --password admin123 --authenticationDatabase admin

db-shell-influx:
	@echo "InfluxDB 쿼리 도구:"
	@echo "브라우저: http://localhost:8086"

db-reset:
	docker-compose down -v mongodb influxdb
	docker-compose up -d mongodb influxdb redis
	sleep 10
	make db-init

# ===========================================
# Docker & Infrastructure
# ===========================================

docker-up:
	docker-compose up -d mongodb influxdb redis

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f

docker-clean:
	docker-compose down -v
	docker system prune -f

# ===========================================
# Application
# ===========================================

dev: docker-up
	sleep 10
	make db-init
	poetry run python services/mvp-trading-service/src/main.py

api:
	poetry run uvicorn services.mvp-trading-service.src.main:app --reload --host 0.0.0.0 --port 8000

dashboard:
	poetry run streamlit run services/mvp-trading-service/src/dashboard/main.py --server.port 8501

# ===========================================
# Celery
# ===========================================

celery-worker:
	cd services/mvp-trading-service && poetry run celery -A src.core.celery_app worker --loglevel=info

celery-beat:
	cd services/mvp-trading-service && poetry run celery -A src.core.celery_app beat --loglevel=info

celery-flower:
	cd services/mvp-trading-service && poetry run celery -A src.core.celery_app flower

# ===========================================
# Utilities
# ===========================================

verify:
	poetry run python scripts/setup/verify_setup.py

logs:
	tail -f logs/trading.log

help:
	@echo "One Bailey MVP - Available Commands:"
	@echo ""
	@echo "Setup:"
	@echo "  setup        - Complete environment setup"
	@echo "  install      - Install dependencies"
	@echo ""
	@echo "Development:"
	@echo "  dev          - Start full development server"
	@echo "  api          - Start FastAPI only"
	@echo "  dashboard    - Start Streamlit only"
	@echo ""
	@echo "Database:"
	@echo "  db-init      - Initialize databases"
	@echo "  db-reset     - Reset databases"
	@echo ""
	@echo "Quality:"
	@echo "  test         - Run tests"
	@echo "  format       - Format code"
	@echo "  lint         - Run linting"
	@echo "  verify       - Verify environment"
