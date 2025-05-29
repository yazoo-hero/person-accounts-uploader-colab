.PHONY: help install install-dev test test-cov lint format type-check security clean build docs run

# Default target
help:
	@echo "Available commands:"
	@echo "  make install       Install production dependencies"
	@echo "  make install-dev   Install all dependencies including dev"
	@echo "  make test          Run tests"
	@echo "  make test-cov      Run tests with coverage"
	@echo "  make lint          Run linting checks"
	@echo "  make format        Format code with black and isort"
	@echo "  make type-check    Run type checking with mypy"
	@echo "  make security      Run security checks"
	@echo "  make clean         Clean up temporary files"
	@echo "  make build         Build distribution packages"
	@echo "  make docs          Build documentation"
	@echo "  make run           Run the application"

install:
	pip install -r requirements.txt

install-dev:
	pip install -r requirements.txt -r requirements-dev.txt
	pre-commit install

test:
	PYTHONPATH=. pytest tests/ -v

test-cov:
	PYTHONPATH=. pytest tests/ -v --cov=src --cov-report=term-missing --cov-report=html

lint:
	black --check src tests
	isort --check-only src tests
	flake8 src tests --max-line-length=100 --extend-ignore=E203,W503
	pylint src

format:
	black src tests
	isort src tests

type-check:
	mypy src --ignore-missing-imports

security:
	bandit -r src -f json -o bandit-report.json
	safety check

clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf .coverage
	rm -rf htmlcov
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf dist
	rm -rf build

build: clean
	python -m build

docs:
	cd docs && make html

run:
	python -m src.main