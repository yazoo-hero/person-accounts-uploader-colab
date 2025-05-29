# Claude Development Guidelines

This document provides guidelines for Claude or other AI assistants working on this codebase.

## Project Overview

This is a person accounts validation and upload tool that integrates Workday and Calabrio systems. The application runs as a Dash web app in both Google Colab notebooks and local environments.

## Development Commands

### Testing
```bash
# Run all tests
make test

# Run tests with coverage
make test-cov

# Run specific test file
PYTHONPATH=. pytest tests/unit/test_config.py -v
```

### Code Quality
```bash
# Run linting checks
make lint

# Format code
make format  

# Type checking
make type-check

# Security checks
make security
```

### Building
```bash
# Build distribution packages
make build

# Clean temporary files
make clean
```

## Project Structure

```
src/
├── core/           # Core business logic
│   ├── data_loader.py    # Data loading from Excel/JSON
│   ├── preprocessor.py   # Data preprocessing and merging
│   ├── calculator.py     # Balance calculations
│   └── validator.py      # Data validation
├── api/            # External API integrations
│   └── calabrio_client.py
├── ui/             # Dash UI components
│   ├── layout.py
│   └── callbacks.py
└── utils/          # Shared utilities
    ├── config.py     # Configuration management
    ├── types.py      # Type definitions
    ├── mappers.py    # Data mapping utilities
    └── exceptions.py # Custom exceptions
```

## Key Design Principles

1. **Type Safety**: All functions should have type hints
2. **Error Handling**: Use custom exceptions from `utils.exceptions`
3. **Logging**: Use structured logging for debugging
4. **Testing**: Maintain >80% test coverage
5. **Security**: Never commit secrets or API keys

## Common Tasks

### Adding a New Module

1. Create the module in the appropriate directory
2. Add type hints to all functions
3. Create corresponding test file in `tests/unit/`
4. Update imports in `__init__.py`

### Modifying Data Processing

1. Check `src/core/data_loader.py` for loading logic
2. Update `src/core/preprocessor.py` for merging/cleaning
3. Modify `src/core/calculator.py` for calculations
4. Add tests for any changes

### Working with Configurations

- Environment variables: Use `.env` file (see `.env.example`)
- JSON configs: Located in `config/` directory
- Access via `Config` class in `src/utils/config.py`

## CI/CD Pipeline

GitHub Actions runs on:
- Push to main/develop branches
- Pull requests to main

Pipeline includes:
- Linting (black, isort, flake8)
- Type checking (mypy)
- Tests (pytest with coverage)
- Security scanning (bandit, safety)

## Google Colab Considerations

- Use `jupyter-dash` for inline display
- Set `JUPYTER_DASH_MODE=inline` in environment
- Test with both `validation_results_recreation_colab.ipynb` and local notebook

## Troubleshooting

### Import Errors
- Ensure `PYTHONPATH=.` is set
- Check virtual environment activation

### Test Failures
- Check test fixtures in `tests/conftest.py`
- Verify mock data matches expected format

### Type Checking Issues
- Install type stubs: `pip install pandas-stubs types-requests`
- Use `# type: ignore` sparingly for third-party issues