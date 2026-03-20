# Development Guide

## Setup

```bash
git clone https://github.com/d0nk3yhm/pbix-mcp.git
cd pbix-mcp
pip install -e ".[dev]"
```

## Running Tests

```bash
# Fast tests (~140 pass from fresh clone, ~8 skip without private files)
pytest -m "not slow"

# With coverage
pytest -m "not slow" --cov=src/pbix_mcp --cov-report=term-missing

# All tests (requires private PBIX test corpus)
PBIX_TEST_SAMPLES=/path/to/samples pytest -v
```

## Test Architecture

| File | Purpose | Count |
|------|---------|-------|
| `test_dax_engine.py` | DAX function unit tests | 55 (6 skip without PBIX) |
| `test_dax_accuracy.py` | DAX evaluation accuracy | 50 |
| `test_golden.py` | Round-trip and artifact tests | 15 (2 skip without PBIX) |
| `test_fixtures.py` | Public fixture verification | 18 |
| `test_beta_features.py` | RLS, password, doctor tests | 10 |
| `test_cross_report.py` | 4-file integration tests | 19 (all skip without PBIX) |

## Private Test Corpus

Set `PBIX_TEST_SAMPLES` environment variable to point to your PBIX test files.
The integration tests expect:
- `GeoSales_Dashboard.pbix` (71 measures)
- `Agents Performance - Dashboard.pbix` (42 measures)
- `Ecommerce Conversion Dashboard.pbix` (70 measures)
- `IT_Support_Ticket_Desk.pbix` (21 measures)

## Linting & Type Checking

```bash
ruff check src/ tests/
python -m mypy src/pbix_mcp/ --ignore-missing-imports
```

mypy has ~239 errors as of v0.1.0. These are tracked for gradual cleanup.

## Adding a New DAX Function

1. Add handler method `_fn_yourfunction` to `DAXEngine` in `engine.py`
2. Register it in the dispatch dict at the top of `__init__`
3. Add unit test in `test_dax_accuracy.py`
4. Update `docs/supported-dax.md`

## Adding a New MCP Tool

1. Add `@mcp.tool()` function in `server.py`
2. Return `ToolResponse.ok(...)` or `ToolResponse.error(...)`
3. Catch `PBIXMCPError` before `Exception`
4. Add `logger.info(...)` at entry point
5. Update tool count in README, CHANGELOG, CONTRIBUTING
