# Contributing

## Setup

```bash
git clone https://github.com/d0nk3yhm/pbix-mcp.git
cd pbix-mcp
pip install -e ".[dev]"
```

## Running Tests

```bash
# Fast unit tests only
pytest -m "not slow"

# All tests (requires PBIX test files)
pytest -v

# With coverage
pytest --cov=src/pbix_mcp --cov-report=term-missing -m "not slow"
```

## Code Style

- Linting: `ruff check src/ tests/`
- Type checking: `mypy src/pbix_mcp/`

## Project Layout

```
src/pbix_mcp/
  server.py              # MCP server (60 tools)
  cli.py                 # Entry point (pbix-mcp-server)
  builder.py             # PBIX file builder (create from scratch with row data)
  errors.py              # Typed exceptions with stable error codes
  logging_config.py      # Diagnostic logging (normal/debug/trace)
  dax/
    engine.py            # DAX evaluator (154 functions)
    calc_tables.py       # Calculated table + column support
  formats/
    abf_rebuild.py       # ABF archive format (read, modify, build from scratch)
    datamodel_roundtrip.py  # XPress9 compress/decompress
    vertipaq_encoder.py  # VertiPaq column encoding (5 data types)
  models/
    requests.py          # Tool input models (FilterContext, DimensionRef)
    responses.py         # Tool output models (ToolResponse, DAXEvalResponse)
tests/
  test_dax_engine.py     # Unit tests (55)
  test_dax_accuracy.py   # Accuracy tests (50)
  test_golden.py         # Golden tests (15) — round-trips, exact values, PBIX from scratch
  test_fixtures.py       # Fixture tests (18) — public PBIX verification, package imports
  test_cross_report.py   # Integration tests (19) — 4 real PBIX files
```

## Commit Messages

Use conventional format:
- `feat:` new feature or tool
- `fix:` bug fix
- `refactor:` code restructure without behavior change
- `test:` test additions
- `docs:` documentation only
- `chore:` build/CI changes
