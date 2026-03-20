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
  server.py              # MCP server (51 tools)
  cli.py                 # Entry point
  errors.py              # Typed exceptions
  logging_config.py      # Diagnostic logging
  dax/
    engine.py            # DAX evaluator (154 functions)
    calc_tables.py       # Calculated table support
  formats/
    abf_rebuild.py       # ABF archive format
    datamodel_roundtrip.py  # XPress9 compress/decompress
    vertipaq_encoder.py  # VertiPaq column encoding
  models/
    requests.py          # Tool input models
    responses.py         # Tool output models
tests/
  test_dax_engine.py     # Unit tests (55)
  test_dax_accuracy.py   # Accuracy tests (50)
  test_cross_report.py   # Integration tests (19)
```

## Commit Messages

Use conventional format:
- `feat:` new feature or tool
- `fix:` bug fix
- `refactor:` code restructure without behavior change
- `test:` test additions
- `docs:` documentation only
- `chore:` build/CI changes
