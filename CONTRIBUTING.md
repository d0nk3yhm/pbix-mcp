# Contributing

## Setup

```bash
git clone https://github.com/d0nk3yhm/pbix-mcp.git
cd pbix-mcp
pip install -e ".[dev]"
```

## Running Tests

```bash
# Fast unit tests only (173 pass, 27 skip without test corpus/private files)
pytest -m "not slow"

# Download public test corpus, then run integration tests
python scripts/download_test_corpus.py
PBIX_TEST_SAMPLES=test_corpus pytest -v

# With coverage
pytest --cov=src/pbix_mcp --cov-report=term-missing -m "not slow"
```

## Code Style

- Linting: `ruff check src/ tests/`
- Type checking: `mypy src/pbix_mcp/`

## Project Layout

```
src/pbix_mcp/
  server.py              # MCP server (78 tools)
  cli.py                 # Entry point (pbix-mcp-server --log-level debug)
  builder.py             # PBIX file builder (create from scratch with row data)
  errors.py              # Typed exceptions with stable error codes (12 classes)
  logging_config.py      # Diagnostic logging (normal/debug/trace)
  dax/
    engine.py            # DAX evaluator (156 functions, best-effort)
    calc_tables.py       # Calculated table + column support
  formats/
    abf_rebuild.py       # ABF archive format (read, modify, build from scratch)
    datamodel_roundtrip.py  # XPress9 compress/decompress
    vertipaq_encoder.py  # VertiPaq column encoding (6 data types)
  models/
    requests.py          # Tool input models (FilterContext, DimensionRef)
    responses.py         # Tool output models (ToolResponse, DAXEvalResponse)
tests/
  test_dax_engine.py     # Unit tests (55; 6 skip without private files)
  test_dax_accuracy.py   # Accuracy tests (50)
  test_golden.py         # Golden tests (15; 2 skip without private files)
  test_fixtures.py       # Fixture tests (18; ships with repo)
  test_beta_features.py  # Beta feature tests (10; RLS, password, doctor)
  test_cross_report.py   # Integration tests (19; requires 4 private PBIX files)
```

## Commit Messages

Use conventional format:
- `feat:` new feature or tool
- `fix:` bug fix
- `refactor:` code restructure without behavior change
- `test:` test additions
- `docs:` documentation only
- `chore:` build/CI changes
