# Contributing

## Setup

```bash
git clone https://github.com/d0nk3yhm/pbix-mcp.git
cd pbix-mcp
pip install -e ".[dev]"
```

## Running Tests

```bash
# Fast unit tests only (199 pass, 9 skip, 19 slow/integration deselected)
pytest -m "not slow"

# Download public test corpus, then run integration tests
# (integration tests also need: pip install pbixray)
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
  server.py              # MCP server (101 tools)
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
    vertipaq_encoder.py  # VertiPaq column encoding (6 data types, Huffman string store)
  models/
    requests.py          # Tool input models (FilterContext, DimensionRef)
    responses.py         # Tool output models (ToolResponse, DAXEvalResponse)
tests/
  test_dax_engine.py     # Unit tests (55; 6 skip without private files)
  test_dax_accuracy.py   # Accuracy tests (50)
  test_golden.py         # Golden tests (41; 3 skip without the public test corpus)
  test_fixtures.py       # Fixture tests (18; ships with repo)
  test_beta_features.py  # Beta feature tests (10; RLS, password, doctor)
  test_cross_report.py   # Integration tests (19; requires the public test corpus:
                         #   python scripts/download_test_corpus.py)
```

## Commit Messages

Use conventional format:
- `feat:` new feature or tool
- `fix:` bug fix
- `refactor:` code restructure without behavior change
- `test:` test additions
- `docs:` documentation only
- `chore:` build/CI changes
