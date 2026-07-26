# Development Guide

## Setup

```bash
git clone https://github.com/d0nk3yhm/pbix-mcp.git
cd pbix-mcp
pip install -e ".[dev]"
```

## Running Tests

```bash
# Fast tests (778 pass, 10 skip, 25 slow/integration deselected)
pytest -m "not slow"

# With coverage
pytest -m "not slow" --cov=src/pbix_mcp --cov-report=term-missing

# Download the public test corpus, then run all tests
# (integration tests also need: pip install pbixray)
python scripts/download_test_corpus.py
PBIX_TEST_SAMPLES=test_corpus pytest -v
```

## Test Architecture

| File | Purpose | Count |
|------|---------|-------|
| `test_dax_engine.py` | DAX function unit tests | 70 (6 skip without the corpus) |
| `test_dax_accuracy.py` | DAX evaluation accuracy | 72 |
| `test_golden.py` | Round-trip and artifact tests | 49 (3 skip without the corpus) |
| `test_fixtures.py` | Public fixture verification | 18 |
| `test_beta_features.py` | RLS, password, doctor tests | 10 |
| `test_cross_report.py` | 4-file integration tests | 19 (all skip without the corpus) |
| `test_dax_multihop.py` | Multi-hop DAX + empty-selection + bidirectional | 15 |
| `test_found_issues.py` | OpenBI-found regressions (measure-name forms, sort authoring, eval defaults, MAXID) | 32 |
| `test_images.py` | Image / registered-resource authoring, Desktop container parity | 23 |
| `test_rich_content.py` | Deneb references, ImageUrl DataCategory, field parameters, SVG measures | 22 |
| `test_zip_safety.py` | ZIP + path-traversal hardening (bomb, Zip-Slip, `_safe_join`, `set_theme`) | 10 |
| `test_perf_per_dimension.py` | Bucketed per-dimension eval (correctness, adversarial, fuzz, perf) | 14 |
| `test_pbir_reader.py` | PBIR read/write, bookmarks, format normalization | 38 |
| `test_report_editing.py` | rename/reorder/hide/duplicate/move, on both formats | 38 |
| `test_sort_by_column.py` | Sort-by-column authoring (7 skip without the corpus) | 10 |
| `test_pbir_schema_conformance.py` | PBIR output vs Microsoft's published schemas | 3 (integration) |

## Public Test Corpus

`python scripts/download_test_corpus.py` fetches four dashboards from the
MIT-licensed [Power-BI-Design-Files](https://github.com/Dashboard-Design/Power-BI-Design-Files)
repository (Copyright (c) 2024 Sajjad Ahmadi) into `test_corpus/`. Point
`PBIX_TEST_SAMPLES` at that directory to run the integration tests.

The tests resolve these exact filenames:
- `GeoSales_Dashboard.pbix` (71 measures)
- `Agents_Performance.pbix` (42 measures)
- `Ecommerce_Conversion.pbix` (70 measures)
- `IT_Support.pbix` (21 measures)

## Linting & Type Checking

```bash
ruff check src/ tests/
python -m mypy src/pbix_mcp/ --ignore-missing-imports
```

mypy has 144 errors (CI baseline is 145 — see `.github/workflows/ci.yml`). CI fails if the error count exceeds 145; the baseline is ratcheted down as errors are cleaned up. Tracked for gradual cleanup.

## Validating PBIR output

Every PBIR file declares a `$schema` on `developer.microsoft.com`. To check
what the writer emits against Microsoft's own contract rather than against our
reader:

```bash
python scripts/validate_pbir_schemas.py path/to/report.pbix
```

Schemas are cached under `.pbir_schema_cache/`. When the service stamps a
version newer than the public index (it often does), the script falls back to
the highest published minor of the same major and says so. The same check runs
as `tests/test_pbir_schema_conformance.py` (marked `integration`).

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
5. Update tool count in README, CHANGELOG, CONTRIBUTING, docs/architecture.md, docs/tool-contracts.md
