# Contributing

Thanks for helping improve pbix-mcp. Before sending a change, please read the
provenance and sign-off requirements below — they keep the project's clean-room
posture intact and are a condition of every contribution being merged.

## Contributions and provenance (clean-room)

pbix-mcp is an **independent, clean-room reimplementation** of the Power BI file
format, developed for interoperability. Keeping it clean-room is what lets the
project exist, so every contribution holds to the same discipline the
maintainers do.

**By contributing, you certify that your contribution is either:**

- your own original work, or
- derived only from **lawful, permitted sources** — published public
  documentation, public specifications (e.g. the Microsoft Open Specifications
  such as MS-XLDM), lawful black-box / differential observation of files you have
  the right to inspect, or third-party code under a license compatible with this
  project's (MIT) whose notices you have preserved.

**Do NOT contribute the following — such contributions will be rejected or
reverted:**

- Microsoft (or any other third party's) **proprietary source code**, in whole
  or in part.
- Code, constants, or data **decompiled, disassembled, or extracted from a
  proprietary binary** (a Power BI Desktop DLL, `msmdsrv`, a shipped Microsoft
  assembly, extracted Power BI JavaScript/CSS, etc.).
- **Leaked, confidential, or NDA-covered** material from any source.
- Microsoft **binaries, keys, credentials, or tokens**, or **copyrighted assets**
  (icons, fonts, themes, sample datasets under a restrictive license).
- Real **secrets** of any kind (passwords, connection strings, API tokens) in
  code, tests, or fixtures.

Format knowledge in this project is obtained the same way: public docs, public
specs, and differential observation of lawfully-obtained files — never from
Microsoft source or decompiled binaries. When you learn a fact by observing a
file, record how in `docs/reverse-engineering/` so the provenance trail stays
auditable. See
[docs/reverse-engineering/methodology.md](docs/reverse-engineering/methodology.md).

If you are ever unsure whether a source is permitted, open an issue and ask
**before** writing the code.

## Developer Certificate of Origin (sign-off)

Contributions are accepted under the **Developer Certificate of Origin (DCO) 1.1**
(<https://developercertificate.org/>). The DCO is a lightweight attestation — not
a copyright assignment — that you have the right to submit the work under the
project's license.

Sign off every commit:

```bash
git commit -s
```

which appends a line to the commit message:

```
Signed-off-by: Your Name <your.email@example.com>
```

Signing off certifies that you agree to the DCO 1.1 text **and** to the
clean-room provenance certification above. Commits without a `Signed-off-by` line
may be asked to amend before merge.

## Setup

```bash
git clone https://github.com/d0nk3yhm/pbix-mcp.git
cd pbix-mcp
pip install -e ".[dev]"
```

## Running Tests

```bash
# Fast unit tests only (~1,700 pass; slow/integration deselected)
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
  server.py              # MCP server (132 tools)
  cli.py                 # Entry point (pbix-mcp-server --log-level debug)
  builder.py             # PBIX file builder (create from scratch with row data)
  errors.py              # Typed exceptions with stable error codes (12 classes)
  logging_config.py      # Diagnostic logging (normal/debug/trace)
  dax/
    engine.py            # DAX evaluator (verified parity: all 435 query-evaluable DAX functions; 32 more are non-evaluable in Desktop itself)
    calc_tables.py       # Calculated table + column support
  formats/
    abf_rebuild.py       # ABF archive format (read, modify, build from scratch)
    datamodel_roundtrip.py  # XPress9 compress/decompress
    vertipaq_encoder.py  # VertiPaq column encoding (6 data types, Huffman string store)
  models/
    requests.py          # Tool input models (FilterContext, DimensionRef)
    responses.py         # Tool output models (ToolResponse, DAXEvalResponse)
tests/
  test_dax_engine.py     # Unit tests (70; 6 skip without the public test corpus)
  test_dax_accuracy.py   # Accuracy tests (72)
  test_golden.py         # Golden tests (49; 3 skip without the public test corpus)
  test_fixtures.py       # Fixture tests (18; ships with repo)
  test_beta_features.py  # Beta feature tests (10; RLS, password, doctor)
  test_dax_multihop.py   # Multi-hop DAX + empty-selection + bidirectional (15)
  test_found_issues.py   # OpenBI-found regressions: measure-name forms, sort authoring, eval defaults (32)
  test_images.py         # Image / registered-resource authoring, Desktop container parity (23)
  test_rich_content.py   # Deneb refs, ImageUrl DataCategory, field parameters, SVG measures (22)
  test_zip_safety.py     # ZIP + path-traversal hardening: bomb, Zip-Slip, _safe_join, set_theme (10)
  test_perf_per_dimension.py  # Bucketed per-dimension eval: correctness, adversarial, fuzz, perf (14)
  test_cross_report.py   # Integration tests (19; requires the public test corpus:
                         #   python scripts/download_test_corpus.py)
```

## Where the open work is

Two independent issue streams, and "everything is closed" has to mean both:

- **GitHub issues** — `gh issue list`.
- **OpenBI findings** — the OpenBI front-end drives the whole engine to build real
  reports, so it is where real-world bugs surface. It reports them as numbered
  markdown in its own repo, NOT here. [docs/openbi-findings-ledger.md](docs/openbi-findings-ledger.md)
  is the audited status of every item ever reported that way. Findings #18 sat
  unimplemented through four releases because nothing tracked it; that ledger is
  the fix. Re-audit before declaring the queue clear.

## Commit Messages

Use conventional format:
- `feat:` new feature or tool
- `fix:` bug fix
- `refactor:` code restructure without behavior change
- `test:` test additions
- `docs:` documentation only
- `chore:` build/CI changes
