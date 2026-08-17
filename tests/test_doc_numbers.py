"""Ratchet: the DAX-coverage numbers stated in the docs must match golden.json.

`tests/conformance/golden.json` is the ground truth for how many value probes and
golden-backed functions exist. These counts drift the moment a probe is added --
which is exactly how the docs went stale (356 -> 359 value probes, 259 -> 260
golden-backed functions). This test ties the prose to the artifact: add or remove
a probe and the doc strings must be updated in the same commit, or CI fails here.

If this test fails, do NOT just bump the number in the test -- update the doc
strings it points at (README.md, docs/supported-dax.md, docs/dax-coverage.md) to
the new golden.json-derived counts, then the test passes.
"""

import json
import os

_HERE = os.path.dirname(__file__)
_ROOT = os.path.dirname(_HERE)
_GOLDEN = os.path.join(_HERE, "conformance", "golden.json")

# 467 live DAX surface - 32 classified-out (curated in docs/dax-coverage.md).
# Not derivable from golden.json alone (the 32 list is curated); stable enough to
# pin here so a change forces a conscious doc update.
IMPLEMENTED_TOTAL = 435


def _counts():
    """Return (value_probes, golden_backed_functions) by the harness's own rule:
    a probe record is a refusal iff it has an "error" key, else a value probe."""
    with open(_GOLDEN, encoding="utf-8") as fh:
        golden = json.load(fh)
    value_probes = 0
    golden_backed = 0
    for _func, rows in golden.items():
        func_value = 0
        for rec in rows:
            if "error" in rec:
                continue
            value_probes += 1
            func_value += 1
        if func_value:
            golden_backed += 1
    return value_probes, golden_backed


def _read(rel):
    with open(os.path.join(_ROOT, rel), encoding="utf-8") as fh:
        return fh.read()


def test_value_probe_count_matches_docs():
    value_probes, _ = _counts()
    phrase = f"{value_probes} value probe"
    for rel in ("README.md", "docs/supported-dax.md"):
        assert phrase in _read(rel), (
            f"{rel} must state '{phrase}' (golden.json currently has "
            f"{value_probes} value probes)."
        )


def test_golden_backed_function_count_matches_docs():
    _, golden_backed = _counts()
    text = _read("docs/dax-coverage.md")
    assert f"{golden_backed} functions carry value goldens" in text, (
        f"dax-coverage.md must state '{golden_backed} functions carry value "
        f"goldens' (golden.json currently has {golden_backed})."
    )
    assert f"**{golden_backed}** functions have" in text, (
        f"dax-coverage.md must state '**{golden_backed}** functions have' "
        f"per-function goldens."
    )


def test_corpus_pinned_count_matches_docs():
    _, golden_backed = _counts()
    corpus_pinned = IMPLEMENTED_TOTAL - golden_backed
    text = _read("docs/dax-coverage.md")
    assert f"{corpus_pinned} core functions predate the harness" in text, (
        f"dax-coverage.md must state '{corpus_pinned} core functions predate "
        f"the harness' ({IMPLEMENTED_TOTAL} implemented - {golden_backed} "
        f"golden-backed = {corpus_pinned} corpus-pinned)."
    )


def test_headline_surface_numbers_present():
    # Stable, load-bearing counts; if they change the docs must change too.
    text = _read("docs/dax-coverage.md")
    for n in ("467", str(IMPLEMENTED_TOTAL), "32"):
        assert n in text, f"dax-coverage.md must state the {n} count."


def _tool_count():
    """Authoritative MCP tool count: the @mcp.tool decorators in server.py."""
    path = os.path.join(_ROOT, "src", "pbix_mcp", "server.py")
    with open(path, encoding="utf-8") as fh:
        return sum(1 for line in fh if line.startswith("@mcp.tool"))


def test_tool_count_matches_docs():
    n = _tool_count()
    phrase = f"{n} tools"
    for rel in ("README.md", "docs/architecture.md", "docs/tool-contracts.md",
                "CONTRIBUTING.md"):
        assert phrase in _read(rel), (
            f"{rel} must state '{phrase}' (server.py registers {n} @mcp.tool "
            f"decorators)."
        )


def _doctor_check_count():
    """Authoritative pbix_doctor check count: the _check() calls in its body."""
    import inspect
    import re

    from pbix_mcp import server
    return len(re.findall(r'_check\("', inspect.getsource(server.pbix_doctor)))


def test_doctor_check_count_matches_docs():
    """'17-point diagnostic' sat in the docs while the tool ran 19 checks —
    two invariants (issues #43 and #53) had been added without the prose
    following. Tie the number to the code so it cannot drift again."""
    n = _doctor_check_count()
    phrase = f"{n}-point"
    for rel in ("README.md", "docs/tool-contracts.md"):
        assert phrase in _read(rel), (
            f"{rel} must state '{phrase} diagnostic' (pbix_doctor runs {n} "
            f"_check() calls).")


def _corpus_count():
    """Default public test corpus size = the download script's two dicts."""
    import importlib.util
    path = os.path.join(_ROOT, "scripts", "download_test_corpus.py")
    spec = importlib.util.spec_from_file_location("_dl_corpus", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return len(mod.DASHBOARDS) + len(mod.MS_SAMPLES)


def test_corpus_count_matches_docs():
    n = _corpus_count()
    for rel in ("README.md", "docs/development.md"):
        text = _read(rel)
        assert f"{n} report" in text or f"{n}-report" in text, (
            f"{rel} must state the corpus size as {n} reports "
            f"(download_test_corpus.py fetches {n})."
        )


def test_readme_tools_section_is_complete():
    """The README '## Tools (N)' section must carry the real tool count in
    its heading, list EVERY registered tool exactly once, list nothing that
    is not registered, and each category heading's count must match the
    number of tools listed under it. This is how 'Exposes 130 tools' and
    '## Tools (128)' coexisted in one file: the phrase ratchet above only
    checks '{n} tools', which the heading's '(128)' never matched."""
    import re

    path = os.path.join(_ROOT, "src", "pbix_mcp", "server.py")
    with open(path, encoding="utf-8") as fh:
        src = fh.read()
    actual = set(re.findall(r"@mcp\.tool\(\)\s*\ndef (\w+)\(", src))
    n = len(actual)

    readme = _read("README.md")
    m = re.search(r"^## Tools \((\d+)\)$(.*?)^## ", readme,
                  re.MULTILINE | re.DOTALL)
    assert m, "README.md must have a '## Tools (N)' section"
    assert int(m.group(1)) == n, (
        f"README '## Tools ({m.group(1)})' heading is stale — server.py "
        f"registers {n} tools.")
    section = m.group(2)

    listed = re.findall(r"`(pbix_\w+)`", section)
    assert len(listed) == len(set(listed)), (
        f"README tools section lists duplicates: "
        f"{sorted(x for x in set(listed) if listed.count(x) > 1)}")
    missing = actual - set(listed)
    stale = set(listed) - actual
    assert not missing, f"tools missing from the README section: {sorted(missing)}"
    assert not stale, f"README lists unregistered tools: {sorted(stale)}"

    for cat_n, cat_body in re.findall(
            r"^### .+ \((\d+)\)\n(.*?)(?=^### |\Z)", section,
            re.MULTILINE | re.DOTALL):
        got = len(re.findall(r"`(pbix_\w+)`", cat_body))
        assert got == int(cat_n), (
            f"a README tool category claims ({cat_n}) but lists {got} tools:"
            f"\n{cat_body[:200]}")
