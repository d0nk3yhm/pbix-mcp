"""Issue #173: PROPER is an Excel function, not a DAX one.

Desktop refuses a measure that uses it ("Failed to resolve name 'PROPER'. It
is not a valid table, variable, or function name"; build_b166.py's
PROPER("hello WORLD o'neil 2nd")), where the engine evaluated it to
"Hello World O'Neil 2Nd". It is now an unknown function, as in Desktop: the
measure is BLANK and reports PROPER as unsupported."""
from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit


def test_proper_is_not_a_dax_function():
    from pbix_mcp.dax import engine as de

    eng = de.DAXEngine()
    ctx = de.DAXContext({"T": {"columns": ["c"], "rows": [["x"]]}}, {})
    assert eng._eval_expr('PROPER("hello WORLD")', ctx) is None
    assert "PROPER" in eng.unsupported_functions


def test_the_function_catalog_has_no_proper():
    from pbix_mcp.dax import engine as de
    from pbix_mcp.dax.function_catalog import FUNCTION_CATALOG

    assert "PROPER" not in {name for name, _cat in FUNCTION_CATALOG}
    assert not hasattr(de.DAXEngine, "_fn_proper")
