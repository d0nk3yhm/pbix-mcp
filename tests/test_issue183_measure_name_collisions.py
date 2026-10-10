"""Issue #183: two measures whose names are equal ignoring case -- in one table
or in two -- are refused, because Power BI Desktop cannot load such a file.

Analysis Services keeps measure names in one case-insensitive namespace across
the model. The builder checked tables, columns and measure-vs-column names,
but never one measure against another, and pbix_datamodel_add_measure compared
names with SQLite's case-sensitive `=`. Desktop's model never answered for a
file holding "m" and "M" (build_b178.py; the same hang stopped
build_b174c.py's first run, whose probes "f_Fi_fi" and "f_fi_fi" collided)."""
from __future__ import annotations

import json
import warnings

import pytest

from pbix_mcp.builder import PBIXBuilder

pytestmark = pytest.mark.unit


def _builder(measures):
    b = PBIXBuilder("names")
    b.add_table("T", [{"name": "c", "data_type": "String"}], rows=[{"c": "x"}])
    b.add_table("U", [{"name": "m2", "data_type": "Int64"}], rows=[{"m2": 1}])
    for table, name in measures:
        b.add_measure(table, name, "1")
    b.add_page("Page 1")
    return b


@pytest.mark.parametrize("measures", [
    [("T", "m"), ("T", "M")],
    [("T", "m"), ("T", "m")],
    [("T", "m"), ("U", "m")],
    [("T", "Margin"), ("U", "MARGIN")],
], ids=["case_same_table", "exact_same_table", "exact_two_tables", "case_two_tables"])
def test_colliding_measure_names_are_refused(measures):
    with pytest.raises(ValueError, match="Two measures have names that"):
        _builder(measures).build()


def test_distinct_names_build():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        assert _builder([("T", "m"), ("U", "n"), ("T", "m2")]).build()


def test_add_measure_refuses_a_name_that_differs_only_by_case(tmp_path):
    from pbix_mcp import server as S

    path = tmp_path / "names.pbix"
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        _builder([("T", "Margin")]).save(str(path))
    alias = "issue181"
    assert json.loads(S.pbix_open(str(path), alias))["success"]
    try:
        for table, name in (("T", "margin"), ("U", "MARGIN")):
            got = json.loads(S.pbix_datamodel_add_measure(alias, table, name, "2"))
            assert not got.get("success"), got
            assert "already exists" in json.dumps(got)
        ok = json.loads(S.pbix_datamodel_add_measure(alias, "U", "Margin 2", "2"))
        assert ok.get("success"), ok
    finally:
        S.pbix_close(alias)
