"""Issue #160: a built text column's hierarchy numbers its dictionary.

PBIXBuilder folds the values of a text column that differ only by ASCII case
onto the first spelling (#43), as Desktop's import does, but numbered the
attribute hierarchy (H$ POS_TO_ID) with every spelling: the hierarchy held
data ids past the dictionary's end and pointed at the wrong values.
COUNTROWS(VALUES(T[c])) was 14 for 8 values, ORDER BY / MIN / TOPN scrambled
and MAX(T[c]) failed in Desktop (pfshdata.cpp) until a refresh.

Expected: Power BI Desktop 2.152 over ADOMD after its own refresh
(build_b160.py). Values the collation finds equal (ç / Ç) are ordered by code
point; Desktop's order among them follows no rule (#156)."""
from __future__ import annotations

import importlib

import pytest

pytestmark = pytest.mark.unit

_t156 = importlib.import_module("tests.test_issue156_built_text_order")

VALUES = ['b', 'Ab', 'ab', 'AB', 'c', 'a', 'A', 'B', 'Zeta', 'zeta', 'x', 'aB', 'ç', 'Ç', 'x']
DESKTOP_ORDER = ['a', 'Ab', 'b', 'c', 'ç', 'Ç', 'x', 'Zeta']
DESKTOP_COUNT = ['8', '8', '15']   # COUNTROWS(VALUES(T[c])), DISTINCTCOUNT(T[c]), COUNTROWS(T)


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    from pbix_mcp.builder import PBIXBuilder

    path = str(tmp_path_factory.mktemp("i160") / "b160.pbix")
    b = PBIXBuilder("b160")
    b.add_table("T", [{"name": "c", "data_type": "String"}, {"name": "i", "data_type": "Int64"}],
                rows=[{"c": c, "i": 2 ** n} for n, c in enumerate(VALUES)])
    b.add_page("Page 1")
    b.save(path)
    return path


def test_the_hierarchy_holds_each_dictionary_value_once(built):
    order, _mn, _mx = _t156._hierarchy_order(built, "T", "c")
    assert len(order) == int(DESKTOP_COUNT[0]) == len(set(order))


def test_the_hierarchy_is_desktops_order(built):
    from pbix_mcp.dax.collation import sort_key

    order, mn, mx = _t156._hierarchy_order(built, "T", "c")
    # Desktop's order, its ties (ç / Ç) put in code-point order
    want = sorted(DESKTOP_ORDER, key=lambda s: (sort_key(s), s))
    assert order == want
    assert [sort_key(s) for s in order] == sorted(sort_key(s) for s in DESKTOP_ORDER)
    assert (mn, mx) == (DESKTOP_ORDER[0], DESKTOP_ORDER[-1])


def test_case_variants_do_not_make_a_side_unique(tmp_path):
    """The orientation guess counts a text key as the column stores it: F's
    "k1" / "K1" are one value, so F is not taken for the one side."""
    import os
    import sqlite3
    import tempfile
    import zipfile

    from pbix_mcp.builder import PBIXBuilder
    from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
    from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

    path = str(tmp_path / "orient.pbix")
    b = PBIXBuilder("orient")
    b.add_table("D", [{"name": "k", "data_type": "String"}], rows=[{"k": "k1"}, {"k": "k2"}, {"k": "k3"}])
    b.add_table("F", [{"name": "k", "data_type": "String"}, {"name": "v", "data_type": "Int64"}],
                rows=[{"k": "k1", "v": 1}, {"k": "K1", "v": 2}])
    b.add_relationship("F", "k", "D", "k")
    b.add_page("Page 1")
    b.save(path)
    abf = decompress_datamodel(zipfile.ZipFile(path).read("DataModel"))
    fd, tmp = tempfile.mkstemp(suffix=".db")
    os.write(fd, read_metadata_sqlite(abf))
    os.close(fd)
    try:
        con = sqlite3.connect(tmp)
        got = con.execute("SELECT ft.Name, tt.Name FROM Relationship r JOIN [Table] ft ON ft.ID = r.FromTableID "
                          "JOIN [Table] tt ON tt.ID = r.ToTableID").fetchone()
        con.close()
    finally:
        os.unlink(tmp)
    assert got == ("F", "D")
