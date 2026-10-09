"""Issue #161: an empty text "" is a value of its own, apart from BLANK.

pbix-mcp stored "" as BLANK (0.9.3 to 0.9.131). Desktop's import keeps it:
ISBLANK is FALSE, TREATAS({""}) selects it alone, COUNTA counts it, MIN of a
column is "". Desktop writes it as an ordinary zero-length record on a page
flagged page_contains_nulls (all 10 such pages in the corpus Desktop saved; 0
on the 714 without one), and so does the encoder now. A whitespace-only text
is stripped to "" (#159).

Expected values: Power BI Desktop 2.152 over ADOMD after its own refresh
(build_b161.py), through pbix-mcp's reader and engine."""
from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit


def _evaluate(path, measures):
    """Evaluate {name: DAX} at the grand total of the model at ``path`` as the
    server does (its own reader, then the engine), ADOMD-style text out."""
    import warnings

    from pbix_mcp import server as S
    from pbix_mcp.dax import engine as de

    warnings.simplefilter("ignore")
    alias = "t" + str(abs(hash(path)) % 100000)
    S.pbix_open(path, alias)
    try:
        ctx = S._get_dax_context(alias)
        defs = dict(ctx["measure_defs"], **measures)
        home = next(iter(ctx["tables"]))
        mt = dict(ctx.get("measure_tables") or {}, **{k: home for k in measures})
        got = de.evaluate_measures_smart(
            list(measures), ctx["tables"], defs, {}, ctx["date_table"], ctx["date_column"],
            ctx["relationships"], simulate_row_context=False, measure_tables=mt,
            model_columns=ctx.get("model_columns"), date_tables=ctx.get("date_tables"))
    finally:
        S.pbix_close(alias)
    return {k: _adomd(got.get(k)) for k in measures}


def _adomd(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return "True" if v else "False"
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v)


def _stored(path, table, column):
    """The column's values, row by row, as the model stores them."""
    import warnings

    from pbix_mcp import server as S

    warnings.simplefilter("ignore")
    alias = "s" + str(abs(hash(path)) % 100000)
    S.pbix_open(path, alias)
    try:
        t = S._get_dax_context(alias)["tables"][table]
    finally:
        S.pbix_close(alias)
    i = t["columns"].index(column)
    return [r[i] for r in t["rows"]]


ROWS = [('', 'x'), ('a', 'y'), ('', 'x'), (None, 'z'), ('b', ''), (' ', 'x'), ('\t', 'y'), ('a', '')]            # (c, d) per row; row n holds i = n
DESKTOP_VALUES = [(None, '1'), ('', '4'), ('a', '2'), ('b', '1')]   # VALUES(T[c]) after the refresh: (value, rows)
DESKTOP = [('counts_vals', 'COUNTROWS(VALUES(T[c]))', '4'), ('counts_dc', 'DISTINCTCOUNT(T[c])', '4'), ('counts_dcnb', 'DISTINCTCOUNTNOBLANK(T[c])', '3'), ('counts_cb', 'COUNTBLANK(T[c])', '5'), ('counts_ca', 'COUNTA(T[c])', '7'), ('counts_cx', 'COUNTX(T, T[c])', '7'), ('counts_rows', 'COUNTROWS(T)', '8'), ('counts_d_vals', 'COUNTROWS(VALUES(T[d]))', '4'), ('counts_d_dc', 'DISTINCTCOUNT(T[d])', '4'), ('counts_d_cb', 'COUNTBLANK(T[d])', '2'), ('counts_d_ca', 'COUNTA(T[d])', '8'), ('filters_eq_empty', 'CALCULATE(COUNTROWS(T), T[c] = "")', '5'), ('filters_eq_blank', 'CALCULATE(COUNTROWS(T), T[c] = BLANK())', '5'), ('filters_strict_empty', 'COUNTROWS(FILTER(T, T[c] == ""))', '4'), ('filters_strict_blank', 'COUNTROWS(FILTER(T, T[c] == BLANK()))', '1'), ('filters_isblank', 'COUNTROWS(FILTER(T, ISBLANK(T[c])))', '1'), ('filters_tr_empty', 'CALCULATE(COUNTROWS(T), TREATAS({""}, T[c]))', '4'), ('filters_tr_blank', 'CALCULATE(COUNTROWS(T), TREATAS({BLANK()}, T[c]))', '1'), ('filters_ne_empty', 'CALCULATE(COUNTROWS(T), T[c] <> "")', '3'), ('minmax_min', 'MIN(T[c])', ''), ('minmax_minlen', 'LEN(MIN(T[c]))', '0'), ('minmax_max', 'MAX(T[c])', 'b'), ('minmax_fnb', 'FIRSTNONBLANK(T[c], 1)', ''), ('minmax_fnblen', 'LEN(FIRSTNONBLANK(T[c], 1))', '0'), ('concat_v', 'CONCATENATEX(VALUES(T[c]), "[" & T[c] & "]", ",", T[c], ASC)', '[],[],[a],[b]')]              # (probe, DAX, Desktop's answer)


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    from pbix_mcp.builder import PBIXBuilder

    path = str(tmp_path_factory.mktemp("i161") / "b161.pbix")
    b = PBIXBuilder("b161")
    b.add_table("T", [{"name": "c", "data_type": "String"}, {"name": "d", "data_type": "String"},
                      {"name": "i", "data_type": "Int64"}],
                rows=[{"c": c, "d": d, "i": n} for n, (c, d) in enumerate(ROWS)])
    b.add_measure("T", "S", "SUM(T[i])")
    b.add_page("Page 1")
    b.save(path)
    return path


def test_stored_values_are_desktops(built):
    vals = _stored(built, "T", "c")
    have = {}
    for v in vals:
        have[v] = have.get(v, 0) + 1
    assert sorted(((v, str(n)) for v, n in have.items()), key=repr) == sorted(
        (tuple(x) for x in DESKTOP_VALUES), key=repr)


def test_the_dictionary_holds_an_empty_record_on_a_flagged_page(built):
    import zipfile

    from pbix_mcp.formats.abf_rebuild import list_abf_files, read_abf_file
    from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel
    from pbix_mcp.formats.vertipaq_decoder import decode_dictionary

    abf = decompress_datamodel(zipfile.ZipFile(built).read("DataModel"))
    (e,) = [e for e in list_abf_files(abf) if e["Path"].endswith(".dictionary")
            and not e["Path"].startswith(("H$", "R$")) and ".c (" in e["Path"]]
    raw = read_abf_file(abf, e)
    assert decode_dictionary(raw)[1] == ["", "a", "b"]
    assert raw[61] == 1     # page_contains_nulls of its one page


def test_the_hierarchy_has_blank_then_empty(built):
    import importlib

    t156 = importlib.import_module("tests.test_issue156_built_text_order")
    order, mn, mx = t156._hierarchy_order(built, "T", "c")
    assert order == ["", "a", "b"] and (mn, mx) == ("", "b")


def test_a_store_holding_empty_text_stays_uncompressed():
    """build_b161c.py: Desktop wrote a column of 417 values (13,313
    characters, past the compression threshold) on ONE uncompressed page when
    it holds "" -- the encoder now writes that dictionary byte for byte -- and
    Huffman-compressed the same column without "". A compressed page holding
    "" made Desktop refuse the model."""
    import struct

    from pbix_mcp.formats.vertipaq_encoder import _encode_string_dictionary

    vals = [f"value {i:04d} " + "abcdefghij" * 2 for i in range(420)]
    with_empty = vals[:100] + [""] + vals[101:200] + vals[201:300] + vals[302:]
    without = vals[:300] + vals[301:]
    raw = _encode_string_dictionary(with_empty)
    # page_contains_nulls at byte 61, page_compressed at 78, buffer_used_characters at 91
    assert (raw[61], raw[78], struct.unpack_from("<Q", raw, 91)[0]) == (1, 0, 13313)
    raw = _encode_string_dictionary(without)
    assert (raw[61], raw[78]) == (0, 1)


@pytest.fixture(scope="module")
def answers(built):
    return _evaluate(built, {probe: dax for probe, dax, _want in DESKTOP})


@pytest.mark.parametrize("probe,dax,want", DESKTOP, ids=[p[0] for p in DESKTOP])
def test_answers_match_desktop(answers, probe, dax, want):
    assert answers[probe] == want
