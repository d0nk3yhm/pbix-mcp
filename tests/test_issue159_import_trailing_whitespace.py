"""Issue #159: an imported text is stored as Desktop's import stores it.

PBIXBuilder (and every table pbix-mcp writes) stored each text as supplied.
Desktop's import strips U+0001 to U+0020 -- every C0 control and the space --
from the end of a text: "ab " is stored as "ab", and a filter on "ab " then
finds nothing. A built model answered every column-store question
differently until Desktop refreshed it. Calculated values keep their text
(DAX does not strip), and the partition's M keeps the supplied rows.

Expected values: Power BI Desktop 2.152 over ADOMD after its own refresh
(build_b159.py, build_b159b.py: every C0 / C1 control and Unicode space)."""
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


# (group, values in row order), the model of build_b159.py; row i holds i
GROUPS = [('sp1', ['ab', 'ab ']), ('sp2', ['cd ', 'cd']), ('sp3', ['ef  ', 'ef', 'ef ']), ('tab', ['gh', 'gh\t']), ('nbsp', ['ij', 'ij\xa0']), ('ideo', ['kl', 'kl\u3000']), ('lead', ['mn', ' mn']), ('ensp', ['op', 'op\u2002']), ('lf', ['qr', 'qr\n']), ('cr', ['st', 'st\r']), ('case', ['UV ', 'uv']), ('zwsp', ['wx', 'wx\u200b']), ('inner', ['a b', 'a  b']), ('onlysp', ['   ', 'yz']), ('sptab', ['rs', 'rs \t', 'rs\t '])]

# Desktop's refreshed VALUES: (stored value, the first row holding it, rows)
DESKTOP_STORED = [('ab', 0, 2), ('cd', 2, 2), ('ef', 4, 3), ('gh', 7, 2), ('ij', 9, 1), ('ij\xa0', 10, 1), ('kl', 11, 1), ('kl\u3000', 12, 1), ('mn', 13, 1), (' mn', 14, 1), ('op', 15, 1), ('op\u2002', 16, 1), ('qr', 17, 2), ('st', 19, 2), ('UV', 21, 2), ('wx', 23, 1), ('wx\u200b', 24, 1), ('a b', 25, 1), ('a  b', 26, 1), ('', 27, 1), ('yz', 28, 1), ('rs', 29, 3), (None, 32, 1)]

# (probe, value s, the group's first value, Desktop's cf / tr / xf / vx / eq0):
#   CALCULATE(COUNTROWS(T), T[c] = s), TREATAS({s}, T[c]), FILTER(ALL(T), T[c] = s),
#   FILTER(ALL(T[c]), T[c] = s), IF(s = first, 1, 0)
DESKTOP_FILTERS = [('sp1_0', 'ab', 'ab', ['2', '2', '2', '1', '1']), ('sp1_1', 'ab ', 'ab', [None, None, None, None, '0']), ('sp2_0', 'cd ', 'cd ', [None, None, None, None, '1']), ('sp2_1', 'cd', 'cd ', ['2', '2', '2', '1', '0']), ('sp3_0', 'ef  ', 'ef  ', [None, None, None, None, '1']), ('sp3_1', 'ef', 'ef  ', ['3', '3', '3', '1', '0']), ('sp3_2', 'ef ', 'ef  ', [None, None, None, None, '0']), ('tab_0', 'gh', 'gh', ['2', '2', '2', '1', '1']), ('tab_1', 'gh\t', 'gh', [None, None, None, None, '0']), ('nbsp_0', 'ij', 'ij', ['1', '1', '1', '1', '1']), ('nbsp_1', 'ij\xa0', 'ij', ['1', '1', '1', '1', '0']), ('ideo_0', 'kl', 'kl', ['1', '1', '1', '1', '1']), ('ideo_1', 'kl\u3000', 'kl', ['1', '1', '1', '1', '0']), ('lead_0', 'mn', 'mn', ['1', '1', '1', '1', '1']), ('lead_1', ' mn', 'mn', ['1', '1', '1', '1', '0']), ('ensp_0', 'op', 'op', ['1', '1', '1', '1', '1']), ('ensp_1', 'op\u2002', 'op', ['1', '1', '1', '1', '0']), ('lf_0', 'qr', 'qr', ['2', '2', '2', '1', '1']), ('lf_1', 'qr\n', 'qr', [None, None, None, None, '0']), ('cr_0', 'st', 'st', ['2', '2', '2', '1', '1']), ('cr_1', 'st\r', 'st', [None, None, None, None, '0']), ('case_0', 'UV ', 'UV ', [None, None, None, None, '1']), ('case_1', 'uv', 'UV ', ['2', '2', '2', '1', '0']), ('zwsp_0', 'wx', 'wx', ['1', '1', '1', '1', '1']), ('zwsp_1', 'wx\u200b', 'wx', ['1', '1', '1', '1', '0']), ('inner_0', 'a b', 'a b', ['1', '1', '1', '1', '1']), ('inner_1', 'a  b', 'a b', ['1', '1', '1', '1', '0']), ('onlysp_0', '   ', '   ', [None, None, None, None, '1']), ('onlysp_1', 'yz', '   ', ['1', '1', '1', '1', '0']), ('sptab_0', 'rs', 'rs', ['3', '3', '3', '1', '1']), ('sptab_1', 'rs \t', 'rs', [None, None, None, None, '0']), ('sptab_2', 'rs\t ', 'rs', [None, None, None, None, '0'])]

# (code point, whether Desktop strips it from the end of a text)
DESKTOP_SWEEP = [(1, True), (2, True), (3, True), (4, True), (5, True), (6, True), (7, True), (8, True), (9, True), (10, True), (11, True), (12, True), (13, True), (14, True), (15, True), (16, True), (17, True), (18, True), (19, True), (20, True), (21, True), (22, True), (23, True), (24, True), (25, True), (26, True), (27, True), (28, True), (29, True), (30, True), (31, True), (127, False), (128, False), (129, False), (130, False), (131, False), (132, False), (133, False), (134, False), (135, False), (136, False), (137, False), (138, False), (139, False), (140, False), (141, False), (142, False), (143, False), (144, False), (145, False), (146, False), (147, False), (148, False), (149, False), (150, False), (151, False), (152, False), (153, False), (154, False), (155, False), (156, False), (157, False), (158, False), (159, False), (160, False), (173, False), (5760, False), (6158, False), (8192, False), (8193, False), (8194, False), (8195, False), (8196, False), (8197, False), (8198, False), (8199, False), (8200, False), (8201, False), (8202, False), (8203, False), (8204, False), (8205, False), (8206, False), (8207, False), (8232, False), (8233, False), (8239, False), (8287, False), (8288, False), (12288, False), (65279, False)]


def _rows():
    out = [{"c": v, "i": i} for i, v in enumerate(v for _g, vs in GROUPS for v in vs)]
    out.append({"c": None, "i": len(out)})
    return out


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    from pbix_mcp.builder import PBIXBuilder

    path = str(tmp_path_factory.mktemp("i159") / "b159.pbix")
    b = PBIXBuilder("b159")
    b.add_table("T", [{"name": "c", "data_type": "String"}, {"name": "i", "data_type": "Int64"}],
                rows=_rows())
    b.add_measure("T", "S", "SUM(T[i])")
    b.add_page("Page 1")
    b.save(path)
    return path, b


def test_stored_values_are_desktops(built):
    path, _b = built
    vals = _stored(path, "T", "c")
    have = {}
    for i, v in enumerate(vals):
        first, n = have.get(v, (i, 0))
        have[v] = (first, n + 1)
    assert sorted(((v, f, n) for v, (f, n) in have.items()), key=repr) == sorted(
        ((v, f, n) for v, f, n in DESKTOP_STORED), key=repr)


def _lit(t):
    """A DAX expression for the text t: printable runs as literals, the rest UNICHAR."""
    parts, run = [], ""
    for ch in t:
        if ch.isprintable() and ch not in "\u00a0\u3000\u2002\u200b":
            run += ch
        else:
            if run:
                parts.append('"' + run.replace('"', '""') + '"')
                run = ""
            parts.append(f"UNICHAR({ord(ch)})")
    if run or not parts:
        parts.append('"' + run.replace('"', '""') + '"')
    return " & ".join(parts)


COLS = ("cf", "tr", "xf", "vx", "eq0")


@pytest.fixture(scope="module")
def answers(built):
    measures = {}
    for probe, s, first, _want in DESKTOP_FILTERS:
        v = f"VAR s = {_lit(s)} RETURN "
        measures.update({
            f"{probe}_cf": v + "CALCULATE(COUNTROWS(T), T[c] = s)",
            f"{probe}_tr": v + "CALCULATE(COUNTROWS(T), TREATAS({s}, T[c]))",
            f"{probe}_xf": v + "COUNTROWS(FILTER(ALL(T), T[c] = s))",
            f"{probe}_vx": v + "COUNTROWS(FILTER(ALL(T[c]), T[c] = s))",
            f"{probe}_eq0": v + f"IF(s = {_lit(first)}, 1, 0)"})
    return _evaluate(built[0], measures)


@pytest.mark.parametrize("probe,s,first,want", DESKTOP_FILTERS, ids=[p[0] for p in DESKTOP_FILTERS])
def test_filters_match_desktop(answers, probe, s, first, want):
    assert [answers[f"{probe}_{k}"] for k in COLS] == want


@pytest.mark.parametrize("cp,stripped", DESKTOP_SWEEP, ids=[f"U+{c:04X}" for c, _s in DESKTOP_SWEEP])
def test_which_characters_the_import_strips(cp, stripped):
    from pbix_mcp.formats.vertipaq_encoder import import_text

    assert (import_text("k" + chr(cp)) == "k") is stripped
    assert import_text("k" + chr(cp) * 3 + " ") == ("k" if stripped else "k" + chr(cp) * 3)


def test_leading_and_inner_whitespace_stay():
    from pbix_mcp.formats.vertipaq_encoder import import_text

    assert import_text(" mn") == " mn" and import_text("\tmn") == "\tmn"
    assert import_text("a  b") == "a  b" and import_text("a\tb") == "a\tb"
    assert import_text(" \t ") == ""


def test_a_calculated_column_keeps_its_text(tmp_path):
    from pbix_mcp.builder import PBIXBuilder

    path = str(tmp_path / "calc.pbix")
    b = PBIXBuilder("calc")
    b.add_table("T", [{"name": "c", "data_type": "String"}, {"name": "cc", "data_type": "String"}],
                rows=[{"c": "ab ", "cc": "ab "}, {"c": "x", "cc": " "}], calc_columns=["cc"])
    b.add_page("Page 1")
    b.save(path)
    assert _stored(path, "T", "c") == ["ab", "x"]
    assert _stored(path, "T", "cc") == ["ab ", " "]


def test_the_builder_says_so_and_the_source_keeps_the_text(built):
    import json as _json
    import os
    import sqlite3
    import tempfile
    import zipfile

    from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
    from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

    path, b = built
    kinds = [w for w in b.build_warnings if w["kind"] == "trailing_whitespace"]
    assert len(kinds) == 1 and "'ab '" in kinds[0]["message"]
    abf = decompress_datamodel(zipfile.ZipFile(path).read("DataModel"))
    fd, tmp = tempfile.mkstemp(suffix=".db")
    os.write(fd, read_metadata_sqlite(abf))
    os.close(fd)
    try:
        con = sqlite3.connect(tmp)
        (m,) = con.execute("SELECT QueryDefinition FROM [Partition] WHERE Name = 'T'").fetchone()
        con.close()
    finally:
        os.unlink(tmp)
    import base64
    import re
    import zlib
    b64 = re.search(r'Binary.FromText\("([^"]+)"', m).group(1)
    rows = _json.loads(zlib.decompress(base64.b64decode(b64), -zlib.MAX_WBITS).decode("utf-8"))
    assert ["ab ", "1"] in rows and ["cd ", "2"] in rows
