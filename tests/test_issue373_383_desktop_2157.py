"""Issues #373-#383: answers measured on Power BI Desktop 2.157 (our probe
model F:/_desktop_oracle/build_b371.py, the same answers before and after a
full refresh), asked through pbix_evaluate_dax on the same model built with
PBIXBuilder. Reported in PRs #371 / #372 by @allanon2.

Opp holds three rows of five kinds; _M holds no rows, a Text column, and the
measure Total = 6. A table expression is asked as TOCSV of it. An expected
error is a fragment of Desktop's text (the English of the Norwegian texts
Desktop answered in on the probe machine).
"""
from __future__ import annotations

import json

import pytest

from pbix_mcp import server as S
from pbix_mcp.builder import PBIXBuilder

ERR = object()
D = "DATE(2026,1,6) + TIME(13,30,5)"
CMP = "DAX comparison operations do not support comparing values of type "
CR = "Function CONTAINSROW does not support comparing values of type "

CASES = {
    # --- #374 ROW
    "row_blank_arg": ('COUNTROWS(ROW("x", ))', (ERR, "Argument '2' in ROW function is required.")),
    "row_one_arg": ('COUNTROWS(ROW("x"))', (ERR, "Too few arguments were passed to the ROW function")),
    "row_dup_case": ('COUNTROWS(ROW("a", 1, "A", 2))',
                     (ERR, "Function ROW cannot add column [A], as it has the same name")),
    "row_ok": ('COUNTROWS(ROW("a", 1, "b", 2))', 1),
    # --- #375 ADDCOLUMNS / GROUPBY
    "addcol_existing": ('COUNTROWS(ADDCOLUMNS(Opp, "probability", 1))',
                        (ERR, "Function ADDCOLUMNS cannot add column [probability] since it already exists.")),
    "addcol_dup": ('COUNTROWS(ADDCOLUMNS(Opp, "x", 1, "X", 2))',
                   (ERR, "Function ADDCOLUMNS cannot add column [X], as it has the same name")),
    "addcol_head": ('TOCSV(ADDCOLUMNS(SUMMARIZE(Opp, Opp[Name]), "Rank", 1))', "'Opp'[Name],[Rank]\na,1\nb,1"),
    "groupby_head": ('TOCSV(GROUPBY(Opp, Opp[Name], "Rank", SUMX(CURRENTGROUP(), Opp[Rank])))',
                     "'Opp'[Name],[Rank]\na,4\nb,2"),
    "groupby_dup": ('COUNTROWS(GROUPBY(Opp, Opp[Name], "X", SUMX(CURRENTGROUP(), 1), "x", SUMX(CURRENTGROUP(), 2)))',
                    (ERR, "Function GROUPBY cannot add column [x], as it has the same name")),
    "summarize_head": ('TOCSV(SUMMARIZE(Opp, Opp[Name], "Rank", 1))', "'Opp'[Name],[Rank]\na,1\nb,1"),
    "sumcols_head": ('TOCSV(SUMMARIZECOLUMNS(Opp[Name], "Rank", 1))', "'Opp'[Name],[Rank]\na,1\nb,1"),
    # --- #376 MEDIAN
    "median_text": ("MEDIAN(Opp[Name])", (ERR, "MEDIAN does not support expressions of type string/Boolean/date.")),
    "median_bool": ("MEDIAN(Opp[Flag])", (ERR, "MEDIAN does not support")),
    "median_date": ("MEDIAN(Opp[When])", (ERR, "MEDIAN does not support")),
    "median_num": ("MEDIAN(Opp[Rank])", 2),
    "medianx_text": ("MEDIANX(Opp, Opp[Name])", (ERR, "MEDIANX does not support")),
    # --- #377 tuple IN / CONTAINSROW
    "tuple_filter": ('COUNTROWS(FILTER(Opp, (Opp[Rank], Opp[Name]) IN {(1, "a"), (3, "a")}))', 2),
    "tuple_case": ('COUNTROWS(FILTER(Opp, (Opp[Rank], Opp[Name]) IN {(1, "A")}))', 1),
    "tuple_blank": ("(BLANK(), 1) IN {(BLANK(), 1)}", True),
    "tuple_blank0": ("(0, 1) IN {(BLANK(), 1)}", False),
    "tuple_clash": ('("1", "a") IN {(1, "a")}', (ERR, CR + "Integer with values of type Text")),
    "tuple_arity": ("(1, 2) IN {1, 2}", (ERR, "Function CONTAINSROW must have a value for each column")),
    "containsrow_clash": ('CONTAINSROW({(1, "a")}, "1", "a")', (ERR, CR + "Integer with values of type Text")),
    # --- #378 nested VAR
    "var1": ("VAR a = VAR b = 1 RETURN b + 1 RETURN a", 2),
    "var2": ("VAR a = 1 VAR b = VAR c = 2 RETURN c RETURN a + b", 3),
    "var3": ("VAR a = VAR b = 1 VAR c = 2 RETURN b + c RETURN a * 10", 30),
    # --- #379 measure under the wrong table, a table with no rows
    "measure_wrong_table": ("'Opp'[Total]", (ERR, "Column 'Total' in table 'Opp' cannot be found")),
    "measure_home_table": ("'_M'[Total]", 6),
    "empty_count": ("COUNTROWS('_M')", None),
    # --- #380 UNION / INTERSECT
    "union_date_num": ("TOCSV(UNION({1}, {DATE(2026, 1, 6)}))", "[Value]\n1\n46028"),
    "union_bool_num": ("TOCSV(UNION({1}, {TRUE()}))", "[Value]\n1\n1"),
    "union_in_num": ('1 IN UNION({1}, {"a"})', (ERR, CR + "Text with values of type Integer")),
    "union_in_text": ('"a" IN UNION({1}, {"a"})', True),
    "intersect_clash": ('COUNTROWS(INTERSECT(SELECTCOLUMNS(Opp, "c", Opp[Name]), SELECTCOLUMNS(Opp, "c", Opp[Rank])))',
                        (ERR, "Function 'INTERSECT' does not support joining a column of type Text with a column of type Integer.")),
    # --- #381 comparisons and IN by type
    "cmp_a_1": ('"a" = 1', (ERR, CMP + "Text with values of type Integer")),
    "cmp_1_eqeq_a": ('1 == "a"', (ERR, CMP + "Integer with values of type Text")),
    "cmp_true_1": ("TRUE() = 1", (ERR, CMP + "True/False with values of type Integer")),
    "cmp_date_text": ('DATE(2026,1,5) = "2026-01-05"', (ERR, CMP + "Date with values of type Text")),
    "cmp_iferror": ('IFERROR("a" = 1, 9)', (ERR, CMP + "Text with values of type Integer")),
    "cmp_never_taken": ('IF(FALSE(), "a" = 1, 7)', (ERR, CMP + "Text with values of type Integer")),
    "cmp_filter": ("COUNTROWS(FILTER(Opp, Opp[Name] = 1))", (ERR, CMP + "Text with values of type Integer")),
    "cmp_calc": ("CALCULATE([Total], 1 = Opp[Name])", (ERR, CMP + "Integer with values of type Text")),
    "cmp_empty_col": ("MAX('_M'[Column1]) = 1", (ERR, CMP + "Text with values of type Integer")),
    "cmp_variant": ('IF(1 = 1, "x", 1) = 1', False),
    "cmp_sel_alt": ("SELECTEDVALUE(Opp[Name], 1) = 1", True),
    "in_mixed": ('1 IN {1, "a"}', (ERR, CR + "Text with values of type Integer")),
    "in_mixed_text": ('"A" IN {1, "a"}', True),
    "in_blank_mixed": ('BLANK() IN {1, "a"}', False),
    "in_bool_int": ("TRUE() IN {1}", (ERR, CR + "Integer with values of type True/False")),
    "in_calc": ('CALCULATE([Total], Opp[Flag] IN {"a"})', (ERR, CR + "Text with values of type True/False")),
    "in_chain": ("1 = 1 IN {TRUE()}", (ERR, CR + "True/False with values of type Integer")),
    "in_never_taken": ('IF(FALSE(), CALCULATE([Total], Opp[Rank] IN {"a"}), 7)',
                       (ERR, CR + "Text with values of type Integer")),
    "in_ok": ("1 IN {1, 2}", True),
    # --- #382 date columns against numbers, CONVERT
    "date_eq_serial": ("CALCULATE(COUNTROWS(Opp), Opp[When] = 46027)", 1),
    "date_in_serial": ("CALCULATE(COUNTROWS(Opp), Opp[When] IN {46027})", 1),
    "date_in_far": ("CALCULATE(COUNTROWS(Opp), Opp[When] IN {99999999})", None),
    "treatas_serial": ("CALCULATE(COUNTROWS(Opp), TREATAS({46027}, Opp[When]))", 1),
    "treatas_table": ("TOCSV(TREATAS({46027}, Opp[When]))", "'Opp'[When]\n1/5/2026"),
    "convert_neg": ('FORMAT(CONVERT(-1.25, DATETIME), "yyyy-mm-dd hh:nn")', "1899-12-29 06:00"),
    "convert_neg2": ('FORMAT(CONVERT(-0.5, DATETIME), "yyyy-mm-dd hh:nn")', "1899-12-30 12:00"),
    # --- #383 SUMMARIZECOLUMNS' filter tables
    "sumcols_treatas": ('TOCSV(SUMMARIZECOLUMNS(Opp[Name], TREATAS({1}, Opp[Rank]), "t", [Total]))',
                        "'Opp'[Name],[t]\na,6"),
    "sumcols_filter": ('SUMX(SUMMARIZECOLUMNS(Opp[Name], FILTER(ALL(Opp[Rank]), Opp[Rank] > 1), "t", SUM(Opp[Rank])), [t] * 10 + IF(Opp[Name] = "a", 100, 0))',
                       150),
    # --- #373 FORMAT
    "fmt_us_general": (f'FORMAT({D}, "General Date")', "1/6/2026 1:30:05 PM"),
    "fmt_us_medium_time": (f'FORMAT({D}, "Medium Time")', "01:30 PM"),
    "fmt_us_ampm": (f'FORMAT({D}, "h:nn AMPM")', "1:30 PM"),
    "fmt_au_short": (f'FORMAT({D}, "Short Date", "en-AU")', "6/01/2026"),
    "fmt_au_long": (f'FORMAT({D}, "Long Date", "en-AU")', "Tuesday, 6 January 2026"),
    "fmt_nz_long_time": (f'FORMAT({D}, "Long Time", "en-NZ")', "1:30:05 pm"),
    "fmt_gb_short": (f'FORMAT({D}, "Short Date", "en-GB")', "06/01/2026"),
    "fmt_gb_long": (f'FORMAT({D}, "Long Date", "en-GB")', "06 January 2026"),
    "fmt_gb_general": (f'FORMAT({D}, "General Date", "en-GB")', "06/01/2026 13:30:05"),
    "fmt_ie_long": (f'FORMAT({D}, "Long Date", "en-IE")', "Tuesday 6 January 2026"),
    "fmt_ie_medium_time": (f'FORMAT({D}, "Medium Time", "en-IE")', "01:30 pm"),
    "fmt_ca_short": (f'FORMAT({D}, "Short Date", "en-CA")', "2026-01-06"),
    "fmt_ca_long": (f'FORMAT({D}, "Long Date", "en-CA")', "January 6, 2026"),
    "fmt_ca_picture": (f'FORMAT({D}, "d/MM/yyyy h:mm:ss AMPM", "en-CA")', "6-01-2026 1:30:05 PM"),
    "fmt_medium_date": (f'FORMAT({D}, "Medium Date", "en-GB")', "06-Jan-26"),
    "fmt_short_time": (f'FORMAT({D}, "Short Time", "en-NZ")', "13:30"),
    "fmt_gd_midnight": ('FORMAT(DATE(2026,1,6), "General Date")', "1/6/2026"),
    "fmt_gd_time_only": ('FORMAT(TIME(9,5,0), "General Date")', "9:05:00 AM"),
    "fmt_gd_time_only_gb": ('FORMAT(TIME(9,5,0), "General Date", "en-GB")', "09:05:00"),
    "fmt_gd_zero": ('FORMAT(DATE(1899,12,30), "General Date")', "12:00:00 AM"),
    "fmt_noon": ('FORMAT(TIME(12,0,0), "h:nn AMPM")', "12:00 PM"),
    "fmt_h_ampm_lower": ('FORMAT(TIME(13,0,0), "h ampm", "en-AU")', "1 PM"),
    "fmt_medium_time_am": ('FORMAT(TIME(9,5,0), "Medium Time")', "09:05 AM"),
}


@pytest.fixture(scope="module")
def alias(tmp_path_factory):
    b = PBIXBuilder("b371t")
    cols = [("Rank", "Int64"), ("Name", "String"), ("Flag", "Boolean"), ("When", "DateTime"),
            ("Probability", "Double")]
    rows = [(1, "a", True, "2026-01-05T00:00:00", 0.1), (2, "b", False, "2026-01-06T00:00:00", 0.2),
            (3, "a", True, "2026-01-07T00:00:00", 0.3)]
    b.add_table("Opp", [{"name": c, "data_type": t} for c, t in cols],
                rows=[{c: v for (c, _), v in zip(cols, r)} for r in rows])
    b.add_table("_M", [{"name": "Column1", "data_type": "String"}], rows=[])
    b.add_measure("_M", "Total", "6")
    for k, (dax, _want) in CASES.items():
        b.add_measure("_M", k, dax)
    b.add_page("Page 1")
    path = tmp_path_factory.mktemp("b371") / "b371t.pbix"
    with pytest.warns(UserWarning):          # "_M has no rows", as intended
        b.save(str(path))
    assert json.loads(S.pbix_open(str(path), "t373"))["success"]
    yield "t373"
    S.pbix_close("t373")


@pytest.mark.parametrize("name", list(CASES))
def test_answers_as_desktop_2157(alias, name):
    want = CASES[name][1]
    r = json.loads(S.pbix_evaluate_dax(alias, name, apply_default_filters=False))["results"][0]
    if isinstance(want, tuple) and want[0] is ERR:
        assert r.get("status") == "error", r
        assert want[1] in r["error_message"], r["error_message"]
    else:
        assert r.get("status") != "error", r
        got = r.get("value")
        if isinstance(want, (int, float)) and not isinstance(want, bool) and got is not None:
            assert float(got) == float(want), r
        else:
            assert got == want, r
