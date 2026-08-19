"""Issues #54, #55, #56 — calculated column/table defects found converting
QlikView documents.

#55 is the severe one: a data column named ``RowNumber`` was silently DELETED
by the first calculated column added to its table. No error, no warning, and
the file still opened — the one failure class a caller cannot detect by
readback, because everything else about the file is valid.

#56: a calculated table whose DAX contains a string literal ENDING in a
backslash (a Windows path) did not materialize.

#54: a calculated column that is legitimately blank on every row was refused
with a diagnosis that was measurably false.
"""
import json

import pytest

from pbix_mcp import server as S
from pbix_mcp.dax.engine import DAXContext, DAXEngine

pytestmark = pytest.mark.unit

BS = chr(92)
DOLLAR = chr(36)


def _mk(tmp_path, alias, cols, rows, fname="t.pbix"):
    p = str(tmp_path / fname)
    r = json.loads(S.pbix_create(p, alias, json.dumps(
        [{"name": "T", "columns": cols, "rows": rows}])))
    assert r["success"], r
    return p


def _reopen(alias, p, alias2):
    assert json.loads(S.pbix_save(
        alias, output_path=p, overwrite=True))["success"]
    S.pbix_close(alias)
    assert json.loads(S.pbix_open(p, alias2))["success"]


class TestUserRowNumberColumnSurvives:
    """Issue #55. The system row-number column is Type 3 and carries a GUID
    suffix; the code excluded it by NAME, so a user column called RowNumber
    matched the same filters and was dropped from the rebuild."""

    @pytest.fixture()
    def opened(self, tmp_path):
        alias = "rn55"
        p = _mk(tmp_path, alias,
                [{"name": "RowNumber", "data_type": "Int64"},
                 {"name": "V", "data_type": "Double"}],
                [{"RowNumber": 7, "V": 10.0}, {"RowNumber": 9, "V": 20.0}])
        yield alias, p
        for a in (alias, alias + "_r"):
            S._open_files.pop(a, None)
            S._dax_cache.pop(a, None)

    def test_column_and_values_survive_a_calculated_column(self, opened):
        alias, p = opened
        assert json.loads(S.pbix_datamodel_add_calculated_column(
            alias, "T", "Doubled", "T[V] * 2"))["success"]
        _reopen(alias, p, alias + "_r")
        a2 = alias + "_r"
        data = json.loads(S.pbix_get_table_data(a2, "T"))["message"]
        assert "RowNumber" in data, data
        # the VALUES survive, not just the name
        assert json.loads(S.pbix_datamodel_add_measure(
            a2, "T", "RN", "SUM(T[RowNumber])"))["success"]
        out = json.loads(S.pbix_evaluate_dax(a2, "RN"))
        assert out["results"][0]["value"] == 16
        assert json.loads(S.pbix_datamodel_add_measure(
            a2, "T", "D", "SUM(T[Doubled])"))["success"]
        assert json.loads(S.pbix_evaluate_dax(
            a2, "D"))["results"][0]["value"] == 60.0

    def test_column_is_listed_by_schema_after_the_edit(self, opened):
        alias, _p = opened
        assert json.loads(S.pbix_datamodel_add_calculated_column(
            alias, "T", "Doubled", "T[V] * 2"))["success"]
        schema = json.loads(S.pbix_get_model_schema(alias))["message"]
        user_rows = [ln for ln in schema.splitlines()
                     if ln.startswith("T ") and "RowNumber" in ln]
        # at least one row for the USER column: the system column always
        # carries its GUID suffix, so a bare RowNumber row is the user's
        assert any("RowNumber-" not in ln for ln in user_rows), schema

    def test_control_other_name_also_survives(self, tmp_path):
        """The control from the issue: same steps, different column name."""
        alias = "rn55c"
        _mk(tmp_path, alias,
            [{"name": "RowNum", "data_type": "Int64"},
             {"name": "V", "data_type": "Double"}],
            [{"RowNum": 7, "V": 10.0}], fname="c.pbix")
        try:
            assert json.loads(S.pbix_datamodel_add_calculated_column(
                alias, "T", "Doubled", "T[V] * 2"))["success"]
            assert "RowNum" in json.loads(
                S.pbix_get_table_data(alias, "T"))["message"]
        finally:
            S._open_files.pop(alias, None)
            S._dax_cache.pop(alias, None)


class TestTrailingBackslashLiteral:
    """Issue #56. DAX escapes a quote by DOUBLING it; a backslash is an
    ordinary character. The argument scan applied the C/Python rule, so a
    literal ending in a backslash never closed and the whole call evaluated
    to None."""

    def _ctx(self):
        return DAXContext({"T": {"columns": ["V"], "rows": [[1.0]]}}, {},
                          None, None, None, [])

    def test_engine_evaluates_calls_whose_literal_ends_in_a_backslash(self):
        e = DAXEngine()
        ctx = self._ctx()
        assert e._eval_expr(
            'CONCATENATE("x", "D:' + BS + '")', ctx) == "xD:" + BS
        got = e._eval_expr(
            'ROW("P", "D:' + BS + 'Projects' + BS + '")', ctx)
        assert got and got[0]["P"] == "D:" + BS + "Projects" + BS

    def test_doubled_quote_escape_still_works(self):
        """The DAX escape that IS real must keep working."""
        e = DAXEngine()
        ctx = self._ctx()
        assert e._eval_expr('CONCATENATE("a", "b""c")', ctx) == 'ab"c'
        assert e._eval_expr('LEN("ab")', ctx) == 2

    @pytest.mark.parametrize("label,literal", [
        ("no_backslash", "D:Projects"),
        ("interior", "D:" + BS + "Projects" + BS + "sub"),
        ("trailing_single", "D:" + BS + "Projects" + BS),
        ("trailing_double", "D:" + BS + "Projects" + BS + BS),
    ])
    def test_calculated_table_materializes_the_path(self, tmp_path, label,
                                                    literal):
        alias = "bs56_" + label
        p = _mk(tmp_path, alias, [{"name": "V", "data_type": "Double"}],
                [{"V": 1.0}], fname=label + ".pbix")
        try:
            r = json.loads(S.pbix_datamodel_add_calculated_table(
                alias, "CT", 'ROW("P", "' + literal + '")'))
            assert r["success"], r
            assert "Rows materialized: 1" in r["message"]
            _reopen(alias, p, alias + "_r")
            data = json.loads(S.pbix_get_table_data(alias + "_r", "CT"))
            assert literal in data["message"], data["message"]
        finally:
            for a in (alias, alias + "_r"):
                S._open_files.pop(a, None)
                S._dax_cache.pop(a, None)


class TestAllBlankCalculatedColumnAccepted:
    """Issue #54. An unresolved reference is detected precisely, BY NAME,
    before this point — so an all-blank result has already resolved every
    column it names and is simply the correct answer."""

    #: every value starts with the prefix the IF excludes, so the column is
    #: blank on every row — QlikView's own list box shows nothing either.
    EXPR = 'IF(LEFT(T[FieldTag],1) <> "' + DOLLAR + '", T[FieldTag])'

    @pytest.fixture()
    def opened(self, tmp_path):
        alias = "ab54"
        p = _mk(tmp_path, alias, [{"name": "FieldTag", "data_type": "String"}],
                [{"FieldTag": DOLLAR + "a"}, {"FieldTag": DOLLAR + "b"},
                 {"FieldTag": DOLLAR + "c"}])
        yield alias, p
        for a in (alias, alias + "_r"):
            S._open_files.pop(a, None)
            S._dax_cache.pop(a, None)

    def test_all_blank_column_is_accepted_and_persists(self, opened):
        alias, p = opened
        r = json.loads(S.pbix_datamodel_add_calculated_column(
            alias, "T", "UserTags", self.EXPR))
        assert r["success"], r
        _reopen(alias, p, alias + "_r")
        a2 = alias + "_r"
        assert json.loads(S.pbix_datamodel_add_measure(
            a2, "T", "NB", "COUNTBLANK(T[UserTags])"))["success"]
        assert json.loads(S.pbix_evaluate_dax(
            a2, "NB"))["results"][0]["value"] == 3

    def test_a_genuinely_unresolved_reference_is_still_refused(self, opened):
        """Dropping the all-blank guess must not weaken the real check."""
        alias, _p = opened
        bad = 'IF(LEFT(T[NoSuchCol],1) <> "' + DOLLAR + '", T[NoSuchCol])'
        r = json.loads(S.pbix_datamodel_add_calculated_column(
            alias, "T", "Bogus", bad))
        assert not r["success"]
        msg = r["message"]
        assert "NoSuchCol" in msg or "don't exist" in msg, msg
        # and it must NOT be diagnosed as an all-blank problem
        assert "every row evaluated to blank" not in msg
