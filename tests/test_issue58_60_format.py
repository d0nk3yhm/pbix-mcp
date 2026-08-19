"""Issues #58 and #60: FORMAT() picture literals and culture.

#58 — a VBA/DAX format picture escapes literal text by QUOTING it, and
Desktop emits the contents without the quote characters. The engine printed
them verbatim, so every currency picture carried over from a QlikView
document rendered wrongly:

    FORMAT(1234.5, "\"$ \"#,##0")   was: "$ "1,235      want: $ 1,235

#60 — FORMAT() returns a STRING the canvas prints verbatim, so its
separators come from the MODEL's culture, not the reader's host. Model.Culture
was ignored, the third `locale` argument was ignored, no API set the model
culture, and pbix_get_cultures never reported it.
"""
import json

import pytest

from pbix_mcp import server as S
from pbix_mcp.dax.engine import DAXContext, DAXEngine, _culture_seps
from pbix_mcp.dax.engine import _format_number as F

pytestmark = pytest.mark.unit

Q = chr(34)


def _ctx(culture=None):
    c = DAXContext({"T": {"columns": ["A"], "rows": [[1477000.0]]}}, {},
                   None, None, None, [])
    c.culture = culture
    return c


class TestQuotedLiteralsInFormatPicture:
    """Issue #58. The five shapes the reporter exercises."""

    @pytest.mark.parametrize("picture,value,want", [
        (Q + "$ " + Q + "#,##0", 1234.5, "$ 1,235"),
        (Q + "$ " + Q + "#,##0;(" + Q + "$ " + Q + "#,##0)", -1234.5,
         "($ 1,235)"),
        # A positive value: this shape is here for its literal PREFIX. What a
        # single-section picture whose literal already starts with "-" should
        # do with a NEGATIVE value is a separate question, not measured
        # against Desktop here, so it is deliberately not asserted.
        ("-R$ #,##0.00", 1234.5, "-R$ 1,234.50"),
        ("£#,##0.00", 1234.5, "£1,234.50"),
        ("0.00%", 0.1234, "12.34%"),
    ])
    def test_currency_pictures_render_like_desktop(self, picture, value,
                                                   want):
        assert F(value, picture) == want

    def test_quote_characters_never_reach_the_output(self):
        out = F(1234.5, Q + "$ " + Q + "#,##0")
        assert Q not in out, out

    def test_digits_inside_a_quoted_literal_are_literal(self):
        """A placeholder search that ignored quoting would read the `0` in
        the prefix text as a digit placeholder."""
        assert F(5.0, Q + "0 of " + Q + "0") == "0 of 5"

    def test_semicolon_inside_a_quoted_literal_is_not_a_section_break(self):
        assert F(5.0, Q + "a;b " + Q + "0") == "a;b 5"

    def test_unquoted_pictures_are_unchanged(self):
        """The regression risk: everything that already worked must keep
        working."""
        assert F(1234.5, "#,##0") == "1,235"
        assert F(2297200.9, "$#,##0,.0K") == "$2,297.2K"
        assert F(0.0, "0.0;(0.0);zero") == "zero"
        assert F(1.0, "000") == "001"


class TestFormatResolvesCulture:
    """Issue #60 part 1."""

    def test_locale_argument_is_honoured(self):
        e = DAXEngine()
        got = e._eval_expr(
            'FORMAT(SUM(T[A]), "#,##0.00", "pt-BR")', _ctx())
        assert got == "1.477.000,00"

    def test_model_culture_is_the_fallback(self):
        e = DAXEngine()
        got = e._eval_expr('FORMAT(SUM(T[A]), "#,##0.00")', _ctx("pt-BR"))
        assert got == "1.477.000,00"

    def test_locale_argument_beats_model_culture(self):
        e = DAXEngine()
        got = e._eval_expr(
            'FORMAT(SUM(T[A]), "#,##0.00", "en-US")', _ctx("pt-BR"))
        assert got == "1,477,000.00"

    def test_no_culture_stays_en_us(self):
        e = DAXEngine()
        assert e._eval_expr(
            'FORMAT(SUM(T[A]), "#,##0.00")', _ctx()) == "1,477,000.00"

    def test_an_unknown_culture_falls_back_rather_than_guessing(self):
        assert _culture_seps("zz-ZZ") == (",", ".")
        assert _culture_seps(None) == (",", ".")

    @pytest.mark.parametrize("culture,want", [
        ("pt-BR", "1.477.000,00"),
        ("de-DE", "1.477.000,00"),
        ("en-US", "1,477,000.00"),
        ("en-GB", "1,477,000.00"),
        ("es-MX", "1,477,000.00"),   # region override, not the es default
    ])
    def test_separator_table(self, culture, want):
        g, d = _culture_seps(culture)
        assert F(1477000.0, "#,##0.00", g, d) == want

    def test_culture_applies_to_a_quoted_currency_picture_too(self):
        """The two fixes have to compose: #58's literal and #60's
        separators in one picture."""
        g, d = _culture_seps("pt-BR")
        assert F(1477000.0, Q + "R$ " + Q + "#,##0.00", g, d) == \
            "R$ 1.477.000,00"


class TestModelCultureApiAndReadback:
    """Issue #60 parts 2 and 3, through the reporter's own sequence:
    set, save, close, reopen — not from a call's return value."""

    @pytest.fixture()
    def built(self, tmp_path):
        alias = "cu60"
        p = str(tmp_path / "c.pbix")
        tables = [{"name": "T",
                   "columns": [{"name": "A", "data_type": "Double"}],
                   "rows": [{"A": 1477000.0}]}]
        meas = [{"table": "T", "name": "ByCulture",
                 "expression": 'FORMAT(SUM(T[A]), "#,##0.00")'},
                {"table": "T", "name": "ByArgument",
                 "expression": 'FORMAT(SUM(T[A]), "#,##0.00", "pt-BR")'}]
        assert json.loads(S.pbix_create(
            p, alias, json.dumps(tables),
            measures_json=json.dumps(meas)))["success"]
        yield alias, p
        for a in (alias, alias + "_r"):
            S._open_files.pop(a, None)
            S._dax_cache.pop(a, None)

    def _reopen(self, alias, p):
        assert json.loads(S.pbix_save(
            alias, output_path=p, overwrite=True))["success"]
        S.pbix_close(alias)
        a2 = alias + "_r"
        assert json.loads(S.pbix_open(p, a2))["success"]
        return a2

    def test_setter_persists_and_format_follows_it(self, built):
        alias, p = built
        r = json.loads(S.pbix_set_model_culture(alias, "pt-BR"))
        assert r["success"], r
        assert r["data"]["culture"] == "pt-BR"
        a2 = self._reopen(alias, p)
        for m in ("ByCulture", "ByArgument"):
            got = json.loads(S.pbix_evaluate_dax(a2, m))["results"][0]["value"]
            assert got == "1.477.000,00", (m, got)

    def test_get_cultures_names_the_model_culture(self, built):
        alias, p = built
        assert json.loads(S.pbix_set_model_culture(
            alias, "pt-BR"))["success"]
        a2 = self._reopen(alias, p)
        g = json.loads(S.pbix_get_cultures(a2))
        assert g["data"]["model_culture"] == "pt-BR"
        assert "Model culture: pt-BR" in g["message"]
        # and it stays distinct from the TRANSLATION cultures
        assert "Translation cultures" in g["message"]
        assert "pt-BR" not in g["data"]["translation_cultures"]

    def test_setter_refuses_an_empty_name(self, built):
        alias, _p = built
        r = json.loads(S.pbix_set_model_culture(alias, "  "))
        assert not r["success"]
        assert "required" in r["message"]

    def test_default_model_keeps_en_us_rendering(self, built):
        """Untouched models must be unaffected."""
        alias, p = built
        a2 = self._reopen(alias, p)
        got = json.loads(S.pbix_evaluate_dax(
            a2, "ByCulture"))["results"][0]["value"]
        assert got == "1,477,000.00"
