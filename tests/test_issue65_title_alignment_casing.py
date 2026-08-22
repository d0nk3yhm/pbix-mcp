"""Issue #65: title.alignment was written Capitalized; Power BI writes it
lowercase and ignores the capitalized form.

The trap is that Power BI's casing is NOT uniform, so a blanket rule breaks
one side. Censused across Desktop-authored reports in the local corpus:

    vcObjects.title.alignment          lowercase      36   (0 counter-examples)
    columnFormatting.alignment         Capitalized    57
    columnHeaders.alignment            Capitalized    13
    columnHeaders.titleAlignment       Capitalized     8
    rowHeaders.alignment               Capitalized     9

The capitalized title literal round-trips perfectly through save/reopen, so
a readback test can never catch it — Desktop simply does not recognise the
string and renders the title flush left. That is why these tests assert the
LITERAL, and why they assert both casings side by side.
"""
import json

import pytest

from pbix_mcp import server as S
from pbix_mcp.server import _build_format_objects as B

pytestmark = pytest.mark.unit


def _title_alignment(value):
    props = B({"title": {"alignment": value}})["_vcObjects"]["title"][0]
    return props["properties"]["alignment"]["expr"]["Literal"]["Value"]


def _card_alignment(card, value="Center"):
    spec = ({card: {"S.A": {"alignment": value}}} if card == "columnFormatting"
            else {card: {"alignment": value}})
    props = B(spec)["_objects"][card][0]["properties"]
    return props["alignment"]["expr"]["Literal"]["Value"]


class TestTitleAlignmentIsLowercase:
    @pytest.mark.parametrize("given,want", [
        ("center", "'center'"),
        ("left", "'left'"),
        ("right", "'right'"),
    ])
    def test_lowercase_input_stays_lowercase(self, given, want):
        assert _title_alignment(given) == want

    @pytest.mark.parametrize("given,want", [
        ("Center", "'center'"),
        ("Left", "'left'"),
        ("RIGHT", "'right'"),
        ("  Center  ", "'center'"),
    ])
    def test_caller_casing_is_normalized(self, given, want):
        """A caller writing "Center" must still reach Desktop as 'center' —
        otherwise the title renders flush left and nothing reports it."""
        assert _title_alignment(given) == want

    def test_capitalized_form_is_never_written(self):
        for v in ("center", "Center", "CENTER"):
            assert "'Center'" not in _title_alignment(v)


class TestTableCardsStayCapitalized:
    """The other half of the asymmetry: these were already right, and a
    blanket lowercase rule would have broken all three."""

    @pytest.mark.parametrize("card", [
        "columnFormatting", "columnHeaders", "rowHeaders",
    ])
    def test_capitalized_passes_through(self, card):
        assert _card_alignment(card, "Center") == "'Center'"

    @pytest.mark.parametrize("card", [
        "columnFormatting", "columnHeaders", "rowHeaders",
    ])
    def test_table_cards_are_not_lowercased(self, card):
        assert _card_alignment(card, "Left") == "'Left'"


def test_the_asymmetry_side_by_side():
    """One assertion holding both conventions at once, so a future 'tidy-up'
    that unifies the casing fails here with the reason attached."""
    assert _title_alignment("Center") == "'center'", (
        "title.alignment is lowercase in Desktop-authored files (36/36)")
    assert _card_alignment("columnFormatting", "Center") == "'Center'", (
        "columnFormatting.alignment is Capitalized in Desktop-authored "
        "files (57 occurrences)")


class TestThroughTheTool:
    """End to end, since the literal is what Desktop reads."""

    def test_title_alignment_survives_save_and_reopen(self, tmp_path):
        alias = "i65"
        p = str(tmp_path / "t.pbix")
        assert json.loads(S.pbix_create(p, alias, json.dumps([{
            "name": "T", "columns": [{"name": "V", "data_type": "Double"}],
            "rows": [{"V": 1.0}]}])))["success"]
        try:
            assert json.loads(S.pbix_add_visual(
                alias, 0, "card", 40, 40, 200, 100, ""))["success"]
            assert json.loads(S.pbix_format_visual(alias, 0, 0, json.dumps({
                "title": {"text": "Centred", "alignment": "Center"}})))[
                    "success"]
            out = str(tmp_path / "saved.pbix")
            assert json.loads(S.pbix_save(
                alias, output_path=out, overwrite=True))["success"]
            S.pbix_close(alias)
            a2 = alias + "_r"
            assert json.loads(S.pbix_open(out, a2))["success"]
            raw = json.loads(json.loads(
                S.pbix_get_layout_raw(a2))["message"])
            sv = json.loads(raw["sections"][0]["visualContainers"][0][
                "config"])["singleVisual"]
            lit = sv["vcObjects"]["title"][0]["properties"]["alignment"][
                "expr"]["Literal"]["Value"]
            assert lit == "'center'", lit
        finally:
            for a in (alias, alias + "_r"):
                S._open_files.pop(a, None)
                S._dax_cache.pop(a, None)
