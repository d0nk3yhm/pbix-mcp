"""FORMAT date pictures are VBA-style and case-INSENSITIVE.

The token table this replaced was .NET-cased: `MM` meant month and `mm` meant
MINUTES. DAX pictures are neither. `mmmm` matched `mm` twice and rendered
"0000" instead of "July", and every `mm/dd/yyyy` came out "00/19/2021" -- a
plausible-looking string, never an error.

The length even survived: `MS_Covid_Tracking[Updated]` is a 274-character
sentence ending in `FORMAT([Max date],"mmmm dd, yyyy")`, and "0000" is the same
width as "July", so the LEN fallback the capture harness uses could not see it
either. Only the raw string comparison could.

Every expectation below is what Power BI Desktop's own engine returned for
`FORMAT(DATE(2021,7,19), <picture>)`.
"""
from __future__ import annotations

from datetime import datetime

import pytest

from pbix_mcp.dax import engine as de

D = datetime(2021, 7, 19)          # a Monday


def fmt(value, picture):
    return de.DAXEngine()._format_datetime_pattern(value, picture)


class TestMonthTokensAreMonths:
    @pytest.mark.parametrize("picture,want", [
        ("mmmm", "July"),
        ("mmm", "Jul"),
        ("mm", "07"),
        ("m", "7"),
    ])
    def test_lower_case_m_is_a_month(self, picture, want):
        assert fmt(D, picture) == want

    @pytest.mark.parametrize("picture,want", [
        ("MMMM", "July"),
        ("MMM", "Jul"),
        ("MM", "07"),
    ])
    def test_upper_case_still_works(self, picture, want):
        """The .NET spellings were the ONLY ones that worked before; they must
        keep working."""
        assert fmt(D, picture) == want


class TestWholePictures:
    @pytest.mark.parametrize("picture,want", [
        ("mmmm dd, yyyy", "July 19, 2021"),
        ("mm/dd/yyyy", "07/19/2021"),
        ("yyyy-mm-dd", "2021-07-19"),
        ("mmm d, yyyy", "Jul 19, 2021"),
        ("dddd, mmmm dd, yyyy", "Monday, July 19, 2021"),
    ])
    def test_desktop_pictures(self, picture, want):
        assert fmt(D, picture) == want

    def test_single_letter_tokens_do_not_pad(self):
        assert fmt(datetime(2021, 3, 5), "m/d/yyyy") == "3/5/2021"


class TestMinutesVersusMonths:
    """`m` is a month EXCEPT after an hour token. Desktop pins it exactly:
    FORMAT(<noon>, "mm hh:mm") is "07 12:00" -- the first `mm` is July, the one
    after `hh:` is minutes."""

    NOON = datetime(2021, 7, 19, 12, 0, 0)

    def test_the_same_token_means_both_in_one_picture(self):
        assert fmt(self.NOON, "mm hh:mm") == "07 12:00"

    def test_minutes_after_an_hour(self):
        assert fmt(self.NOON, "hh:mm") == "12:00"

    def test_nn_is_always_minutes(self):
        assert fmt(self.NOON, "hh:nn") == "12:00"

    def test_seconds_and_padding(self):
        d = datetime(2021, 7, 19, 9, 5, 3)
        assert fmt(d, "hh:mm:ss") == "09:05:03"
        assert fmt(d, "h:m:s") == "9:5:3"

    def test_capital_h_is_24_hour(self):
        d = datetime(2021, 7, 19, 15, 4, 0)
        assert fmt(d, "HH:nn") == "15:04"
        assert fmt(d, "hh:nn AM/PM") == "03:04 PM"


class TestNamedPictures:
    @pytest.mark.parametrize("picture,want", [
        ("Long Date", "Monday, July 19, 2021"),
        ("Short Date", "7/19/2021"),
        ("long date", "Monday, July 19, 2021"),
    ])
    def test_named_pictures_render(self, picture, want):
        """These used to fall through and return the picture name itself."""
        assert fmt(D, picture) == want
