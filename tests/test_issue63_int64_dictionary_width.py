"""Issue #63: an Int64 value past signed-32 produced a file Desktop refuses.

Two independent contradictions, both keyed on the dictionary's ENTRY WIDTH:

1. ``DictionaryStorage.IsOperatingOn32`` declares the width (1 = 4-byte,
   0 = 8-byte) and was hardcoded to 1 for every integer-backed type, while
   the encoder widens to 8 bytes as soon as a value leaves signed-32 range.
2. ``hash_information`` — the 6 x int32 block after ``dictionary_type`` —
   also follows the width, and was keyed on the logical type alone.

Ground truth for both comes from a Desktop-authored model in the local
corpus (Contoso BI Sales Dashboard, 49 numeric dictionaries):

    LONG 4-byte -> (-1,  8, 64, 6, -1, -1)     IsOperatingOn32 = 1
    LONG 8-byte -> (-1, 16, 64, 3, -1, -1)     IsOperatingOn32 = 0
    REAL 8-byte -> (-1, 16, 64, 3, -1, -1)     IsOperatingOn32 = 0

Desktop-verified: all six probe shapes below open in Power BI Desktop after
the fix; the three past signed-32 hung indefinitely before it.

Decimal matters because it is stored scaled by 10000, so 214749.0 becomes
2,147,490,000 and needs 8-byte entries even though the unscaled value looks
small. The issue listed that as an untested adjacent risk; it is real.
"""
import json
import struct
import zipfile

import pytest

from pbix_mcp import server as S
from pbix_mcp.formats.abf_rebuild import list_abf_files, read_abf_file
from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel
from pbix_mcp.formats.vertipaq_encoder import (
    dictionary_element_size_from_blob,
    dictionary_is_operating_on_32,
)

pytestmark = pytest.mark.unit

HASH_4B = (-1, 8, 64, 6, -1, -1)
HASH_8B = (-1, 16, 64, 3, -1, -1)
S32_MAX = 2147483647


def _build(tmp_path, name, dtype, values):
    """Build a one-column file and return (element_width, hash_info, op32)."""
    p = str(tmp_path / f"{name}.pbix")
    alias = "i63_" + name
    assert json.loads(S.pbix_create(p, alias, json.dumps([{
        "name": "T", "columns": [{"name": "v", "data_type": dtype}],
        "rows": [{"v": v} for v in values]}])))["success"]
    try:
        assert json.loads(S.pbix_save(
            alias, output_path=p, overwrite=True))["success"]
    finally:
        S._open_files.pop(alias, None)
        S._dax_cache.pop(alias, None)

    with zipfile.ZipFile(p) as zf:
        abf = decompress_datamodel(zf.read("DataModel"))
    width = hash_info = None
    for e in list_abf_files(abf):
        path = e.get("Path", "")
        if path.endswith(".dictionary") and ".v (" in path:
            blob = read_abf_file(abf, e)
            width = dictionary_element_size_from_blob(blob)
            hash_info = struct.unpack_from("<6i", blob, 4)
    assert width is not None, "no dictionary for column v"
    return width, hash_info


class TestIsOperatingOn32MatchesEntryWidth:
    @pytest.mark.parametrize("label,dtype,values,want_width,want_op32", [
        ("s32_max", "Int64", [1, S32_MAX], 4, 1),
        ("s32_max_plus_1", "Int64", [1, S32_MAX + 1], 8, 0),
        ("s32_min_minus_1", "Int64", [1, -(1 << 31) - 1], 8, 0),
        ("issue_7e9", "Int64", [7000000000], 8, 0),
        ("fifteen_digit", "Int64", [800000000000000], 8, 0),
        ("bool", "Boolean", [True, False], 4, 1),
        # stored x10000: 214749.0 -> 2,147,490,000, past signed-32
        ("decimal_small", "Decimal", [1.5], 4, 1),
        ("decimal_over_s32", "Decimal", [214749.0], 8, 0),
    ])
    def test_flag_follows_the_values(self, label, dtype, values,
                                     want_width, want_op32):
        assert dictionary_is_operating_on_32(dtype, values) == want_op32
        # and the predicate agrees with the width the encoder will use
        assert (4 if want_op32 == 1 else 8) == want_width

    def test_nulls_do_not_widen_the_dictionary(self):
        """A null must not be treated as a value that needs 8 bytes."""
        assert dictionary_is_operating_on_32("Int64", [1, None, 2]) == 1
        assert dictionary_is_operating_on_32("Int64", [None]) == 1

    def test_string_and_float_are_never_four_byte(self):
        assert dictionary_is_operating_on_32("String", ["a"]) == 0
        assert dictionary_is_operating_on_32("Double", [1.5]) == 0


class TestBuiltFileIsSelfConsistent:
    """The file on disk is what Desktop reads, so assert against it."""

    @pytest.mark.parametrize("label,dtype,values,want_width", [
        ("small", "Int64", [1, 2], 4),
        ("s32_max", "Int64", [S32_MAX], 4),
        ("over_s32", "Int64", [7000000000], 8),
        ("fifteen_digit", "Int64", [800000000000000], 8),
        ("dec_small", "Decimal", [1.5], 4),
        ("dec_over", "Decimal", [214749.0], 8),
    ])
    def test_width_and_hash_match_desktop(self, tmp_path, label, dtype,
                                          values, want_width):
        width, hash_info = _build(tmp_path, label, dtype, values)
        assert width == want_width
        assert hash_info == (HASH_8B if want_width == 8 else HASH_4B)


class TestDoctorCatchesTheContradiction:
    """The issue's own point: every engine-side check passed a refused file."""

    def test_planted_flag_contradiction_is_reported(self, tmp_path):
        alias = "i63doc"
        p = str(tmp_path / "planted.pbix")
        assert json.loads(S.pbix_create(p, alias, json.dumps([{
            "name": "T", "columns": [{"name": "v", "data_type": "Int64"}],
            "rows": [{"v": 7000000000}]}])))["success"]
        try:
            # restore the pre-fix state: 8-byte entries, flag claims 4-byte
            assert json.loads(S.pbix_datamodel_modify_metadata(
                alias, "UPDATE DictionaryStorage SET IsOperatingOn32 = 1"
            ))["success"]
            out = json.loads(S.pbix_doctor(alias))["message"]
            line = next(ln for ln in out.splitlines()
                        if "Dictionary width" in ln)
            assert not line.strip().startswith("✅"), line
            assert "8-byte entries" in line and "IsOperatingOn32=1" in line
            assert "refuse to open" in line
        finally:
            S._open_files.pop(alias, None)
            S._dax_cache.pop(alias, None)

    def test_healthy_wide_dictionary_passes(self, tmp_path):
        alias = "i63ok"
        p = str(tmp_path / "ok.pbix")
        assert json.loads(S.pbix_create(p, alias, json.dumps([{
            "name": "T", "columns": [{"name": "v", "data_type": "Int64"}],
            "rows": [{"v": 7000000000}]}])))["success"]
        try:
            out = json.loads(S.pbix_doctor(alias))["message"]
            line = next(ln for ln in out.splitlines()
                        if "Dictionary width" in ln)
            assert line.strip().startswith("✅"), line
        finally:
            S._open_files.pop(alias, None)
            S._dax_cache.pop(alias, None)
