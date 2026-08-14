"""Issue #43: VertiPaq's string store is CASE-INSENSITIVE.

'VAN DER SAR' and 'van der SAR' are ONE value to Analysis Services; writing
both as separate dictionary entries made Desktop reject the whole model
('A duplicate value has been detected in the Unique Value store'). The
encoder now folds string identity with casefold() — one dictionary entry per
folded key, the FIRST spelling seen stored, every case variant's rows mapped
to it (Desktop's own import behavior). pbix_doctor gained a DBCC-style
'String dictionary invariants' check that flags case-colliding entries in
files written by older versions.
"""
import json
import os
import uuid

import pytest

from pbix_mcp import server

pytestmark = pytest.mark.unit

AW = os.path.join(os.path.dirname(__file__), "..", "test_samples",
                  "Adventure Works DW 2020.pbix")


def _players_tables():
    return [{"name": "Players",
             "columns": [{"name": "Player Name", "data_type": "String"}],
             "rows": [{"Player Name": "VAN DER SAR"},
                      {"Player Name": "van der SAR"},
                      {"Player Name": "PELE"}]}]


class TestCaseFoldedDictionary:
    @pytest.fixture()
    def players(self, tmp_path):
        alias = "cs_" + uuid.uuid4().hex[:8]
        p = str(tmp_path / "players.pbix")
        r = json.loads(server.pbix_create(
            p, alias, json.dumps(_players_tables())))
        assert r.get("success"), r
        yield alias, p
        server._open_files.pop(alias, None)
        server._dax_cache.pop(alias, None)

    def test_case_variants_fold_to_first_spelling(self, players):
        alias, _p = players
        out = json.loads(server.pbix_get_table_data(alias, "Players"))
        rows = [ln.strip() for ln in out["message"].splitlines()[1:]
                if ln.strip()]
        # 3 rows survive; the case variant reads back as the FIRST spelling
        assert rows == ["VAN DER SAR", "VAN DER SAR", "PELE"]

    def test_distinct_count_is_folded(self, players):
        alias, _p = players
        st = json.loads(server.pbix_table_stats(alias, "Players"))
        assert "distinct=2" in st["message"]

    def test_stored_dictionary_has_no_case_collisions(self, players):
        _alias, p = players
        import zipfile

        from pbix_mcp.formats.abf_rebuild import list_abf_files
        from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel
        from pbix_mcp.formats.vertipaq_decoder import decode_dictionary
        abf = decompress_datamodel(zipfile.ZipFile(p).read("DataModel"))
        for f in list_abf_files(abf):
            if "Player Name" in f["Path"] and f["Path"].endswith(".dictionary"):
                _t, values = decode_dictionary(
                    abf[f["m_cbOffsetHeader"]:f["m_cbOffsetHeader"] + f["Size"]])
                strings = [v for v in values if isinstance(v, str)]
                assert strings == ["VAN DER SAR", "PELE"]
                folded = [s.casefold() for s in strings]
                assert len(folded) == len(set(folded))
                return
        pytest.fail("Player Name dictionary not found")

    def test_doctor_invariant_check_green_on_new_file(self, players):
        alias, _p = players
        out = json.loads(server.pbix_doctor(alias))
        line = next(ln for ln in out["message"].splitlines()
                    if "String dictionary invariants" in ln)
        assert "✅" in line and "no case-folded duplicates" in line

    def test_dax_sees_folded_values(self, players):
        alias, _p = players
        r = json.loads(server.pbix_datamodel_add_measure(
            alias, "Players", "N", "COUNTROWS(Players)"))
        assert r.get("success"), r
        r = json.loads(server.pbix_datamodel_add_measure(
            alias, "Players", "D", "DISTINCTCOUNT(Players[Player Name])"))
        assert r.get("success"), r
        out = json.loads(server.pbix_evaluate_dax(alias, "N,D"))
        vals = {x["name"]: x.get("value") for x in out["results"]}
        assert vals == {"N": 3, "D": 2}


class TestDoctorFlagsLegacyCollisions:
    def test_doctor_red_on_pre_fix_dictionary(self, tmp_path, monkeypatch):
        # Simulate a file written by an older version: disable the fold for
        # the build only, then doctor must go RED on the produced dictionary.
        import pbix_mcp.formats.vertipaq_encoder as enc
        real = enc._val_key

        def legacy(v):
            if isinstance(v, float) and v != v:
                return ("__nan__",)
            return v
        monkeypatch.setattr(enc, "_val_key", legacy)
        alias = "leg_" + uuid.uuid4().hex[:8]
        p = str(tmp_path / "legacy.pbix")
        r = json.loads(server.pbix_create(
            p, alias, json.dumps(_players_tables())))
        assert r.get("success"), r
        monkeypatch.setattr(enc, "_val_key", real)
        try:
            out = json.loads(server.pbix_doctor(alias))
            line = next(ln for ln in out["message"].splitlines()
                        if "String dictionary invariants" in ln)
            assert "❌" in line
            assert "case-colliding" in line
            assert "VAN DER SAR" in line and "van der SAR" in line
        finally:
            server._open_files.pop(alias, None)
            server._dax_cache.pop(alias, None)

    def test_doctor_green_on_desktop_authored_file(self):
        if not os.path.exists(AW):
            pytest.skip("Adventure Works sample not available")
        alias = "aw_" + uuid.uuid4().hex[:8]
        assert json.loads(server.pbix_open(AW, alias))["success"]
        try:
            out = json.loads(server.pbix_doctor(alias))
            line = next(ln for ln in out["message"].splitlines()
                        if "String dictionary invariants" in ln)
            assert "✅" in line
        finally:
            server._open_files.pop(alias, None)
            server._dax_cache.pop(alias, None)
