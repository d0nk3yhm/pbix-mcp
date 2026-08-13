"""'Compression failed or not effective' on real-world PBIX files.

The xpress9 binding's encoder cannot re-emit already-compressed
(incompressible) 2 MiB VertiPaq chunks — its output lands a few hundred
bytes OVER the input and the wrapper raises — so every metadata edit routed
through compress_datamodel failed on real models (Power BI's own encoder
stores those chunks slightly UNDER the input).

The fix ladder in compress_datamodel:
1. primary single-threaded re-encode (unchanged behavior);
2. on failure + original_dm: a multi-threaded container reusing the
   original's unchanged session prefix VERBATIM (XPress9 windows chain
   within a session, so only prefixes are reusable; each thread-group is an
   independent session, which legally mixes Power BI's groups with ours);
3. else: the uncompressed ABF format every reader of this entry accepts.
"""
import json
import os
import shutil
import struct
import zipfile

import pytest

import pbix_mcp.formats.datamodel_roundtrip as dr
from pbix_mcp.formats.datamodel_roundtrip import (
    COMPRESS_CHUNK_SIZE,
    _compress_hybrid_reuse,
    _detect_format,
    _parse_session_groups,
    compress_datamodel,
    decompress_datamodel,
)

pytestmark = pytest.mark.unit

AW = os.path.join(os.path.dirname(__file__), "..", "test_samples",
                  "Adventure Works DW 2020.pbix")

INCOMPRESSIBLE_ERR = ValueError(
    "Compression failed or not effective: uncompressed size 2097152, "
    "compressed size 2097168")


def _aw_dm():
    if not os.path.exists(AW):
        pytest.skip("Adventure Works DW 2020.pbix sample not available")
    return zipfile.ZipFile(AW).read("DataModel")


@pytest.fixture()
def force_incompressible(monkeypatch):
    """Make the primary path fail exactly like on a real incompressible chunk."""
    def _fail(abf_bytes, chunk_size):
        raise INCOMPRESSIBLE_ERR
    monkeypatch.setattr(dr, "_compress_single_threaded", _fail)


class TestHybridReuse:
    def test_hybrid_on_desktop_authored_stream_round_trips(self):
        dm = _aw_dm()
        abf = decompress_datamodel(dm)
        edited = bytearray(abf)
        pos = abf.find(b"SQLite format 3")
        assert pos > 0
        edited[pos + 100 : pos + 132] = b"Z" * 32
        out = _compress_hybrid_reuse(bytes(edited), dm, COMPRESS_CHUNK_SIZE)
        assert _detect_format(out) == "multi_threaded"
        assert decompress_datamodel(out) == bytes(edited)

    def test_prefix_chunks_are_reused_verbatim(self):
        dm = _aw_dm()
        abf = decompress_datamodel(dm)
        edited = abf[:-64] + b"Y" * 64          # change only the very tail
        out = _compress_hybrid_reuse(edited, dm, COMPRESS_CHUNK_SIZE)
        # the original's first chunk payload must appear byte-verbatim
        first_payload = _parse_session_groups(dm)[0][0][1]
        assert first_payload in out
        # and the container declares a non-empty prefix rectangle
        assert struct.unpack_from("<Q", out, 102 + 16)[0] == 1  # prefix threads

    def test_hybrid_output_is_reusable_by_the_next_edit(self):
        dm = _aw_dm()
        abf = decompress_datamodel(dm)
        e1 = abf[:-64] + b"A" * 64
        h1 = _compress_hybrid_reuse(e1, dm, COMPRESS_CHUNK_SIZE)
        e2 = e1[:-64] + b"B" * 64
        h2 = _compress_hybrid_reuse(e2, h1, COMPRESS_CHUNK_SIZE)
        assert _detect_format(h2) == "multi_threaded"
        assert decompress_datamodel(h2) == e2


class TestCompressLadder:
    def test_primary_failure_falls_to_hybrid(self, force_incompressible):
        dm = _aw_dm()
        abf = decompress_datamodel(dm)
        edited = abf[:-32] + b"Q" * 32
        out = compress_datamodel(edited, original_dm=dm)
        assert _detect_format(out) == "multi_threaded"
        assert decompress_datamodel(out) == edited

    def test_no_original_falls_to_uncompressed(self, force_incompressible):
        dm = _aw_dm()
        abf = decompress_datamodel(dm)
        out = compress_datamodel(abf)
        assert _detect_format(out) == "uncompressed"
        assert decompress_datamodel(out) == abf

    def test_unrelated_errors_still_raise(self, monkeypatch):
        def _boom(abf_bytes, chunk_size):
            raise ValueError("something else entirely")
        monkeypatch.setattr(dr, "_compress_single_threaded", _boom)
        with pytest.raises(ValueError, match="something else"):
            compress_datamodel(b"\xff\xfeanything")

    def test_informative_error_when_no_fallback_possible(
            self, force_incompressible):
        # no original, and a blob without the stream-storage signature
        with pytest.raises(ValueError, match="xpress9"):
            compress_datamodel(b"no signature here" * 10)

    def test_primary_path_unchanged_for_compressible_input(self):
        abf = dr.STREAM_STORAGE_SIGNATURE + b"compressible " * 100000
        out = compress_datamodel(abf)
        assert _detect_format(out) == "single_threaded"
        assert decompress_datamodel(out) == abf


class TestEndToEndMetadataEdit:
    def test_add_measure_on_real_file_under_incompressible_condition(
            self, tmp_path, force_incompressible):
        if not os.path.exists(AW):
            pytest.skip("Adventure Works DW 2020.pbix sample not available")
        from pbix_mcp import server
        p = str(tmp_path / "aw.pbix")
        shutil.copy(AW, p)
        alias = "inc_e2e"
        try:
            assert json.loads(server.pbix_open(p, alias))["success"]
            work_dm = os.path.join(
                server._open_files[alias]["work_dir"], "DataModel")
            for i in (1, 2):   # chained edits must both stay compressed
                r = json.loads(server.pbix_datamodel_add_measure(
                    alias, "Sales", f"inc_probe{i}", str(i)))
                assert r.get("success"), r
                with open(work_dm, "rb") as f:
                    assert _detect_format(f.read()) == "multi_threaded"
            out = str(tmp_path / "out.pbix")
            assert json.loads(server.pbix_save(
                alias, output_path=out, overwrite=True))["success"]
        finally:
            server._open_files.pop(alias, None)
            server._dax_cache.pop(alias, None)

        # the saved file must reopen and the measures evaluate
        try:
            assert json.loads(server.pbix_open(out, "inc_e2e2"))["success"]
            r = json.loads(server.pbix_evaluate_dax(
                "inc_e2e2", "inc_probe1,inc_probe2"))
            vals = {x["name"]: x.get("value") for x in r.get("results", [])}
            assert vals == {"inc_probe1": 1, "inc_probe2": 2}
        finally:
            server._open_files.pop("inc_e2e2", None)
            server._dax_cache.pop("inc_e2e2", None)
