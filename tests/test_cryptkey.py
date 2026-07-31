"""CryptKey.bin is independently generated, not lifted from a Microsoft file.

Clean-room result (docs/reverse-engineering/experiments/cryptkey.md): the
144-byte CryptKey is a fixed-format container whose scaffold is a format
constant (byte-identical across 25 lawfully generated corpus files) and whose
variable region accepts our own non-degenerate bytes -- Power BI Desktop loads
files carrying our random / hash-derived key region and rejects only a
degenerate all-zero region. ``build_cryptkey()`` reproduces a valid key from
the scaffold plus a self-authored SHA-512 keystream, so no Microsoft key
material ships in the package.
"""
from __future__ import annotations

import io
import zipfile

from pbix_mcp.builder import PBIXBuilder
from pbix_mcp.formats.abf_from_scratch import build_cryptkey
from pbix_mcp.formats.abf_rebuild import find_abf_file, list_abf_files, read_abf_file
from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

# 16-byte prefix of the OLD extracted-from-Microsoft variable region (from the
# pre-generator constants). None of it may appear in our generated output.
_OLD_EXTRACTED_MIDDLE = bytes.fromhex("9270d94ab3f7014a7f3d8cda8a0b13dc")
_VAR = set(i for i in range(60, 122) if i != 84)


class TestGeneratedCryptKey:
    def test_shape_and_self_authored(self):
        k = build_cryptkey()
        assert len(k) == 144
        assert k == build_cryptkey()              # deterministic
        assert k[:16] == k[-16:]                  # GUID bookend
        assert any(k[i] for i in _VAR)            # non-degenerate middle
        assert _OLD_EXTRACTED_MIDDLE not in k     # no lifted MS key bytes

    def test_seed_varies_middle_only(self):
        a = build_cryptkey(seed=b"one")
        b = build_cryptkey(seed=b"two")
        assert a != b
        assert all(a[i] == b[i] for i in range(144) if i not in _VAR)

    def test_builder_embeds_generated_key(self):
        b = PBIXBuilder("CryptKeyTest")
        b.add_table("F", [{"name": "v", "data_type": "Double"}],
                    rows=[{"v": 1.0}, {"v": 2.0}])
        b.add_measure("F", "Total", "SUM(F[v])")
        data = b.build()
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            abf = decompress_datamodel(z.read("DataModel"))
        ck = read_abf_file(abf, find_abf_file(list_abf_files(abf),
                                              "CryptKey.bin"))
        assert ck == build_cryptkey()
        assert _OLD_EXTRACTED_MIDDLE not in ck
