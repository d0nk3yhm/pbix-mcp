"""Regression tests for PBIX/ZIP extraction hardening.

Guards _extract_pbix / _validate_zip_members against decompression bombs and
path traversal in untrusted archives. See server._validate_zip_members.
"""
import os
import tempfile
import zipfile

import pytest

from pbix_mcp import server
from pbix_mcp.errors import InvalidPBIXError


def _write_zip(path, members):
    """members: list of (arcname, data, compress_type)."""
    with zipfile.ZipFile(path, "w") as zf:
        for arcname, data, ct in members:
            zi = zipfile.ZipInfo(arcname)
            zi.compress_type = ct
            zf.writestr(zi, data)


class TestZipSafety:
    def test_benign_archive_extracts(self):
        """A normal small archive extracts without error."""
        with tempfile.TemporaryDirectory() as td:
            zpath = os.path.join(td, "ok.zip")
            _write_zip(zpath, [
                ("Version", b"1.28", zipfile.ZIP_DEFLATED),
                ("dir/inner.txt", b"hello" * 100, zipfile.ZIP_DEFLATED),
            ])
            dest = os.path.join(td, "out")
            os.makedirs(dest)
            server._extract_pbix(zpath, dest)
            assert os.path.exists(os.path.join(dest, "Version"))
            assert os.path.exists(os.path.join(dest, "dir", "inner.txt"))

    def test_high_ratio_bomb_rejected(self):
        """A highly-compressible large member is refused before extraction."""
        with tempfile.TemporaryDirectory() as td:
            zpath = os.path.join(td, "bomb.zip")
            # 200 MiB of zeros -> compresses to a few KiB: ratio >> 100
            _write_zip(zpath, [
                ("bomb.bin", b"\x00" * (200 * 1024 * 1024), zipfile.ZIP_DEFLATED),
            ])
            dest = os.path.join(td, "out")
            os.makedirs(dest)
            with pytest.raises(InvalidPBIXError, match="(?i)ratio|bomb"):
                server._extract_pbix(zpath, dest)
            # nothing was written
            assert os.listdir(dest) == []

    def test_too_many_members_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            zpath = os.path.join(td, "many.zip")
            with zipfile.ZipFile(zpath, "w") as zf:
                for i in range(server._ZIP_MAX_MEMBERS + 1):
                    zf.writestr(f"f{i}", b"x")
            dest = os.path.join(td, "out")
            os.makedirs(dest)
            with pytest.raises(InvalidPBIXError, match="(?i)many entries|bomb"):
                server._extract_pbix(zpath, dest)

    def test_path_traversal_rejected(self):
        """A member resolving outside the destination is refused."""
        with tempfile.TemporaryDirectory() as td:
            zpath = os.path.join(td, "evil.zip")
            # Force a traversal name past ZipInfo (writestr keeps the raw name).
            zi = zipfile.ZipInfo("....//....//escape.txt".replace("//", os.sep))
            with zipfile.ZipFile(zpath, "w") as zf:
                zf.writestr(zi, b"pwned")
                # a genuinely escaping absolute-ish name
                zf.writestr("../../escape2.txt", b"pwned")
            dest = os.path.join(td, "out")
            os.makedirs(dest)
            with pytest.raises(InvalidPBIXError, match="(?i)traversal|outside"):
                server._extract_pbix(zpath, dest)

    def test_symlink_member_rejected(self):
        """An archive entry flagged as a symlink is refused."""
        with tempfile.TemporaryDirectory() as td:
            zpath = os.path.join(td, "link.zip")
            zi = zipfile.ZipInfo("link")
            # S_IFLNK (0o120000) in the high 16 bits of external_attr
            zi.external_attr = (0o120777) << 16
            with zipfile.ZipFile(zpath, "w") as zf:
                zf.writestr(zi, b"/etc/passwd")
            dest = os.path.join(td, "out")
            os.makedirs(dest)
            with pytest.raises(InvalidPBIXError, match="(?i)symlink"):
                server._extract_pbix(zpath, dest)
