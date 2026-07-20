"""Regression tests for PBIX/ZIP extraction hardening.

Guards _extract_pbix / _validate_zip_members against decompression bombs and
path traversal in untrusted archives. See server._validate_zip_members.
"""
import json
import os
import tempfile
import zipfile

import pytest

from pbix_mcp import server
from pbix_mcp.builder import PBIXBuilder
from pbix_mcp.errors import InvalidPBIXError, UnsafeWriteError


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


def _minimal_pbix(path):
    b = PBIXBuilder("T")
    b.add_table("Items", [{"name": "ID", "data_type": "Int64"}], rows=[{"ID": 1}])
    b.save(path)


class TestSafeJoin:
    def test_rejects_parent_traversal(self):
        with tempfile.TemporaryDirectory() as td:
            base = os.path.join(td, "work")
            os.makedirs(base)
            with pytest.raises(UnsafeWriteError):
                server._safe_join(base, "../../evil.json")

    def test_rejects_absolute_path(self):
        with tempfile.TemporaryDirectory() as td:
            base = os.path.join(td, "work")
            os.makedirs(base)
            outside = os.path.join(td, "evil.json")
            with pytest.raises(UnsafeWriteError):
                server._safe_join(base, outside)

    def test_allows_contained_path(self):
        with tempfile.TemporaryDirectory() as td:
            base = os.path.join(td, "work")
            os.makedirs(base)
            got = server._safe_join(base, "sub", "ok.json")
            assert got == os.path.realpath(os.path.join(base, "sub", "ok.json"))


class TestSetThemeTraversal:
    """CWE-22/CWE-73: pbix_set_theme filename must not escape work_dir."""

    def test_traversal_filename_refused_no_file_written(self, tmp_path):
        p = str(tmp_path / "t.pbix")
        _minimal_pbix(p)
        alias = "sec1"
        try:
            server.pbix_open(p, alias)
            wd = server._open_files[alias]["work_dir"]
            base = os.path.join(wd, "Report", "StaticResources",
                                "RegisteredResources")
            target_outside = tmp_path / "PWNED.json"
            trav = os.path.relpath(str(target_outside), base)
            res = json.loads(server.pbix_set_theme(alias, '{"name":"x"}', filename=trav))
            assert res["success"] is False
            assert not target_outside.exists(), "arbitrary file write escaped work_dir!"
        finally:
            server._open_files.pop(alias, None)

    def test_legitimate_filename_still_works(self, tmp_path):
        p = str(tmp_path / "t.pbix")
        _minimal_pbix(p)
        alias = "sec2"
        try:
            server.pbix_open(p, alias)
            wd = server._open_files[alias]["work_dir"]
            res = json.loads(server.pbix_set_theme(
                alias, '{"name":"Ok","dataColors":["#2E86DE"]}', filename="Ok.json"))
            assert res["success"] is True
            # A custom theme lands in RegisteredResources (item type 201), NOT
            # BaseThemes — and is registered as a customTheme overlay on a valid
            # built-in baseTheme so Power BI Desktop actually applies its palette.
            written = os.path.join(wd, "Report", "StaticResources",
                                   "RegisteredResources", "Ok.json")
            assert os.path.exists(written)
            assert not os.path.exists(os.path.join(
                wd, "Report", "StaticResources", "SharedResources", "BaseThemes", "Ok.json"))
            layout = server._get_layout(wd)
            cfg = json.loads(layout["config"]) if isinstance(layout.get("config"), str) else layout["config"]
            tc = cfg["themeCollection"]
            assert tc["baseTheme"]["name"] == "CY24SU10" and tc["baseTheme"]["type"] == 2
            assert tc["customTheme"]["name"] == "Ok.json" and tc["customTheme"]["type"] == 1
            reg = next(pk["resourcePackage"] for pk in layout["resourcePackages"]
                       if pk["resourcePackage"]["name"] == "RegisteredResources")
            assert any(it["type"] == 201 and it["path"] == "Ok.json" for it in reg["items"])
        finally:
            server._open_files.pop(alias, None)
