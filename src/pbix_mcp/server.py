"""
Power BI PBIX MCP Editor Server
================================
Full read/write MCP server for .pbix and .pbit files.

Capabilities:
  READ  — Report layout, visuals, pages, filters, DataMashup (M queries),
          DataModel schema/measures/relationships, settings, metadata
  WRITE — Report layout/visuals/pages/filters, DataMashup M code, settings,
          metadata. DataModel metadata via XPress9 round-trip.

Architecture:
  - PBIX files are ZIP archives
  - We extract components, allow granular inspection/editing, and repack
  - DataModel reading uses native ABF/VertiPaq decoder (XPress9 decompression)
  - DataModel writing works via ABF round-trip (decompress → modify → recompress)
"""

import io
import json
import os
import shutil
import sqlite3
import struct
import tempfile
import traceback
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from mcp.server.fastmcp import FastMCP

from pbix_mcp.errors import (
    ABFRebuildError,
    DataModelCompressionError,
    FileAlreadyOpenError,
    FileNotOpenError,
    InvalidPBIXError,
    LayoutParseError,
    PBIXMCPError,
    SessionError,
    UnsafeWriteError,
    UnsupportedFormatError,
)
from pbix_mcp.logging_config import logger
from pbix_mcp.models.requests import DimensionRef, FilterContext
from pbix_mcp.models.responses import DAXEvalResponse, DAXResult, ToolResponse

# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------
mcp = FastMCP(
    "PowerBI-PBIX-Editor",
    instructions="Full read/write editor for Power BI .pbix/.pbit files",
)

# ---------------------------------------------------------------------------
# State: track open files
# ---------------------------------------------------------------------------
_open_files: dict[str, dict] = {}
# key = alias (user-chosen or auto), value = {
#   "path": str,              # original file path
#   "work_dir": str,          # temp extraction directory
#   "is_pbit": bool,
#   "modified": bool,
# }


# ============================= HELPERS =====================================

def _ensure_open(alias: str) -> dict:
    if alias not in _open_files:
        raise FileNotOpenError(
            f"No file open with alias '{alias}'. "
            f"Open files: {list(_open_files.keys()) or '(none)'}"
        )
    return _open_files[alias]


def _extract_pbix(pbix_path: str, work_dir: str) -> None:
    """Extract a PBIX/PBIT ZIP to work_dir."""
    with zipfile.ZipFile(pbix_path, "r") as zf:
        zf.extractall(work_dir)


def _repack_pbix(work_dir: str, output_path: str) -> None:
    """Repack work_dir into a PBIX/PBIT ZIP file."""
    # Delete SecurityBindings — Power BI Desktop rejects modified files
    # that still have the original SecurityBindings
    sec_path = os.path.join(work_dir, "SecurityBindings")
    sec_removed = False
    if os.path.exists(sec_path):
        os.remove(sec_path)
        sec_removed = True

    # Update [Content_Types].xml to remove SecurityBindings reference
    if sec_removed:
        ct_path = os.path.join(work_dir, "[Content_Types].xml")
        if os.path.exists(ct_path):
            with open(ct_path, "r", encoding="utf-8") as f:
                ct_xml = f.read()
            ct_xml = ct_xml.replace(
                '<Override PartName="/SecurityBindings" ContentType=""/>',
                ""
            )
            with open(ct_path, "w", encoding="utf-8") as f:
                f.write(ct_xml)

    # Files that must NOT be included in the final ZIP
    _EXCLUDE_FILES = {
        "DataModel.abf",     # temp file from pbix_datamodel_decompress
        "metadata.sqlitedb", # extracted by ModelReader / tools — stale, causes PBI crash
    }
    # Suffixes that are temp artifacts
    _EXCLUDE_SUFFIXES = (".abf", ".tmp", ".bak", ".sqlitedb")

    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(work_dir):
            for file in files:
                # Skip temp/artifact files
                if file in _EXCLUDE_FILES or file.endswith(_EXCLUDE_SUFFIXES):
                    continue
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, work_dir).replace("\\", "/")
                # DataModel should be stored, not deflated (it's XPress9 compressed)
                if file == "DataModel":
                    zf.write(file_path, arcname, compress_type=zipfile.ZIP_STORED)
                else:
                    zf.write(file_path, arcname)


def _read_json_component(work_dir: str, rel_path: str) -> Any:
    """Read a JSON component from the extracted work dir."""
    full = os.path.join(work_dir, rel_path)
    if not os.path.exists(full):
        return None
    enc = _detect_encoding(full)
    with open(full, "r", encoding=enc) as f:
        return json.load(f)


def _write_json_component(work_dir: str, rel_path: str, data: Any) -> None:
    """Write a JSON component back, preserving original encoding."""
    full = os.path.join(work_dir, rel_path)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    enc = _detect_encoding(full) if os.path.exists(full) else "utf-16-le"
    text = json.dumps(data, indent=2, ensure_ascii=False)
    with open(full, "wb") as f:
        f.write(text.encode(enc))


def _read_datamashup_m_code(work_dir: str) -> str | None:
    """Extract M code from the DataMashup binary.

    The DataMashup is a binary stream that embeds a ZIP archive.
    We scan for the PK signature to find the inner ZIP, then read
    Formulas/Section1.m from it.
    """
    dm_path = os.path.join(work_dir, "DataMashup")
    if not os.path.exists(dm_path):
        return None

    with open(dm_path, "rb") as f:
        data = f.read()

    # Find the inner ZIP (PK\x03\x04 signature)
    pk_offset = data.find(b"PK\x03\x04")
    if pk_offset == -1:
        return None

    # Find the end of the ZIP (scan for end-of-central-directory)
    eocd_sig = b"PK\x05\x06"
    eocd_pos = data.rfind(eocd_sig)
    if eocd_pos == -1:
        return None

    # EOCD is 22 bytes minimum, but may have a comment
    eocd_comment_len = struct.unpack_from("<H", data, eocd_pos + 20)[0]
    zip_end = eocd_pos + 22 + eocd_comment_len

    zip_data = data[pk_offset:zip_end]

    try:
        with zipfile.ZipFile(io.BytesIO(zip_data), "r") as inner_zf:
            for candidate in [
                "Formulas/Section1.m",
                "formulas/Section1.m",
                "Section1.m",
            ]:
                if candidate in inner_zf.namelist():
                    return inner_zf.read(candidate).decode("utf-8-sig")

            return f"[No Section1.m found. Archive contains: {inner_zf.namelist()}]"
    except zipfile.BadZipFile:
        return "[Could not parse inner DataMashup ZIP]"


def _write_datamashup_m_code(work_dir: str, new_m_code: str) -> bool:
    """Replace M code inside the DataMashup binary.

    Strategy: locate the inner ZIP, extract it, replace Section1.m,
    rebuild the inner ZIP, splice it back into the binary stream.
    """
    dm_path = os.path.join(work_dir, "DataMashup")
    if not os.path.exists(dm_path):
        return False

    with open(dm_path, "rb") as f:
        data = f.read()

    pk_offset = data.find(b"PK\x03\x04")
    if pk_offset == -1:
        return False

    eocd_sig = b"PK\x05\x06"
    eocd_pos = data.rfind(eocd_sig)
    if eocd_pos == -1:
        return False

    eocd_comment_len = struct.unpack_from("<H", data, eocd_pos + 20)[0]
    zip_end = eocd_pos + 22 + eocd_comment_len

    old_zip_data = data[pk_offset:zip_end]

    # Rebuild inner ZIP with new M code
    new_zip_buf = io.BytesIO()
    try:
        with zipfile.ZipFile(io.BytesIO(old_zip_data), "r") as old_zf:
            with zipfile.ZipFile(new_zip_buf, "w", zipfile.ZIP_DEFLATED) as new_zf:
                for item in old_zf.namelist():
                    if item.endswith("Section1.m"):
                        new_zf.writestr(item, new_m_code.encode("utf-8"))
                    else:
                        new_zf.writestr(item, old_zf.read(item))
    except zipfile.BadZipFile:
        return False

    new_zip_bytes = new_zip_buf.getvalue()

    # Splice: prefix + new_zip + suffix
    prefix = data[:pk_offset]
    suffix = data[zip_end:]

    new_data = prefix + new_zip_bytes + suffix

    # If there's a size field at pk_offset - 4, update it
    if pk_offset >= 4:
        old_size = struct.unpack_from("<I", prefix, pk_offset - 4)[0]
        old_zip_len = zip_end - pk_offset
        if old_size == old_zip_len:
            new_data = bytearray(new_data)
            struct.pack_into("<I", new_data, pk_offset - 4, len(new_zip_bytes))
            new_data = bytes(new_data)

    with open(dm_path, "wb") as f:
        f.write(new_data)

    return True


def _detect_encoding(file_path: str) -> str:
    """Detect if a file is UTF-16-LE, UTF-8 BOM, or plain UTF-8."""
    with open(file_path, "rb") as f:
        header = f.read(4)
    if header[:2] == b"\xff\xfe":
        return "utf-16-le"
    if header[:3] == b"\xef\xbb\xbf":
        return "utf-8-sig"
    if len(header) >= 2 and header[1:2] == b"\x00":
        return "utf-16-le"
    return "utf-8"


def _get_layout(work_dir: str) -> dict | None:
    """Read the Report/Layout JSON."""
    layout_path = os.path.join(work_dir, "Report", "Layout")
    if not os.path.exists(layout_path):
        return None
    enc = _detect_encoding(layout_path)
    with open(layout_path, "r", encoding=enc) as f:
        return json.load(f)


def _set_layout(work_dir: str, layout: dict) -> None:
    """Write the Report/Layout JSON back in UTF-16-LE (Power BI native)."""
    layout_path = os.path.join(work_dir, "Report", "Layout")
    os.makedirs(os.path.dirname(layout_path), exist_ok=True)
    text = json.dumps(layout, ensure_ascii=False)
    with open(layout_path, "wb") as f:
        f.write(text.encode("utf-16-le"))


def _parse_visual_config(vc: dict) -> dict:
    """Parse the 'config' JSON string inside a visual container."""
    config_str = vc.get("config", "{}")
    if isinstance(config_str, str):
        try:
            return json.loads(config_str)
        except json.JSONDecodeError:
            return {}
    return config_str if isinstance(config_str, dict) else {}


def _get_visual_type(config: dict) -> str:
    """Extract visual type from parsed config."""
    sc = config.get("singleVisual", config.get("singleVisualGroup", {}))
    if sc:
        return sc.get("visualType", "unknown")
    return "unknown"


def _get_visual_name(config: dict) -> str:
    """Extract the visual name from config."""
    return config.get("name", "")


def _set_value_by_dot_path(obj: Any, path: str, value: Any) -> None:
    """Set a nested value using a dot-separated path like 'a.b.c'."""
    keys = path.split(".")
    for key in keys[:-1]:
        if isinstance(obj, dict):
            obj = obj.setdefault(key, {})
        elif isinstance(obj, list):
            idx = int(key)
            obj = obj[idx]
        else:
            raise ValueError(f"Cannot traverse into {type(obj)} at key '{key}'")
    final_key = keys[-1]
    if isinstance(obj, dict):
        obj[final_key] = value
    elif isinstance(obj, list):
        obj[int(final_key)] = value
    else:
        raise ValueError(f"Cannot set key '{final_key}' on {type(obj)}")


# ============================= MCP TOOLS ===================================

# ---- Section 3: File Management ----

@mcp.tool()
def pbix_open(file_path: str, alias: str = "") -> str:
    """Open a PBIX or PBIT file for editing.

    Args:
        file_path: Full path to the .pbix or .pbit file
        alias: Short name to reference this file (auto-generated if empty)
    """
    file_path = os.path.abspath(file_path)
    if not os.path.exists(file_path):
        raise InvalidPBIXError(f"File not found: {file_path}")

    ext = os.path.splitext(file_path)[1].lower()
    if ext not in (".pbix", ".pbit"):
        raise InvalidPBIXError(f"Expected .pbix or .pbit file, got '{ext}'")

    if not alias:
        alias = Path(file_path).stem

    if alias in _open_files:
        raise FileAlreadyOpenError(f"Alias '{alias}' is already in use. Close it first or choose a different alias.")

    # Create work directory
    work_dir = os.path.join(
        tempfile.gettempdir(),
        f"pbix_mcp_{alias}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
    )
    os.makedirs(work_dir, exist_ok=True)

    try:
        logger.info("Opening %s as '%s'", file_path, alias)
        _extract_pbix(file_path, work_dir)
        logger.debug("Extracted to %s", work_dir)
    except PBIXMCPError:
        raise
    except Exception as e:
        logger.error("Failed to extract %s: %s", file_path, e)
        shutil.rmtree(work_dir, ignore_errors=True)
        raise InvalidPBIXError(f"Failed to extract: {e}")

    # Detect DirectQuery / composite models by checking for connections in DataModel
    _dq_flag = False
    dm_path = os.path.join(work_dir, "DataModel")
    if os.path.exists(dm_path):
        try:
            from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
            from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel
            dm_bytes = open(dm_path, "rb").read()
            abf = decompress_datamodel(dm_bytes)
            db_bytes = read_metadata_sqlite(abf)
            import sqlite3
            tmp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
            tmp_db.write(db_bytes)
            tmp_db.close()
            conn = sqlite3.connect(tmp_db.name)
            # Check for DirectQuery partitions (Mode=1 is DirectQuery, Mode=0 is Import)
            # Note: Type=4 for both Import and DirectQuery; Mode distinguishes them
            dq_partitions = conn.execute(
                "SELECT COUNT(*) FROM [Partition] WHERE Mode = 1"
            ).fetchone()[0]
            conn.close()
            os.unlink(tmp_db.name)
            if dq_partitions > 0:
                _dq_flag = True
                logger.warning(
                    "DirectQuery detected: %d DirectQuery partition(s). "
                    "Data operations (table reads, DAX evaluation) will not work. "
                    "Layout, measures, and metadata operations are still available.",
                    dq_partitions,
                )
        except Exception:
            pass  # If detection fails, continue — the file might still be usable

    _open_files[alias] = {
        "path": file_path,
        "work_dir": work_dir,
        "is_pbit": ext == ".pbit",
        "modified": False,
        "is_directquery": _dq_flag,
    }

    # Inventory
    components = []
    for root, dirs, files in os.walk(work_dir):
        for f in files:
            rel = os.path.relpath(os.path.join(root, f), work_dir)
            size = os.path.getsize(os.path.join(root, f))
            components.append(f"  {rel} ({size:,} bytes)")

    return ToolResponse.ok(
        f"Opened '{file_path}' as '{alias}'\n"
        f"Type: {'PBIT template' if ext == '.pbit' else 'PBIX report'}"
        f"{' ⚠️ DirectQuery detected — data operations unavailable, layout/measures/metadata OK' if _dq_flag else ''}\n"
        f"Components:\n" + "\n".join(sorted(components))
    ).to_text()


@mcp.tool()
def pbix_save(alias: str, output_path: str = "", overwrite: bool = False, backup: bool = True) -> str:
    """Save/repack the modified PBIX/PBIT file.

    Creates an automatic .bak backup before overwriting (unless backup=False).
    Set overwrite=False to refuse overwriting an existing file.

    Args:
        alias: The alias of the open file
        output_path: Where to save. Empty = overwrite original.
        overwrite: If False (default), refuse to overwrite an existing file
        backup: If True (default), create a .bak backup before overwriting
    """
    try:
        info = _ensure_open(alias)
        work_dir = info["work_dir"]
        target = output_path or info["path"]
        target = os.path.abspath(target)
        logger.info("Saving '%s' to %s (overwrite=%s, backup=%s)", alias, target, overwrite, backup)

        # Safety: refuse overwrite if explicitly disabled
        if not overwrite and os.path.exists(target) and target != info["path"]:
            raise UnsafeWriteError(f"'{target}' already exists and overwrite=False. Use overwrite=True or choose a different path.")

        # If overwriting original, create backup
        if backup and target == info["path"] and os.path.exists(target):
            backup_path = target + ".bak"
            shutil.copy2(target, backup_path)

        _repack_pbix(work_dir, target)
        info["modified"] = False
        size = os.path.getsize(target)
        return ToolResponse.ok(f"Saved '{alias}' to {target} ({size:,} bytes)").to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        raise SessionError(f"Save failed: {e}")


@mcp.tool()
def pbix_close(alias: str, force: bool = False) -> str:
    """Close an open file and clean up temporary files.

    Refuses to close files with unsaved changes unless force=True.

    Args:
        alias: The alias of the open file
        force: If False (default), refuse to close files with unsaved changes
    """
    try:
        info = _ensure_open(alias)
        work_dir = info["work_dir"]

        if info.get("modified") and not force:
            raise UnsafeWriteError(
                f"'{alias}' has unsaved changes. Use pbix_save first, or pbix_close with force=True to discard changes."
            )

        shutil.rmtree(work_dir, ignore_errors=True)
        logger.info("Closed '%s'", alias)
        del _open_files[alias]
        return ToolResponse.ok(f"Closed '{alias}'.").to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        raise SessionError(f"Close failed: {e}")


@mcp.tool()
def pbix_list_open() -> str:
    """List all currently open PBIX/PBIT files."""
    if not _open_files:
        return ToolResponse.ok("No files currently open.").to_text()
    lines = []
    for alias, info in _open_files.items():
        status = "modified" if info.get("modified") else "clean"
        ftype = "PBIT" if info.get("is_pbit") else "PBIX"
        lines.append(f"  {alias}: {info['path']} [{ftype}, {status}]")
    return ToolResponse.ok("Open files:\n" + "\n".join(lines)).to_text()


# ---- Section 4: Report Layout tools ----

@mcp.tool()
def pbix_get_pages(alias: str) -> str:
    """List all pages in the report with visual counts.

    Args:
        alias: The alias of the open file
    """
    try:
        info = _ensure_open(alias)
        layout = _get_layout(info["work_dir"])
        if not layout:
            raise LayoutParseError("No layout found in this file")

        sections = layout.get("sections", [])
        lines = [f"Report has {len(sections)} page(s):\n"]
        for i, sec in enumerate(sections):
            name = sec.get("displayName", f"Page {i}")
            vis_count = len(sec.get("visualContainers", []))
            width = sec.get("width", "?")
            height = sec.get("height", "?")
            hidden = " [HIDDEN]" if sec.get("config", "").find('"visibility":1') >= 0 else ""
            lines.append(f"  [{i}] {name} — {vis_count} visuals, {width}x{height}{hidden}")
        return ToolResponse.ok("\n".join(lines)).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        raise LayoutParseError(str(e))


@mcp.tool()
def pbix_get_page_visuals(alias: str, page_index: int = 0) -> str:
    """List all visuals on a specific page.

    Args:
        alias: The alias of the open file
        page_index: Zero-based page index
    """
    try:
        info = _ensure_open(alias)
        layout = _get_layout(info["work_dir"])
        if not layout:
            raise LayoutParseError("No layout found")

        sections = layout.get("sections", [])
        if page_index < 0 or page_index >= len(sections):
            raise LayoutParseError(f"Page index {page_index} out of range (0-{len(sections)-1})")

        page = sections[page_index]
        page_name = page.get("displayName", f"Page {page_index}")
        containers = page.get("visualContainers", [])

        lines = [f"Page '{page_name}' has {len(containers)} visual(s):\n"]
        for i, vc in enumerate(containers):
            config = _parse_visual_config(vc)
            vtype = _get_visual_type(config)
            vname = _get_visual_name(config)
            x = vc.get("x", 0)
            y = vc.get("y", 0)
            w = vc.get("width", 0)
            h = vc.get("height", 0)
            lines.append(f"  [{i}] {vtype} (name={vname}) at ({x},{y}) size {w}x{h}")
        return ToolResponse.ok("\n".join(lines)).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        raise LayoutParseError(str(e))


@mcp.tool()
def pbix_get_visual_detail(alias: str, page_index: int, visual_index: int) -> str:
    """Get the full configuration JSON for a specific visual.

    Args:
        alias: The alias of the open file
        page_index: Zero-based page index
        visual_index: Zero-based visual index on the page
    """
    try:
        info = _ensure_open(alias)
        layout = _get_layout(info["work_dir"])
        if not layout:
            raise LayoutParseError("No layout found")

        sections = layout.get("sections", [])
        if page_index < 0 or page_index >= len(sections):
            raise LayoutParseError(f"Page index {page_index} out of range")

        containers = sections[page_index].get("visualContainers", [])
        if visual_index < 0 or visual_index >= len(containers):
            raise LayoutParseError(f"Visual index {visual_index} out of range")

        vc = containers[visual_index]
        config = _parse_visual_config(vc)
        result = {
            "x": vc.get("x", 0),
            "y": vc.get("y", 0),
            "width": vc.get("width", 0),
            "height": vc.get("height", 0),
            "z": vc.get("z", 0),
            "config": config,
        }
        # Include query and dataTransforms if present
        for key in ("query", "dataTransforms", "filters"):
            raw = vc.get(key)
            if raw:
                if isinstance(raw, str):
                    try:
                        result[key] = json.loads(raw)
                    except json.JSONDecodeError:
                        result[key] = raw
                else:
                    result[key] = raw

        return ToolResponse.ok(json.dumps(result, indent=2, ensure_ascii=False)).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        raise LayoutParseError(str(e))


@mcp.tool()
def pbix_set_visual_property(
    alias: str, page_index: int, visual_index: int,
    property_path: str, value: str
) -> str:
    """Set a property on a visual using a dot-path (e.g. 'singleVisual.title.text').

    Args:
        alias: The alias of the open file
        page_index: Zero-based page index
        visual_index: Zero-based visual index on the page
        property_path: Dot-separated path into the config JSON
        value: New value (JSON-encoded string, e.g. '"hello"' or '42' or 'true')
    """
    try:
        info = _ensure_open(alias)
        layout = _get_layout(info["work_dir"])
        if not layout:
            raise LayoutParseError("No layout found")

        sections = layout.get("sections", [])
        if page_index < 0 or page_index >= len(sections):
            raise LayoutParseError(f"Page index {page_index} out of range")

        containers = sections[page_index].get("visualContainers", [])
        if visual_index < 0 or visual_index >= len(containers):
            raise LayoutParseError(f"Visual index {visual_index} out of range")

        vc = containers[visual_index]
        config = _parse_visual_config(vc)

        # Parse the value as JSON
        try:
            parsed_value = json.loads(value)
        except json.JSONDecodeError:
            parsed_value = value  # treat as raw string

        _set_value_by_dot_path(config, property_path, parsed_value)

        # Write config back
        vc["config"] = json.dumps(config, ensure_ascii=False)
        _set_layout(info["work_dir"], layout)
        info["modified"] = True
        return ToolResponse.ok(f"Set {property_path} = {value} on page {page_index}, visual {visual_index}").to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        raise LayoutParseError(str(e))


@mcp.tool()
def pbix_update_visual_json(
    alias: str, page_index: int, visual_index: int, config_json: str
) -> str:
    """Replace the entire config JSON for a visual.

    Args:
        alias: The alias of the open file
        page_index: Zero-based page index
        visual_index: Zero-based visual index on the page
        config_json: Complete config JSON string to replace
    """
    try:
        info = _ensure_open(alias)
        layout = _get_layout(info["work_dir"])
        if not layout:
            raise LayoutParseError("No layout found")

        sections = layout.get("sections", [])
        if page_index < 0 or page_index >= len(sections):
            raise LayoutParseError(f"Page index {page_index} out of range")

        containers = sections[page_index].get("visualContainers", [])
        if visual_index < 0 or visual_index >= len(containers):
            raise LayoutParseError(f"Visual index {visual_index} out of range")

        # Validate JSON
        try:
            new_config = json.loads(config_json)
        except json.JSONDecodeError as e:
            raise LayoutParseError(f"Invalid JSON: {e}")

        containers[visual_index]["config"] = json.dumps(new_config, ensure_ascii=False)
        _set_layout(info["work_dir"], layout)
        info["modified"] = True
        return ToolResponse.ok(f"Updated visual config on page {page_index}, visual {visual_index}").to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        raise LayoutParseError(str(e))


@mcp.tool()
def pbix_add_page(alias: str, display_name: str, width: int = 1280, height: int = 720) -> str:
    """Add a new blank page to the report.

    Args:
        alias: The alias of the open file
        display_name: Name for the new page
        width: Page width in pixels (default 1280)
        height: Page height in pixels (default 720)
    """
    try:
        info = _ensure_open(alias)
        layout = _get_layout(info["work_dir"])
        if not layout:
            raise LayoutParseError("No layout found")

        import uuid
        new_section = {
            "displayName": display_name,
            "displayOption": 0,
            "name": str(uuid.uuid4()).replace("-", ""),
            "width": width,
            "height": height,
            "visualContainers": [],
            "config": json.dumps({"visibility": 0}),
            "filters": "[]",
            "ordinal": len(layout.get("sections", [])),
        }

        layout.setdefault("sections", []).append(new_section)
        _set_layout(info["work_dir"], layout)
        info["modified"] = True
        idx = len(layout["sections"]) - 1
        return ToolResponse.ok(f"Added page '{display_name}' at index {idx} ({width}x{height})").to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        raise LayoutParseError(str(e))


@mcp.tool()
def pbix_remove_page(alias: str, page_index: int) -> str:
    """Remove a page from the report.

    Args:
        alias: The alias of the open file
        page_index: Zero-based page index to remove
    """
    try:
        info = _ensure_open(alias)
        layout = _get_layout(info["work_dir"])
        if not layout:
            raise LayoutParseError("No layout found")

        sections = layout.get("sections", [])
        if page_index < 0 or page_index >= len(sections):
            raise LayoutParseError(f"Page index {page_index} out of range")

        removed = sections.pop(page_index)
        name = removed.get("displayName", f"Page {page_index}")
        _set_layout(info["work_dir"], layout)
        info["modified"] = True
        return ToolResponse.ok(f"Removed page '{name}' (was index {page_index}). {len(sections)} pages remain.").to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        raise LayoutParseError(str(e))


@mcp.tool()
def pbix_create(
    file_path: str,
    alias: str = "",
    tables_json: str = "",
    measures_json: str = "",
    relationships_json: str = "",
) -> str:
    """Create a new PBIX file and open it for editing.

    Builds a valid PBIX entirely from scratch — no templates or skeletons.
    Every layer is generated from code: PBIX ZIP shell, ABF binary container,
    db.xml, metadata SQLite, VertiPaq column data, and report layout.

    Args:
        file_path: Where to save the new file (e.g. "my_report.pbix")
        alias: Alias for the opened file (auto-generated if empty)
        tables_json: Optional JSON array of tables with columns and rows, e.g.
            '[{"name": "Sales", "columns": [{"name": "Amount", "data_type": "Double"},
              {"name": "Product", "data_type": "String"}],
              "rows": [{"Amount": 100.0, "Product": "Widget"}]}]'
            Supported data_type values: String, Int64, Double, DateTime, Decimal, Boolean
            Optional per-table fields:
            - "source_csv": "/path/to/data.csv" — M expression references CSV for Refresh
            - "source_db": {"type": "sqlserver", "server": "localhost", "database": "mydb",
              "table": "orders"} — M expression references database for Refresh/DirectQuery.
              Supported types: "sqlserver", "mysql", "sqlite", "postgresql",
              "mariadb" (MySQL DirectQuery via MariaDB adapter),
              "excel" (needs path+sheet), "json"/"web"/"api" (needs url),
              "azuresql"/"azure" (same as sqlserver for Azure SQL)
            - "mode": "directquery" — live database queries (default: "import").
              DirectQuery requires source_db and a running database server.
        measures_json: Optional JSON array of measures, e.g.
            '[{"table": "Sales", "name": "Total", "expression": "SUM(Sales[Amount])"}]'
        relationships_json: Optional JSON array of relationships, e.g.
            '[{"from_table": "Sales", "from_column": "ProductID",
              "to_table": "Products", "to_column": "ProductID"}]'
    """
    try:
        from pbix_mcp.builder import PBIXBuilder

        builder = PBIXBuilder()

        if tables_json:
            for tdef in json.loads(tables_json):
                builder.add_table(
                    tdef["name"],
                    tdef.get("columns", []),
                    rows=tdef.get("rows"),
                    hidden=tdef.get("hidden", False),
                    source_csv=tdef.get("source_csv"),
                    source_db=tdef.get("source_db"),
                    mode=tdef.get("mode", "import"),
                )

        if measures_json:
            for mdef in json.loads(measures_json):
                builder.add_measure(
                    mdef["table"],
                    mdef["name"],
                    mdef["expression"],
                    mdef.get("description", ""),
                )

        if relationships_json:
            for rdef in json.loads(relationships_json):
                builder.add_relationship(
                    rdef["from_table"],
                    rdef["from_column"],
                    rdef["to_table"],
                    rdef["to_column"],
                )

        builder.add_page("Page 1")

        abs_path = builder.save(file_path)
        size = os.path.getsize(abs_path)

        # Auto-open the created file
        result = pbix_open(abs_path, alias)
        return ToolResponse.ok(f"Created '{abs_path}' ({size:,} bytes) and opened it.\n{result}").to_text()

    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_add_visual(
    alias: str,
    page_index: int,
    visual_type: str,
    x: int = 0,
    y: int = 0,
    width: int = 300,
    height: int = 200,
    config_json: str = "",
) -> str:
    """Add a new visual to a report page.

    Supports all Power BI visual types: card, table, clusteredBarChart,
    clusteredColumnChart, lineChart, pieChart, donutChart, shape (buttons),
    image, slicer, textbox, and any custom visual type.

    Args:
        alias: The alias of the open file
        page_index: Zero-based page index
        visual_type: Visual type (e.g. "card", "clusteredBarChart", "shape", "image", "textbox")
        x: X position in pixels
        y: Y position in pixels
        width: Width in pixels
        height: Height in pixels
        config_json: Optional full config JSON to merge (for advanced properties)
    """
    try:
        info = _ensure_open(alias)
        layout = _get_layout(info["work_dir"])
        if not layout:
            raise LayoutParseError("No layout found")

        sections = layout.get("sections", [])
        if page_index < 0 or page_index >= len(sections):
            raise LayoutParseError(f"Page index {page_index} out of range")

        import uuid
        visual_name = str(uuid.uuid4()).replace("-", "")[:16]

        config = {
            "name": visual_name,
            "singleVisual": {
                "visualType": visual_type,
            },
        }

        # Merge custom config if provided
        if config_json:
            try:
                custom = json.loads(config_json)
                if isinstance(custom, dict):
                    for key, val in custom.items():
                        if key == "singleVisual" and isinstance(val, dict):
                            config["singleVisual"].update(val)
                        else:
                            config[key] = val
            except json.JSONDecodeError:
                raise LayoutParseError("Invalid config_json")

        container = {
            "x": x,
            "y": y,
            "width": width,
            "height": height,
            "config": json.dumps(config, ensure_ascii=False),
        }

        page = sections[page_index]
        page.setdefault("visualContainers", []).append(container)
        _set_layout(info["work_dir"], layout)
        info["modified"] = True

        idx = len(page["visualContainers"]) - 1
        page_name = page.get("displayName", f"Page {page_index}")
        return ToolResponse.ok(f"Added {visual_type} visual at ({x},{y}) {width}x{height} on '{page_name}' (index {idx})").to_text()

    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        raise LayoutParseError(str(e))


@mcp.tool()
def pbix_remove_visual(alias: str, page_index: int, visual_index: int) -> str:
    """Remove a visual from a report page.

    Args:
        alias: The alias of the open file
        page_index: Zero-based page index
        visual_index: Zero-based visual index on the page
    """
    try:
        info = _ensure_open(alias)
        layout = _get_layout(info["work_dir"])
        if not layout:
            raise LayoutParseError("No layout found")

        sections = layout.get("sections", [])
        if page_index < 0 or page_index >= len(sections):
            raise LayoutParseError(f"Page index {page_index} out of range")

        containers = sections[page_index].get("visualContainers", [])
        if visual_index < 0 or visual_index >= len(containers):
            raise LayoutParseError(f"Visual index {visual_index} out of range")

        removed = containers.pop(visual_index)
        config = _parse_visual_config(removed)
        vtype = _get_visual_type(config)

        _set_layout(info["work_dir"], layout)
        info["modified"] = True
        return ToolResponse.ok(f"Removed {vtype} visual (was index {visual_index}). {len(containers)} visuals remain.").to_text()

    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        raise LayoutParseError(str(e))


@mcp.tool()
def pbix_get_layout_raw(alias: str) -> str:
    """Get the raw Report/Layout JSON.

    Args:
        alias: The alias of the open file
    """
    try:
        info = _ensure_open(alias)
        layout = _get_layout(info["work_dir"])
        if not layout:
            raise LayoutParseError("No layout found")
        return ToolResponse.ok(json.dumps(layout, indent=2, ensure_ascii=False)).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        raise LayoutParseError(str(e))


@mcp.tool()
def pbix_set_layout_raw(alias: str, layout_json: str) -> str:
    """Write raw layout JSON back to Report/Layout.

    Args:
        alias: The alias of the open file
        layout_json: Complete layout JSON string
    """
    try:
        info = _ensure_open(alias)
        try:
            layout = json.loads(layout_json)
        except json.JSONDecodeError as e:
            raise LayoutParseError(f"Invalid JSON: {e}")
        _set_layout(info["work_dir"], layout)
        info["modified"] = True
        return ToolResponse.ok("Layout updated.").to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        raise LayoutParseError(str(e))


@mcp.tool()
def pbix_get_filters(alias: str, page_index: int = -1) -> str:
    """Get report-level or page-level filters.

    Args:
        alias: The alias of the open file
        page_index: Page index for page filters, or -1 for report-level filters
    """
    try:
        info = _ensure_open(alias)
        layout = _get_layout(info["work_dir"])
        if not layout:
            raise LayoutParseError("No layout found")

        if page_index == -1:
            # Report-level filters
            filters_raw = layout.get("filters", "[]")
        else:
            sections = layout.get("sections", [])
            if page_index < 0 or page_index >= len(sections):
                raise LayoutParseError(f"Page index {page_index} out of range")
            filters_raw = sections[page_index].get("filters", "[]")

        if isinstance(filters_raw, str):
            try:
                filters = json.loads(filters_raw)
            except json.JSONDecodeError:
                filters = filters_raw
        else:
            filters = filters_raw

        level = f"page {page_index}" if page_index >= 0 else "report"
        return ToolResponse.ok(f"Filters ({level}):\n{json.dumps(filters, indent=2, ensure_ascii=False)}").to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        raise LayoutParseError(str(e))


@mcp.tool()
def pbix_set_filters(alias: str, filters_json: str, page_index: int = -1) -> str:
    """Set report-level or page-level filters.

    Args:
        alias: The alias of the open file
        filters_json: JSON array of filter definitions
        page_index: Page index for page filters, or -1 for report-level filters
    """
    try:
        info = _ensure_open(alias)
        layout = _get_layout(info["work_dir"])
        if not layout:
            raise LayoutParseError("No layout found")

        # Validate JSON
        try:
            json.loads(filters_json)
        except json.JSONDecodeError as e:
            raise LayoutParseError(f"Invalid JSON: {e}")

        if page_index == -1:
            layout["filters"] = filters_json
        else:
            sections = layout.get("sections", [])
            if page_index < 0 or page_index >= len(sections):
                raise LayoutParseError(f"Page index {page_index} out of range")
            sections[page_index]["filters"] = filters_json

        _set_layout(info["work_dir"], layout)
        info["modified"] = True
        level = f"page {page_index}" if page_index >= 0 else "report"
        return ToolResponse.ok(f"Filters updated ({level}).").to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        raise LayoutParseError(str(e))


@mcp.tool()
def pbix_get_settings(alias: str) -> str:
    """Get report settings from Report/Settings JSON.

    Args:
        alias: The alias of the open file
    """
    try:
        info = _ensure_open(alias)
        settings = _read_json_component(info["work_dir"], os.path.join("Report", "Settings"))
        if settings is None:
            return ToolResponse.ok("No Settings found.").to_text()
        return ToolResponse.ok(json.dumps(settings, indent=2, ensure_ascii=False)).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_set_settings(alias: str, settings_json: str) -> str:
    """Write report settings back to Report/Settings.

    Args:
        alias: The alias of the open file
        settings_json: Complete settings JSON string
    """
    try:
        info = _ensure_open(alias)
        try:
            settings = json.loads(settings_json)
        except json.JSONDecodeError as e:
            raise LayoutParseError(f"Invalid JSON: {e}")
        _write_json_component(info["work_dir"], os.path.join("Report", "Settings"), settings)
        info["modified"] = True
        return ToolResponse.ok("Settings updated.").to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_get_bookmarks(alias: str) -> str:
    """Get report bookmarks.

    Args:
        alias: The alias of the open file
    """
    try:
        info = _ensure_open(alias)
        layout = _get_layout(info["work_dir"])
        if not layout:
            raise LayoutParseError("No layout found")

        config_str = layout.get("config", "{}")
        if isinstance(config_str, str):
            try:
                config = json.loads(config_str)
            except json.JSONDecodeError:
                config = {}
        else:
            config = config_str

        bookmarks = config.get("bookmarks", [])
        if not bookmarks:
            return ToolResponse.ok("No bookmarks found.").to_text()

        lines = [f"Report has {len(bookmarks)} bookmark(s):\n"]
        for i, bm in enumerate(bookmarks):
            name = bm.get("displayName", bm.get("name", f"Bookmark {i}"))
            lines.append(f"  [{i}] {name}")
        return ToolResponse.ok("\n".join(lines)).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        raise LayoutParseError(str(e))


@mcp.tool()
def pbix_add_bookmark(
    alias: str,
    display_name: str,
    target_page: str = "",
    hidden_visuals: str = "",
    report_filter_json: str = "",
) -> str:
    """Create a report bookmark that captures page and visual state.

    Args:
        alias: The alias of the open file
        display_name: Name for the bookmark (e.g. "Sales Overview", "Q4 Filter")
        target_page: Optional page displayName or index to navigate to when bookmark is applied.
                     If empty, bookmark targets the first page.
        hidden_visuals: Optional comma-separated list of visual names to hide when
                        bookmark is applied (e.g. "visual_0,visual_2"). Other visuals
                        stay visible.
        report_filter_json: Optional JSON array of report-level filters to apply
                            when bookmark is activated, e.g.
                            '[{"target":{"table":"Sales","column":"Region"},"operator":"In","values":["West"]}]'
    """
    import uuid as _uuid

    try:
        info = _ensure_open(alias)
        layout = _get_layout(info["work_dir"])
        if not layout:
            raise LayoutParseError("No layout found")

        sections = layout.get("sections", [])
        if not sections:
            raise LayoutParseError("Report has no pages")

        # Resolve target page
        target_section = None
        if target_page:
            # Try numeric index first
            try:
                idx = int(target_page)
                if 0 <= idx < len(sections):
                    target_section = sections[idx]
            except ValueError:
                pass
            # Try display name match
            if not target_section:
                for sec in sections:
                    if sec.get("displayName", "").lower() == target_page.lower():
                        target_section = sec
                        break
            if not target_section:
                raise LayoutParseError(
                    f"Page '{target_page}' not found. "
                    f"Available: {[s.get('displayName') for s in sections]}"
                )
        else:
            target_section = sections[0]

        section_name = target_section.get("name", "ReportSection1")

        # Build visual state — all visuals visible unless in hidden list
        hidden_set = set()
        if hidden_visuals:
            hidden_set = {v.strip() for v in hidden_visuals.split(",") if v.strip()}

        visual_states = {}
        for vc in target_section.get("visualContainers", []):
            vc_config_str = vc.get("config", "{}")
            try:
                vc_config = json.loads(vc_config_str) if isinstance(vc_config_str, str) else vc_config_str
            except json.JSONDecodeError:
                continue
            vname = vc_config.get("name", "")
            if vname:
                visual_states[vname] = {
                    "visualType": vc_config.get("singleVisual", {}).get("visualType", "unknown"),
                    "display": {"mode": "hidden" if vname in hidden_set else "visible"},
                }

        # Build bookmark object
        bookmark_id = str(_uuid.uuid4()).replace("-", "")[:20]
        bookmark = {
            "displayName": display_name,
            "name": f"Bookmark{bookmark_id}",
            "explorationState": {
                "version": "1.2",
                "activeSection": section_name,
                "filters": {
                    "byExpr": [],
                    "byColumn": [],
                },
            },
            "options": {
                "targetVisualNames": list(visual_states.keys()) if visual_states else [],
            },
        }

        # Add visual display states if any visuals hidden
        if hidden_set:
            bookmark["explorationState"]["sections"] = {
                section_name: {
                    "visualContainers": {
                        vname: {"singleVisual": {"display": state["display"]}}
                        for vname, state in visual_states.items()
                    }
                }
            }

        # Add report-level filters if provided
        if report_filter_json:
            try:
                filters = json.loads(report_filter_json)
                bookmark["explorationState"]["filters"]["byExpr"] = filters
            except json.JSONDecodeError:
                raise LayoutParseError("Invalid report_filter_json — must be valid JSON array")

        # Insert into layout config
        config_str = layout.get("config", "{}")
        if isinstance(config_str, str):
            try:
                config = json.loads(config_str)
            except json.JSONDecodeError:
                config = {}
        else:
            config = config_str

        config.setdefault("bookmarks", []).append(bookmark)
        layout["config"] = json.dumps(config, ensure_ascii=False)

        _set_layout(info["work_dir"], layout)
        info["modified"] = True

        hidden_msg = f", hiding: {hidden_visuals}" if hidden_visuals else ""
        return ToolResponse.ok(
            f"Created bookmark '{display_name}' → page '{target_section.get('displayName')}'"
            f"{hidden_msg}. Total bookmarks: {len(config['bookmarks'])}"
        ).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        raise LayoutParseError(str(e))


@mcp.tool()
def pbix_remove_bookmark(alias: str, bookmark_index: int) -> str:
    """Remove a bookmark by index.

    Args:
        alias: The alias of the open file
        bookmark_index: Zero-based index of the bookmark to remove (from pbix_get_bookmarks)
    """
    try:
        info = _ensure_open(alias)
        layout = _get_layout(info["work_dir"])
        if not layout:
            raise LayoutParseError("No layout found")

        config_str = layout.get("config", "{}")
        if isinstance(config_str, str):
            try:
                config = json.loads(config_str)
            except json.JSONDecodeError:
                config = {}
        else:
            config = config_str

        bookmarks = config.get("bookmarks", [])
        if not bookmarks:
            return ToolResponse.ok("No bookmarks to remove.").to_text()

        if bookmark_index < 0 or bookmark_index >= len(bookmarks):
            raise LayoutParseError(
                f"Index {bookmark_index} out of range (0–{len(bookmarks) - 1})"
            )

        removed = bookmarks.pop(bookmark_index)
        name = removed.get("displayName", removed.get("name", "?"))
        config["bookmarks"] = bookmarks
        layout["config"] = json.dumps(config, ensure_ascii=False)

        _set_layout(info["work_dir"], layout)
        info["modified"] = True
        return ToolResponse.ok(
            f"Removed bookmark '{name}'. Remaining: {len(bookmarks)}"
        ).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        raise LayoutParseError(str(e))


@mcp.tool()
def pbix_get_metadata(alias: str) -> str:
    """Get file metadata — component inventory and sizes.

    Args:
        alias: The alias of the open file
    """
    try:
        info = _ensure_open(alias)
        work_dir = info["work_dir"]
        lines = [f"Metadata for '{alias}' ({info['path']}):\n"]
        total = 0
        for root, dirs, files in os.walk(work_dir):
            for f in files:
                fp = os.path.join(root, f)
                rel = os.path.relpath(fp, work_dir)
                size = os.path.getsize(fp)
                total += size
                lines.append(f"  {rel}: {size:,} bytes")
        lines.append(f"\nTotal: {total:,} bytes")
        return ToolResponse.ok("\n".join(lines)).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


# ---- Section 5: Resources & Theme tools ----

@mcp.tool()
def pbix_list_resources(alias: str) -> str:
    """List all static resources (images, custom visuals, themes).

    Args:
        alias: The alias of the open file
    """
    try:
        info = _ensure_open(alias)
        work_dir = info["work_dir"]
        resource_dirs = [
            "Report/StaticResources",
            "Report/CustomVisuals",
        ]
        lines = ["Resources:\n"]
        found = False
        for rd in resource_dirs:
            rd_full = os.path.join(work_dir, rd)
            if os.path.isdir(rd_full):
                for root, dirs, files in os.walk(rd_full):
                    for f in files:
                        fp = os.path.join(root, f)
                        rel = os.path.relpath(fp, work_dir)
                        size = os.path.getsize(fp)
                        lines.append(f"  {rel} ({size:,} bytes)")
                        found = True
        if not found:
            return ToolResponse.ok("No resources found.").to_text()
        return ToolResponse.ok("\n".join(lines)).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_add_custom_visual(alias: str, pbiviz_path: str) -> str:
    """Import a custom visual (.pbiviz) into the report.

    Extracts the .pbiviz package, embeds the visual files into the PBIX,
    and registers it in the layout's resourcePackages. After importing,
    use pbix_add_visual with the returned visual_type to place it on a page.

    Args:
        alias: The alias of the open file
        pbiviz_path: Absolute path to the .pbiviz file
    """
    import shutil
    import zipfile as _zf

    try:
        info = _ensure_open(alias)
        work_dir = info["work_dir"]

        if not os.path.exists(pbiviz_path):
            raise LayoutParseError(f"File not found: {pbiviz_path}")

        # .pbiviz is a ZIP — extract and parse manifest
        if not _zf.is_zipfile(pbiviz_path):
            raise LayoutParseError("Not a valid .pbiviz file (not a ZIP archive)")

        with _zf.ZipFile(pbiviz_path, "r") as zf:
            names = zf.namelist()

            # Find pbiviz.json or package.json for metadata
            manifest = None
            manifest_name = None
            for candidate in ["pbiviz.json", "package.json"]:
                if candidate in names:
                    manifest_name = candidate
                    raw = zf.read(candidate)
                    manifest = json.loads(raw)
                    break

            if not manifest and manifest_name != "package.json":
                # Try to find it in a subdirectory
                for n in names:
                    if n.endswith("pbiviz.json"):
                        manifest_name = n
                        raw = zf.read(n)
                        manifest = json.loads(raw)
                        break

            if not manifest:
                raise LayoutParseError(
                    "No pbiviz.json or package.json found in .pbiviz file. "
                    f"Contents: {names[:10]}"
                )

            # Extract visual metadata
            if manifest_name and "pbiviz" in manifest_name:
                visual_info = manifest.get("visual", {})
                visual_guid = visual_info.get("guid", "")
                visual_name = visual_info.get("name", "CustomVisual")
                display_name = visual_info.get("displayName", visual_name)
                version = visual_info.get("version", "1.0.0.0")
                api_version = manifest.get("apiVersion", "2.6.0")
            else:
                # package.json fallback
                visual_name = manifest.get("name", "CustomVisual")
                display_name = manifest.get("displayName", visual_name)
                visual_guid = manifest.get("guid", "")
                version = manifest.get("version", "1.0.0.0")
                api_version = manifest.get("apiVersion", "2.6.0")

            if not visual_guid:
                # Generate a GUID if not present
                import uuid as _uuid
                visual_guid = visual_name + _uuid.uuid4().hex[:32].upper()

            # Create CustomVisuals directory in the PBIX work dir
            cv_dir = os.path.join(work_dir, "Report", "CustomVisuals", visual_name)
            os.makedirs(cv_dir, exist_ok=True)

            # Extract all files into the custom visual directory
            resource_files = []
            for name in names:
                # Skip directories and manifest files at root
                if name.endswith("/"):
                    continue

                # Determine target path inside cv_dir
                # .pbiviz files may have files at root or in resources/
                target = os.path.join(cv_dir, name)
                os.makedirs(os.path.dirname(target), exist_ok=True)

                with zf.open(name) as src:
                    with open(target, "wb") as dst:
                        shutil.copyfileobj(src, dst)

                resource_files.append(name)

        # Find the main JS file for registration
        main_js = None
        for rf in resource_files:
            if rf.endswith(".js") and ("visual" in rf.lower() or "prod" in rf.lower()):
                main_js = rf
                break
        if not main_js:
            # Fallback: first JS file
            for rf in resource_files:
                if rf.endswith(".js"):
                    main_js = rf
                    break

        # Register in layout's resourcePackages
        layout = _get_layout(work_dir)
        if not layout:
            raise LayoutParseError("No layout found")

        # Parse existing resourcePackages
        resource_packages = layout.get("resourcePackages", [])

        # Build resource items list
        items = []
        for rf in resource_files:
            # Determine type code
            if rf.endswith(".js"):
                item_type = 5  # JavaScript
            elif rf.endswith(".css"):
                item_type = 6  # CSS
            elif rf.endswith(".png") or rf.endswith(".svg") or rf.endswith(".jpg"):
                item_type = 3  # Image
            elif rf.endswith(".json"):
                item_type = 4  # JSON config
            else:
                item_type = 0  # Other

            items.append({
                "type": item_type,
                "path": f"{visual_name}/{rf}",
                "name": os.path.splitext(os.path.basename(rf))[0],
            })

        # Check if this visual is already registered
        existing_idx = None
        for i, rp in enumerate(resource_packages):
            pkg = rp.get("resourcePackage", rp)
            if pkg.get("name") == visual_name:
                existing_idx = i
                break

        new_package = {
            "resourcePackage": {
                "name": visual_name,
                "type": 7,  # Custom visual type
                "items": items,
                "disabled": False,
            }
        }

        if existing_idx is not None:
            resource_packages[existing_idx] = new_package
        else:
            resource_packages.append(new_package)

        layout["resourcePackages"] = resource_packages
        _set_layout(work_dir, layout)
        info["modified"] = True

        # The visual type used in pbix_add_visual
        visual_type = visual_guid

        return ToolResponse.ok(
            f"Custom visual '{display_name}' imported successfully!\n"
            f"  GUID: {visual_guid}\n"
            f"  Version: {version}\n"
            f"  Files: {len(resource_files)} extracted to Report/CustomVisuals/{visual_name}/\n"
            f"  Main JS: {main_js or 'N/A'}\n\n"
            f"To place on a page, use:\n"
            f"  pbix_add_visual(alias, page_index, visual_type=\"{visual_type}\", ...)"
        ).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        raise LayoutParseError(str(e))


@mcp.tool()
def pbix_remove_custom_visual(alias: str, visual_name: str) -> str:
    """Remove a custom visual package from the report.

    Args:
        alias: The alias of the open file
        visual_name: Name of the custom visual (from pbix_list_resources)
    """
    import shutil

    try:
        info = _ensure_open(alias)
        work_dir = info["work_dir"]

        # Remove files
        cv_dir = os.path.join(work_dir, "Report", "CustomVisuals", visual_name)
        if os.path.isdir(cv_dir):
            shutil.rmtree(cv_dir)

        # Remove from resourcePackages
        layout = _get_layout(work_dir)
        if layout:
            resource_packages = layout.get("resourcePackages", [])
            layout["resourcePackages"] = [
                rp for rp in resource_packages
                if rp.get("resourcePackage", rp).get("name") != visual_name
            ]
            _set_layout(work_dir, layout)

        info["modified"] = True
        return ToolResponse.ok(
            f"Custom visual '{visual_name}' removed from report."
        ).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        raise LayoutParseError(str(e))


@mcp.tool()
def pbix_get_theme(alias: str) -> str:
    """Get the current report theme JSON.

    Args:
        alias: The alias of the open file
    """
    try:
        info = _ensure_open(alias)
        work_dir = info["work_dir"]
        theme_dir = os.path.join(work_dir, "Report", "StaticResources", "SharedResources", "BaseThemes")
        if not os.path.isdir(theme_dir):
            return ToolResponse.ok("No theme directory found.").to_text()

        themes = []
        for f in sorted(os.listdir(theme_dir)):
            if f.endswith(".json"):
                fp = os.path.join(theme_dir, f)
                with open(fp, "r", encoding="utf-8") as fh:
                    theme = json.load(fh)
                themes.append(f"Theme file: {f}\n{json.dumps(theme, indent=2, ensure_ascii=False)}")
        if not themes:
            return ToolResponse.ok("No theme JSON files found.").to_text()
        return ToolResponse.ok("\n\n".join(themes)).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_set_theme(alias: str, theme_json: str, filename: str = "CY24SU11.json") -> str:
    """Set the report theme JSON.

    Args:
        alias: The alias of the open file
        theme_json: Complete theme JSON string
        filename: Theme filename (default: CY24SU11.json)
    """
    try:
        info = _ensure_open(alias)
        work_dir = info["work_dir"]
        theme_dir = os.path.join(work_dir, "Report", "StaticResources", "SharedResources", "BaseThemes")
        os.makedirs(theme_dir, exist_ok=True)

        try:
            theme = json.loads(theme_json)
        except json.JSONDecodeError as e:
            raise LayoutParseError(f"Invalid JSON: {e}")

        fp = os.path.join(theme_dir, filename)
        with open(fp, "w", encoding="utf-8") as fh:
            json.dump(theme, fh, indent=2, ensure_ascii=False)
        info["modified"] = True
        return ToolResponse.ok(f"Theme saved to {filename}").to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_get_linguistic_schema(alias: str) -> str:
    """Get the Q&A linguistic schema XML.

    Args:
        alias: The alias of the open file
    """
    try:
        info = _ensure_open(alias)
        work_dir = info["work_dir"]
        ls_path = os.path.join(work_dir, "Report", "LinguisticSchema")
        if not os.path.exists(ls_path):
            return ToolResponse.ok("No linguistic schema found.").to_text()
        enc = _detect_encoding(ls_path)
        with open(ls_path, "r", encoding=enc) as f:
            return ToolResponse.ok(f.read()).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_set_linguistic_schema(alias: str, schema_xml: str) -> str:
    """Set (replace) the Q&A linguistic schema XML.

    Args:
        alias: The alias of the open file
        schema_xml: The new linguistic schema XML content
    """
    try:
        info = _ensure_open(alias)
        work_dir = info["work_dir"]
        ls_path = os.path.join(work_dir, "Report", "LinguisticSchema")
        os.makedirs(os.path.dirname(ls_path), exist_ok=True)
        # Write in UTF-16-LE (Power BI native)
        with open(ls_path, "wb") as f:
            f.write(schema_xml.encode("utf-16-le"))
        info["modified"] = True
        return ToolResponse.ok("Linguistic schema updated.").to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


# ---- Section 6: DataMashup (M Code) tools ----

@mcp.tool()
def pbix_get_m_code(alias: str) -> str:
    """Get the Power Query M code from the DataMashup.

    Args:
        alias: The alias of the open file
    """
    try:
        info = _ensure_open(alias)
        m_code = _read_datamashup_m_code(info["work_dir"])
        if m_code is None:
            return ToolResponse.ok("No DataMashup found in this file.").to_text()
        return ToolResponse.ok(m_code).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_set_m_code(alias: str, m_code: str) -> str:
    """Set the Power Query M code in the DataMashup.

    Args:
        alias: The alias of the open file
        m_code: New M code to write into the DataMashup
    """
    try:
        info = _ensure_open(alias)
        ok = _write_datamashup_m_code(info["work_dir"], m_code)
        if not ok:
            return ToolResponse.error("Failed to write M code. DataMashup may not exist or be corrupt.", PBIXMCPError.code).to_text()
        info["modified"] = True
        return ToolResponse.ok("M code updated in DataMashup.").to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


# ---- Section 7: DataModel READ tools (native ABF/VertiPaq) ----

@mcp.tool()
def pbix_get_model_schema(alias: str) -> str:
    """Get the data model schema — all tables, columns, and data types.

    Args:
        alias: The alias of the open file
    """
    try:
        info = _ensure_open(alias)
        from pbix_mcp.formats.model_reader import ModelReader, format_schema_table
        model = ModelReader(info["path"], work_dir=info.get("work_dir"))
        schema = model.schema
        return ToolResponse.ok(format_schema_table(schema)).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), DataModelCompressionError.code).to_text()


@mcp.tool()
def pbix_get_model_measures(alias: str) -> str:
    """Get all DAX measures from the data model.

    Args:
        alias: The alias of the open file
    """
    try:
        info = _ensure_open(alias)
        from pbix_mcp.formats.model_reader import ModelReader, format_measures_table
        model = ModelReader(info["path"], work_dir=info.get("work_dir"))
        measures = model.dax_measures
        return ToolResponse.ok(format_measures_table(measures)).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), DataModelCompressionError.code).to_text()


@mcp.tool()
def pbix_get_model_relationships(alias: str) -> str:
    """Get all relationships in the data model.

    Args:
        alias: The alias of the open file
    """
    try:
        info = _ensure_open(alias)
        from pbix_mcp.formats.model_reader import ModelReader, format_relationships_table
        model = ModelReader(info["path"], work_dir=info.get("work_dir"))
        rels = model.relationships
        return ToolResponse.ok(format_relationships_table(rels)).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), DataModelCompressionError.code).to_text()


@mcp.tool()
def pbix_get_model_power_query(alias: str) -> str:
    """Get Power Query expressions from the model.

    This reads M expressions as stored in the DataModel itself
    (different from the DataMashup M code).

    Args:
        alias: The alias of the open file
    """
    try:
        info = _ensure_open(alias)
        from pbix_mcp.formats.model_reader import ModelReader, format_power_query_table
        model = ModelReader(info["path"], work_dir=info.get("work_dir"))
        pq = model.power_query
        return ToolResponse.ok(format_power_query_table(pq)).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), DataModelCompressionError.code).to_text()


@mcp.tool()
def pbix_list_data_sources(alias: str) -> str:
    """List all data sources with connection details for each table.

    Parses M expressions from Partition.QueryDefinition to extract
    connection type, server, database, table, mode, and file paths.

    Args:
        alias: The alias of the open file
    """
    try:
        info = _ensure_open(alias)
        from pbix_mcp.formats.model_reader import ModelReader
        model = ModelReader(info["path"], work_dir=info.get("work_dir"))
        pq = model.power_query

        import re
        mode_names = {0: "Import", 1: "DirectQuery"}
        lines = []
        for entry in pq:
            tname = entry.get("TableName", "")
            expr = entry.get("Expression", "")
            if not expr:
                continue

            # Parse connection type and parameters from M expression
            source_type = "Embedded"
            details = {}

            if "Sql.Database(" in expr:
                source_type = "SQL Server"
                m = re.search(r'Sql\.Database\("([^"]*)",\s*"([^"]*)"', expr)
                if m:
                    details["server"] = m.group(1)
                    details["database"] = m.group(2)
                m2 = re.search(r'Schema="([^"]*)".*?Item="([^"]*)"', expr)
                if m2:
                    details["schema"] = m2.group(1)
                    details["table"] = m2.group(2)
            elif "PostgreSQL.Database(" in expr:
                source_type = "PostgreSQL"
                m = re.search(r'PostgreSQL\.Database\("([^"]*)",\s*"([^"]*)"', expr)
                if m:
                    details["server"] = m.group(1)
                    details["database"] = m.group(2)
                m2 = re.search(r'Schema="([^"]*)".*?Item="([^"]*)"', expr)
                if m2:
                    details["schema"] = m2.group(1)
                    details["table"] = m2.group(2)
            elif "MySQL.Database(" in expr:
                source_type = "MySQL"
                m = re.search(r'MySQL\.Database\("([^"]*)",\s*"([^"]*)"', expr)
                if m:
                    details["server"] = m.group(1)
                    details["database"] = m.group(2)
                m2 = re.search(r'Schema="([^"]*)".*?Item="([^"]*)"', expr)
                if m2:
                    details["schema"] = m2.group(1)
                    details["table"] = m2.group(2)
            elif "MariaDB.Contents(" in expr:
                source_type = "MariaDB"
                m = re.search(r'MariaDB\.Contents\("([^"]*)",\s*"([^"]*)"', expr)
                if m:
                    details["server"] = m.group(1)
                    details["database"] = m.group(2)
            elif "Odbc.DataSource(" in expr and "SQLite" in expr:
                source_type = "SQLite"
                m = re.search(r'Database=([^;"\}]+)', expr)
                if m:
                    details["path"] = m.group(1)
            elif "Csv.Document(" in expr:
                source_type = "CSV"
                m = re.search(r'File\.Contents\("([^"]*)"', expr)
                if m:
                    details["path"] = m.group(1)
            elif "Excel.Workbook(" in expr:
                source_type = "Excel"
                m = re.search(r'File\.Contents\("([^"]*)"', expr)
                if m:
                    details["path"] = m.group(1)
                m2 = re.search(r'Item="([^"]*)"', expr)
                if m2:
                    details["sheet"] = m2.group(1)
            elif "Json.Document(" in expr or "Web.Contents(" in expr:
                source_type = "JSON/Web"
                m = re.search(r'Web\.Contents\("([^"]*)"', expr)
                if m:
                    details["url"] = m.group(1)
            elif "#table(" in expr:
                source_type = "Embedded"

            # Get mode from metadata
            mode_str = "Import"
            try:
                mode_rows = model._query_metadata(
                    "SELECT p.Mode FROM Partition p JOIN [Table] t ON p.TableID = t.ID "
                    "WHERE t.Name = ? AND t.ModelID = 1 "
                    "AND t.Name NOT LIKE 'H$%' AND t.Name NOT LIKE 'R$%'",
                    (tname,)
                )
                if mode_rows:
                    mode_str = mode_names.get(mode_rows[0].get("Mode", 0), "Import")
            except Exception:
                pass

            detail_str = ", ".join(f"{k}={v}" for k, v in details.items())
            lines.append(f"  {tname}: {source_type} ({mode_str}){' — ' + detail_str if detail_str else ''}")

        if not lines:
            return ToolResponse.ok("No data sources found.").to_text()
        return ToolResponse.ok(f"Data sources ({len(lines)} tables):\n\n" + "\n".join(lines)).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_update_data_source(
    alias: str, table_name: str, new_source_json: str
) -> str:
    """Update a table's data source connection without full DataModel rebuild.

    Changes the M expression (Partition.QueryDefinition) and optionally the
    mode (Import/DirectQuery). This is a lightweight metadata-only operation
    that does NOT regenerate VertiPaq data.

    Args:
        alias: The alias of the open file
        table_name: Table to update
        new_source_json: JSON with new connection parameters. Examples:
            '{"server": "new-server.example.com", "database": "prod_db"}'
            '{"type": "postgresql", "server": "pg.local", "port": 5432, "database": "analytics", "table": "orders"}'
            '{"type": "csv", "path": "C:/data/sales.csv"}'
            '{"mode": "directquery"}'
            Supported types: sqlserver, postgresql, mysql, mariadb, sqlite, csv, excel, json, azuresql
    """
    try:
        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")
        if not os.path.exists(dm_path):
            return ToolResponse.error("No DataModel found.", DataModelCompressionError.code).to_text()

        new_source = json.loads(new_source_json)
        from pbix_mcp.builder import _build_m_expression

        def _do_update(conn: sqlite3.Connection):
            conn.row_factory = sqlite3.Row

            # Find the partition for this table
            row = conn.execute(
                "SELECT p.ID, p.QueryDefinition, p.Mode, t.ID as TableID "
                "FROM Partition p JOIN [Table] t ON p.TableID = t.ID "
                "WHERE t.Name = ? AND t.ModelID = 1 "
                "AND t.Name NOT LIKE 'H$%' AND t.Name NOT LIKE 'R$%'",
                (table_name,)
            ).fetchone()
            if not row:
                raise ValueError(f"Table '{table_name}' not found")

            part_id = row["ID"]
            current_mode = row["Mode"] or 0

            # Read column definitions for M expression generation
            cols = [{"name": c["ExplicitName"],
                     "data_type": {2: "String", 6: "Int64", 8: "Double",
                                   9: "DateTime", 10: "Decimal", 11: "Boolean"
                                   }.get(c["ExplicitDataType"], "String")}
                    for c in conn.execute(
                        "SELECT ExplicitName, ExplicitDataType FROM [Column] "
                        "WHERE TableID = ? AND Type = 1 ORDER BY ID",
                        (row["TableID"],)
                    )]

            # Determine new mode
            new_mode = current_mode
            if "mode" in new_source:
                new_mode = 1 if new_source["mode"] == "directquery" else 0

            # Build source_db dict for M expression generator
            source_db = None
            source_csv = None
            is_dq = new_mode == 1

            src_type = new_source.get("type", "").lower()
            if src_type in ("sqlserver", "azuresql", "azure"):
                source_db = {
                    "type": src_type if src_type != "azure" else "azuresql",
                    "server": new_source.get("server", "localhost"),
                    "database": new_source.get("database", ""),
                    "table": new_source.get("table", table_name),
                    "schema": new_source.get("schema", "dbo"),
                }
            elif src_type == "postgresql":
                source_db = {
                    "type": "postgresql",
                    "server": new_source.get("server", "localhost"),
                    "port": new_source.get("port", 5432),
                    "database": new_source.get("database", ""),
                    "table": new_source.get("table", table_name),
                    "schema": new_source.get("schema", "public"),
                }
            elif src_type == "mysql":
                source_db = {
                    "type": "mysql",
                    "server": new_source.get("server", "localhost"),
                    "port": new_source.get("port", 3306),
                    "database": new_source.get("database", ""),
                    "table": new_source.get("table", table_name),
                }
            elif src_type == "mariadb":
                source_db = {
                    "type": "mariadb",
                    "server": new_source.get("server", "localhost"),
                    "port": new_source.get("port", 3306),
                    "database": new_source.get("database", ""),
                    "table": new_source.get("table", table_name),
                }
            elif src_type == "sqlite":
                source_db = {
                    "type": "sqlite",
                    "path": new_source.get("path", ""),
                    "table": new_source.get("table", table_name),
                }
            elif src_type == "csv":
                source_csv = new_source.get("path", "")
            elif src_type == "excel":
                source_db = {
                    "type": "excel",
                    "path": new_source.get("path", ""),
                    "sheet": new_source.get("sheet", "Sheet1"),
                }
            elif src_type in ("json", "web", "api"):
                source_db = {
                    "type": "json",
                    "url": new_source.get("url", ""),
                }
            elif not src_type and ("server" in new_source or "database" in new_source):
                # Partial update — rewrite with same type, infer from current M expression
                current_qd = row["QueryDefinition"] or ""
                if "Sql.Database(" in current_qd:
                    source_db = {"type": "sqlserver"}
                elif "PostgreSQL.Database(" in current_qd:
                    source_db = {"type": "postgresql", "port": 5432, "schema": "public"}
                elif "MySQL.Database(" in current_qd:
                    source_db = {"type": "mysql", "port": 3306}
                else:
                    source_db = {"type": "sqlserver"}
                # Merge new params
                for k, v in new_source.items():
                    if k != "mode":
                        source_db[k] = v
                if "table" not in source_db:
                    source_db["table"] = table_name

            if source_db or source_csv:
                new_m = _build_m_expression(
                    table_name, cols,
                    source_csv=source_csv,
                    source_db=source_db,
                    is_directquery=is_dq,
                )
                conn.execute(
                    "UPDATE Partition SET QueryDefinition = ?, Mode = ? WHERE ID = ?",
                    (new_m, new_mode, part_id),
                )
            elif "mode" in new_source:
                # Mode-only change
                conn.execute(
                    "UPDATE Partition SET Mode = ? WHERE ID = ?",
                    (new_mode, part_id),
                )
            else:
                raise ValueError("No recognized connection parameters in new_source_json")

            conn.commit()

        old_size, new_size = _modify_metadata_only(dm_path, _do_update)
        info["modified"] = True

        src_type = new_source.get("type", "connection")
        return ToolResponse.ok(
            f"Data source updated for '{table_name}': {src_type}\n"
            f"  DataModel: {old_size:,} → {new_size:,} bytes (lightweight, no rebuild)"
        ).to_text()
    except json.JSONDecodeError as e:
        return ToolResponse.error(f"Invalid JSON: {e}", "INVALID_INPUT").to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_get_model_columns(alias: str) -> str:
    """Get all DAX calculated columns from the model.

    Args:
        alias: The alias of the open file
    """
    try:
        info = _ensure_open(alias)
        from pbix_mcp.formats.model_reader import ModelReader, format_dax_columns_table
        model = ModelReader(info["path"], work_dir=info.get("work_dir"))
        cols = model.dax_columns
        return ToolResponse.ok(format_dax_columns_table(cols)).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), DataModelCompressionError.code).to_text()


@mcp.tool()
def pbix_get_table_data(alias: str, table_name: str, max_rows: int = 50) -> str:
    """Get sample data from a table in the data model.

    Args:
        alias: The alias of the open file
        table_name: Name of the table to query
        max_rows: Maximum rows to return (default 50)
    """
    try:
        info = _ensure_open(alias)
        if info.get("is_directquery"):
            return ToolResponse.error(
                "This file uses DirectQuery — table data is not stored locally. "
                "Use layout, measure, and metadata tools instead.",
                UnsupportedFormatError.code,
            ).to_text()
        from pbix_mcp.formats.model_reader import ModelReader, format_table_data
        model = ModelReader(info["path"], work_dir=info.get("work_dir"))
        table_data = model.get_table(table_name, max_rows=max_rows)
        if not table_data["columns"] or not table_data["rows"]:
            return ToolResponse.ok(f"No data found in table '{table_name}'.").to_text()
        return ToolResponse.ok(format_table_data(table_data, max_rows=max_rows)).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), DataModelCompressionError.code).to_text()


def _rebuild_datamodel(
    info: dict,
    table_updates: dict[str, dict] | None = None,
    extra_tables: list[dict] | None = None,
    extra_measures: list[dict] | None = None,
    extra_relationships: list[dict] | None = None,
    remove_tables: set[str] | None = None,
    remove_relationships: list[tuple[str, str, str, str]] | None = None,
) -> tuple[int, int]:
    """Rebuild the entire DataModel using the builder pipeline.

    Reads all existing tables, measures, relationships, and row data.
    Applies updates/additions/removals, then regenerates the DataModel from scratch.

    Args:
        info: Open file info dict from _ensure_open()
        table_updates: {table_name: {"columns": [...], "rows": [...]}} to replace
        extra_tables: New tables to add: [{"name", "columns", "rows"}, ...]
        extra_measures: New measures: [{"table", "name", "expression", "format_string"}, ...]
        extra_relationships: New rels: [{"from_table", "from_column", "to_table", "to_column"}, ...]
        remove_tables: Set of table names to exclude from rebuild
        remove_relationships: List of (from_table, from_col, to_table, to_col) to exclude

    Returns (old_dm_size, new_dm_size).
    """
    from pbix_mcp.builder import PBIXBuilder
    from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
    from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel
    from pbix_mcp.formats.vertipaq_decoder import read_table_from_abf

    table_updates = table_updates or {}
    extra_tables = extra_tables or []
    extra_measures = extra_measures or []
    extra_relationships = extra_relationships or []
    remove_tables = remove_tables or set()
    remove_relationships = remove_relationships or []

    dm_path = os.path.join(info["work_dir"], "DataModel")
    with open(dm_path, "rb") as f:
        dm_bytes = f.read()

    abf = decompress_datamodel(dm_bytes)
    meta_bytes = read_metadata_sqlite(abf)

    # Read structure from metadata
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.write(meta_bytes)
    tmp.close()
    try:
        conn = sqlite3.connect(tmp.name)
        conn.row_factory = sqlite3.Row

        _AMO_TO_TYPE = {2: "String", 6: "Int64", 8: "Double", 9: "DateTime",
                        10: "Decimal", 11: "Boolean"}

        # Get all existing user tables
        tables = []
        for trow in conn.execute(
            "SELECT ID, Name FROM [Table] WHERE ModelID = 1 "
            "AND Name NOT LIKE 'H$%' AND Name NOT LIKE 'R$%' ORDER BY ID"
        ):
            tid, tname = trow["ID"], trow["Name"]
            cols = []
            for crow in conn.execute(
                "SELECT ExplicitName, ExplicitDataType FROM [Column] "
                "WHERE TableID = ? AND Type = 1 ORDER BY ID", (tid,)
            ):
                dt = _AMO_TO_TYPE.get(crow["ExplicitDataType"], "String")
                cols.append({"name": crow["ExplicitName"], "data_type": dt})
            tables.append({"name": tname, "columns": cols})

        # Get existing measures
        measures = []
        for mrow in conn.execute(
            "SELECT t.Name as tbl, m.Name, m.Expression, m.FormatString "
            "FROM Measure m JOIN [Table] t ON m.TableID = t.ID"
        ):
            measures.append({
                "table": mrow["tbl"], "name": mrow["Name"],
                "expression": mrow["Expression"],
                "format_string": mrow["FormatString"] or "",
            })

        # Get existing relationships
        rels = []
        for rrow in conn.execute(
            "SELECT ft.Name as ft, fc.ExplicitName as fc, "
            "tt.Name as tt, tc.ExplicitName as tc "
            "FROM Relationship r "
            "JOIN [Table] ft ON r.FromTableID = ft.ID "
            "JOIN [Column] fc ON r.FromColumnID = fc.ID "
            "JOIN [Table] tt ON r.ToTableID = tt.ID "
            "JOIN [Column] tc ON r.ToColumnID = tc.ID"
        ):
            rels.append({
                "from_table": rrow["ft"], "from_column": rrow["fc"],
                "to_table": rrow["tt"], "to_column": rrow["tc"],
            })

        conn.close()
    finally:
        os.unlink(tmp.name)

    # Build new DataModel via builder
    builder = PBIXBuilder()

    # Add existing tables (with optional row updates), skip removed tables
    for tinfo in tables:
        tname = tinfo["name"]
        if tname in remove_tables:
            continue
        if tname in table_updates:
            upd = table_updates[tname]
            builder.add_table(tname, upd["columns"], rows=upd["rows"])
        else:
            # Read existing row data from VertiPaq
            try:
                td = read_table_from_abf(abf, tname, meta_bytes)
                existing_rows = [
                    dict(zip(td["columns"], row_vals))
                    for row_vals in td.get("rows", [])
                ]
                builder.add_table(tname, tinfo["columns"], rows=existing_rows)
            except Exception:
                builder.add_table(tname, tinfo["columns"], rows=[])

    # Add new tables
    for et in extra_tables:
        builder.add_table(et["name"], et["columns"], rows=et.get("rows", []))

    # Add all measures (existing + new), skip measures on removed tables
    for m in measures:
        if m["table"] not in remove_tables:
            builder.add_measure(m["table"], m["name"], m["expression"], m["format_string"])
    for m in extra_measures:
        builder.add_measure(m["table"], m["name"], m["expression"],
                            m.get("format_string", ""))

    # Add all relationships (existing + new), skip removed ones and those referencing removed tables
    remove_rel_set = {(r[0], r[1], r[2], r[3]) for r in remove_relationships}
    for r in rels:
        key = (r["from_table"], r["from_column"], r["to_table"], r["to_column"])
        if key in remove_rel_set:
            continue
        if r["from_table"] in remove_tables or r["to_table"] in remove_tables:
            continue
        builder.add_relationship(
            r["from_table"], r["from_column"], r["to_table"], r["to_column"]
        )
    for r in extra_relationships:
        builder.add_relationship(
            r["from_table"], r["from_column"], r["to_table"], r["to_column"]
        )

    new_pbix = builder.build()

    # Extract new DataModel from builder output
    import io
    import zipfile
    new_z = zipfile.ZipFile(io.BytesIO(new_pbix))
    new_dm = new_z.read("DataModel")

    # Write new DataModel
    with open(dm_path, "wb") as f:
        f.write(new_dm)

    return len(dm_bytes), len(new_dm)


@mcp.tool()
def pbix_set_table_data(alias: str, table_name: str, data_json: str) -> str:
    """Write/replace actual row data in a table in the DataModel (VertiPaq).

    This encodes the data into VertiPaq column format (IDF + IDFMETA +
    dictionary + HIDX) and rebuilds the ABF with the new column files.
    The DataModel is then XPress9 recompressed.

    Args:
        alias: The alias of the open file
        table_name: Name of the table to write data to
        data_json: JSON object with 'columns' and 'rows':
            {
              "columns": [
                {"name": "Col1", "data_type": "String", "nullable": true},
                {"name": "Col2", "data_type": "Int64", "nullable": false}
              ],
              "rows": [
                {"Col1": "hello", "Col2": 42},
                {"Col1": "world", "Col2": 99}
              ]
            }
            Supported data_types: String, Int64, Float64, DateTime, Decimal, Boolean
    """
    try:
        info = _ensure_open(alias)
        data = json.loads(data_json)
        columns = data.get("columns", [])
        rows = data.get("rows", [])
        if not columns or not rows:
            return ToolResponse.error("'columns' and 'rows' are required and must not be empty.", ABFRebuildError.code).to_text()

        dm_path = os.path.join(info["work_dir"], "DataModel")
        if not os.path.exists(dm_path):
            return ToolResponse.error("No DataModel found.", DataModelCompressionError.code).to_text()

        # Check if table exists — update existing or add new
        from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
        from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel
        with open(dm_path, "rb") as f:
            dm_check = f.read()
        abf_check = decompress_datamodel(dm_check)
        meta_check = read_metadata_sqlite(abf_check)
        tmp_check = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp_check.write(meta_check)
        tmp_check.close()
        try:
            conn_check = sqlite3.connect(tmp_check.name)
            exists = conn_check.execute(
                "SELECT 1 FROM [Table] WHERE Name = ? AND ModelID = 1", (table_name,)
            ).fetchone()
            conn_check.close()
        finally:
            os.unlink(tmp_check.name)

        if exists:
            old_size, new_size = _rebuild_datamodel(
                info,
                table_updates={table_name: {"columns": columns, "rows": rows}},
            )
            action = "updated"
        else:
            old_size, new_size = _rebuild_datamodel(
                info,
                extra_tables=[{"name": table_name, "columns": columns, "rows": rows}],
            )
            action = "created"

        info["modified"] = True
        return ToolResponse.ok(
            f"Table '{table_name}' {action}: {len(rows)} rows, {len(columns)} columns\n"
            f"  DataModel: {old_size:,} → {new_size:,} bytes"
        ).to_text()
    except json.JSONDecodeError as e:
        return ToolResponse.error(f"Invalid JSON: {e}", ABFRebuildError.code).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_update_table_rows(alias: str, table_name: str, rows_json: str) -> str:
    """Update row data in an existing table, inferring column types from current schema.

    Reads the existing column definitions from the DataModel metadata,
    then encodes the new rows into VertiPaq format.

    Args:
        alias: The alias of the open file
        table_name: Name of the existing table
        rows_json: JSON array of row objects, e.g. [{"Col1": "val", "Col2": 42}, ...]
    """
    try:
        info = _ensure_open(alias)
        rows = json.loads(rows_json)
        if not rows:
            return ToolResponse.error("rows must not be empty.", ABFRebuildError.code).to_text()

        dm_path = os.path.join(info["work_dir"], "DataModel")
        if not os.path.exists(dm_path):
            return ToolResponse.error("No DataModel found.", DataModelCompressionError.code).to_text()

        # Read column definitions from existing metadata
        from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
        from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

        with open(dm_path, "rb") as f:
            dm_bytes = f.read()
        abf = decompress_datamodel(dm_bytes)
        meta_bytes = read_metadata_sqlite(abf)

        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.write(meta_bytes)
        tmp.close()
        try:
            conn = sqlite3.connect(tmp.name)
            conn.row_factory = sqlite3.Row
            _AMO_TO_TYPE = {2: "String", 6: "Int64", 8: "Double", 9: "DateTime",
                            10: "Decimal", 11: "Boolean"}
            col_rows = conn.execute(
                """SELECT c.ExplicitName, c.ExplicitDataType
                   FROM [Column] c
                   JOIN [Table] t ON c.TableID = t.ID
                   WHERE t.Name = ? AND c.Type = 1
                   ORDER BY c.ID""",
                (table_name,)
            ).fetchall()
            conn.close()
        finally:
            os.unlink(tmp.name)

        if not col_rows:
            return ToolResponse.error(
                f"Table '{table_name}' not found or has no user columns.",
                "TABLE_NOT_FOUND"
            ).to_text()

        columns = [{"name": cr["ExplicitName"],
                     "data_type": _AMO_TO_TYPE.get(cr["ExplicitDataType"], "String")}
                    for cr in col_rows]

        old_size, new_size = _rebuild_datamodel(
            info,
            table_updates={table_name: {"columns": columns, "rows": rows}},
        )
        info["modified"] = True
        col_names = [c["name"] for c in columns]
        return ToolResponse.ok(
            f"Table '{table_name}' updated: {len(rows)} rows, {len(columns)} columns\n"
            f"  Columns: {', '.join(col_names)}\n"
            f"  DataModel: {old_size:,} → {new_size:,} bytes"
        ).to_text()
    except json.JSONDecodeError as e:
        return ToolResponse.error(f"Invalid JSON: {e}", ABFRebuildError.code).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_list_tables(alias: str) -> str:
    """List all tables in the data model with row/column counts.

    Args:
        alias: The alias of the open file
    """
    try:
        info = _ensure_open(alias)
        from pbix_mcp.formats.model_reader import ModelReader, format_statistics_table
        model = ModelReader(info["path"], work_dir=info.get("work_dir"))
        stats = model.statistics
        return ToolResponse.ok(format_statistics_table(stats)).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), DataModelCompressionError.code).to_text()


# ---- Section 7b: Lightweight metadata-only modification ----

def _modify_metadata_only(
    dm_path: str, modifier_fn: Callable[[sqlite3.Connection], None]
) -> tuple[int, int]:
    """Lightweight metadata modification — no full DataModel rebuild.

    Only modifies metadata.sqlitedb inside the ABF. Does NOT regenerate
    VertiPaq binary data, H$ hierarchies, or R$ relationship indexes.

    Safe for: Partition.QueryDefinition, Partition.Mode changes.
    NOT safe for: adding/removing tables, columns, or relationships.

    Returns (old_dm_size, new_dm_size).
    """
    from pbix_mcp.formats.abf_rebuild import rebuild_abf_with_modified_sqlite
    from pbix_mcp.formats.datamodel_roundtrip import (
        compress_datamodel,
        decompress_datamodel,
    )

    with open(dm_path, "rb") as f:
        dm_bytes = f.read()

    abf = decompress_datamodel(dm_bytes)
    new_abf = rebuild_abf_with_modified_sqlite(abf, modifier_fn)
    new_dm = compress_datamodel(new_abf)

    with open(dm_path, "wb") as f:
        f.write(new_dm)

    return len(dm_bytes), len(new_dm)


# ---- Section 8: DataModel WRITE tools (via XPress9 round-trip) ----

def _modify_metadata_sqlite(
    dm_path: str, modifier_fn: Callable[[sqlite3.Connection], None],
    info: dict | None = None,
) -> tuple:
    """Modify metadata via full DataModel rebuild.

    Applies modifier_fn to a temporary copy of the current metadata to
    determine the changes, then reads the modified measures/relationships
    and rebuilds the entire DataModel via the builder pipeline.

    This avoids ALL post-build ABF modification which causes
    NullReferenceException at RunModelSchemaValidation.

    Args:
        dm_path: Path to the DataModel file inside the work_dir
        modifier_fn: Function that receives a sqlite3.Connection and should
                     make changes + commit.
        info: Open file info dict (required for full rebuild)

    Returns:
        Tuple of (original_dm_bytes, new_dm_bytes, None)
    """
    from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
    from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

    with open(dm_path, "rb") as f:
        dm_bytes = f.read()

    if info is None:
        work_dir = os.path.dirname(dm_path)
        info = {"work_dir": work_dir, "path": dm_path}

    # Apply the modifier to a TEMPORARY copy of metadata to see what changed.
    # Then rebuild the entire DataModel with the modified metadata's
    # measures and relationships baked in.
    abf = decompress_datamodel(dm_bytes)
    meta_bytes = read_metadata_sqlite(abf)

    fd, tmp_path = tempfile.mkstemp(suffix=".sqlitedb")
    try:
        os.write(fd, meta_bytes)
        os.close(fd)
        fd = None

        conn = sqlite3.connect(tmp_path)
        conn.row_factory = sqlite3.Row
        try:
            # Apply the modifier
            modifier_fn(conn)
            conn.commit()

            # Read the MODIFIED measures (these will replace the builder's measures)
            _AMO_TO_TYPE = {2: "String", 6: "Int64", 8: "Double", 9: "DateTime",
                            10: "Decimal", 11: "Boolean"}

            modified_measures = []
            for mrow in conn.execute(
                "SELECT t.Name as tbl, m.Name, m.Expression, m.FormatString "
                "FROM Measure m JOIN [Table] t ON m.TableID = t.ID"
            ):
                modified_measures.append({
                    "table": mrow["tbl"], "name": mrow["Name"],
                    "expression": mrow["Expression"],
                    "format_string": mrow["FormatString"] or "",
                })

            # Read tables and relationships from modified metadata
            modified_tables = []
            for trow in conn.execute(
                "SELECT ID, Name FROM [Table] WHERE ModelID = 1 "
                "AND Name NOT LIKE 'H$%' AND Name NOT LIKE 'R$%' ORDER BY ID"
            ):
                cols = [{"name": c["ExplicitName"],
                         "data_type": _AMO_TO_TYPE.get(c["ExplicitDataType"], "String")}
                        for c in conn.execute(
                            "SELECT ExplicitName, ExplicitDataType FROM [Column] "
                            "WHERE TableID = ? AND Type = 1 ORDER BY ID", (trow["ID"],))]
                modified_tables.append({"name": trow["Name"], "columns": cols})

            modified_rels = []
            for rrow in conn.execute(
                "SELECT ft.Name as ft, fc.ExplicitName as fc, "
                "tt.Name as tt, tc.ExplicitName as tc "
                "FROM Relationship r "
                "JOIN [Table] ft ON r.FromTableID = ft.ID "
                "JOIN [Column] fc ON r.FromColumnID = fc.ID "
                "JOIN [Table] tt ON r.ToTableID = tt.ID "
                "JOIN [Column] tc ON r.ToColumnID = tc.ID"
            ):
                modified_rels.append({
                    "from_table": rrow["ft"], "from_column": rrow["fc"],
                    "to_table": rrow["tt"], "to_column": rrow["tc"],
                })
        finally:
            conn.close()
    finally:
        if fd is not None:
            os.close(fd)
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    # Now rebuild using the builder with the modified state.
    # We pass all measures/rels as the "current" state — _rebuild_datamodel
    # reads its own measures/rels from metadata, so we need to override.
    from pbix_mcp.builder import PBIXBuilder
    from pbix_mcp.formats.vertipaq_decoder import read_table_from_abf

    builder = PBIXBuilder()
    for tinfo in modified_tables:
        tname = tinfo["name"]
        try:
            td = read_table_from_abf(abf, tname, meta_bytes)
            existing_rows = [dict(zip(td["columns"], rv))
                             for rv in td.get("rows", [])]
            builder.add_table(tname, tinfo["columns"], rows=existing_rows)
        except Exception:
            builder.add_table(tname, tinfo["columns"], rows=[])

    for m in modified_measures:
        builder.add_measure(m["table"], m["name"], m["expression"], m["format_string"])

    for r in modified_rels:
        builder.add_relationship(
            r["from_table"], r["from_column"], r["to_table"], r["to_column"]
        )

    new_pbix = builder.build()

    import io
    import zipfile
    new_z = zipfile.ZipFile(io.BytesIO(new_pbix))
    new_dm = new_z.read("DataModel")

    with open(dm_path, "wb") as f:
        f.write(new_dm)

    return dm_bytes, new_dm, None


@mcp.tool()
def pbix_datamodel_query_metadata(alias: str, sql_query: str) -> str:
    """Run a read-only SQL query on the DataModel's metadata SQLite.

    Args:
        alias: The alias of the open file
        sql_query: SQL query to run (e.g., "SELECT Name, Expression FROM Measure")
    """
    try:
        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")
        if not os.path.exists(dm_path):
            return ToolResponse.error("No DataModel found in this file.", DataModelCompressionError.code).to_text()

        from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
        from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

        with open(dm_path, "rb") as f:
            dm_bytes = f.read()

        abf = decompress_datamodel(dm_bytes)
        meta_bytes = read_metadata_sqlite(abf)

        if not meta_bytes:
            return ToolResponse.error("Could not extract metadata.sqlitedb from ABF.", DataModelCompressionError.code).to_text()

        # Write to temp file for sqlite3
        tmp = os.path.join(info["work_dir"], "_meta_query.tmp")
        with open(tmp, "wb") as f:
            f.write(meta_bytes)

        try:
            conn = sqlite3.connect(tmp)
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(sql_query)
            rows = cursor.fetchall()
            columns = [d[0] for d in cursor.description] if cursor.description else []
            conn.close()
        finally:
            os.remove(tmp)

        if not rows:
            return ToolResponse.ok("Query returned no results.").to_text()

        # Format output
        lines = [" | ".join(columns)]
        lines.append("-" * len(lines[0]))
        for row in rows[:200]:
            lines.append(" | ".join(str(row[c]) for c in columns))
        result = "\n".join(lines)
        if len(rows) > 200:
            result += f"\n... ({len(rows)} total rows, showing first 200)"
        return ToolResponse.ok(result).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(f"{str(e)}\n{traceback.format_exc()}", e.code).to_text()


@mcp.tool()
def pbix_datamodel_modify_metadata(alias: str, sql_statement: str) -> str:
    """Execute a SQL DDL/DML statement on the DataModel's metadata SQLite.

    This allows direct manipulation of the metadata database (tables, measures,
    columns, relationships, etc.). The ABF is fully rebuilt.

    Args:
        alias: The alias of the open file
        sql_statement: SQL statement to execute (INSERT, UPDATE, DELETE, ALTER, etc.)
    """
    try:
        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")
        if not os.path.exists(dm_path):
            return ToolResponse.error("No DataModel found.", DataModelCompressionError.code).to_text()

        changes = [0]

        def _do_sql(conn: sqlite3.Connection):
            conn.execute(sql_statement)
            changes[0] = conn.total_changes
            conn.commit()

        old_size, new_size = _modify_metadata_only(dm_path, _do_sql)
        info["modified"] = True
        return ToolResponse.ok(
            f"SQL executed successfully.\n"
            f"  Changes: {changes[0]}\n"
            f"  DataModel: {old_size:,} → {new_size:,} bytes"
        ).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(f"{str(e)}\n{traceback.format_exc()}", e.code).to_text()


@mcp.tool()
def pbix_datamodel_modify_measure(
    alias: str, measure_name: str, new_expression: str,
    new_format_string: str = ""
) -> str:
    """Modify a DAX measure's expression in the DataModel.

    Performs a full ABF rebuild so expressions of any length are supported.

    Args:
        alias: The alias of the open file
        measure_name: Name of the measure to modify
        new_expression: New DAX expression for the measure
        new_format_string: Optional new format string
    """
    try:
        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")
        if not os.path.exists(dm_path):
            return ToolResponse.error("No DataModel found.", DataModelCompressionError.code).to_text()

        old_info = {}

        def _do_modify(conn: sqlite3.Connection):
            c = conn.cursor()
            c.execute("SELECT ID, Expression FROM Measure WHERE Name = ?", (measure_name,))
            row = c.fetchone()
            if not row:
                raise ValueError(f"Measure '{measure_name}' not found")
            old_info["id"] = row[0]
            old_info["expression"] = row[1]

            updates = ["Expression = ?"]
            params = [new_expression]
            if new_format_string:
                updates.append("FormatString = ?")
                params.append(new_format_string)
            params.append(measure_name)

            c.execute(f"UPDATE Measure SET {', '.join(updates)} WHERE Name = ?", params)
            conn.commit()

        old_size, new_size = _modify_metadata_only(dm_path, _do_modify)
        info["modified"] = True
        return ToolResponse.ok(
            f"Measure '{measure_name}' updated:\n"
            f"  Old: {old_info.get('expression', '?')}\n"
            f"  New: {new_expression}\n"
            f"  DataModel: {old_size:,} → {new_size:,} bytes"
        ).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(f"{str(e)}\n{traceback.format_exc()}", e.code).to_text()


@mcp.tool()
def pbix_datamodel_add_measure(
    alias: str, table_name: str, measure_name: str, expression: str,
    format_string: str = "", description: str = ""
) -> str:
    """Create a new DAX measure in the specified table.

    The ABF is fully rebuilt so measures of any size are supported.

    Args:
        alias: The alias of the open file
        table_name: Table to add the measure to
        measure_name: Name of the new measure
        expression: DAX expression
        format_string: Optional format string
        description: Optional description
    """
    try:
        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")
        if not os.path.exists(dm_path):
            return ToolResponse.error("No DataModel found.", DataModelCompressionError.code).to_text()

        def _do_add(conn: sqlite3.Connection):
            c = conn.cursor()

            # Get table ID
            c.execute("SELECT ID FROM [Table] WHERE Name = ?", (table_name,))
            trow = c.fetchone()
            if not trow:
                raise ValueError(f"Table '{table_name}' not found")
            table_id = trow[0]

            # Check if measure already exists
            c.execute("SELECT ID FROM Measure WHERE Name = ?", (measure_name,))
            if c.fetchone():
                raise ValueError(f"Measure '{measure_name}' already exists")

            # Get next ID from MAXID (PBI's global ID counter).
            # MAXID is always >= the highest ID across all tables.
            # Using MAX(ID) per table misses IDs in system tables like
            # AttributeHierarchyStorage, SegmentMapStorage, etc.
            maxid_row = c.execute(
                "SELECT Value FROM DBPROPERTIES WHERE Name = 'MAXID'"
            ).fetchone()
            max_id = int(maxid_row[0]) if maxid_row else 0
            new_id = max_id + 1

            # Use Windows FILETIME timestamp (matching builder format)
            import datetime
            now = datetime.datetime.utcnow()
            epoch = datetime.datetime(1601, 1, 1)
            filetime = int((now - epoch).total_seconds() * 10_000_000)

            # Generate a LineageTag UUID
            import uuid
            lineage_tag = str(uuid.uuid4())

            c.execute(
                """INSERT INTO Measure (ID, TableID, Name, Description, DataType,
                    Expression, FormatString, IsHidden, State, ModifiedTime,
                    StructureModifiedTime, KPIID, IsSimpleMeasure, ErrorMessage,
                    DisplayFolder, DetailRowsDefinitionID, DataCategory,
                    FormatStringDefinitionID, LineageTag, SourceLineageTag)
                VALUES (?, ?, ?, ?, 6, ?, ?, 0, 1, ?, ?, 0, 0, NULL,
                    NULL, 0, NULL, 0, ?, NULL)""",
                (new_id, table_id, measure_name, description or None,
                 expression, format_string or None,
                 filetime, filetime, lineage_tag)
            )
            conn.commit()

        old_size, new_size = _modify_metadata_only(dm_path, _do_add)
        info["modified"] = True
        return ToolResponse.ok(
            f"Measure '{measure_name}' added to table '{table_name}':\n"
            f"  Expression: {expression}\n"
            f"  DataModel: {old_size:,} → {new_size:,} bytes"
        ).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(f"{str(e)}\n{traceback.format_exc()}", e.code).to_text()


@mcp.tool()
def pbix_datamodel_remove_measure(alias: str, measure_name: str) -> str:
    """Delete a DAX measure from the DataModel.

    The ABF is fully rebuilt.

    Args:
        alias: The alias of the open file
        measure_name: Name of the measure to remove
    """
    try:
        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")
        if not os.path.exists(dm_path):
            return ToolResponse.error("No DataModel found.", DataModelCompressionError.code).to_text()

        old_info = {}

        def _do_remove(conn: sqlite3.Connection):
            c = conn.cursor()
            c.execute(
                "SELECT m.ID, m.Expression, t.Name FROM Measure m "
                "JOIN [Table] t ON m.TableID = t.ID "
                "WHERE m.Name = ?",
                (measure_name,)
            )
            row = c.fetchone()
            if not row:
                raise ValueError(f"Measure '{measure_name}' not found")
            old_info["id"] = row[0]
            old_info["expression"] = row[1]
            old_info["table"] = row[2]

            c.execute("DELETE FROM Measure WHERE Name = ?", (measure_name,))
            conn.commit()

        old_size, new_size = _modify_metadata_only(dm_path, _do_remove)
        info["modified"] = True
        return ToolResponse.ok(
            f"Measure '{measure_name}' removed from table '{old_info.get('table', '?')}':\n"
            f"  Old expression: {old_info.get('expression', '?')}\n"
            f"  DataModel: {old_size:,} → {new_size:,} bytes"
        ).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(f"{str(e)}\n{traceback.format_exc()}", e.code).to_text()


@mcp.tool()
def pbix_datamodel_add_relationship(
    alias: str,
    from_table: str,
    from_column: str,
    to_table: str,
    to_column: str,
) -> str:
    """Add a relationship between two tables. Rebuilds the DataModel.

    Creates a cross-table relationship with R$ index tables in VertiPaq.
    The from side is many (fact), the to side is one (dimension).

    Args:
        alias: The alias of the open file
        from_table: Fact table name (many side)
        from_column: Foreign key column in fact table
        to_table: Dimension table name (one side)
        to_column: Primary key column in dimension table
    """
    try:
        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")
        if not os.path.exists(dm_path):
            return ToolResponse.error("No DataModel found.", DataModelCompressionError.code).to_text()

        old_size, new_size = _rebuild_datamodel(
            info,
            extra_relationships=[{
                "from_table": from_table, "from_column": from_column,
                "to_table": to_table, "to_column": to_column,
            }],
        )
        info["modified"] = True
        return ToolResponse.ok(
            f"Relationship added: {from_table}.{from_column} → {to_table}.{to_column}\n"
            f"  DataModel: {old_size:,} → {new_size:,} bytes"
        ).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_datamodel_remove_relationship(
    alias: str,
    from_table: str,
    from_column: str,
    to_table: str,
    to_column: str,
) -> str:
    """Remove a relationship between two tables. Rebuilds the DataModel.

    Args:
        alias: The alias of the open file
        from_table: Fact table name (many side)
        from_column: Foreign key column in fact table
        to_table: Dimension table name (one side)
        to_column: Primary key column in dimension table
    """
    try:
        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")
        if not os.path.exists(dm_path):
            return ToolResponse.error("No DataModel found.", DataModelCompressionError.code).to_text()

        old_size, new_size = _rebuild_datamodel(
            info,
            remove_relationships=[(from_table, from_column, to_table, to_column)],
        )
        info["modified"] = True
        return ToolResponse.ok(
            f"Relationship removed: {from_table}.{from_column} → {to_table}.{to_column}\n"
            f"  DataModel: {old_size:,} → {new_size:,} bytes"
        ).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_datamodel_remove_table(alias: str, table_name: str) -> str:
    """Remove a table and its measures/relationships from the DataModel.

    Rebuilds the DataModel without the specified table. All measures hosted
    on the table and all relationships referencing it are also removed.

    Args:
        alias: The alias of the open file
        table_name: Name of the table to remove
    """
    try:
        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")
        if not os.path.exists(dm_path):
            return ToolResponse.error("No DataModel found.", DataModelCompressionError.code).to_text()

        old_size, new_size = _rebuild_datamodel(
            info,
            remove_tables={table_name},
        )
        info["modified"] = True
        return ToolResponse.ok(
            f"Table '{table_name}' removed (with its measures and relationships)\n"
            f"  DataModel: {old_size:,} → {new_size:,} bytes"
        ).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_datamodel_add_field_parameter(
    alias: str, parameter_name: str, fields_json: str
) -> str:
    """Create a field parameter — a slicer-driven column/measure switcher.

    Field parameters let users dynamically choose which column or measure
    to display in a visual via a slicer.

    Args:
        alias: The alias of the open file
        parameter_name: Name for the field parameter table (e.g. "Metric Selector")
        fields_json: JSON array of fields to include, e.g.
            '[{"display": "Revenue", "ref": "Sales[Revenue]"},
              {"display": "Profit",  "ref": "Sales[Profit]"},
              {"display": "Units",   "ref": "Sales[Units]"}]'
            Each entry has "display" (label shown in slicer) and "ref"
            (table[column] or table[measure] reference).
    """
    try:
        fields = json.loads(fields_json)
        if not fields or not isinstance(fields, list):
            raise ValueError("fields_json must be a non-empty JSON array")

        for f in fields:
            if "display" not in f or "ref" not in f:
                raise ValueError("Each field must have 'display' and 'ref' keys")

        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")
        if not os.path.exists(dm_path):
            return ToolResponse.error("No DataModel found.", DataModelCompressionError.code).to_text()

        # Field parameters create a new table in metadata, but adding a table
        # requires VertiPaq data files in the ABF (H$ hierarchies, partition data).
        # Without them, PBI crashes with TMCacheManager::CreateEmptyCollectionsForAllParents.
        # This tool currently only modifies metadata — it needs a full DataModel rebuild
        # to generate VertiPaq storage for the new table.  Block until that's implemented.
        return ToolResponse.error(
            "Field parameters require creating a new table with VertiPaq storage. "
            "This tool currently only modifies metadata — use pbix_create with the "
            "field parameter table defined upfront, or add field parameters in PBI Desktop.",
            "NOT_IMPLEMENTED"
        ).to_text()

        # Build DAX expression for display purposes
        rows_dax = []
        for i, f in enumerate(fields):
            ref = f["ref"]
            display = f["display"].replace('"', '""')
            if "'" not in ref and "[" in ref:
                tbl = ref.split("[")[0]
                col = ref.split("[")[1].rstrip("]")
                ref = f"'{tbl}'[{col}]"
            rows_dax.append(f'    ("{display}", NAMEOF({ref}), {i})')
        dax_expr = "{\n" + ",\n".join(rows_dax) + "\n}"

        # Build M expression — field parameters need a valid M #table() so the
        # M engine can parse the partition on file load.  The DAX NAMEOF/tuple
        # syntax cannot go into QueryDefinition (the M engine rejects it).
        m_cols = (
            f'type table [{parameter_name} = text, '
            f'{parameter_name} Fields = text, '
            f'{parameter_name} Order = number]'
        )
        m_rows = []
        for i, f in enumerate(fields):
            display_m = f["display"].replace('"', '""')
            ref_m = f["ref"].replace('"', '""')
            m_rows.append(f'{{"{display_m}", "{ref_m}", {i}}}')
        m_expr = f"let\n    Source = #table({m_cols}, {{{', '.join(m_rows)}}})\nin\n    Source"

        def _do_add(conn: sqlite3.Connection):
            c = conn.cursor()

            c.execute("SELECT COALESCE(MAX(ID), 0) + 1 FROM [Table]")
            table_id = c.fetchone()[0]

            c.execute(
                "INSERT INTO [Table] (ID, ModelID, Name, IsHidden, "
                "ModifiedTime, StructureModifiedTime, SystemFlags, "
                "ShowAsVariationsOnly, IsPrivate, "
                "CalculationGroupID, ExcludeFromModelRefresh) "
                "VALUES (?, 1, ?, 0, datetime('now'), datetime('now'), 0, 0, 0, 0, 0)",
                (table_id, parameter_name)
            )

            c.execute("SELECT COALESCE(MAX(ID), 0) + 1 FROM [Column]")
            col_base_id = c.fetchone()[0]

            c.execute(
                "INSERT INTO [Column] (ID, TableID, ExplicitName, ExplicitDataType, "
                "Type, IsHidden, IsAvailableInMDX, ModifiedTime, StructureModifiedTime) "
                "VALUES (?, ?, ?, 2, 2, 0, 1, datetime('now'), datetime('now'))",
                (col_base_id, table_id, parameter_name)
            )
            c.execute(
                "INSERT INTO [Column] (ID, TableID, ExplicitName, ExplicitDataType, "
                "Type, IsHidden, IsAvailableInMDX, ModifiedTime, StructureModifiedTime) "
                "VALUES (?, ?, ?, 2, 2, 1, 0, datetime('now'), datetime('now'))",
                (col_base_id + 1, table_id, f"{parameter_name} Fields")
            )
            c.execute(
                "INSERT INTO [Column] (ID, TableID, ExplicitName, ExplicitDataType, "
                "Type, IsHidden, IsAvailableInMDX, ModifiedTime, StructureModifiedTime) "
                "VALUES (?, ?, ?, 6, 2, 1, 0, datetime('now'), datetime('now'))",
                (col_base_id + 2, table_id, f"{parameter_name} Order")
            )

            c.execute("SELECT COALESCE(MAX(ID), 0) + 1 FROM [Partition]")
            part_id = c.fetchone()[0]

            # Use M expression (not DAX) — the M engine parses QueryDefinition on load
            c.execute(
                "INSERT INTO [Partition] (ID, TableID, Name, "
                "Type, Mode, State, ModifiedTime, RefreshedTime, "
                "QueryDefinition) "
                "VALUES (?, ?, ?, 2, 0, 1, datetime('now'), datetime('now'), ?)",
                (part_id, table_id, parameter_name, m_expr)
            )

            conn.commit()

        old_size, new_size = _modify_metadata_only(dm_path, _do_add)
        info["modified"] = True

        field_list = ", ".join(f["display"] for f in fields)
        return ToolResponse.ok(
            f"Field parameter '{parameter_name}' created with {len(fields)} fields: {field_list}\n"
            f"  DAX: {dax_expr}\n"
            f"  DataModel: {old_size:,} → {new_size:,} bytes\n"
            f"Use as a slicer to let users switch between these fields in visuals."
        ).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(f"{str(e)}\n{traceback.format_exc()}", e.code).to_text()


@mcp.tool()
def pbix_datamodel_add_calculation_group(
    alias: str, group_name: str, items_json: str, precedence: int = 0
) -> str:
    """Create a calculation group — dynamic measure modifiers (YTD, QTD, PY, etc.).

    Calculation groups apply DAX transformations to any measure used in a visual.
    For example, a "Time Intelligence" group with items "Current", "YTD", "PY"
    lets users switch between time calculations via a slicer.

    Args:
        alias: The alias of the open file
        group_name: Name for the calculation group table (e.g. "Time Intelligence")
        items_json: JSON array of calculation items, e.g.
            '[{"name": "Current", "expression": "SELECTEDMEASURE()"},
              {"name": "YTD", "expression": "CALCULATE(SELECTEDMEASURE(), DATESYTD(''Date''[Date]))"},
              {"name": "PY",  "expression": "CALCULATE(SELECTEDMEASURE(), SAMEPERIODLASTYEAR(''Date''[Date]))"}]'
            Each item has "name" (display label) and "expression" (DAX using SELECTEDMEASURE()).
        precedence: Evaluation order when multiple calc groups exist (default 0)
    """
    try:
        items = json.loads(items_json)
        if not items or not isinstance(items, list):
            raise ValueError("items_json must be a non-empty JSON array")
        for item in items:
            if "name" not in item or "expression" not in item:
                raise ValueError("Each item must have 'name' and 'expression' keys")

        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")
        if not os.path.exists(dm_path):
            return ToolResponse.error("No DataModel found.", DataModelCompressionError.code).to_text()

        # Calculation groups create a new table in metadata, but adding a table
        # requires VertiPaq data files in the ABF (H$ hierarchies, partition data).
        # Without them, PBI crashes with TMCacheManager::CreateEmptyCollectionsForAllParents.
        # This tool currently only modifies metadata — it needs a full DataModel rebuild.
        return ToolResponse.error(
            "Calculation groups require creating a new table with VertiPaq storage. "
            "This tool currently only modifies metadata — use pbix_create with the "
            "calculation group defined upfront, or add calculation groups in PBI Desktop.",
            "NOT_IMPLEMENTED"
        ).to_text()

        def _do_add(conn: sqlite3.Connection):
            c = conn.cursor()

            # Check if group name already exists
            c.execute("SELECT ID FROM [Table] WHERE Name = ?", (group_name,))
            if c.fetchone():
                raise ValueError(f"Table '{group_name}' already exists")

            # Get next IDs
            c.execute("SELECT COALESCE(MAX(ID), 0) + 1 FROM [Table]")
            table_id = c.fetchone()[0]

            c.execute("SELECT COALESCE(MAX(ID), 0) + 1 FROM CalculationGroup")
            cg_id = c.fetchone()[0]

            # Create CalculationGroup
            c.execute(
                "INSERT INTO CalculationGroup (ID, TableID, Precedence, "
                "ModifiedTime) VALUES (?, ?, ?, datetime('now'))",
                (cg_id, table_id, precedence)
            )

            # Create the table with CalculationGroupID set
            c.execute(
                "INSERT INTO [Table] (ID, ModelID, Name, IsHidden, "
                "ModifiedTime, StructureModifiedTime, SystemFlags, "
                "ShowAsVariationsOnly, IsPrivate, "
                "CalculationGroupID, ExcludeFromModelRefresh) "
                "VALUES (?, 1, ?, 0, datetime('now'), datetime('now'), 0, 0, 0, ?, 0)",
                (table_id, group_name, cg_id)
            )

            # Create the Name column (shows item names in slicers)
            c.execute("SELECT COALESCE(MAX(ID), 0) + 1 FROM [Column]")
            col_id = c.fetchone()[0]

            c.execute(
                "INSERT INTO [Column] (ID, TableID, ExplicitName, ExplicitDataType, "
                "Type, IsHidden, IsAvailableInMDX, ModifiedTime, StructureModifiedTime) "
                "VALUES (?, ?, 'Name', 2, 2, 0, 1, datetime('now'), datetime('now'))",
                (col_id, table_id)
            )

            # Create Ordinal column (for sorting)
            c.execute(
                "INSERT INTO [Column] (ID, TableID, ExplicitName, ExplicitDataType, "
                "Type, IsHidden, IsAvailableInMDX, ModifiedTime, StructureModifiedTime) "
                "VALUES (?, ?, 'Ordinal', 6, 2, 1, 0, datetime('now'), datetime('now'))",
                (col_id + 1, table_id)
            )

            # Create CalculationItems
            c.execute("SELECT COALESCE(MAX(ID), 0) + 1 FROM CalculationItem")
            item_base_id = c.fetchone()[0]

            for i, item in enumerate(items):
                c.execute(
                    "INSERT INTO CalculationItem (ID, CalculationGroupID, Name, "
                    "Expression, Ordinal, ModifiedTime) "
                    "VALUES (?, ?, ?, ?, ?, datetime('now'))",
                    (item_base_id + i, cg_id, item["name"], item["expression"], i)
                )

            # Partition: calc groups need a valid M partition so file opens
            c.execute("SELECT COALESCE(MAX(ID), 0) + 1 FROM [Partition]")
            part_id = c.fetchone()[0]
            m_expr = (
                "let\n"
                "    Source = #table(type table [Name = text, Ordinal = number], {})\n"
                "in\n"
                "    Source"
            )
            c.execute(
                "INSERT INTO [Partition] (ID, TableID, Name, "
                "Type, Mode, State, ModifiedTime, RefreshedTime, "
                "QueryDefinition) "
                "VALUES (?, ?, ?, 2, 0, 1, datetime('now'), datetime('now'), ?)",
                (part_id, table_id, group_name, m_expr)
            )

            conn.commit()

        old_size, new_size = _modify_metadata_only(dm_path, _do_add)
        info["modified"] = True

        item_list = ", ".join(item["name"] for item in items)
        return ToolResponse.ok(
            f"Calculation group '{group_name}' created with {len(items)} items: {item_list}\n"
            f"  Precedence: {precedence}\n"
            f"  DataModel: {old_size:,} → {new_size:,} bytes\n"
            f"Add to a slicer — measures in visuals will be modified by the selected item."
        ).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_datamodel_modify_column(
    alias: str, table_name: str, column_name: str,
    property_name: str, new_value: str
) -> str:
    """Modify a column property in the DataModel metadata.

    Supports string, integer, and float columns. The ABF is fully rebuilt.

    Args:
        alias: The alias of the open file
        table_name: Name of the table containing the column
        column_name: Name of the column to modify
        property_name: Property to change (e.g., 'FormatString', 'IsHidden', 'Description')
        new_value: New value for the property
    """
    try:
        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")
        if not os.path.exists(dm_path):
            return ToolResponse.error("No DataModel found.", DataModelCompressionError.code).to_text()

        def _do_modify(conn: sqlite3.Connection):
            c = conn.cursor()
            c.execute(
                "SELECT c.ID FROM [Column] c "
                "JOIN [Table] t ON c.TableID = t.ID "
                "WHERE t.Name = ? AND c.ExplicitName = ?",
                (table_name, column_name)
            )
            row = c.fetchone()
            if not row:
                raise ValueError(
                    f"Column '{column_name}' not found in table '{table_name}'"
                )

            # Try numeric conversion
            try:
                val = int(new_value)
            except ValueError:
                try:
                    val = float(new_value)
                except ValueError:
                    val = new_value

            c.execute(
                f"UPDATE [Column] SET [{property_name}] = ? "
                f"WHERE ID = ?",
                (val, row[0])
            )
            conn.commit()

        old_size, new_size = _modify_metadata_only(dm_path, _do_modify)
        info["modified"] = True
        return ToolResponse.ok(
            f"Column '{table_name}'.'{column_name}' updated:\n"
            f"  {property_name} = {new_value}\n"
            f"  DataModel: {old_size:,} → {new_size:,} bytes"
        ).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(f"{str(e)}\n{traceback.format_exc()}", e.code).to_text()


@mcp.tool()
def pbix_datamodel_decompress(alias: str) -> str:
    """Decompress the DataModel from a PBIX into raw ABF format.

    This decompresses the XPress9-compressed DataModel and saves the
    raw ABF file for inspection. The ABF contains the full VertiPaq
    storage engine data.

    Args:
        alias: The alias of the open file
    """
    try:
        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")
        if not os.path.exists(dm_path):
            return ToolResponse.error("No DataModel found.", DataModelCompressionError.code).to_text()

        from pbix_mcp.formats.abf_rebuild import list_abf_files
        from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

        with open(dm_path, "rb") as f:
            dm_bytes = f.read()

        logger.info("Decompressing DataModel (%d bytes) for '%s'", len(dm_bytes), alias)
        abf = decompress_datamodel(dm_bytes)
        logger.debug("Decompressed to %d bytes ABF", len(abf))
        abf_path = dm_path + ".abf"
        with open(abf_path, "wb") as f:
            f.write(abf)

        file_log = list_abf_files(abf)
        summary = [f"Decompressed DataModel: {len(dm_bytes):,} → {len(abf):,} bytes"]
        summary.append(f"ABF saved to: {abf_path}")
        summary.append(f"\nABF contains {len(file_log)} files:")
        for entry in file_log:
            summary.append(f"  {entry['Path']} ({entry['Size']:,} bytes)")
        return ToolResponse.ok("\n".join(summary)).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(f"{str(e)}\n{traceback.format_exc()}", DataModelCompressionError.code).to_text()


@mcp.tool()
def pbix_datamodel_recompress(alias: str, abf_path: str = "") -> str:
    """Recompress a modified ABF file back into the DataModel.

    After using pbix_datamodel_decompress to get the ABF, you can
    decompress and modify the ABF (or any of its internal files), call
    this to XPress9-compress it back into the DataModel. The next
    pbix_save will include the updated DataModel.

    Workflow:
      1. pbix_datamodel_decompress(alias)  ->  saves .abf
      2. Modify the .abf (directly, or via modify_measure / modify_metadata)
      3. pbix_datamodel_recompress(alias)   ->  compresses .abf back into DataModel

    Args:
        alias: The alias of the open file
        abf_path: Path to the ABF file to compress. Default: the .abf
                  next to the DataModel.
    """
    try:
        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")

        from pbix_mcp.formats.datamodel_roundtrip import compress_datamodel

        if not abf_path:
            abf_path = dm_path + ".abf"

        if not os.path.exists(abf_path):
            return ToolResponse.error(
                f"ABF file not found at {abf_path}. Run pbix_datamodel_decompress first.",
                ABFRebuildError.code
            ).to_text()

        with open(abf_path, "rb") as f:
            abf_bytes = f.read()

        logger.info("Recompressing ABF (%d bytes) for '%s'", len(abf_bytes), alias)

        # Validate ABF starts with BOM
        if not abf_bytes[:2] == b"\xff\xfe":
            return ToolResponse.error(
                f"File does not look like a valid ABF (expected \\xff\\xfe BOM, got {abf_bytes[:2].hex()}).",
                ABFRebuildError.code
            ).to_text()

        # Read original DataModel size for comparison
        orig_size = os.path.getsize(dm_path) if os.path.exists(dm_path) else 0

        new_dm = compress_datamodel(abf_bytes)

        with open(dm_path, "wb") as f:
            f.write(new_dm)

        info["modified"] = True
        return ToolResponse.ok(
            f"Recompressed ABF -> DataModel:\n"
            f"  ABF size:          {len(abf_bytes):>12,} bytes\n"
            f"  Old DataModel:     {orig_size:>12,} bytes\n"
            f"  New DataModel:     {len(new_dm):>12,} bytes\n"
            f"  XPress9 blocks:    {(len(abf_bytes) + 2097151) // 2097152}\n"
            f"  Saved to: {dm_path}"
        ).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(f"{str(e)}\n{traceback.format_exc()}", ABFRebuildError.code).to_text()


@mcp.tool()
def pbix_datamodel_replace_file(alias: str, internal_path: str, new_content_path: str) -> str:
    """Replace a specific file inside the ABF (decompressed DataModel).

    This lets you swap out any internal ABF file — for example, replace
    metadata.sqlitedb with a modified version.

    Files can be ANY size — the ABF is fully rebuilt with updated offsets
    and headers.

    Args:
        alias: The alias of the open file
        internal_path: Partial path to match inside the ABF (e.g. 'metadata.sqlitedb')
        new_content_path: Path to the replacement file on disk
    """
    try:
        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")
        if not os.path.exists(dm_path):
            return ToolResponse.error("No DataModel found.", DataModelCompressionError.code).to_text()

        if not os.path.exists(new_content_path):
            return ToolResponse.error(f"Replacement file not found: {new_content_path}", ABFRebuildError.code).to_text()

        from pbix_mcp.formats.abf_rebuild import (
            find_abf_file,
            list_abf_files,
            rebuild_abf_with_replacement,
        )
        from pbix_mcp.formats.datamodel_roundtrip import compress_datamodel, decompress_datamodel

        with open(dm_path, "rb") as f:
            dm_bytes = f.read()

        with open(new_content_path, "rb") as f:
            new_content = f.read()

        abf = decompress_datamodel(dm_bytes)
        file_log = list_abf_files(abf)
        entry = find_abf_file(file_log, internal_path)
        if not entry:
            return ToolResponse.error(f"No file matching '{internal_path}' in ABF.", ABFRebuildError.code).to_text()

        fname = entry["Path"]
        new_abf = rebuild_abf_with_replacement(abf, {internal_path: new_content})
        new_dm = compress_datamodel(new_abf)

        with open(dm_path, "wb") as f:
            f.write(new_dm)

        info["modified"] = True
        return ToolResponse.ok(
            f"Replaced '{fname}' in ABF (full rebuild):\n"
            f"  Old file size: {entry['Size']:,} bytes\n"
            f"  New file size: {len(new_content):,} bytes\n"
            f"  ABF: {len(abf):,} -> {len(new_abf):,} bytes\n"
            f"  DataModel recompressed: {len(dm_bytes):,} -> {len(new_dm):,} bytes"
        ).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(f"{str(e)}\n{traceback.format_exc()}", ABFRebuildError.code).to_text()


@mcp.tool()
def pbix_datamodel_extract_file(alias: str, internal_path: str, output_path: str = "") -> str:
    """Extract a specific file from inside the ABF (decompressed DataModel).

    Args:
        alias: The alias of the open file
        internal_path: Partial path to match inside the ABF (e.g. 'metadata.sqlitedb')
        output_path: Where to save the extracted file. Default: next to DataModel.
    """
    try:
        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")
        if not os.path.exists(dm_path):
            return ToolResponse.error("No DataModel found.", DataModelCompressionError.code).to_text()

        from pbix_mcp.formats.abf_rebuild import find_abf_file, list_abf_files, read_abf_file
        from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

        with open(dm_path, "rb") as f:
            dm_bytes = f.read()

        abf = decompress_datamodel(dm_bytes)
        file_log = list_abf_files(abf)
        entry = find_abf_file(file_log, internal_path)
        if not entry:
            return ToolResponse.error(f"No file matching '{internal_path}' in ABF.", DataModelCompressionError.code).to_text()

        content = read_abf_file(abf, entry)

        if not output_path:
            fname = os.path.basename(entry["Path"])
            output_path = os.path.join(info["work_dir"], fname)

        with open(output_path, "wb") as f:
            f.write(content)

        return ToolResponse.ok(
            f"Extracted '{entry['Path']}' ({len(content):,} bytes)\n"
            f"  ABF path: {entry['Path']}\n"
            f"  Saved to: {output_path}"
        ).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(f"{str(e)}\n{traceback.format_exc()}", DataModelCompressionError.code).to_text()


@mcp.tool()
def pbix_datamodel_list_abf_files(alias: str) -> str:
    """List all files inside the ABF (decompressed DataModel).

    Args:
        alias: The alias of the open file
    """
    try:
        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")
        if not os.path.exists(dm_path):
            return ToolResponse.error("No DataModel found.", DataModelCompressionError.code).to_text()

        from pbix_mcp.formats.abf_rebuild import list_abf_files
        from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

        with open(dm_path, "rb") as f:
            dm_bytes = f.read()

        abf = decompress_datamodel(dm_bytes)
        files = list_abf_files(abf)

        lines = [f"ABF contains {len(files)} files ({len(abf):,} bytes decompressed):\n"]
        for entry in files:
            lines.append(f"  {entry['Path']} ({entry['Size']:,} bytes)")
        return ToolResponse.ok("\n".join(lines)).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(f"{str(e)}\n{traceback.format_exc()}", DataModelCompressionError.code).to_text()


# ---- Section 9: DAX Evaluation Engine ----

# Cache for DAX context data per alias (tables + measures + relationships)
_dax_cache: dict = {}

def _get_dax_context(alias: str) -> dict:
    """Build or retrieve cached DAX context (tables, measures, relationships)."""
    if alias in _dax_cache:
        return _dax_cache[alias]

    info = _ensure_open(alias)
    from pbix_mcp.formats.model_reader import ModelReader
    model = ModelReader(info["path"], work_dir=info.get("work_dir"))

    # Load measures
    measures_list = model.dax_measures
    measure_defs = {}
    for m in measures_list:
        measure_defs[m.get('Name', '')] = m.get('Expression', '')

    # Load relationships
    rels_list = model.relationships
    relationships = []
    for r in rels_list:
        relationships.append({
            'FromTable': r.get('FromTableName', ''),
            'FromColumn': r.get('FromColumnName', ''),
            'ToTable': r.get('ToTableName', ''),
            'ToColumn': r.get('ToColumnName', ''),
            'IsActive': r.get('IsActive', 1),
        })

    # Load all user-facing tables
    schema_list = model.schema
    table_names = sorted(set(r['TableName'] for r in schema_list))
    tables = {}
    for tname in table_names:
        if tname.startswith('H$') or tname.startswith('R$'):
            continue
        try:
            td = model.get_table(tname)
            if td and td.get('columns') and td.get('rows'):
                tables[tname] = {
                    'columns': td['columns'],
                    'rows': td['rows'],
                }
        except Exception:
            continue

    # --- Load calculated tables from ABF metadata ---
    # Uses calc_tables.py as the single source of truth for evaluating
    # DATATABLE, GENERATESERIES, CALENDAR, and other calculated table expressions
    # that exist only as DAX in metadata, not in VertiPaq column stores.
    try:
        from pbix_mcp.dax.calc_tables import load_calculated_tables
        tables = load_calculated_tables(info["path"], tables, relationships)
    except Exception:
        pass  # If calculated table loading fails, continue without them

    # Performance warning for large tables
    _LARGE_TABLE_THRESHOLD = 100_000
    for tname, tdata in tables.items():
        row_count = len(tdata.get('rows', []))
        if row_count > _LARGE_TABLE_THRESHOLD:
            logger.warning("Table '%s' has %d rows — DAX evaluation may be slow", tname, row_count)

    # Detect date table — try multiple heuristics
    date_table = None
    date_column = None
    # Pass 1: table name contains 'date' AND has a 'Date' column
    for tname, tdata in tables.items():
        if 'date' in tname.lower():
            if 'Date' in tdata['columns']:
                date_table = tname
                date_column = 'Date'
                break
    # Pass 2: table name starts with common date-table prefixes (dimDate, DimDate, DateTable, Calendar)
    if not date_table:
        for tname, tdata in tables.items():
            tlow = tname.lower().replace(' ', '').replace('-', '').replace('_', '')
            if tlow in ('dimdate', 'datetable', 'calendar', 'datekey', 'dates'):
                for cname in tdata['columns']:
                    if cname.lower() == 'date':
                        date_table = tname
                        date_column = cname
                        break
                if date_table:
                    break
    # Pass 3: any table with a 'Date' column that also has Year/Month columns (likely a date dimension)
    if not date_table:
        for tname, tdata in tables.items():
            cols_lower = [c.lower() for c in tdata['columns']]
            if 'date' in cols_lower and ('year' in cols_lower or 'month' in cols_lower):
                date_col_idx = cols_lower.index('date')
                date_table = tname
                date_column = tdata['columns'][date_col_idx]
                break

    # --- Load default slicer filters from report layout ---
    # These are the slicer values that Power BI applies when you first open
    # the report (before any user interaction). Without them, measures using
    # SELECTEDVALUE on parameter tables return BLANK.
    default_filters = {}
    try:
        default_filters = _get_all_default_filters(info["work_dir"])
    except Exception:
        pass

    ctx = {
        'tables': tables,
        'measure_defs': measure_defs,
        'date_table': date_table,
        'date_column': date_column,
        'relationships': relationships,
        'default_filters': default_filters,
        'work_dir': info["work_dir"],
    }
    _dax_cache[alias] = ctx
    return ctx


@mcp.tool()
def pbix_evaluate_dax(
    alias: str,
    measures: str,
    filter_context: str = "",
) -> str:
    """Evaluate one or more DAX measures against the data model.

    Uses the built-in DAX engine to compute measure values, supporting:
    SUM, AVERAGE, DIVIDE, IF, CALCULATE, DATEADD, REMOVEFILTERS, ALL,
    MAXX, SUMX, VAR/RETURN, and 25+ other DAX functions.

    Supports relationship-based filter propagation (star-schema joins).

    Args:
        alias: The alias of the open file
        measures: Comma-separated measure names to evaluate, e.g. "Sales,Profit Margin,Sales LY"
        filter_context: Optional JSON filter context, e.g. '{"dim-Date.Year": [2015]}'
    """
    try:
        info = _ensure_open(alias)
        if info.get("is_directquery"):
            return ToolResponse.error(
                "This file uses DirectQuery — DAX evaluation requires local data. "
                "Use layout, measure, and metadata tools instead.",
                UnsupportedFormatError.code,
            ).to_text()

        from pbix_mcp.dax import engine as dax_engine

        ctx = _get_dax_context(alias)
        measure_names = [m.strip() for m in measures.split(',') if m.strip()]

        parsed_fc = FilterContext.from_json_str(filter_context)
        if parsed_fc.filters:
            fc = parsed_fc.filters
        else:
            # Auto-apply default slicer filters from the report layout
            fc = ctx.get('default_filters') or None

        # Reset unsupported tracker before evaluation
        dax_engine._engine.unsupported_functions.clear()
        logger.info("Evaluating %d measures for '%s'", len(measure_names), alias)

        results = dax_engine.evaluate_measures_smart(
            measure_names, ctx['tables'], ctx['measure_defs'],
            fc, ctx['date_table'], ctx['date_column'],
            ctx.get('relationships')
        )

        # Build structured response with DAXResult objects
        unsupported = set(dax_engine._engine.unsupported_functions)
        dax_results = []
        for name, val in results.items():
            if val is not None:
                dax_results.append(DAXResult(name=name, value=val, status="ok"))
            elif unsupported:
                # Value is None and unsupported functions were hit — mark as unsupported
                dax_results.append(DAXResult(
                    name=name, value=None, status="unsupported",
                    error_message=f"Uses unsupported function(s): {', '.join(sorted(unsupported))}",
                ))
            else:
                dax_results.append(DAXResult(name=name, value=None, status="blank"))

        warnings = []
        if unsupported:
            warnings.append(f"{len(unsupported)} unsupported DAX function(s): {', '.join(sorted(unsupported))}")

        response = DAXEvalResponse(
            success=True,
            results=dax_results,
            warnings=warnings,
        )
        logger.debug("DAX eval complete: %d ok, %d blank",
                      sum(1 for r in dax_results if r.status == "ok"),
                      sum(1 for r in dax_results if r.status == "blank"))
        return response.to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(f"{str(e)}\n{traceback.format_exc()}", e.code).to_text()


@mcp.tool()
def pbix_evaluate_dax_per_dimension(
    alias: str,
    measures: str,
    dimension: str,
    filter_context: str = "",
    max_values: int = 20,
) -> str:
    """Evaluate DAX measures for each value of a dimension (e.g., Sales per State).

    Iterates over unique values of a dimension column and evaluates measures
    with that dimension value as a filter. Uses relationship-based propagation.

    Args:
        alias: The alias of the open file
        measures: Comma-separated measure names, e.g. "Sales,Sales LY,Sales change"
        dimension: Table.Column to iterate over, e.g. "dim-Geo.State"
        filter_context: Optional JSON base filter, e.g. '{"dim-Date.Year": [2015]}'
        max_values: Maximum dimension values to evaluate (default 20)
    """
    try:
        from pbix_mcp.dax import engine as dax_engine

        ctx = _get_dax_context(alias)
        measure_names = [m.strip() for m in measures.split(',') if m.strip()]
        parsed_fc = FilterContext.from_json_str(filter_context)
        base_fc = parsed_fc.filters

        try:
            dim_ref = DimensionRef.parse(dimension)
        except ValueError as e:
            return ToolResponse.error(e.message, e.code).to_text()
        dim_table, dim_col = dim_ref.table, dim_ref.column

        # Get unique dimension values
        tbl = ctx['tables'].get(dim_table)
        if not tbl:
            return ToolResponse.error(f"Table '{dim_table}' not found", PBIXMCPError.code).to_text()
        col_idx = next((i for i, c in enumerate(tbl['columns']) if c == dim_col), -1)
        if col_idx < 0:
            return ToolResponse.error(f"Column '{dim_col}' not found in '{dim_table}'", PBIXMCPError.code).to_text()

        unique_vals = list(set(row[col_idx] for row in tbl['rows'] if row[col_idx] is not None))
        unique_vals.sort(key=lambda x: str(x))

        lines = [f"DAX per {dimension} ({len(unique_vals)} values, showing {min(len(unique_vals), max_values)}):\n"]

        # Header
        header = f"{'Value':<25s}"
        for m in measure_names:
            header += f"  {m:>15s}"
        lines.append(header)
        lines.append("-" * len(header))

        for val in unique_vals[:max_values]:
            fc = dict(base_fc)
            fc[dimension] = [val]
            results = dax_engine.evaluate_measures_batch(
                measure_names, ctx['tables'], ctx['measure_defs'],
                fc, ctx['date_table'], ctx['date_column'],
                ctx.get('relationships')
            )

            row_str = f"{str(val):<25s}"
            for m in measure_names:
                v = results.get(m)
                if isinstance(v, float):
                    if abs(v) < 2 and abs(v) > 0.001:
                        row_str += f"  {v:>14.1%}"
                    else:
                        row_str += f"  {v:>15,.2f}"
                elif isinstance(v, int):
                    row_str += f"  {v:>15,}"
                elif v is None:
                    row_str += f"  {'(null)':>15s}"
                else:
                    row_str += f"  {str(v):>15s}"
            lines.append(row_str)

        return ToolResponse.ok("\n".join(lines)).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(f"{str(e)}\n{traceback.format_exc()}", e.code).to_text()


def _get_layout_pbir(work_dir: str) -> dict | None:
    """Read PBIR-format layout as a legacy-compatible structure.

    PBIR stores each visual as a separate JSON file under
    Report/definition/pages/<pageId>/visuals/<visualId>/visual.json.
    We convert these into the legacy { sections: [ { visualContainers: [...] } ] } format.
    """
    pages_json = os.path.join(work_dir, "Report", "definition", "pages", "pages.json")
    if not os.path.exists(pages_json):
        return None

    try:
        with open(pages_json, "r", encoding="utf-8") as f:
            pages_meta = json.load(f)
    except Exception:
        return None

    pages_dir = os.path.dirname(pages_json)
    sections = []

    # pages_meta is typically a list of page objects with 'id' or a directory listing
    page_dirs = []
    if isinstance(pages_meta, list):
        for pm in pages_meta:
            pid = pm.get("id") or pm.get("name", "")
            if pid:
                page_dirs.append(pid)
    elif isinstance(pages_meta, dict):
        # Single page or some other structure
        for pid in os.listdir(pages_dir):
            pdir = os.path.join(pages_dir, pid)
            if os.path.isdir(pdir) and os.path.exists(os.path.join(pdir, "visuals")):
                page_dirs.append(pid)

    for pid in page_dirs:
        visuals_dir = os.path.join(pages_dir, pid, "visuals")
        if not os.path.isdir(visuals_dir):
            continue

        containers = []
        for vid in os.listdir(visuals_dir):
            visual_json = os.path.join(visuals_dir, vid, "visual.json")
            if not os.path.exists(visual_json):
                continue
            try:
                with open(visual_json, "r", encoding="utf-8") as f:
                    vdata = json.load(f)
                # PBIR visual.json has { visual: { visualType, objects, ... } }
                # Convert to legacy format: config = { singleVisual: { ... } }
                visual_obj = vdata.get("visual", vdata)
                container = {
                    "config": json.dumps({"singleVisual": visual_obj}),
                    "x": vdata.get("position", {}).get("x", 0),
                    "y": vdata.get("position", {}).get("y", 0),
                    "width": vdata.get("position", {}).get("width", 0),
                    "height": vdata.get("position", {}).get("height", 0),
                }
                containers.append(container)
            except Exception:
                continue

        sections.append({
            "displayName": pid,
            "visualContainers": containers,
        })

    return {"sections": sections} if sections else None


def _extract_default_filters_dict(work_dir: str, page_index: int = 0) -> dict:
    """Internal: extract default slicer filters as a dict for programmatic use.

    Handles both In-type (value list) and Comparison-type (equality/range) filters.
    Returns { 'Entity.Property': [values] } suitable for use as filter_context.
    """
    layout = _get_layout(work_dir)
    if not layout:
        # Try PBIR format
        layout = _get_layout_pbir(work_dir)
    if not layout:
        return {}

    sections = layout.get("sections", [])
    if page_index < 0 or page_index >= len(sections):
        return {}

    page = sections[page_index]
    containers = page.get("visualContainers", [])
    filters = {}

    import re as _re

    def _parse_literal(lit):
        """Parse a literal value from filter JSON."""
        if lit is None:
            return None
        s = str(lit)
        # Numeric literals (possibly suffixed with D/L for double/long)
        num_match = _re.match(r'^(-?\d+(?:\.\d+)?)[DL]?$', s, _re.IGNORECASE)
        if num_match:
            return float(num_match.group(1)) if '.' in num_match.group(1) else int(num_match.group(1))
        # Datetime literals: datetime'2024-01-01T00:00:00'
        dt_match = _re.match(r"^datetime'([^']+)'$", s, _re.IGNORECASE)
        if dt_match:
            return dt_match.group(1)  # Return the ISO datetime string
        # Power BI escapes single quotes as '' in filter JSON —
        # normalize to single quotes to match actual data values
        s = s.replace("''", "'")
        if s.startswith("'") and s.endswith("'"):
            s = s[1:-1]
        return s

    def _resolve_column(col_expr, from_entries):
        """Resolve Entity.Property from a column expression and From entries."""
        source = col_expr.get("Expression", {}).get("SourceRef", {}).get("Source")
        prop = col_expr.get("Property")
        from_entry = next((f for f in from_entries if f.get("Name") == source), {})
        entity = from_entry.get("Entity")
        if entity and prop:
            return f"{entity}.{prop}"
        return None

    for vc in containers:
        config = _parse_visual_config(vc)
        sv = config.get("singleVisual", {})

        # Check for filter in objects.general
        general_arr = sv.get("objects", {}).get("general", [])
        for gen in general_arr:
            filter_obj = gen.get("properties", {}).get("filter", {}).get("filter", {})
            if not filter_obj or not filter_obj.get("Where"):
                continue

            from_entries = filter_obj.get("From", [])

            for where in filter_obj["Where"]:
                cond = where.get("Condition", {})

                # --- In-type: value list filters ---
                if "In" in cond:
                    expr = cond["In"].get("Expressions", [{}])[0]
                    values = cond["In"].get("Values", [])
                    col_expr = expr.get("Column", {})
                    key = _resolve_column(col_expr, from_entries)

                    if key and values:
                        vals = []
                        for v in values:
                            lit = v[0].get("Literal", {}).get("Value") if v else None
                            parsed = _parse_literal(lit)
                            if parsed is not None:
                                vals.append(parsed)
                        if vals:
                            filters[key] = vals

                # --- Comparison-type: equality / range filters ---
                if "Comparison" in cond:
                    comp = cond["Comparison"]
                    kind = comp.get("ComparisonKind", 0)  # 0=Equal, 1=GT, 2=GTE, 3=LT, 4=LTE
                    left = comp.get("Left", {})
                    right = comp.get("Right", {})

                    # Left side should be a column reference
                    col_expr = left.get("Column", {})
                    key = _resolve_column(col_expr, from_entries)

                    # Right side should be a literal value
                    lit = right.get("Literal", {}).get("Value")
                    parsed = _parse_literal(lit)

                    if key and parsed is not None:
                        if kind == 0:
                            # Equality: single value filter
                            filters[key] = [parsed]
                        else:
                            # Range filter (GT/GTE/LT/LTE) — store as single value
                            # for SELECTEDVALUE to work on numeric slicers
                            filters[key] = [parsed]

    return filters


def _get_all_default_filters(work_dir: str) -> dict:
    """Get default filters merged across all pages."""
    layout = _get_layout(work_dir)
    if not layout:
        layout = _get_layout_pbir(work_dir)
    if not layout:
        return {}

    all_filters = {}
    sections = layout.get("sections", [])
    for i in range(len(sections)):
        page_filters = _extract_default_filters_dict(work_dir, i)
        # Merge — later pages don't overwrite earlier ones
        for k, v in page_filters.items():
            if k not in all_filters:
                all_filters[k] = v
    return all_filters


@mcp.tool()
def pbix_get_default_filters(alias: str, page_index: int = 0) -> str:
    """Extract default slicer filter selections from a report page.

    Reads the filter config from slicer visuals (advancedSlicerVisual, slicer)
    to determine what the dashboard's default filtered state is.
    Supports both In-type (value list) and Comparison-type (equality/range) filters.

    Args:
        alias: The alias of the open file
        page_index: Zero-based page index (default 0)
    """
    try:
        info = _ensure_open(alias)
        filters = _extract_default_filters_dict(info["work_dir"], page_index)

        if not filters:
            return "No default slicer filters found on this page."

        lines = ["Default slicer filters:\n"]
        for key, vals in filters.items():
            lines.append(f"  {key}: {vals}")
        lines.append("\nUse as filter_context in pbix_evaluate_dax:")
        lines.append(f"  {json.dumps(filters)}")
        return "\n".join(lines)
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        raise LayoutParseError(f"{str(e)}\n{traceback.format_exc()}")


@mcp.tool()
def pbix_get_visual_positions(alias: str, page_index: int = 0) -> str:
    """Get all visual positions with parent group offset resolution.

    For visuals inside groups, the raw x/y coordinates are relative to the
    parent group. This tool resolves them to absolute page coordinates.

    Args:
        alias: The alias of the open file
        page_index: Zero-based page index
    """
    try:
        info = _ensure_open(alias)
        layout = _get_layout(info["work_dir"])
        if not layout:
            raise LayoutParseError("No layout found")

        sections = layout.get("sections", [])
        if page_index < 0 or page_index >= len(sections):
            raise LayoutParseError(f"Page index {page_index} out of range")

        page = sections[page_index]
        containers = page.get("visualContainers", [])

        # Pass 1: build group positions map
        group_positions = {}
        for vc in containers:
            config = _parse_visual_config(vc)
            name = config.get("name", "")
            if config.get("singleVisualGroup"):
                group_positions[name] = {"x": vc.get("x", 0), "y": vc.get("y", 0)}

        # Pass 2: resolve absolute positions
        lines = [f"Visual positions (absolute, {len(containers)} visuals):\n"]
        for i, vc in enumerate(containers):
            config = _parse_visual_config(vc)
            vtype = _get_visual_type(config)
            x = vc.get("x", 0)
            y = vc.get("y", 0)
            w = vc.get("width", 0)
            h = vc.get("height", 0)

            parent_group = config.get("parentGroupName")
            if parent_group and parent_group in group_positions:
                x += group_positions[parent_group]["x"]
                y += group_positions[parent_group]["y"]
                lines.append(f"  [{i}] {vtype:<30s} at ({x:.0f},{y:.0f}) {w:.0f}x{h:.0f}  [child of group]")
            else:
                lines.append(f"  [{i}] {vtype:<30s} at ({x:.0f},{y:.0f}) {w:.0f}x{h:.0f}")

        return ToolResponse.ok("\n".join(lines)).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        raise LayoutParseError(f"{str(e)}\n{traceback.format_exc()}")


@mcp.tool()
def pbix_clear_dax_cache(alias: str = "") -> str:
    """Clear the DAX engine data cache.

    Call this after modifying measures or table data to force fresh evaluation.

    Args:
        alias: Clear cache for specific alias, or all if empty
    """
    global _dax_cache
    if alias:
        _dax_cache.pop(alias, None)
        return ToolResponse.ok(f"DAX cache cleared for '{alias}'").to_text()
    else:
        _dax_cache.clear()
        return ToolResponse.ok("DAX cache cleared for all files").to_text()


# ---- Section 10: Calculated Columns ----

@mcp.tool()
def pbix_evaluate_calculated_columns(alias: str) -> str:
    """Evaluate all calculated columns in the data model.

    Finds columns with DAX expressions in the metadata, evaluates them
    per-row against actual table data, and adds the results to the
    cached data context. This is useful when calculated columns were
    defined but their values aren't materialized in VertiPaq.

    Args:
        alias: The alias of the open file
    """
    try:
        info = _ensure_open(alias)

        # Force re-build of DAX context with calculated columns
        global _dax_cache
        _dax_cache.pop(alias, None)
        ctx = _get_dax_context(alias)

        # Check if any calculated columns were evaluated
        dm_path = os.path.join(info["work_dir"], "DataModel")
        if not os.path.exists(dm_path):
            return ToolResponse.error("No DataModel found", DataModelCompressionError.code).to_text()

        from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
        from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

        with open(dm_path, "rb") as f:
            dm = f.read()
        abf = decompress_datamodel(dm)
        db_bytes = read_metadata_sqlite(abf)

        import sqlite3
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.write(db_bytes)
        tmp.close()
        try:
            conn = sqlite3.connect(tmp.name)
            calc_cols = conn.execute("""
                SELECT c.ExplicitName, c.Expression, t.Name
                FROM [Column] c JOIN [Table] t ON c.TableID = t.ID
                WHERE c.Expression IS NOT NULL AND c.Expression != ''
                  AND c.ExplicitName IS NOT NULL
                  AND c.ExplicitName NOT LIKE 'RowNumber%'
                  AND t.ModelID = 1
            """).fetchall()
            conn.close()
        finally:
            os.unlink(tmp.name)

        if not calc_cols:
            return ToolResponse.ok("No calculated columns found in the data model.").to_text()

        lines = [f"Calculated columns ({len(calc_cols)}):\n"]
        for cc in calc_cols:
            tname = cc[2]
            cname = cc[0]
            expr = cc[1][:60].strip().replace('\n', ' ')
            tbl = ctx['tables'].get(tname)
            if tbl and cname in tbl['columns']:
                lines.append(f"  ✅ {tname}[{cname}] = {expr}...")
            else:
                lines.append(f"  ⚠ {tname}[{cname}] = {expr}... (not evaluated)")

        return ToolResponse.ok("\n".join(lines)).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


# ---- Section 11: RLS (Row-Level Security) ----

@mcp.tool()
def pbix_get_rls_roles(alias: str) -> str:
    """Get all Row-Level Security roles and their table filter expressions.

    Returns role definitions and the DAX filter expressions that define
    what data each role can see.

    Args:
        alias: The alias of the open file
    """
    try:
        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")
        if not os.path.exists(dm_path):
            return ToolResponse.error("No DataModel found", DataModelCompressionError.code).to_text()

        from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
        from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

        with open(dm_path, "rb") as f:
            dm = f.read()
        abf = decompress_datamodel(dm)
        db_bytes = read_metadata_sqlite(abf)

        import sqlite3
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.write(db_bytes)
        tmp.close()
        try:
            conn = sqlite3.connect(tmp.name)
            conn.row_factory = sqlite3.Row

            roles = conn.execute("SELECT * FROM [Role]").fetchall()
            if not roles:
                return ToolResponse.ok("No RLS roles defined in this file.").to_text()

            lines = [f"RLS Roles ({len(roles)}):\n"]
            for role in roles:
                role_id = role["ID"]
                role_name = role["Name"] if "Name" in role.keys() else f"Role {role_id}"
                lines.append(f"  Role: {role_name} (ID={role_id})")

                # Get table permissions for this role
                perms = conn.execute(
                    "SELECT * FROM [TablePermission] WHERE RoleID = ?",
                    (role_id,)
                ).fetchall()
                for perm in perms:
                    table_id = perm["TableID"]
                    filter_expr = perm.get("FilterExpression", perm.get("QueryExpression", ""))
                    table_name = conn.execute(
                        "SELECT Name FROM [Table] WHERE ID = ?", (table_id,)
                    ).fetchone()
                    tname = table_name["Name"] if table_name else f"Table {table_id}"
                    lines.append(f"    {tname}: {filter_expr}")

                # Get role members
                members = conn.execute(
                    "SELECT * FROM [RoleMembership] WHERE RoleID = ?",
                    (role_id,)
                ).fetchall()
                if members:
                    lines.append(f"    Members: {len(members)}")

            conn.close()
            return ToolResponse.ok("\n".join(lines)).to_text()
        finally:
            os.unlink(tmp.name)
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_set_rls_role(
    alias: str,
    role_name: str,
    table_name: str,
    filter_expression: str,
    description: str = "",
) -> str:
    """Create or update a Row-Level Security role with a DAX filter expression.

    The filter expression is a DAX boolean expression that determines which
    rows are visible to the role. For example:
      'dim-Geo'[Country] = "USA"
      'Sales'[Amount] > 1000

    Args:
        alias: The alias of the open file
        role_name: Name of the RLS role (e.g., "US Sales Only")
        table_name: Table to apply the filter to
        filter_expression: DAX filter expression (e.g., 'Sales'[Region] = "West")
        description: Optional role description
    """
    try:
        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")
        if not os.path.exists(dm_path):
            return ToolResponse.error("No DataModel found", DataModelCompressionError.code).to_text()

        def _do_set(conn: sqlite3.Connection):
            conn.row_factory = sqlite3.Row
            role = conn.execute(
                "SELECT ID FROM [Role] WHERE Name = ?", (role_name,)
            ).fetchone()
            if role:
                role_id = role["ID"]
            else:
                max_id = conn.execute("SELECT MAX(ID) FROM [Role]").fetchone()[0] or 0
                role_id = max_id + 1
                conn.execute(
                    "INSERT INTO [Role] (ID, ModelID, Name, Description) VALUES (?, 1, ?, ?)",
                    (role_id, role_name, description),
                )

            table_row = conn.execute(
                "SELECT ID FROM [Table] WHERE Name = ? AND ModelID = 1",
                (table_name,)
            ).fetchone()
            if not table_row:
                raise ValueError(f"Table '{table_name}' not found")
            table_id = table_row["ID"]

            existing = conn.execute(
                "SELECT ID FROM [TablePermission] WHERE RoleID = ? AND TableID = ?",
                (role_id, table_id)
            ).fetchone()
            if existing:
                conn.execute(
                    "UPDATE [TablePermission] SET FilterExpression = ? WHERE ID = ?",
                    (filter_expression, existing["ID"]),
                )
            else:
                max_perm = conn.execute("SELECT MAX(ID) FROM [TablePermission]").fetchone()[0] or 0
                conn.execute(
                    "INSERT INTO [TablePermission] (ID, RoleID, TableID, FilterExpression) VALUES (?, ?, ?, ?)",
                    (max_perm + 1, role_id, table_id, filter_expression),
                )
            conn.commit()

        old_size, new_size = _modify_metadata_only(dm_path, _do_set)
        info["modified"] = True
        return ToolResponse.ok(f"RLS role '{role_name}' set on '{table_name}' with filter: {filter_expression}").to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_evaluate_rls(
    alias: str,
    role_name: str,
    table_name: str,
    max_rows: int = 10,
) -> str:
    """Evaluate an RLS role's filter and show which rows would be visible.

    Uses the DAX engine to evaluate the role's filter expression against
    actual table data.

    Args:
        alias: The alias of the open file
        role_name: Name of the RLS role to evaluate
        table_name: Table to check visibility for
        max_rows: Maximum rows to show (default 10)
    """
    try:
        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")
        if not os.path.exists(dm_path):
            return ToolResponse.error("No DataModel found", DataModelCompressionError.code).to_text()

        from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
        from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

        with open(dm_path, "rb") as f:
            dm = f.read()
        abf = decompress_datamodel(dm)
        db_bytes = read_metadata_sqlite(abf)

        import sqlite3
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.write(db_bytes)
        tmp.close()
        try:
            conn = sqlite3.connect(tmp.name)
            conn.row_factory = sqlite3.Row

            role = conn.execute("SELECT ID FROM [Role] WHERE Name = ?", (role_name,)).fetchone()
            if not role:
                conn.close()
                return ToolResponse.error(f"Role '{role_name}' not found", PBIXMCPError.code).to_text()

            table_row = conn.execute(
                "SELECT ID FROM [Table] WHERE Name = ? AND ModelID = 1", (table_name,)
            ).fetchone()
            if not table_row:
                conn.close()
                return ToolResponse.error(f"Table '{table_name}' not found", PBIXMCPError.code).to_text()

            perm = conn.execute(
                "SELECT FilterExpression FROM [TablePermission] WHERE RoleID = ? AND TableID = ?",
                (role["ID"], table_row["ID"])
            ).fetchone()
            conn.close()

            if not perm or not perm["FilterExpression"]:
                return ToolResponse.ok(f"Role '{role_name}' has no filter on table '{table_name}' — all rows visible.").to_text()

            filter_expr = perm["FilterExpression"]

            # Load table data and evaluate filter
            ctx = _get_dax_context(alias)
            tbl = ctx['tables'].get(table_name)
            if not tbl:
                return ToolResponse.error(f"Table '{table_name}' has no data", PBIXMCPError.code).to_text()

            from pbix_mcp.dax import engine as dax_engine
            eng = dax_engine.DAXEngine()

            total = len(tbl['rows'])
            visible = 0
            sample_rows = []

            for row in tbl['rows']:
                # Build row context
                row_expr = filter_expr
                for ci, cn in enumerate(tbl['columns']):
                    val = row[ci]
                    for pat in [f"'{table_name}'[{cn}]", f"{table_name}[{cn}]"]:
                        if pat in row_expr:
                            if isinstance(val, str):
                                row_expr = row_expr.replace(pat, f'"{val}"')
                            elif val is None:
                                row_expr = row_expr.replace(pat, 'BLANK()')
                            else:
                                row_expr = row_expr.replace(pat, str(val))

                eval_ctx = dax_engine.DAXContext(
                    ctx['tables'], ctx['measure_defs'], None, None, None,
                    ctx.get('relationships', [])
                )
                result = eng._eval_expr(row_expr, eval_ctx)
                if result is True or result == 1:
                    visible += 1
                    if len(sample_rows) < max_rows:
                        sample_rows.append({tbl['columns'][i]: row[i] for i in range(min(5, len(tbl['columns'])))})

            lines = [
                f"RLS evaluation for role '{role_name}' on '{table_name}':",
                f"  Filter: {filter_expr}",
                f"  Visible: {visible}/{total} rows ({visible/total*100:.1f}%)\n",
            ]
            if sample_rows:
                lines.append(f"  Sample visible rows (first {len(sample_rows)}):")
                for sr in sample_rows:
                    lines.append(f"    {sr}")

            return ToolResponse.ok("\n".join(lines)).to_text()
        finally:
            os.unlink(tmp.name)
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


# ---- Section 11: Diagnostics ----

@mcp.tool()
def pbix_get_password(alias: str) -> str:
    """Extract embedded passwords from a PBIX file.

    Scans the data model for password-like tables (tables with 'password'
    in the name) and DAX measures that reference them (ISFILTERED, SELECTEDVALUE).
    Extracts the expected password value from the DAX expression.

    This is useful for dashboards that use a password-slicer gate pattern
    where the report is locked until the correct password is entered.

    Args:
        alias: The alias of the open file
    """
    try:
        info = _ensure_open(alias)
        from pbix_mcp.formats.model_reader import ModelReader
        model = ModelReader(info["path"], work_dir=info.get("work_dir"))

        results = []

        # Strategy 1: Find tables with 'password' in the name and read their data
        schema = model.schema
        table_names = sorted(set(r['TableName'] for r in schema))
        for tname in table_names:
            if "password" in tname.lower():
                try:
                    td = model.get_table(tname)
                    if td and td.get('columns') and td.get('rows'):
                        # Get unique values per column
                        for ci, col in enumerate(td['columns']):
                            vals = sorted(set(
                                row[ci] for row in td['rows']
                                if ci < len(row) and row[ci] is not None
                            ), key=str)
                            if vals:
                                results.append(f"Table '{tname}', column '{col}': {len(vals)} values")
                                for v in vals[:10]:
                                    results.append(f"  {v}")
                                if len(vals) > 10:
                                    results.append(f"  ... and {len(vals) - 10} more")
                except Exception:
                    pass

        # Strategy 2: Find DAX measures that check passwords
        measures_list = model.dax_measures
        if measures_list:
            import re as _re
            for m_row in measures_list:
                expr = m_row.get("Expression", "")
                name = m_row.get("Name", "")
                if not expr:
                    continue
                # Look for SELECTEDVALUE(...[...]) = "value" patterns near password context
                for m in _re.finditer(
                    r"""SELECTEDVALUE\s*\(\s*'?([^')]+)'?\s*\[([^\]]+)\]\s*\)\s*=\s*["']([^"']+)["']""",
                    expr, _re.IGNORECASE
                ):
                    table = m.group(1).strip()
                    column = m.group(2).strip()
                    password = m.group(3)
                    if "password" in table.lower() or "password" in column.lower() or "password" in name.lower():
                        results.append(f"  >>> PASSWORD: \"{password}\"  (from SELECTEDVALUE('{table}'[{column}]) in measure '{name}')")

                # Also look for hardcoded password strings near password context
                skip_words = {"correct", "wrong", "true", "false", "password",
                              "enjoy", "dashboard", "filter", "warning", "error",
                              "selected", "value", "blank"}
                for m in _re.finditer(r'''["']([^"'\n]{3,30})["']''', expr):
                    candidate = m.group(1).strip()
                    if candidate.lower() in skip_words:
                        continue
                    # Only flag if near a password-related context
                    context_start = max(0, m.start() - 200)
                    context = expr[context_start:m.end()].lower()
                    if "password" in context:
                        # Skip obvious UI text
                        if any(w in candidate.lower() for w in ["correct", "wrong", "enjoy", "⚠", "✔"]):
                            continue
                        results.append(f"  Candidate in measure '{name}': \"{candidate}\"")

        if not results:
            return ToolResponse.ok("No password tables or password-checking measures found in this file.").to_text()

        return ToolResponse.ok("Password analysis:\n" + "\n".join(results)).to_text()

    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_doctor(alias: str) -> str:
    """Run comprehensive diagnostics on an open PBIX/PBIT file.

    Performs a full health check across every layer of the file:
    ZIP structure, report layout, DataModel compression, ABF archive,
    SQLite metadata, data source connections, storage modes,
    VertiPaq column data, relationships, measures, calculated tables,
    RLS roles, and slicer filters.

    Args:
        alias: The alias of the open file
    """
    checks = []

    def _check(name, fn):
        try:
            result = fn()
            checks.append(f"  ✅ {name}: {result}")
            return True
        except Exception as e:
            checks.append(f"  ❌ {name}: {e}")
            return False

    try:
        info = _ensure_open(alias)
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        raise SessionError(f"Doctor failed: {e}")

    checks.append(f"Diagnostics for '{alias}':\n")

    # 1. File basics
    _check("File exists", lambda: f"{os.path.getsize(info['path']):,} bytes" if os.path.exists(info['path']) else "MISSING")
    _check("File type", lambda: "PBIT" if info.get("is_pbit") else "PBIX")

    # 2. Layout
    def check_layout():
        layout = _get_layout(info["work_dir"])
        if layout:
            pages = len(layout.get("sections", []))
            return f"{pages} pages (legacy format)"
        pbir = _get_layout_pbir(info["work_dir"])
        if pbir:
            pages = len(pbir.get("sections", []))
            return f"{pages} pages (PBIR format)"
        return "No layout found"
    _check("Report layout", check_layout)

    # --- Decompress DataModel ONCE for all subsequent checks ---
    dm_path = os.path.join(info["work_dir"], "DataModel")
    abf_data = None
    abf_files = None
    db_conn = None
    db_tmp_path = None

    def _init_datamodel():
        nonlocal abf_data, abf_files, db_conn, db_tmp_path
        if abf_data is not None:
            return
        import tempfile

        from pbix_mcp.formats.abf_rebuild import list_abf_files, read_metadata_sqlite
        from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel
        with open(dm_path, "rb") as f:
            dm = f.read()
        abf_data = decompress_datamodel(dm)
        abf_files = list_abf_files(abf_data)
        db_bytes = read_metadata_sqlite(abf_data)
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.write(db_bytes)
        tmp.close()
        db_tmp_path = tmp.name
        db_conn = sqlite3.connect(db_tmp_path)

    try:
        # 3. DataModel
        def check_datamodel():
            if not os.path.exists(dm_path):
                return "NOT FOUND"
            size = os.path.getsize(dm_path)
            _init_datamodel()
            return f"{size:,} bytes compressed, {len(abf_data):,} bytes decompressed"
        _check("DataModel (XPress9)", check_datamodel)

        # 4. ABF contents
        def check_abf():
            _init_datamodel()
            return f"{len(abf_files)} internal files"
        _check("ABF archive", check_abf)

        # 5. SQLite metadata
        def check_sqlite():
            _init_datamodel()
            tables = db_conn.execute("SELECT COUNT(*) FROM [Table] WHERE ModelID=1").fetchone()[0]
            measures = db_conn.execute("SELECT COUNT(*) FROM [Measure]").fetchone()[0]
            rels = db_conn.execute("SELECT COUNT(*) FROM [Relationship]").fetchone()[0]
            return f"{tables} tables, {measures} measures, {rels} relationships"
        _check("Metadata SQLite", check_sqlite)

        # 6. Data sources & storage modes
        def check_data_sources():
            _init_datamodel()
            c = db_conn.cursor()
            mode_names = {0: "Import", 1: "DirectQuery", 2: "Dual"}
            results = []
            c.execute("""SELECT t.Name, p.Mode, SUBSTR(p.QueryDefinition, 1, 60)
                         FROM Partition p JOIN [Table] t ON p.TableID = t.ID
                         WHERE t.Name NOT LIKE 'H$%' AND t.Name NOT LIKE 'R$%'
                         AND p.QueryDefinition IS NOT NULL
                         ORDER BY t.Name""")
            modes = set()
            sources = set()
            for row in c.fetchall():
                tname, mode, qd = row
                mode_str = mode_names.get(mode, f"Unknown({mode})")
                modes.add(mode_str)
                if qd:
                    if "PostgreSQL.Database" in qd:
                        sources.add("PostgreSQL")
                    elif "MySQL.Database" in qd:
                        sources.add("MySQL")
                    elif "Sql.Database" in qd:
                        sources.add("SQL Server")
                    elif "Odbc.DataSource" in qd:
                        sources.add("ODBC")
                    elif "Excel.Workbook" in qd:
                        sources.add("Excel")
                    elif "Web.Contents" in qd:
                        sources.add("Web/JSON")
                    elif "#table(" in qd:
                        sources.add("Embedded (Import)")
                    else:
                        sources.add("Other M expression")
                results.append(f"    {tname}: {mode_str}")
            summary = f"Modes: {', '.join(sorted(modes))} | Sources: {', '.join(sorted(sources))}"
            return summary + "\n" + "\n".join(results)
        _check("Data sources & storage modes", check_data_sources)

        # 7. Per-table column breakdown
        def check_columns():
            _init_datamodel()
            c = db_conn.cursor()
            c.execute("""SELECT t.Name, COUNT(*) as cols,
                         GROUP_CONCAT(DISTINCT CASE col.ExplicitDataType
                             WHEN 2 THEN 'String' WHEN 6 THEN 'Int64' WHEN 8 THEN 'Double'
                             WHEN 9 THEN 'DateTime' WHEN 10 THEN 'Decimal' WHEN 11 THEN 'Boolean'
                             ELSE 'Type' || col.ExplicitDataType END)
                         FROM [Column] col JOIN [Table] t ON col.TableID = t.ID
                         WHERE t.Name NOT LIKE 'H$%' AND t.Name NOT LIKE 'R$%'
                         AND col.Type = 1
                         GROUP BY t.Name ORDER BY t.Name""")
            lines = []
            total_cols = 0
            for row in c.fetchall():
                tname, ncols, types = row
                total_cols += ncols
                lines.append(f"    {tname}: {ncols} columns ({types})")
            return f"{total_cols} total data columns\n" + "\n".join(lines)
        _check("Column breakdown", check_columns)

        # 8. VertiPaq table data (row counts from ColumnStorage metadata)
        def check_tables():
            _init_datamodel()
            c = db_conn.cursor()
            c.execute("""SELECT t.Name,
                         MAX(cs.Statistics_RowCount) as row_count
                         FROM [Table] t
                         JOIN [Column] col ON col.TableID = t.ID
                         LEFT JOIN ColumnStorage cs ON cs.ColumnID = col.ID
                         WHERE t.Name NOT LIKE 'H$%' AND t.Name NOT LIKE 'R$%'
                         AND col.Type = 1
                         GROUP BY t.Name ORDER BY t.Name""")
            lines = []
            total_rows = 0
            table_count = 0
            for row in c.fetchall():
                tname, rcount = row
                rcount = rcount or 0
                total_rows += rcount
                table_count += 1
                lines.append(f"    {tname}: {rcount:,} rows")
            return f"{table_count} tables, {total_rows:,} total rows\n" + "\n".join(lines)
        _check("VertiPaq data (row counts)", check_tables)

        # 9. Relationships
        def check_relationships():
            _init_datamodel()
            c = db_conn.cursor()
            c.execute("""SELECT ft.Name, fc.ExplicitName, tt.Name, tc.ExplicitName, r.IsActive
                         FROM Relationship r
                         JOIN [Table] ft ON r.FromTableID = ft.ID
                         JOIN [Column] fc ON r.FromColumnID = fc.ID
                         JOIN [Table] tt ON r.ToTableID = tt.ID
                         JOIN [Column] tc ON r.ToColumnID = tc.ID""")
            lines = []
            for row in c.fetchall():
                active = "active" if row[4] else "inactive"
                lines.append(f"    {row[0]}.{row[1]} → {row[2]}.{row[3]} ({active})")
            if not lines:
                return "None"
            return f"{len(lines)} relationships\n" + "\n".join(lines)
        _check("Relationships", check_relationships)

        # 10. Measures
        def check_measures():
            _init_datamodel()
            c = db_conn.cursor()
            c.execute("""SELECT t.Name, m.Name, m.Expression
                         FROM Measure m JOIN [Table] t ON m.TableID = t.ID""")
            lines = []
            for row in c.fetchall():
                expr = row[2][:40] + "..." if len(row[2]) > 40 else row[2]
                lines.append(f"    [{row[0]}] {row[1]} = {expr}")
            if not lines:
                return "None"
            return f"{len(lines)} measures\n" + "\n".join(lines)
        _check("DAX measures", check_measures)

        # 11. RLS roles
        def check_rls():
            _init_datamodel()
            c = db_conn.cursor()
            c.execute("SELECT COUNT(*) FROM Role WHERE ModelID=1")
            count = c.fetchone()[0]
            return f"{count} roles" if count else "None"
        _check("Row-Level Security (RLS)", check_rls)

        # 12. Calculated tables (detected via partition type or expression)
        def check_calc():
            _init_datamodel()
            c = db_conn.cursor()
            c.execute("""SELECT t.Name FROM [Table] t
                         JOIN Partition p ON p.TableID = t.ID
                         WHERE t.Name NOT LIKE 'H$%' AND t.Name NOT LIKE 'R$%'
                         AND (p.Type = 4 OR (p.QueryDefinition IS NOT NULL
                              AND p.QueryDefinition NOT LIKE '%#table(%'))
                         AND p.QueryDefinition LIKE '%=%'""")
            calc = [r[0] for r in c.fetchall()]
            return f"{len(calc)} calculated tables" if calc else "None"
        _check("Calculated tables", check_calc)

        # 13. Default slicer filters
        def check_filters():
            filters = _get_all_default_filters(info["work_dir"])
            if filters:
                return f"{len(filters)} default slicer filters"
            return "None"
        _check("Default slicer filters", check_filters)

        # 14. Tables without VertiPaq storage (metadata exists, ABF files missing)
        def check_tables_have_storage():
            _init_datamodel()
            c = db_conn.cursor()
            c.execute("""SELECT t.ID, t.Name FROM [Table] t
                         WHERE t.Name NOT LIKE 'H$%' AND t.Name NOT LIKE 'R$%'
                         AND t.ModelID = 1""")
            abf_paths = [f.get("Path", "") for f in abf_files] if abf_files else []
            abf_str = "\n".join(abf_paths)
            missing = []
            for row in c.fetchall():
                tid, tname = row
                # Check if any ABF file references this table's ID
                # Table data files use pattern: TableName (ID).tbl\...
                marker = f"{tname} ({tid}).tbl"
                if marker not in abf_str:
                    missing.append(tname)
            if missing:
                raise ValueError(
                    f"{len(missing)} table(s) in metadata have NO VertiPaq storage — "
                    f"PBI will crash (TMCacheManager): {', '.join(missing)}"
                )
            return "All metadata tables have VertiPaq storage"
        _check("Table/storage consistency", check_tables_have_storage)

        # 15. Orphaned foreign key references
        def check_orphaned_refs():
            _init_datamodel()
            c = db_conn.cursor()
            issues = []
            # Table.RefreshPolicyID → RefreshPolicy.ID
            c.execute("""SELECT t.Name, t.RefreshPolicyID FROM [Table] t
                         WHERE t.RefreshPolicyID IS NOT NULL AND t.RefreshPolicyID != 0
                         AND t.RefreshPolicyID NOT IN (SELECT ID FROM RefreshPolicy)""")
            for row in c.fetchall():
                issues.append(f"Table '{row[0]}' → missing RefreshPolicy ID {row[1]}")
            # Table.CalculationGroupID → CalculationGroup.ID
            c.execute("""SELECT t.Name, t.CalculationGroupID FROM [Table] t
                         WHERE t.CalculationGroupID IS NOT NULL AND t.CalculationGroupID != 0
                         AND t.CalculationGroupID NOT IN (SELECT ID FROM CalculationGroup)""")
            for row in c.fetchall():
                issues.append(f"Table '{row[0]}' → missing CalculationGroup ID {row[1]}")
            # CalculationGroup.TableID → Table.ID
            c.execute("""SELECT cg.ID, cg.TableID FROM CalculationGroup cg
                         WHERE cg.TableID NOT IN (SELECT ID FROM [Table])""")
            for row in c.fetchall():
                issues.append(f"CalculationGroup {row[0]} → missing Table ID {row[1]}")
            if issues:
                raise ValueError(
                    f"{len(issues)} orphaned reference(s) — PBI will reject file:\n    "
                    + "\n    ".join(issues)
                )
            return "No orphaned references"
        _check("Metadata referential integrity", check_orphaned_refs)

        # 16. Expression rows without DataMashup
        def check_expressions():
            _init_datamodel()
            c = db_conn.cursor()
            c.execute("SELECT COUNT(*) FROM Expression")
            expr_count = c.fetchone()[0]
            if expr_count > 0:
                mashup_path = os.path.join(info["work_dir"], "DataMashup")
                if not os.path.exists(mashup_path):
                    raise ValueError(
                        f"{expr_count} Expression row(s) in metadata but no DataMashup — "
                        f"PBI will reject with PFE_TM_ENUM_VALUES_VALIDATION_FAILED"
                    )
            return f"{expr_count} expressions (DataMashup present)" if expr_count else "None"
        _check("Expression/DataMashup consistency", check_expressions)

        # 17. MAXID consistency
        def check_maxid():
            _init_datamodel()
            c = db_conn.cursor()
            c.execute("SELECT Value FROM DBPROPERTIES WHERE Name = 'MAXID'")
            row = c.fetchone()
            if not row:
                raise ValueError("MAXID not found in DBPROPERTIES")
            maxid = int(row[0])
            # Find actual max ID across all object tables
            actual_max = 0
            for tbl in ("Table", "Column", "Measure", "Partition", "Relationship",
                        "Role", "CalculationGroup", "CalculationItem"):
                try:
                    c.execute(f"SELECT MAX(ID) FROM [{tbl}]")
                    r = c.fetchone()
                    if r and r[0]:
                        actual_max = max(actual_max, r[0])
                except Exception:
                    pass
            if maxid < actual_max:
                raise ValueError(
                    f"MAXID={maxid} but highest object ID is {actual_max} — "
                    f"PBI will crash with TMCCollectionObject::Add assertion"
                )
            return f"MAXID={maxid} (highest ID={actual_max})"
        _check("MAXID consistency", check_maxid)

    finally:
        # Clean up shared resources
        if db_conn:
            db_conn.close()
        if db_tmp_path and os.path.exists(db_tmp_path):
            os.unlink(db_tmp_path)

    return ToolResponse.ok("\n".join(checks)).to_text()


# ---- Section 10b: TMDL Export ----


def _tmdl_escape(value: str) -> str:
    """Escape a string value for TMDL format."""
    if not value:
        return ""
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _export_tmdl_from_sqlite(conn: sqlite3.Connection, output_dir: str) -> dict:
    """Export metadata SQLite to TMDL folder structure.

    Returns dict with counts of exported objects.
    """
    c = conn.cursor()
    stats = {"tables": 0, "columns": 0, "measures": 0, "relationships": 0, "roles": 0}

    # ---- database.tmdl ----
    c.execute("SELECT Name, Culture FROM Model LIMIT 1")
    model_row = c.fetchone()
    db_name = model_row[0] if model_row else "Model"
    compat = 1567  # Default PBI compatibility level

    with open(os.path.join(output_dir, "database.tmdl"), "w", encoding="utf-8") as f:
        f.write(f"database {db_name}\n")
        f.write(f"\tcompatibilityLevel: {compat}\n")

    # ---- model.tmdl ----
    culture = model_row[1] if model_row and model_row[1] else "en-US"

    with open(os.path.join(output_dir, "model.tmdl"), "w", encoding="utf-8") as f:
        f.write("model Model\n")
        f.write(f"\tculture: {culture}\n")

    # ---- tables/ ----
    tables_dir = os.path.join(output_dir, "tables")
    os.makedirs(tables_dir, exist_ok=True)

    c.execute("SELECT ID, Name, Description, IsHidden FROM [Table] ORDER BY ID")
    tables = c.fetchall()

    for table_id, table_name, table_desc, is_hidden in tables:
        # Skip internal system tables (H$=hierarchy, R$=relationship, U$=user hierarchy)
        if table_name.startswith(("H$", "R$", "U$")):
            continue
        lines = [f"table '{_tmdl_escape(table_name)}'"]
        if table_desc:
            lines.append(f"\tdescription: {table_desc}")
        if is_hidden:
            lines.append("\tisHidden")
        lines.append("")

        # Columns
        c.execute(
            "SELECT ExplicitName, InferredName, ExplicitDataType, InferredDataType, "
            "IsHidden, IsKey, SourceColumn, Expression, FormatString, Description, Type "
            "FROM [Column] WHERE TableID = ? ORDER BY ID",
            (table_id,)
        )
        _dtype_map = {
            2: "string", 6: "int64", 8: "double", 9: "dateTime", 10: "decimal", 11: "boolean"
        }
        for col in c.fetchall():
            col_name = col[0] or col[1] or "?"
            dtype_id = col[2] if col[2] else (col[3] if col[3] else 2)
            dtype = _dtype_map.get(dtype_id, "string")
            is_col_hidden = col[4]
            is_key = col[5]
            source_col = col[6]
            expression = col[7]
            fmt_str = col[8]
            col_desc = col[9]
            col_type = col[10]  # 1=data, 2=calculated, 3=rowNumber

            if col_type == 3:
                continue  # Skip RowNumber system columns

            if expression and col_type == 2:
                lines.append(f"\tcolumn '{_tmdl_escape(col_name)}' = {expression}")
            else:
                lines.append(f"\tcolumn '{_tmdl_escape(col_name)}'")

            lines.append(f"\t\tdataType: {dtype}")
            if source_col:
                lines.append(f"\t\tsourceColumn: {source_col}")
            if is_col_hidden:
                lines.append("\t\tisHidden")
            if is_key:
                lines.append("\t\tisKey")
            if fmt_str:
                lines.append(f"\t\tformatString: {fmt_str}")
            if col_desc:
                lines.append(f"\t\tdescription: {col_desc}")
            lines.append("")
            stats["columns"] += 1

        # Measures
        c.execute(
            "SELECT Name, Expression, FormatString, Description, IsHidden, DisplayFolder "
            "FROM Measure WHERE TableID = ? ORDER BY ID",
            (table_id,)
        )
        for meas in c.fetchall():
            m_name, m_expr, m_fmt, m_desc, m_hidden, m_folder = meas
            lines.append(f"\tmeasure '{_tmdl_escape(m_name)}' = {m_expr}")
            if m_fmt:
                lines.append(f"\t\tformatString: {m_fmt}")
            if m_desc:
                lines.append(f"\t\tdescription: {m_desc}")
            if m_hidden:
                lines.append("\t\tisHidden")
            if m_folder:
                lines.append(f"\t\tdisplayFolder: {m_folder}")
            lines.append("")
            stats["measures"] += 1

        # Partitions
        c.execute(
            "SELECT Name, QueryDefinition, Mode, Type FROM [Partition] "
            "WHERE TableID = ? ORDER BY ID",
            (table_id,)
        )
        for part in c.fetchall():
            p_name, p_query, p_mode, p_type = part
            if p_query:
                mode_str = "directQuery" if p_mode == 1 else "import"
                if p_type == 4:
                    # Calculated partition
                    lines.append(f"\tpartition '{_tmdl_escape(p_name)}' = calculated")
                else:
                    lines.append(f"\tpartition '{_tmdl_escape(p_name)}' = m")
                    lines.append(f"\t\tmode: {mode_str}")
                lines.append("\t\tsource =")
                for qline in p_query.split("\n"):
                    lines.append(f"\t\t\t{qline}")
                lines.append("")

        # Write table TMDL
        safe_name = table_name.replace("/", "_").replace("\\", "_").replace(":", "_")
        with open(os.path.join(tables_dir, f"{safe_name}.tmdl"), "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        stats["tables"] += 1

    # ---- relationships.tmdl ----
    c.execute(
        "SELECT r.Name, r.IsActive, r.CrossFilteringBehavior, "
        "ft.Name, fc.ExplicitName, tt.Name, tc.ExplicitName "
        "FROM [Relationship] r "
        "JOIN [Table] ft ON r.FromTableID = ft.ID "
        "JOIN [Column] fc ON r.FromColumnID = fc.ID "
        "JOIN [Table] tt ON r.ToTableID = tt.ID "
        "JOIN [Column] tc ON r.ToColumnID = tc.ID "
        "ORDER BY r.ID"
    )
    rels = c.fetchall()
    if rels:
        lines = []
        for rel in rels:
            r_name, is_active, cross_filter, from_tbl, from_col, to_tbl, to_col = rel
            lines.append(f"relationship {r_name or ''}")
            lines.append(f"\tfromColumn: '{_tmdl_escape(from_tbl)}'.'{_tmdl_escape(from_col)}'")
            lines.append(f"\ttoColumn: '{_tmdl_escape(to_tbl)}'.'{_tmdl_escape(to_col)}'")
            if not is_active:
                lines.append("\tisActive: false")
            cfb_map = {0: "oneDirection", 1: "bothDirections", 2: "automatic"}
            if cross_filter in cfb_map:
                lines.append(f"\tcrossFilteringBehavior: {cfb_map[cross_filter]}")
            lines.append("")
            stats["relationships"] += 1

        with open(os.path.join(output_dir, "relationships.tmdl"), "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")

    # ---- roles/ ----
    c.execute("SELECT ID, Name, Description FROM Role ORDER BY ID")
    roles = c.fetchall()
    if roles:
        roles_dir = os.path.join(output_dir, "roles")
        os.makedirs(roles_dir, exist_ok=True)
        for role_id, role_name, role_desc in roles:
            lines = [f"role '{_tmdl_escape(role_name)}'"]
            if role_desc:
                lines.append(f"\tdescription: {role_desc}")

            c.execute(
                "SELECT t.Name, tp.FilterExpression FROM TablePermission tp "
                "JOIN [Table] t ON tp.TableID = t.ID "
                "WHERE tp.RoleID = ? ORDER BY tp.ID",
                (role_id,)
            )
            for tbl_name, filter_expr in c.fetchall():
                lines.append(f"\ttablePermission '{_tmdl_escape(tbl_name)}'")
                if filter_expr:
                    lines.append(f"\t\tfilterExpression: {filter_expr}")
            lines.append("")

            safe_name = role_name.replace("/", "_").replace("\\", "_")
            with open(os.path.join(roles_dir, f"{safe_name}.tmdl"), "w", encoding="utf-8") as f:
                f.write("\n".join(lines) + "\n")
            stats["roles"] += 1

    return stats


@mcp.tool()
def pbix_set_incremental_refresh(
    alias: str,
    table_name: str,
    archive_periods: int = 36,
    archive_granularity: str = "month",
    refresh_periods: int = 12,
    refresh_granularity: str = "month",
    detect_changes_column: str = "",
    mode: str = "import",
) -> str:
    """Configure incremental refresh policy for a table.

    Incremental refresh partitions a table by date range so only recent
    data is refreshed, dramatically reducing refresh time for large datasets.

    Requires the table's M expression to filter on RangeStart/RangeEnd parameters.
    These DateTime parameters are automatically created if they don't exist.

    Args:
        alias: The alias of the open file
        table_name: Table to apply the refresh policy to
        archive_periods: Number of periods to keep as historical (default 36)
        archive_granularity: Granularity for archive window — "day", "month",
                             "quarter", or "year" (default "month")
        refresh_periods: Number of periods to refresh each time (default 12)
        refresh_granularity: Granularity for refresh window — "day", "month",
                             "quarter", or "year" (default "month")
        detect_changes_column: Optional column name for change detection
                               (e.g. "ModifiedDate"). If set, only partitions
                               where this column changed will be refreshed.
        mode: "import" (default) or "hybrid". Hybrid adds a DirectQuery
              partition for real-time data on top of import partitions.
    """
    try:
        _GRAN_MAP = {"day": 1, "month": 2, "quarter": 3, "year": 4}
        _MODE_MAP = {"import": 0, "hybrid": 1}

        if archive_granularity not in _GRAN_MAP:
            raise ValueError(f"archive_granularity must be one of {list(_GRAN_MAP.keys())}")
        if refresh_granularity not in _GRAN_MAP:
            raise ValueError(f"refresh_granularity must be one of {list(_GRAN_MAP.keys())}")
        if mode not in _MODE_MAP:
            raise ValueError("mode must be 'import' or 'hybrid'")

        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")
        if not os.path.exists(dm_path):
            return ToolResponse.error("No DataModel found.", DataModelCompressionError.code).to_text()

        # Incremental refresh requires a DataMashup with M expressions that
        # filter on RangeStart/RangeEnd.  Without it, PBI rejects the file.
        mashup_path = os.path.join(info["work_dir"], "DataMashup")
        if not os.path.exists(mashup_path):
            return ToolResponse.error(
                "Incremental refresh requires a DataMashup section with M expressions "
                "that filter on RangeStart/RangeEnd parameters. This file has no "
                "DataMashup (it uses embedded data). Use source_csv or source_db when "
                "creating tables to enable incremental refresh.",
                "INVALID_OPERATION"
            ).to_text()

        policy_info = {}

        def _do_set(conn: sqlite3.Connection):
            c = conn.cursor()

            # Find table
            c.execute("SELECT ID FROM [Table] WHERE Name = ?", (table_name,))
            trow = c.fetchone()
            if not trow:
                raise ValueError(f"Table '{table_name}' not found")
            table_id = trow[0]

            # Only insert RangeStart/RangeEnd expressions if the file has a DataMashup.
            # Without a DataMashup, Expression rows cause PBI to reject the file with
            # PFE_TM_ENUM_VALUES_VALIDATION_FAILED because the expressions have no
            # corresponding M query section to resolve against.
            has_mashup = os.path.exists(os.path.join(info["work_dir"], "DataMashup"))
            if has_mashup:
                for param_name in ("RangeStart", "RangeEnd"):
                    c.execute("SELECT ID FROM Expression WHERE Name = ?", (param_name,))
                    if not c.fetchone():
                        c.execute("SELECT COALESCE(MAX(ID), 0) + 1 FROM Expression")
                        expr_id = c.fetchone()[0]
                        # Kind=1 = M expression (parameters are M expressions with meta annotations)
                        c.execute(
                            "INSERT INTO Expression (ID, ModelID, Name, Kind, "
                            "Expression, ModifiedTime) "
                            "VALUES (?, 1, ?, 1, ?, datetime('now'))",
                            (expr_id, param_name,
                             '#datetime(2020, 1, 1, 0, 0, 0) meta [IsParameterQuery=true, '
                             'Type="DateTime", IsParameterQueryRequired=true]')
                        )

            # Build polling expression for change detection
            polling_expr = ""
            if detect_changes_column:
                polling_expr = (
                    f"let\n"
                    f"    Source = {table_name},\n"
                    f"    MaxDate = List.Max(Source[{detect_changes_column}])\n"
                    f"in\n"
                    f"    MaxDate"
                )

            # Check for existing policy
            c.execute(
                "SELECT ID FROM RefreshPolicy WHERE TableID = ?",
                (table_id,)
            )
            existing = c.fetchone()

            if existing:
                # Update existing policy
                policy_id = existing[0]
                c.execute(
                    "UPDATE RefreshPolicy SET "
                    "PolicyType=1, RollingWindowGranularity=?, RollingWindowPeriods=?, "
                    "IncrementalGranularity=?, IncrementalPeriods=?, "
                    "IncrementalPeriodsOffset=?, PollingExpression=?, Mode=? "
                    "WHERE ID=?",
                    (_GRAN_MAP[archive_granularity], archive_periods,
                     _GRAN_MAP[refresh_granularity], refresh_periods,
                     -1 if mode == "hybrid" else 0,
                     polling_expr, _MODE_MAP[mode], policy_id)
                )
            else:
                # Create new policy
                c.execute("SELECT COALESCE(MAX(ID), 0) + 1 FROM RefreshPolicy")
                policy_id = c.fetchone()[0]
                c.execute(
                    "INSERT INTO RefreshPolicy (ID, TableID, PolicyType, "
                    "RollingWindowGranularity, RollingWindowPeriods, "
                    "IncrementalGranularity, IncrementalPeriods, "
                    "IncrementalPeriodsOffset, PollingExpression, Mode) "
                    "VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?)",
                    (policy_id, table_id,
                     _GRAN_MAP[archive_granularity], archive_periods,
                     _GRAN_MAP[refresh_granularity], refresh_periods,
                     -1 if mode == "hybrid" else 0,
                     polling_expr, _MODE_MAP[mode])
                )

            # Link table to policy
            c.execute(
                "UPDATE [Table] SET RefreshPolicyID = ? WHERE ID = ?",
                (policy_id, table_id)
            )

            conn.commit()
            policy_info["policy_id"] = policy_id
            policy_info["mode"] = mode

        old_size, new_size = _modify_metadata_only(dm_path, _do_set)
        info["modified"] = True

        detect_msg = f"\n  Change detection: {detect_changes_column}" if detect_changes_column else ""
        return ToolResponse.ok(
            f"Incremental refresh policy set on '{table_name}':\n"
            f"  Archive: {archive_periods} {archive_granularity}(s)\n"
            f"  Refresh: {refresh_periods} {refresh_granularity}(s)\n"
            f"  Mode: {mode}{detect_msg}\n"
            f"  DataModel: {old_size:,} → {new_size:,} bytes\n\n"
            f"The table's M expression must filter on RangeStart/RangeEnd parameters.\n"
            f"Power BI will automatically create date-based partitions on first refresh."
        ).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(f"{str(e)}\n{traceback.format_exc()}", e.code).to_text()


@mcp.tool()
def pbix_get_incremental_refresh(alias: str) -> str:
    """Get incremental refresh policies for all tables.

    Args:
        alias: The alias of the open file
    """
    try:
        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")
        if not os.path.exists(dm_path):
            return ToolResponse.error("No DataModel found.", DataModelCompressionError.code).to_text()

        import tempfile

        from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
        from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

        with open(dm_path, "rb") as f:
            dm_bytes = f.read()

        abf = decompress_datamodel(dm_bytes)
        db_bytes = read_metadata_sqlite(abf)

        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.write(db_bytes)
        tmp.close()

        try:
            conn = sqlite3.connect(tmp.name)
            c = conn.cursor()

            _GRAN_NAMES = {1: "day", 2: "month", 3: "quarter", 4: "year"}
            _MODE_NAMES = {0: "import", 1: "hybrid"}

            c.execute(
                "SELECT rp.ID, t.Name, rp.PolicyType, "
                "rp.RollingWindowGranularity, rp.RollingWindowPeriods, "
                "rp.IncrementalGranularity, rp.IncrementalPeriods, "
                "rp.IncrementalPeriodsOffset, rp.PollingExpression, rp.Mode "
                "FROM RefreshPolicy rp "
                "JOIN [Table] t ON rp.TableID = t.ID "
                "ORDER BY rp.ID"
            )
            policies = c.fetchall()
            conn.close()
        finally:
            os.unlink(tmp.name)

        if not policies:
            return ToolResponse.ok("No incremental refresh policies configured.").to_text()

        lines = [f"Incremental refresh policies ({len(policies)}):\n"]
        for p in policies:
            pid, tbl, ptype, rw_gran, rw_periods, inc_gran, inc_periods, offset, polling, pmode = p
            lines.append(f"  Table: {tbl}")
            lines.append(f"    Archive: {rw_periods} {_GRAN_NAMES.get(rw_gran, '?')}(s)")
            lines.append(f"    Refresh: {inc_periods} {_GRAN_NAMES.get(inc_gran, '?')}(s)")
            lines.append(f"    Mode: {_MODE_NAMES.get(pmode, '?')}")
            if polling:
                lines.append("    Change detection: enabled")
            lines.append("")

        return ToolResponse.ok("\n".join(lines)).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(f"{str(e)}\n{traceback.format_exc()}", e.code).to_text()


@mcp.tool()
def pbix_export_tmdl(alias: str, output_path: str = "") -> str:
    """Export the data model as TMDL (Tabular Model Definition Language) files.

    TMDL is a human-readable, Git-friendly text format for Power BI models.
    Creates a folder with .tmdl files for tables, relationships, roles, etc.

    Args:
        alias: The alias of the open file
        output_path: Output directory path. Defaults to <pbix_dir>/<alias>_tmdl/
    """
    try:
        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")
        if not os.path.exists(dm_path):
            return ToolResponse.error("No DataModel found.", DataModelCompressionError.code).to_text()

        # Determine output directory
        if not output_path:
            pbix_dir = os.path.dirname(info.get("original_path", info["work_dir"]))
            output_path = os.path.join(pbix_dir, f"{alias}_tmdl")

        os.makedirs(output_path, exist_ok=True)

        from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
        from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

        with open(dm_path, "rb") as f:
            dm_bytes = f.read()

        abf = decompress_datamodel(dm_bytes)
        db_bytes = read_metadata_sqlite(abf)

        import tempfile
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.write(db_bytes)
        tmp.close()

        try:
            conn = sqlite3.connect(tmp.name)
            stats = _export_tmdl_from_sqlite(conn, output_path)
            conn.close()
        finally:
            os.unlink(tmp.name)

        summary = (
            f"TMDL exported to: {output_path}\n"
            f"  Tables: {stats['tables']}\n"
            f"  Columns: {stats['columns']}\n"
            f"  Measures: {stats['measures']}\n"
            f"  Relationships: {stats['relationships']}\n"
            f"  Roles: {stats['roles']}\n"
            f"Files are Git-friendly text — diff, merge, and version control your model."
        )
        return ToolResponse.ok(summary).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(f"{str(e)}\n{traceback.format_exc()}", e.code).to_text()


# ---- Section 11: MCP main ----

if __name__ == "__main__":
    mcp.run()
