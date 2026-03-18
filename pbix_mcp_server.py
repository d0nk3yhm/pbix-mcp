"""
Power BI PBIX MCP Editor Server
================================
Full read/write MCP server for .pbix and .pbit files.

Capabilities:
  READ  — Report layout, visuals, pages, filters, DataMashup (M queries),
          DataModel schema/measures/relationships (via PBIXRay), settings, metadata
  WRITE — Report layout/visuals/pages/filters, DataMashup M code, settings,
          metadata. DataModel metadata via XPress9 round-trip.

Architecture:
  - PBIX files are ZIP archives
  - We extract components, allow granular inspection/editing, and repack
  - DataModel reading uses PBIXRay (Xpress9 decompression)
  - DataModel writing works via ABF round-trip (decompress → modify → recompress)
"""

import json
import zipfile
import shutil
import os
import io
import struct
import tempfile
import copy
import sqlite3
import traceback
from pathlib import Path
from datetime import datetime
from typing import Any, Optional, Callable

from mcp.server.fastmcp import FastMCP

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
        raise ValueError(
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
    }
    # Suffixes that are temp artifacts
    _EXCLUDE_SUFFIXES = (".abf", ".tmp", ".bak")

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


def _read_datamashup_m_code(work_dir: str) -> Optional[str]:
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


def _get_layout(work_dir: str) -> Optional[dict]:
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
        return f"Error: File not found: {file_path}"

    ext = os.path.splitext(file_path)[1].lower()
    if ext not in (".pbix", ".pbit"):
        return f"Error: Expected .pbix or .pbit file, got '{ext}'"

    if not alias:
        alias = Path(file_path).stem

    # Create work directory
    work_dir = os.path.join(
        tempfile.gettempdir(),
        f"pbix_mcp_{alias}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
    )
    os.makedirs(work_dir, exist_ok=True)

    try:
        _extract_pbix(file_path, work_dir)
    except Exception as e:
        shutil.rmtree(work_dir, ignore_errors=True)
        return f"Error extracting: {e}"

    _open_files[alias] = {
        "path": file_path,
        "work_dir": work_dir,
        "is_pbit": ext == ".pbit",
        "modified": False,
    }

    # Inventory
    components = []
    for root, dirs, files in os.walk(work_dir):
        for f in files:
            rel = os.path.relpath(os.path.join(root, f), work_dir)
            size = os.path.getsize(os.path.join(root, f))
            components.append(f"  {rel} ({size:,} bytes)")

    return (
        f"Opened '{file_path}' as '{alias}'\n"
        f"Type: {'PBIT template' if ext == '.pbit' else 'PBIX report'}\n"
        f"Components:\n" + "\n".join(sorted(components))
    )


@mcp.tool()
def pbix_save(alias: str, output_path: str = "") -> str:
    """Save/repack the modified PBIX/PBIT file.

    Args:
        alias: The alias of the open file
        output_path: Where to save. Empty = overwrite original.
    """
    try:
        info = _ensure_open(alias)
        work_dir = info["work_dir"]
        target = output_path or info["path"]
        target = os.path.abspath(target)

        # If overwriting original, create backup
        if target == info["path"] and os.path.exists(target):
            backup = target + ".bak"
            shutil.copy2(target, backup)

        _repack_pbix(work_dir, target)
        info["modified"] = False
        size = os.path.getsize(target)
        return f"Saved '{alias}' to {target} ({size:,} bytes)"
    except Exception as e:
        return f"Error saving: {e}"


@mcp.tool()
def pbix_close(alias: str) -> str:
    """Close an open file and clean up temporary files.

    Args:
        alias: The alias of the open file
    """
    try:
        info = _ensure_open(alias)
        work_dir = info["work_dir"]
        warning = ""
        if info.get("modified"):
            warning = f"Warning: '{alias}' had unsaved changes. "
        shutil.rmtree(work_dir, ignore_errors=True)
        del _open_files[alias]
        return f"{warning}Closed '{alias}'."
    except Exception as e:
        return f"Error closing: {e}"


@mcp.tool()
def pbix_list_open() -> str:
    """List all currently open PBIX/PBIT files."""
    if not _open_files:
        return "No files currently open."
    lines = []
    for alias, info in _open_files.items():
        status = "modified" if info.get("modified") else "clean"
        ftype = "PBIT" if info.get("is_pbit") else "PBIX"
        lines.append(f"  {alias}: {info['path']} [{ftype}, {status}]")
    return "Open files:\n" + "\n".join(lines)


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
            return "No layout found in this file."

        sections = layout.get("sections", [])
        lines = [f"Report has {len(sections)} page(s):\n"]
        for i, sec in enumerate(sections):
            name = sec.get("displayName", f"Page {i}")
            vis_count = len(sec.get("visualContainers", []))
            width = sec.get("width", "?")
            height = sec.get("height", "?")
            hidden = " [HIDDEN]" if sec.get("config", "").find('"visibility":1') >= 0 else ""
            lines.append(f"  [{i}] {name} — {vis_count} visuals, {width}x{height}{hidden}")
        return "\n".join(lines)
    except Exception as e:
        return f"Error: {e}"


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
            return "No layout found."

        sections = layout.get("sections", [])
        if page_index < 0 or page_index >= len(sections):
            return f"Error: Page index {page_index} out of range (0-{len(sections)-1})"

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
        return "\n".join(lines)
    except Exception as e:
        return f"Error: {e}"


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
            return "No layout found."

        sections = layout.get("sections", [])
        if page_index < 0 or page_index >= len(sections):
            return f"Error: Page index {page_index} out of range"

        containers = sections[page_index].get("visualContainers", [])
        if visual_index < 0 or visual_index >= len(containers):
            return f"Error: Visual index {visual_index} out of range"

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

        return json.dumps(result, indent=2, ensure_ascii=False)
    except Exception as e:
        return f"Error: {e}"


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
            return "No layout found."

        sections = layout.get("sections", [])
        if page_index < 0 or page_index >= len(sections):
            return f"Error: Page index {page_index} out of range"

        containers = sections[page_index].get("visualContainers", [])
        if visual_index < 0 or visual_index >= len(containers):
            return f"Error: Visual index {visual_index} out of range"

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
        return f"Set {property_path} = {value} on page {page_index}, visual {visual_index}"
    except Exception as e:
        return f"Error: {e}"


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
            return "No layout found."

        sections = layout.get("sections", [])
        if page_index < 0 or page_index >= len(sections):
            return f"Error: Page index {page_index} out of range"

        containers = sections[page_index].get("visualContainers", [])
        if visual_index < 0 or visual_index >= len(containers):
            return f"Error: Visual index {visual_index} out of range"

        # Validate JSON
        try:
            new_config = json.loads(config_json)
        except json.JSONDecodeError as e:
            return f"Error: Invalid JSON: {e}"

        containers[visual_index]["config"] = json.dumps(new_config, ensure_ascii=False)
        _set_layout(info["work_dir"], layout)
        info["modified"] = True
        return f"Updated visual config on page {page_index}, visual {visual_index}"
    except Exception as e:
        return f"Error: {e}"


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
            return "No layout found."

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
        return f"Added page '{display_name}' at index {idx} ({width}x{height})"
    except Exception as e:
        return f"Error: {e}"


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
            return "No layout found."

        sections = layout.get("sections", [])
        if page_index < 0 or page_index >= len(sections):
            return f"Error: Page index {page_index} out of range"

        removed = sections.pop(page_index)
        name = removed.get("displayName", f"Page {page_index}")
        _set_layout(info["work_dir"], layout)
        info["modified"] = True
        return f"Removed page '{name}' (was index {page_index}). {len(sections)} pages remain."
    except Exception as e:
        return f"Error: {e}"


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
            return "No layout found."
        return json.dumps(layout, indent=2, ensure_ascii=False)
    except Exception as e:
        return f"Error: {e}"


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
            return f"Error: Invalid JSON: {e}"
        _set_layout(info["work_dir"], layout)
        info["modified"] = True
        return "Layout updated."
    except Exception as e:
        return f"Error: {e}"


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
            return "No layout found."

        if page_index == -1:
            # Report-level filters
            filters_raw = layout.get("filters", "[]")
        else:
            sections = layout.get("sections", [])
            if page_index < 0 or page_index >= len(sections):
                return f"Error: Page index {page_index} out of range"
            filters_raw = sections[page_index].get("filters", "[]")

        if isinstance(filters_raw, str):
            try:
                filters = json.loads(filters_raw)
            except json.JSONDecodeError:
                filters = filters_raw
        else:
            filters = filters_raw

        level = f"page {page_index}" if page_index >= 0 else "report"
        return f"Filters ({level}):\n{json.dumps(filters, indent=2, ensure_ascii=False)}"
    except Exception as e:
        return f"Error: {e}"


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
            return "No layout found."

        # Validate JSON
        try:
            json.loads(filters_json)
        except json.JSONDecodeError as e:
            return f"Error: Invalid JSON: {e}"

        if page_index == -1:
            layout["filters"] = filters_json
        else:
            sections = layout.get("sections", [])
            if page_index < 0 or page_index >= len(sections):
                return f"Error: Page index {page_index} out of range"
            sections[page_index]["filters"] = filters_json

        _set_layout(info["work_dir"], layout)
        info["modified"] = True
        level = f"page {page_index}" if page_index >= 0 else "report"
        return f"Filters updated ({level})."
    except Exception as e:
        return f"Error: {e}"


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
            return "No Settings found."
        return json.dumps(settings, indent=2, ensure_ascii=False)
    except Exception as e:
        return f"Error: {e}"


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
            return f"Error: Invalid JSON: {e}"
        _write_json_component(info["work_dir"], os.path.join("Report", "Settings"), settings)
        info["modified"] = True
        return "Settings updated."
    except Exception as e:
        return f"Error: {e}"


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
            return "No layout found."

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
            return "No bookmarks found."

        lines = [f"Report has {len(bookmarks)} bookmark(s):\n"]
        for i, bm in enumerate(bookmarks):
            name = bm.get("displayName", bm.get("name", f"Bookmark {i}"))
            lines.append(f"  [{i}] {name}")
        return "\n".join(lines)
    except Exception as e:
        return f"Error: {e}"


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
        return "\n".join(lines)
    except Exception as e:
        return f"Error: {e}"


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
            return "No resources found."
        return "\n".join(lines)
    except Exception as e:
        return f"Error: {e}"


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
            return "No theme directory found."

        for f in os.listdir(theme_dir):
            if f.endswith(".json"):
                fp = os.path.join(theme_dir, f)
                with open(fp, "r", encoding="utf-8") as fh:
                    theme = json.load(fh)
                return f"Theme file: {f}\n{json.dumps(theme, indent=2, ensure_ascii=False)}"
        return "No theme JSON files found."
    except Exception as e:
        return f"Error: {e}"


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
            return f"Error: Invalid JSON: {e}"

        fp = os.path.join(theme_dir, filename)
        with open(fp, "w", encoding="utf-8") as fh:
            json.dump(theme, fh, indent=2, ensure_ascii=False)
        info["modified"] = True
        return f"Theme saved to {filename}"
    except Exception as e:
        return f"Error: {e}"


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
            return "No linguistic schema found."
        enc = _detect_encoding(ls_path)
        with open(ls_path, "r", encoding=enc) as f:
            return f.read()
    except Exception as e:
        return f"Error: {e}"


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
        return "Linguistic schema updated."
    except Exception as e:
        return f"Error: {e}"


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
            return "No DataMashup found in this file."
        return m_code
    except Exception as e:
        return f"Error: {e}"


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
            return "Error: Failed to write M code. DataMashup may not exist or be corrupt."
        info["modified"] = True
        return "M code updated in DataMashup."
    except Exception as e:
        return f"Error: {e}"


# ---- Section 7: DataModel READ tools (via PBIXRay) ----

@mcp.tool()
def pbix_get_model_schema(alias: str) -> str:
    """Get the data model schema — all tables, columns, and data types.

    Args:
        alias: The alias of the open file
    """
    try:
        info = _ensure_open(alias)
        from pbixray import PBIXRay
        model = PBIXRay(info["path"])
        schema = model.schema
        return schema.to_string(max_rows=500, max_colwidth=80)
    except Exception as e:
        return f"Error reading model schema: {e}"


@mcp.tool()
def pbix_get_model_measures(alias: str) -> str:
    """Get all DAX measures from the data model.

    Args:
        alias: The alias of the open file
    """
    try:
        info = _ensure_open(alias)
        from pbixray import PBIXRay
        model = PBIXRay(info["path"])
        measures = model.dax_measures
        if measures is None or (hasattr(measures, 'empty') and measures.empty):
            return "No DAX measures found."
        return measures.to_string(max_rows=200, max_colwidth=120)
    except Exception as e:
        return f"Error reading measures: {e}"


@mcp.tool()
def pbix_get_model_relationships(alias: str) -> str:
    """Get all relationships in the data model.

    Args:
        alias: The alias of the open file
    """
    try:
        info = _ensure_open(alias)
        from pbixray import PBIXRay
        model = PBIXRay(info["path"])
        rels = model.relationships
        if rels is None or (hasattr(rels, 'empty') and rels.empty):
            return "No relationships found."
        return rels.to_string(max_rows=200, max_colwidth=80)
    except Exception as e:
        return f"Error reading relationships: {e}"


@mcp.tool()
def pbix_get_model_power_query(alias: str) -> str:
    """Get Power Query expressions from the model (via PBIXRay).

    This reads M expressions as stored in the DataModel itself
    (different from the DataMashup M code).

    Args:
        alias: The alias of the open file
    """
    try:
        info = _ensure_open(alias)
        from pbixray import PBIXRay
        model = PBIXRay(info["path"])
        pq = model.power_query
        if pq is None or (hasattr(pq, 'empty') and pq.empty):
            return "No Power Query expressions found in model."
        return pq.to_string(max_rows=200, max_colwidth=200)
    except Exception as e:
        return f"Error reading power query: {e}"


@mcp.tool()
def pbix_get_model_columns(alias: str) -> str:
    """Get all DAX calculated columns from the model.

    Args:
        alias: The alias of the open file
    """
    try:
        info = _ensure_open(alias)
        from pbixray import PBIXRay
        model = PBIXRay(info["path"])
        cols = model.dax_columns
        if cols is None or (hasattr(cols, 'empty') and cols.empty):
            return "No DAX columns found."
        return cols.to_string(max_rows=200, max_colwidth=120)
    except Exception as e:
        return f"Error reading columns: {e}"


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
        from pbixray import PBIXRay
        model = PBIXRay(info["path"])
        df = model.get_table(table_name)
        if df is None or (hasattr(df, 'empty') and df.empty):
            return f"No data found in table '{table_name}'."
        return df.head(max_rows).to_string(max_rows=max_rows, max_colwidth=60)
    except Exception as e:
        return f"Error reading table data: {e}"


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
            return "Error: 'columns' and 'rows' are required and must not be empty."

        from datamodel_roundtrip import decompress_datamodel, compress_datamodel
        from abf_rebuild import read_metadata_sqlite, rebuild_abf_with_replacement
        from vertipaq_encoder import encode_table_data, update_table_in_abf

        dm_path = os.path.join(info["work_dir"], "DataModel")
        if not os.path.exists(dm_path):
            return "Error: No DataModel found."

        with open(dm_path, "rb") as f:
            dm_bytes = f.read()

        abf = decompress_datamodel(dm_bytes)

        # Get partition number from existing ABF file listing
        from abf_rebuild import list_abf_files
        file_log = list_abf_files(abf)
        partition_num = None
        tbl_prefix = f"{table_name}.tbl"
        for entry in file_log:
            path = entry.get("Path", entry.get("StoragePath", ""))
            if tbl_prefix in path:
                # Extract partition number from path like "Table.tbl\26.prt\..."
                parts = path.replace("\\", "/").split("/")
                for p in parts:
                    if p.endswith(".prt"):
                        try:
                            partition_num = int(p.replace(".prt", ""))
                        except ValueError:
                            pass
                        break
                if partition_num is not None:
                    break

        if partition_num is None:
            partition_num = 1  # Default for new tables

        # Read existing metadata SQLite
        meta_bytes = read_metadata_sqlite(abf)

        # Encode and update ABF
        new_abf = update_table_in_abf(abf, table_name, columns, rows, meta_bytes)

        # Recompress
        new_dm = compress_datamodel(new_abf)

        with open(dm_path, "wb") as f:
            f.write(new_dm)

        info["modified"] = True
        return (
            f"Table '{table_name}' data written: {len(rows)} rows, {len(columns)} columns\n"
            f"  DataModel: {len(dm_bytes):,} → {len(new_dm):,} bytes\n"
            f"  ABF: {len(abf):,} → {len(new_abf):,} bytes"
        )
    except json.JSONDecodeError as e:
        return f"Error: Invalid JSON: {e}"
    except Exception as e:
        import traceback
        return f"Error writing table data: {e}\n{traceback.format_exc()}"


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
            return "Error: rows must not be empty."

        from datamodel_roundtrip import decompress_datamodel, compress_datamodel
        from abf_rebuild import read_metadata_sqlite
        from vertipaq_encoder import update_table_in_abf
        import sqlite3

        dm_path = os.path.join(info["work_dir"], "DataModel")
        if not os.path.exists(dm_path):
            return "Error: No DataModel found."

        with open(dm_path, "rb") as f:
            dm_bytes = f.read()

        abf = decompress_datamodel(dm_bytes)
        meta_bytes = read_metadata_sqlite(abf)

        # Read column definitions from metadata
        import tempfile
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.write(meta_bytes)
        tmp.close()
        try:
            conn = sqlite3.connect(tmp.name)
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """SELECT c.ExplicitName, c.ExplicitDataType, c.IsNullable
                   FROM [Column] c
                   JOIN [Table] t ON c.TableID = t.ID
                   WHERE t.Name = ? AND c.Type = 1
                   ORDER BY c.ID""",
                (table_name,)
            )
            col_rows = cursor.fetchall()
            conn.close()
        finally:
            os.unlink(tmp.name)

        if not col_rows:
            return f"Error: Table '{table_name}' not found or has no user columns."

        # Map ExplicitDataType codes to type names
        type_map = {2: "String", 6: "Int64", 8: "Float64", 9: "DateTime",
                    10: "Decimal", 11: "Boolean", 17: "String"}
        columns = []
        for cr in col_rows:
            dt = cr["ExplicitDataType"] or 2
            columns.append({
                "name": cr["ExplicitName"],
                "data_type": type_map.get(dt, "String"),
                "nullable": bool(cr["IsNullable"]) if cr["IsNullable"] is not None else True
            })

        # Encode and update
        new_abf = update_table_in_abf(abf, table_name, columns, rows, meta_bytes)
        new_dm = compress_datamodel(new_abf)

        with open(dm_path, "wb") as f:
            f.write(new_dm)

        info["modified"] = True
        col_names = [c["name"] for c in columns]
        return (
            f"Table '{table_name}' updated: {len(rows)} rows, {len(columns)} columns\n"
            f"  Columns: {', '.join(col_names)}\n"
            f"  DataModel: {len(dm_bytes):,} → {len(new_dm):,} bytes"
        )
    except json.JSONDecodeError as e:
        return f"Error: Invalid JSON: {e}"
    except Exception as e:
        import traceback
        return f"Error: {e}\n{traceback.format_exc()}"


@mcp.tool()
def pbix_list_tables(alias: str) -> str:
    """List all tables in the data model with row/column counts.

    Args:
        alias: The alias of the open file
    """
    try:
        info = _ensure_open(alias)
        from pbixray import PBIXRay
        model = PBIXRay(info["path"])
        stats = model.statistics
        if stats is None or (hasattr(stats, 'empty') and stats.empty):
            return "No tables found."
        return stats.to_string(max_rows=100, max_colwidth=60)
    except Exception as e:
        return f"Error listing tables: {e}"


# ---- Section 8: DataModel WRITE tools (via XPress9 round-trip) ----

def _modify_metadata_sqlite(
    dm_path: str, modifier_fn: Callable[[sqlite3.Connection], None]
) -> tuple:
    """Decompress DataModel, modify metadata.sqlitedb, recompress.

    Args:
        dm_path: Path to the DataModel file inside the work_dir
        modifier_fn: Function that receives a sqlite3.Connection and should
                     make changes + commit.

    Returns:
        Tuple of (original_dm_bytes, new_dm_bytes, new_abf_bytes)
    """
    from datamodel_roundtrip import decompress_datamodel, compress_datamodel
    from abf_rebuild import (
        rebuild_abf_with_modified_sqlite, list_abf_files,
        read_metadata_sqlite,
    )

    with open(dm_path, "rb") as f:
        dm_bytes = f.read()

    # Decompress DataModel → raw ABF
    abf = decompress_datamodel(dm_bytes)

    # Use rebuild_abf_with_modified_sqlite which:
    #   1. Extracts metadata.sqlitedb from ABF
    #   2. Opens it with sqlite3, passes connection to modifier_fn
    #   3. Rebuilds ABF with the modified sqlite file
    def _sqlite_modifier(conn: sqlite3.Connection):
        modifier_fn(conn)

    new_abf = rebuild_abf_with_modified_sqlite(abf, _sqlite_modifier)

    # Recompress ABF → new DataModel
    new_dm = compress_datamodel(new_abf)

    # Write new DataModel back
    with open(dm_path, "wb") as f:
        f.write(new_dm)

    return dm_bytes, new_dm, new_abf


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
            return "Error: No DataModel found in this file."

        from datamodel_roundtrip import decompress_datamodel
        from abf_rebuild import read_metadata_sqlite, list_abf_files

        with open(dm_path, "rb") as f:
            dm_bytes = f.read()

        abf = decompress_datamodel(dm_bytes)
        meta_bytes = read_metadata_sqlite(abf)

        if not meta_bytes:
            return "Error: Could not extract metadata.sqlitedb from ABF."

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
            return "Query returned no results."

        # Format output
        lines = [" | ".join(columns)]
        lines.append("-" * len(lines[0]))
        for row in rows[:200]:
            lines.append(" | ".join(str(row[c]) for c in columns))
        result = "\n".join(lines)
        if len(rows) > 200:
            result += f"\n... ({len(rows)} total rows, showing first 200)"
        return result
    except Exception as e:
        return f"Error: {e}\n{traceback.format_exc()}"


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
            return "Error: No DataModel found."

        changes = [0]

        def _do_sql(conn: sqlite3.Connection):
            conn.execute(sql_statement)
            changes[0] = conn.total_changes
            conn.commit()

        dm_bytes, new_dm, new_abf = _modify_metadata_sqlite(dm_path, _do_sql)
        info["modified"] = True
        return (
            f"SQL executed successfully.\n"
            f"  Changes: {changes[0]}\n"
            f"  DataModel: {len(dm_bytes):,} → {len(new_dm):,} bytes"
        )
    except Exception as e:
        return f"Error: {e}\n{traceback.format_exc()}"


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
            return "Error: No DataModel found."

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

        dm_bytes, new_dm, new_abf = _modify_metadata_sqlite(dm_path, _do_modify)
        info["modified"] = True
        return (
            f"Measure '{measure_name}' updated:\n"
            f"  Old: {old_info.get('expression', '?')}\n"
            f"  New: {new_expression}\n"
            f"  DataModel: {len(dm_bytes):,} → {len(new_dm):,} bytes"
        )
    except Exception as e:
        return f"Error: {e}\n{traceback.format_exc()}"


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
            return "Error: No DataModel found."

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

            # Get next ID
            c.execute("SELECT COALESCE(MAX(ID), 0) + 1 FROM Measure")
            new_id = c.fetchone()[0]

            c.execute(
                "INSERT INTO Measure (ID, TableID, Name, Expression, FormatString, "
                "Description, IsHidden, ModifiedTime) "
                "VALUES (?, ?, ?, ?, ?, ?, 0, datetime('now'))",
                (new_id, table_id, measure_name, expression, format_string, description)
            )
            conn.commit()

        dm_bytes, new_dm, new_abf = _modify_metadata_sqlite(dm_path, _do_add)
        info["modified"] = True
        return (
            f"Measure '{measure_name}' added to table '{table_name}':\n"
            f"  Expression: {expression}\n"
            f"  DataModel: {len(dm_bytes):,} → {len(new_dm):,} bytes"
        )
    except Exception as e:
        return f"Error: {e}\n{traceback.format_exc()}"


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
            return "Error: No DataModel found."

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

        dm_bytes, new_dm, new_abf = _modify_metadata_sqlite(dm_path, _do_remove)
        info["modified"] = True
        return (
            f"Measure '{measure_name}' removed from table '{old_info.get('table', '?')}':\n"
            f"  Old expression: {old_info.get('expression', '?')}\n"
            f"  DataModel: {len(dm_bytes):,} → {len(new_dm):,} bytes"
        )
    except Exception as e:
        return f"Error: {e}\n{traceback.format_exc()}"


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
            return "Error: No DataModel found."

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

        dm_bytes, new_dm, new_abf = _modify_metadata_sqlite(dm_path, _do_modify)
        info["modified"] = True
        return (
            f"Column '{table_name}'.'{column_name}' updated:\n"
            f"  {property_name} = {new_value}\n"
            f"  DataModel: {len(dm_bytes):,} → {len(new_dm):,} bytes"
        )
    except Exception as e:
        return f"Error: {e}\n{traceback.format_exc()}"


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
            return "Error: No DataModel found."

        from datamodel_roundtrip import decompress_datamodel
        from abf_rebuild import list_abf_files

        with open(dm_path, "rb") as f:
            dm_bytes = f.read()

        abf = decompress_datamodel(dm_bytes)
        abf_path = dm_path + ".abf"
        with open(abf_path, "wb") as f:
            f.write(abf)

        file_log = list_abf_files(abf)
        summary = [f"Decompressed DataModel: {len(dm_bytes):,} → {len(abf):,} bytes"]
        summary.append(f"ABF saved to: {abf_path}")
        summary.append(f"\nABF contains {len(file_log)} files:")
        for entry in file_log:
            summary.append(f"  {entry['Path']} ({entry['Size']:,} bytes)")
        return "\n".join(summary)
    except Exception as e:
        return f"Error: {e}\n{traceback.format_exc()}"


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

        from datamodel_roundtrip import compress_datamodel

        if not abf_path:
            abf_path = dm_path + ".abf"

        if not os.path.exists(abf_path):
            return (
                f"Error: ABF file not found at {abf_path}\n"
                f"Run pbix_datamodel_decompress first."
            )

        with open(abf_path, "rb") as f:
            abf_bytes = f.read()

        # Validate ABF starts with BOM
        if not abf_bytes[:2] == b"\xff\xfe":
            return (
                f"Error: File does not look like a valid ABF "
                f"(expected \\xff\\xfe BOM, got {abf_bytes[:2].hex()})."
            )

        # Read original DataModel size for comparison
        orig_size = os.path.getsize(dm_path) if os.path.exists(dm_path) else 0

        new_dm = compress_datamodel(abf_bytes)

        with open(dm_path, "wb") as f:
            f.write(new_dm)

        info["modified"] = True
        return (
            f"Recompressed ABF -> DataModel:\n"
            f"  ABF size:          {len(abf_bytes):>12,} bytes\n"
            f"  Old DataModel:     {orig_size:>12,} bytes\n"
            f"  New DataModel:     {len(new_dm):>12,} bytes\n"
            f"  XPress9 blocks:    {(len(abf_bytes) + 2097151) // 2097152}\n"
            f"  Saved to: {dm_path}"
        )
    except Exception as e:
        return f"Error: {e}\n{traceback.format_exc()}"


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
            return "Error: No DataModel found."

        if not os.path.exists(new_content_path):
            return f"Error: Replacement file not found: {new_content_path}"

        from datamodel_roundtrip import decompress_datamodel, compress_datamodel
        from abf_rebuild import (
            rebuild_abf_with_replacement, list_abf_files, find_abf_file,
        )

        with open(dm_path, "rb") as f:
            dm_bytes = f.read()

        with open(new_content_path, "rb") as f:
            new_content = f.read()

        abf = decompress_datamodel(dm_bytes)
        file_log = list_abf_files(abf)
        entry = find_abf_file(file_log, internal_path)
        if not entry:
            return f"Error: No file matching '{internal_path}' in ABF."

        fname = entry["Path"]
        new_abf = rebuild_abf_with_replacement(abf, {internal_path: new_content})
        new_dm = compress_datamodel(new_abf)

        with open(dm_path, "wb") as f:
            f.write(new_dm)

        info["modified"] = True
        return (
            f"Replaced '{fname}' in ABF (full rebuild):\n"
            f"  Old file size: {entry['Size']:,} bytes\n"
            f"  New file size: {len(new_content):,} bytes\n"
            f"  ABF: {len(abf):,} -> {len(new_abf):,} bytes\n"
            f"  DataModel recompressed: {len(dm_bytes):,} -> {len(new_dm):,} bytes"
        )
    except Exception as e:
        return f"Error: {e}\n{traceback.format_exc()}"


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
            return "Error: No DataModel found."

        from datamodel_roundtrip import decompress_datamodel
        from abf_rebuild import list_abf_files, find_abf_file, read_abf_file

        with open(dm_path, "rb") as f:
            dm_bytes = f.read()

        abf = decompress_datamodel(dm_bytes)
        file_log = list_abf_files(abf)
        entry = find_abf_file(file_log, internal_path)
        if not entry:
            return f"Error: No file matching '{internal_path}' in ABF."

        content = read_abf_file(abf, entry)

        if not output_path:
            fname = os.path.basename(entry["Path"])
            output_path = os.path.join(info["work_dir"], fname)

        with open(output_path, "wb") as f:
            f.write(content)

        return (
            f"Extracted '{entry['Path']}' ({len(content):,} bytes)\n"
            f"  ABF path: {entry['Path']}\n"
            f"  Saved to: {output_path}"
        )
    except Exception as e:
        return f"Error: {e}\n{traceback.format_exc()}"


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
            return "Error: No DataModel found."

        from datamodel_roundtrip import decompress_datamodel
        from abf_rebuild import ABFArchive, list_abf_files

        with open(dm_path, "rb") as f:
            dm_bytes = f.read()

        abf = decompress_datamodel(dm_bytes)
        files = list_abf_files(abf)

        lines = [f"ABF contains {len(files)} files ({len(abf):,} bytes decompressed):\n"]
        for entry in files:
            lines.append(f"  {entry['Path']} ({entry['Size']:,} bytes)")
        return "\n".join(lines)
    except Exception as e:
        return f"Error: {e}\n{traceback.format_exc()}"


# ---- Section 9: MCP main ----

if __name__ == "__main__":
    mcp.run()
