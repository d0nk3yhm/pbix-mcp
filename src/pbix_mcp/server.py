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


def _repack_pbix(work_dir: str, output_path: str, strip_sensitivity_label: bool = False) -> None:
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

    # Strip MSIP sensitivity label from docProps/custom.xml
    if strip_sensitivity_label:
        custom_path = os.path.join(work_dir, "docProps", "custom.xml")
        if os.path.exists(custom_path):
            import re
            with open(custom_path, "r", encoding="utf-8") as f:
                content = f.read()
            # Remove all MSIP_Label properties
            content = re.sub(
                r'<property[^>]*name="MSIP_Label_[^"]*"[^>]*>.*?</property>',
                "", content
            )
            with open(custom_path, "w", encoding="utf-8") as f:
                f.write(content)

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


# ---- Visual formatting helpers ----

_DISPLAY_UNITS = {
    "none": "1D", "thousands": "1000D", "millions": "1000000D",
    "billions": "1000000000D", "trillions": "1000000000000D", "auto": "0D",
}
_LEGEND_POSITIONS = {
    "top": "'Top'", "bottom": "'Bottom'", "left": "'Left'",
    "right": "'Right'", "topCenter": "'TopCenter'",
    "bottomCenter": "'BottomCenter'", "leftCenter": "'LeftCenter'",
    "rightCenter": "'RightCenter'",
}
_ALIGNMENTS = {"left": "'Left'", "center": "'Center'", "right": "'Right'"}


def _pbi_lit(value) -> dict:
    """Convert a Python value to PBI Literal expression wrapper."""
    if isinstance(value, bool):
        raw = "true" if value else "false"
    elif isinstance(value, int):
        raw = f"{value}L"
    elif isinstance(value, float):
        raw = f"{value}D"
    elif isinstance(value, str):
        raw = f"'{value}'"
    else:
        raw = str(value)
    return {"expr": {"Literal": {"Value": raw}}}


def _pbi_props(mapping: dict, src: dict) -> dict:
    """Build PBI properties dict from a key mapping and source values.

    mapping: {pbi_property_name: (src_key, transform_fn_or_None)}
    src: the user-provided dict for this formatting category
    """
    props = {}
    for pbi_key, (src_key, transform) in mapping.items():
        if src_key in src:
            val = src[src_key]
            if transform:
                val = transform(val)
            props[pbi_key] = _pbi_lit(val)
    return props


def _solid_color(hex_color: str) -> dict:
    """Wrap a hex color in PBI's solid.color structure."""
    return {"solid": {"color": _pbi_lit(hex_color)}}


def _build_format_objects(fmt: dict) -> dict:
    """Convert human-readable format dict to PBI objects structures.

    Returns dict with two keys:
      - "_objects": data formatting (labels, legend, axis, dataPoint, grid, etc.)
      - "_vcObjects": visual container formatting (title, subtitle, background,
        border, dropShadow, padding, spacing, visualHeader, divider, etc.)

    All property names and value formats match PBI Desktop March 2026 ground truth.
    Colors use {"solid": {"color": {"expr": {"Literal": {"Value": "'#hex'"}}}}}
    """
    objects: dict[str, list] = {}
    vc_objects: dict[str, list] = {}

    def _add(category: str, props: dict):
        if props:
            objects[category] = [{"properties": props}]

    def _add_vc(category: str, props: dict):
        if props:
            vc_objects[category] = [{"properties": props}]

    # ================================================================
    # vcObjects — visual container formatting
    # ================================================================

    # --- title ---
    if "title" in fmt:
        t = fmt["title"]
        props = {}
        if "show" in t: props["show"] = _pbi_lit(t["show"])
        if "text" in t: props["text"] = _pbi_lit(t["text"])
        if "fontSize" in t: props["fontSize"] = _pbi_lit(float(t["fontSize"]))
        if "color" in t: props["fontColor"] = _solid_color(t["color"])
        if "fontFamily" in t: props["fontFamily"] = _pbi_lit(t["fontFamily"])
        if "bold" in t: props["bold"] = _pbi_lit(t["bold"])
        if "italic" in t: props["italic"] = _pbi_lit(t["italic"])
        if "alignment" in t:
            raw = _ALIGNMENTS.get(t["alignment"], f"'{t['alignment']}'")
            props["alignment"] = {"expr": {"Literal": {"Value": raw}}}
        if "heading" in t: props["heading"] = _pbi_lit(t["heading"])
        if "titleWrap" in t: props["titleWrap"] = _pbi_lit(t["titleWrap"])
        if "background" in t: props["background"] = _solid_color(t["background"])
        _add_vc("title", props)

    # --- subtitle ---
    if "subtitle" in fmt:
        s = fmt["subtitle"]
        props = {}
        if "show" in s: props["show"] = _pbi_lit(s["show"])
        if "text" in s: props["text"] = _pbi_lit(s["text"])
        if "fontSize" in s: props["fontSize"] = _pbi_lit(float(s["fontSize"]))
        if "color" in s: props["fontColor"] = _solid_color(s["color"])
        if "fontFamily" in s: props["fontFamily"] = _pbi_lit(s["fontFamily"])
        if "titleWrap" in s: props["titleWrap"] = _pbi_lit(s["titleWrap"])
        _add_vc("subTitle", props)

    # --- background ---
    if "background" in fmt:
        bg = fmt["background"]
        props = {}
        if "color" in bg: props["color"] = _solid_color(bg["color"])
        if "transparency" in bg: props["transparency"] = _pbi_lit(float(bg["transparency"]))
        if "show" in bg: props["show"] = _pbi_lit(bg["show"])
        else: props["show"] = _pbi_lit(True)
        _add_vc("background", props)

    # --- border ---
    if "border" in fmt:
        bd = fmt["border"]
        props = {}
        if "show" in bd: props["show"] = _pbi_lit(bd["show"])
        if "color" in bd: props["color"] = _solid_color(bd["color"])
        if "radius" in bd: props["radius"] = _pbi_lit(float(bd["radius"]))
        if "width" in bd: props["width"] = _pbi_lit(float(bd["width"]))
        _add_vc("border", props)

    # --- dropShadow ---
    if "dropShadow" in fmt:
        ds = fmt["dropShadow"]
        props = {}
        if "show" in ds: props["show"] = _pbi_lit(ds["show"])
        if "color" in ds: props["color"] = _solid_color(ds["color"])
        if "position" in ds: props["position"] = _pbi_lit(ds["position"])
        if "preset" in ds: props["preset"] = _pbi_lit(ds["preset"])
        if "angle" in ds: props["angle"] = _pbi_lit(float(ds["angle"]))
        if "blur" in ds: props["shadowBlur"] = _pbi_lit(float(ds["blur"]))
        if "distance" in ds: props["shadowDistance"] = _pbi_lit(float(ds["distance"]))
        if "spread" in ds: props["shadowSpread"] = _pbi_lit(float(ds["spread"]))
        if "transparency" in ds: props["transparency"] = _pbi_lit(float(ds["transparency"]))
        _add_vc("dropShadow", props)

    # --- padding ---
    if "padding" in fmt:
        pd = fmt["padding"]
        props = {}
        if isinstance(pd, (int, float)):
            for side in ("top", "bottom", "left", "right"):
                props[side] = _pbi_lit(int(pd))
        else:
            if "top" in pd: props["top"] = _pbi_lit(int(pd["top"]))
            if "bottom" in pd: props["bottom"] = _pbi_lit(int(pd["bottom"]))
            if "left" in pd: props["left"] = _pbi_lit(int(pd["left"]))
            if "right" in pd: props["right"] = _pbi_lit(int(pd["right"]))
        _add_vc("padding", props)

    # --- spacing ---
    if "spacing" in fmt:
        sp = fmt["spacing"]
        props = {}
        props["customizeSpacing"] = _pbi_lit(True)
        if "belowTitle" in sp: props["spaceBelowTitle"] = _pbi_lit(int(sp["belowTitle"]))
        if "belowSubTitle" in sp: props["spaceBelowSubTitle"] = _pbi_lit(int(sp["belowSubTitle"]))
        if "belowTitleArea" in sp: props["spaceBelowTitleArea"] = _pbi_lit(int(sp["belowTitleArea"]))
        if "vertical" in sp: props["verticalSpacing"] = _pbi_lit(int(sp["vertical"]))
        _add_vc("spacing", props)

    # --- divider ---
    if "divider" in fmt:
        dv = fmt["divider"]
        props = {}
        if "show" in dv: props["show"] = _pbi_lit(dv["show"])
        if "color" in dv: props["color"] = _solid_color(dv["color"])
        if "width" in dv: props["width"] = _pbi_lit(float(dv["width"]))
        if "style" in dv: props["style"] = _pbi_lit(dv["style"])
        if "ignorePadding" in dv: props["ignorePadding"] = _pbi_lit(dv["ignorePadding"])
        _add_vc("divider", props)

    # --- visualHeader ---
    if "visualHeader" in fmt:
        vh = fmt["visualHeader"]
        props = {}
        if "show" in vh: props["show"] = _pbi_lit(vh["show"])
        for btn in ("showOptionsMenu", "showFocusModeButton", "showPinButton",
                     "showFilterRestatementButton", "showTooltipButton",
                     "showDrillUpButton", "showDrillDownLevelButton",
                     "showDrillDownExpandButton", "showDrillToggleButton",
                     "showDrillRoleSelector", "showVisualErrorButton",
                     "showVisualWarningButton", "showVisualInformationButton",
                     "showSeeDataLayoutToggleButton"):
            if btn in vh: props[btn] = _pbi_lit(vh[btn])
        _add_vc("visualHeader", props)

    # --- visualTooltip ---
    if "visualTooltip" in fmt:
        vt = fmt["visualTooltip"]
        props = {}
        if "show" in vt: props["show"] = _pbi_lit(vt["show"])
        if "type" in vt: props["type"] = _pbi_lit(vt["type"])
        if "fontSize" in vt: props["fontSize"] = _pbi_lit(float(vt["fontSize"]))
        if "titleFontColor" in vt: props["titleFontColor"] = _solid_color(vt["titleFontColor"])
        if "valueFontColor" in vt: props["valueFontColor"] = _solid_color(vt["valueFontColor"])
        if "actionFontColor" in vt: props["actionFontColor"] = _solid_color(vt["actionFontColor"])
        if "background" in vt: props["background"] = _solid_color(vt["background"])
        _add_vc("visualTooltip", props)

    # --- stylePreset ---
    if "stylePreset" in fmt:
        _add_vc("stylePreset", {"name": _pbi_lit(fmt["stylePreset"])})

    # --- general (vcObjects) ---
    if "altText" in fmt:
        _add_vc("general", {"altText": _pbi_lit(fmt["altText"])})

    # --- lockAspect ---
    if "lockAspect" in fmt:
        _add_vc("lockAspect", {"show": _pbi_lit(fmt["lockAspect"])})

    # ================================================================
    # objects — data formatting
    # ================================================================

    # --- legend ---
    if "legend" in fmt:
        lg = fmt["legend"]
        props = {}
        if "show" in lg: props["show"] = _pbi_lit(lg["show"])
        if "fontSize" in lg: props["fontSize"] = _pbi_lit(float(lg["fontSize"]))
        if "color" in lg: props["fontColor"] = _solid_color(lg["color"])
        if "fontFamily" in lg: props["fontFamily"] = _pbi_lit(lg["fontFamily"])
        if "position" in lg:
            raw = _LEGEND_POSITIONS.get(lg["position"], f"'{lg['position']}'")
            props["position"] = {"expr": {"Literal": {"Value": raw}}}
        _add("legend", props)

    # --- dataLabels (labels) ---
    if "dataLabels" in fmt:
        dl = fmt["dataLabels"]
        props = {}
        if "show" in dl: props["show"] = _pbi_lit(dl["show"])
        if "fontSize" in dl: props["fontSize"] = _pbi_lit(float(dl["fontSize"]))
        if "color" in dl: props["color"] = _solid_color(dl["color"])
        if "fontFamily" in dl: props["fontFamily"] = _pbi_lit(dl["fontFamily"])
        if "displayUnits" in dl:
            raw = _DISPLAY_UNITS.get(dl["displayUnits"], f"{dl['displayUnits']}D")
            props["labelDisplayUnits"] = {"expr": {"Literal": {"Value": raw}}}
        if "decimalPlaces" in dl: props["labelPrecision"] = _pbi_lit(int(dl["decimalPlaces"]))
        _add("labels", props)

    # --- categoryAxis ---
    if "categoryAxis" in fmt:
        ca = fmt["categoryAxis"]
        props = {}
        if "show" in ca: props["show"] = _pbi_lit(ca["show"])
        if "fontSize" in ca: props["fontSize"] = _pbi_lit(float(ca["fontSize"]))
        if "color" in ca: props["labelColor"] = _solid_color(ca["color"])
        if "fontFamily" in ca: props["fontFamily"] = _pbi_lit(ca["fontFamily"])
        if "title" in ca:
            props["showAxisTitle"] = _pbi_lit(True)
            props["axisTitle"] = _pbi_lit(ca["title"])
        if "titleFontSize" in ca: props["titleFontSize"] = _pbi_lit(float(ca["titleFontSize"]))
        if "gridlineShow" in ca: props["gridlineShow"] = _pbi_lit(ca["gridlineShow"])
        if "innerPadding" in ca: props["innerPadding"] = _pbi_lit(int(ca["innerPadding"]))
        if "invertAxis" in ca: props["invertAxis"] = _pbi_lit(ca["invertAxis"])
        if "concatenateLabels" in ca: props["concatenateLabels"] = _pbi_lit(ca["concatenateLabels"])
        if "axisType" in ca: props["axisType"] = _pbi_lit(ca["axisType"])
        if "start" in ca: props["start"] = _pbi_lit(float(ca["start"]))
        if "end" in ca: props["end"] = _pbi_lit(float(ca["end"]))
        if "switchAxisPosition" in ca: props["switchAxisPosition"] = _pbi_lit(ca["switchAxisPosition"])
        if "preferredCategoryWidth" in ca: props["preferredCategoryWidth"] = _pbi_lit(float(ca["preferredCategoryWidth"]))
        _add("categoryAxis", props)

    # --- valueAxis ---
    if "valueAxis" in fmt:
        va = fmt["valueAxis"]
        props = {}
        if "show" in va: props["show"] = _pbi_lit(va["show"])
        if "fontSize" in va: props["fontSize"] = _pbi_lit(float(va["fontSize"]))
        if "color" in va: props["labelColor"] = _solid_color(va["color"])
        if "fontFamily" in va: props["fontFamily"] = _pbi_lit(va["fontFamily"])
        if "displayUnits" in va:
            raw = _DISPLAY_UNITS.get(va["displayUnits"], f"{va['displayUnits']}D")
            props["labelDisplayUnits"] = {"expr": {"Literal": {"Value": raw}}}
        if "title" in va:
            props["showAxisTitle"] = _pbi_lit(True)
            props["axisTitle"] = _pbi_lit(va["title"])
        if "titleFontSize" in va: props["titleFontSize"] = _pbi_lit(float(va["titleFontSize"]))
        if "gridlineShow" in va: props["gridlineShow"] = _pbi_lit(va["gridlineShow"])
        if "start" in va: props["start"] = _pbi_lit(float(va["start"]))
        if "end" in va: props["end"] = _pbi_lit(float(va["end"]))
        if "switchAxisPosition" in va: props["switchAxisPosition"] = _pbi_lit(va["switchAxisPosition"])
        if "decimalPlaces" in va: props["labelPrecision"] = _pbi_lit(int(va["decimalPlaces"]))
        _add("valueAxis", props)

    # --- dataColors (dataPoint) ---
    if "dataColors" in fmt:
        colors = fmt["dataColors"]
        if isinstance(colors, list) and colors:
            props = {"fill": _solid_color(colors[0])}
            _add("dataPoint", props)

    # --- grid (table/matrix) ---
    if "grid" in fmt:
        gr = fmt["grid"]
        props = {}
        if "gridVertical" in gr: props["gridVertical"] = _pbi_lit(gr["gridVertical"])
        if "gridHorizontal" in gr: props["gridHorizontal"] = _pbi_lit(gr["gridHorizontal"])
        if "rowPadding" in gr: props["rowPadding"] = _pbi_lit(int(gr["rowPadding"]))
        if "outlineColor" in gr: props["outlineColor"] = _solid_color(gr["outlineColor"])
        if "outlineWeight" in gr: props["outlineWeight"] = _pbi_lit(int(gr["outlineWeight"]))
        if "textSize" in gr: props["textSize"] = _pbi_lit(float(gr["textSize"]))
        _add("grid", props)

    # --- columnHeaders (table/matrix) ---
    if "columnHeaders" in fmt:
        ch = fmt["columnHeaders"]
        props = {}
        if "bold" in ch: props["bold"] = _pbi_lit(ch["bold"])
        if "fontSize" in ch: props["fontSize"] = _pbi_lit(float(ch["fontSize"]))
        if "fontFamily" in ch: props["fontFamily"] = _pbi_lit(ch["fontFamily"])
        if "fontColor" in ch: props["fontColor"] = _solid_color(ch["fontColor"])
        if "backColor" in ch: props["backColor"] = _solid_color(ch["backColor"])
        if "alignment" in ch: props["alignment"] = _pbi_lit(ch["alignment"])
        if "autoSizeColumnWidth" in ch: props["autoSizeColumnWidth"] = _pbi_lit(ch["autoSizeColumnWidth"])
        if "wordWrap" in ch: props["wordWrap"] = _pbi_lit(ch["wordWrap"])
        _add("columnHeaders", props)

    # --- values (table rows) ---
    if "values" in fmt:
        vl = fmt["values"]
        props = {}
        if "bold" in vl: props["bold"] = _pbi_lit(vl["bold"])
        if "fontSize" in vl: props["fontSize"] = _pbi_lit(float(vl["fontSize"]))
        if "fontFamily" in vl: props["fontFamily"] = _pbi_lit(vl["fontFamily"])
        if "fontColor" in vl: props["fontColor"] = _solid_color(vl["fontColor"])
        if "backColor" in vl: props["backColor"] = _solid_color(vl["backColor"])
        if "wordWrap" in vl: props["wordWrap"] = _pbi_lit(vl["wordWrap"])
        _add("values", props)

    # --- total (table/matrix totals row) ---
    if "total" in fmt:
        tt = fmt["total"]
        props = {}
        if "show" in tt: props["show"] = _pbi_lit(tt["show"])
        if "bold" in tt: props["bold"] = _pbi_lit(tt["bold"])
        if "fontSize" in tt: props["fontSize"] = _pbi_lit(float(tt["fontSize"]))
        if "fontColor" in tt: props["fontColor"] = _solid_color(tt["fontColor"])
        if "backColor" in tt: props["backColor"] = _solid_color(tt["backColor"])
        _add("total", props)

    # --- outline ---
    if "outline" in fmt:
        ol = fmt["outline"]
        props = {}
        if "show" in ol: props["show"] = _pbi_lit(ol["show"])
        if "weight" in ol: props["weight"] = _pbi_lit(int(ol["weight"]))
        if "color" in ol: props["color"] = _solid_color(ol["color"])
        _add("outline", props)

    # --- shape (buttons, shapes) ---
    if "shape" in fmt:
        sh = fmt["shape"]
        props = {}
        if "map" in sh: props["map"] = _pbi_lit(sh["map"])
        if "rotation" in sh: props["rotation"] = _pbi_lit(int(sh["rotation"]))
        _add("shape", props)

    # --- fill (shape fill) ---
    if "fill" in fmt:
        fl = fmt["fill"]
        props = {}
        if "color" in fl: props["fillColor"] = _solid_color(fl["color"])
        if "transparency" in fl: props["transparency"] = _pbi_lit(float(fl["transparency"]))
        if "show" in fl: props["show"] = _pbi_lit(fl["show"])
        _add("fill", props)

    # --- line (line charts) ---
    if "line" in fmt:
        ln = fmt["line"]
        props = {}
        if "lineStyle" in ln: props["lineStyle"] = _pbi_lit(ln["lineStyle"])
        if "strokeWidth" in ln: props["strokeWidth"] = _pbi_lit(float(ln["strokeWidth"]))
        if "joinType" in ln: props["joinType"] = _pbi_lit(int(ln["joinType"]))
        if "showMarker" in ln: props["showMarker"] = _pbi_lit(ln["showMarker"])
        if "markerShape" in ln: props["markerShape"] = _pbi_lit(ln["markerShape"])
        if "markerSize" in ln: props["markerSize"] = _pbi_lit(int(ln["markerSize"]))
        _add("lineStyles", props)

    # --- categoryLabels (pie/donut) ---
    if "categoryLabels" in fmt:
        cl = fmt["categoryLabels"]
        props = {}
        if "show" in cl: props["show"] = _pbi_lit(cl["show"])
        if "fontSize" in cl: props["fontSize"] = _pbi_lit(float(cl["fontSize"]))
        if "color" in cl: props["categoryLabelFontColor"] = _solid_color(cl["color"])
        if "fontFamily" in cl: props["fontFamily"] = _pbi_lit(cl["fontFamily"])
        _add("categoryLabels", props)

    # --- slices (pie/donut) ---
    if "slices" in fmt:
        sl = fmt["slices"]
        props = {}
        if "innerRadius" in sl: props["innerRadiusRatio"] = _pbi_lit(int(sl["innerRadius"]))
        _add("slices", props)

    # --- general (objects — action buttons) ---
    if "action" in fmt:
        ac = fmt["action"]
        props = {}
        if "type" in ac: props["type"] = _pbi_lit(ac["type"])
        if "navigationSection" in ac: props["navigationSection"] = _pbi_lit(ac["navigationSection"])
        if "bookmark" in ac: props["bookmark"] = _pbi_lit(ac["bookmark"])
        _add("visualLink", props)

    # --- smallMultiples ---
    if "smallMultiples" in fmt:
        sm = fmt["smallMultiples"]
        props = {}
        if "minWidth" in sm: props["minWidth"] = _pbi_lit(int(sm["minWidth"]))
        if "maxWidth" in sm: props["maxWidth"] = _pbi_lit(int(sm["maxWidth"]))
        if "minHeight" in sm: props["minHeight"] = _pbi_lit(int(sm["minHeight"]))
        _add("smallMultiplesLayout", props)

    # --- rowHeaders (matrix) ---
    if "rowHeaders" in fmt:
        rh = fmt["rowHeaders"]
        props = {}
        if "bold" in rh: props["bold"] = _pbi_lit(rh["bold"])
        if "fontSize" in rh: props["fontSize"] = _pbi_lit(float(rh["fontSize"]))
        if "fontFamily" in rh: props["fontFamily"] = _pbi_lit(rh["fontFamily"])
        if "fontColor" in rh: props["fontColor"] = _solid_color(rh["fontColor"])
        if "alignment" in rh: props["alignment"] = _pbi_lit(rh["alignment"])
        _add("rowHeaders", props)

    # --- subTotals (matrix) ---
    if "subTotals" in fmt:
        st = fmt["subTotals"]
        props = {}
        if "bold" in st: props["bold"] = _pbi_lit(st["bold"])
        if "fontSize" in st: props["fontSize"] = _pbi_lit(float(st["fontSize"]))
        if "fontColor" in st: props["fontColor"] = _solid_color(st["fontColor"])
        if "backColor" in st: props["backColor"] = _solid_color(st["backColor"])
        if "columnSubtotals" in st: props["columnSubtotals"] = _pbi_lit(st["columnSubtotals"])
        if "rowSubtotals" in st: props["rowSubtotals"] = _pbi_lit(st["rowSubtotals"])
        _add("subTotals", props)

    # --- referenceLine ---
    if "referenceLine" in fmt:
        rl = fmt["referenceLine"]
        props = {}
        if "show" in rl: props["show"] = _pbi_lit(rl["show"])
        if "displayName" in rl: props["displayName"] = _pbi_lit(rl["displayName"])
        if "color" in rl: props["lineColor"] = _solid_color(rl["color"])
        if "style" in rl: props["style"] = _pbi_lit(rl["style"])
        if "width" in rl: props["width"] = _pbi_lit(float(rl["width"]))
        if "transparency" in rl: props["transparency"] = _pbi_lit(float(rl["transparency"]))
        if "position" in rl: props["position"] = _pbi_lit(rl["position"])
        _add("y1AxisReferenceLine", props)

    # --- donut ---
    if "donut" in fmt:
        dn = fmt["donut"]
        props = {}
        if "innerRadius" in dn: props["innerRadius"] = _pbi_lit(int(dn["innerRadius"]))
        if "radius" in dn: props["radius"] = _pbi_lit(int(dn["radius"]))
        if "maxSlices" in dn: props["maxSlicesVisible"] = _pbi_lit(int(dn["maxSlices"]))
        _add("donut", props)

    # --- bubbles (scatter chart) ---
    if "bubbles" in fmt:
        bb = fmt["bubbles"]
        props = {}
        if "size" in bb: props["bubbleSize"] = _pbi_lit(int(bb["size"]))
        if "shape" in bb: props["markerShape"] = _pbi_lit(bb["shape"])
        if "rangeType" in bb: props["markerRangeType"] = _pbi_lit(bb["rangeType"])
        _add("bubbles", props)

    # --- markers (scatter/line) ---
    if "markers" in fmt:
        mk = fmt["markers"]
        props = {}
        if "borderWidth" in mk: props["borderWidth"] = _pbi_lit(float(mk["borderWidth"]))
        if "transparency" in mk: props["transparency"] = _pbi_lit(float(mk["transparency"]))
        _add("markers", props)

    # --- imageScaling ---
    if "imageScaling" in fmt:
        props = {"imageScalingType": _pbi_lit(fmt["imageScaling"])}
        _add("imageScaling", props)

    # --- card (new card visual styling) ---
    if "card" in fmt and isinstance(fmt["card"], dict):
        cd = fmt["card"]
        props = {}
        if "barShow" in cd: props["barShow"] = _pbi_lit(cd["barShow"])
        if "barColor" in cd: props["barColor"] = _solid_color(cd["barColor"])
        if "barWeight" in cd: props["barWeight"] = _pbi_lit(float(cd["barWeight"]))
        if "cardPadding" in cd: props["cardPadding"] = _pbi_lit(float(cd["cardPadding"]))
        if "outlineStyle" in cd: props["outlineStyle"] = _pbi_lit(float(cd["outlineStyle"]))
        _add("card", props)

    # --- cardTitle ---
    if "cardTitle" in fmt:
        ct = fmt["cardTitle"]
        props = {}
        if "color" in ct: props["color"] = _solid_color(ct["color"])
        if "fontSize" in ct: props["fontSize"] = _pbi_lit(float(ct["fontSize"]))
        _add("cardTitle", props)

    # --- columnFormatting (table/matrix) ---
    if "columnFormatting" in fmt:
        cf = fmt["columnFormatting"]
        props = {}
        if "alignment" in cf: props["alignment"] = _pbi_lit(cf["alignment"])
        if "displayUnits" in cf:
            raw = _DISPLAY_UNITS.get(cf["displayUnits"], f"{cf['displayUnits']}D")
            props["labelDisplayUnits"] = {"expr": {"Literal": {"Value": raw}}}
        if "decimalPlaces" in cf: props["labelPrecision"] = _pbi_lit(int(cf["decimalPlaces"]))
        if "styleHeader" in cf: props["styleHeader"] = _pbi_lit(cf["styleHeader"])
        if "styleTotal" in cf: props["styleTotal"] = _pbi_lit(cf["styleTotal"])
        _add("columnFormatting", props)

    # --- zoom (scatter chart zoom slider) ---
    if "zoom" in fmt:
        _add("zoom", {"show": _pbi_lit(fmt["zoom"])})

    # --- general.objects (image URL, layout, orientation) ---
    if "general" in fmt and isinstance(fmt["general"], dict):
        gn = fmt["general"]
        props = {}
        if "layout" in gn: props["layout"] = _pbi_lit(gn["layout"])
        if "orientation" in gn: props["orientation"] = _pbi_lit(float(gn["orientation"]))
        _add("general", props)

    # --- visualLink (vcObjects — action buttons navigation) ---
    if "visualLink" in fmt:
        vl = fmt["visualLink"]
        props = {}
        if "show" in vl: props["show"] = _pbi_lit(vl["show"])
        if "type" in vl: props["type"] = _pbi_lit(vl["type"])
        if "tooltip" in vl: props["tooltip"] = _pbi_lit(vl["tooltip"])
        if "showDefaultTooltip" in vl: props["showDefaultTooltip"] = _pbi_lit(vl["showDefaultTooltip"])
        if "navigationSection" in vl: props["navigationSection"] = _pbi_lit(vl["navigationSection"])
        if "bookmark" in vl: props["bookmark"] = _pbi_lit(vl["bookmark"])
        _add_vc("visualLink", props)

    # --- visualHeaderTooltip (vcObjects) ---
    if "visualHeaderTooltip" in fmt:
        vht = fmt["visualHeaderTooltip"]
        props = {}
        if "text" in vht: props["text"] = _pbi_lit(vht["text"])
        if "type" in vht: props["type"] = _pbi_lit(vht["type"])
        if "bold" in vht: props["bold"] = _pbi_lit(vht["bold"])
        if "fontSize" in vht: props["fontSize"] = _pbi_lit(float(vht["fontSize"]))
        if "fontFamily" in vht: props["fontFamily"] = _pbi_lit(vht["fontFamily"])
        if "transparency" in vht: props["transparency"] = _pbi_lit(float(vht["transparency"]))
        if "background" in vht: props["themedBackground"] = _solid_color(vht["background"])
        if "titleFontColor" in vht: props["themedTitleFontColor"] = _solid_color(vht["titleFontColor"])
        _add_vc("visualHeaderTooltip", props)

    return {"_objects": objects, "_vcObjects": vc_objects}


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
def pbix_save(alias: str, output_path: str = "", overwrite: bool = False, backup: bool = True,
              strip_sensitivity_label: bool = False) -> str:
    """Save/repack the modified PBIX/PBIT file.

    Creates an automatic .bak backup before overwriting (unless backup=False).
    Set overwrite=False to refuse overwriting an existing file.

    Args:
        alias: The alias of the open file
        output_path: Where to save. Empty = overwrite original.
        overwrite: If False (default), refuse to overwrite an existing file
        backup: If True (default), create a .bak backup before overwriting
        strip_sensitivity_label: If True, remove MSIP sensitivity labels from the file
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

        _repack_pbix(work_dir, target, strip_sensitivity_label=strip_sensitivity_label)
        info["modified"] = False
        # Clear DAX cache since data may have changed
        _dax_cache.pop(alias, None)
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
        # Clear DAX cache to avoid stale data on reopen
        _dax_cache.pop(alias, None)
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
def pbix_format_visual(
    alias: str, page_index: int, visual_index: int, format_json: str
) -> str:
    """Format a visual with human-readable properties (colors, titles, fonts).

    Converts simple formatting options to PBI's internal objects structure.
    Merges with existing formatting — only specified properties are changed.

    Args:
        alias: The alias of the open file
        page_index: Zero-based page index
        visual_index: Zero-based visual index on the page
        format_json: JSON object with formatting options. Supported keys:

            title: {text, show, fontSize, color, fontFamily, bold, italic, alignment}
            subtitle: {text, show, fontSize, color, fontFamily}
            dataLabels: {show, fontSize, color, displayUnits, decimalPlaces}
            legend: {show, position, fontSize, color}
                position: "top", "bottom", "left", "right", "topCenter"
            categoryAxis: {show, fontSize, color, title, gridlineShow, innerPadding,
                invertAxis, axisType, start, end, switchAxisPosition}
            valueAxis: {show, fontSize, displayUnits, title, gridlineShow, start, end,
                decimalPlaces, switchAxisPosition}
                displayUnits: "none", "thousands", "millions", "billions", "auto"
            background: {color, transparency}
            border: {show, color, radius, width}
            dropShadow: {show, color, angle, blur, distance, spread, transparency,
                position, preset}
            padding: number | {top, bottom, left, right}
            spacing: {belowTitle, belowSubTitle, belowTitleArea, vertical}
            divider: {show, color, width, style, ignorePadding}
            visualHeader: {show, showOptionsMenu, showFocusModeButton, showPinButton,
                showFilterRestatementButton, showTooltipButton, showDrillUpButton, ...}
            visualTooltip: {show, type, fontSize, titleFontColor, valueFontColor,
                actionFontColor, background}
            dataColors: ["#hex1", "#hex2", ...]
            grid: {gridVertical, gridHorizontal, rowPadding, outlineColor, outlineWeight}
            columnHeaders: {bold, fontSize, fontFamily, fontColor, backColor, alignment}
            values: {bold, fontSize, fontFamily, fontColor, backColor, wordWrap}
            total: {show, bold, fontSize, fontColor, backColor}
            outline: {show, weight, color}
            fill: {color, transparency, show}
            line: {lineStyle, strokeWidth, showMarker, markerShape, markerSize}
            categoryLabels: {show, fontSize, color, fontFamily}
            slices: {innerRadius}
            smallMultiples: {minWidth, maxWidth, minHeight}
            stylePreset: "name"
            altText: "description"
            lockAspect: true/false

            Example: {"title": {"text": "Sales", "fontSize": 16}, "dataLabels": {"show": true}}
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

        try:
            fmt = json.loads(format_json)
        except json.JSONDecodeError as e:
            raise LayoutParseError(f"Invalid format_json: {e}")

        vc = containers[visual_index]
        config = _parse_visual_config(vc)
        sv = config.setdefault("singleVisual", {})
        existing_objects = sv.setdefault("objects", {})

        result = _build_format_objects(fmt)
        new_objects = result.get("_objects", {})
        new_vc_objects = result.get("_vcObjects", {})

        # Merge data formatting into singleVisual.objects
        for category, entries in new_objects.items():
            existing_objects[category] = entries

        # Merge container formatting into singleVisual.vcObjects
        if new_vc_objects:
            existing_vc_objects = sv.setdefault("vcObjects", {})
            for category, entries in new_vc_objects.items():
                existing_vc_objects[category] = entries

        vc["config"] = json.dumps(config, ensure_ascii=False)
        _set_layout(info["work_dir"], layout)
        info["modified"] = True

        applied = list(new_objects.keys()) + list(new_vc_objects.keys())
        return ToolResponse.ok(
            f"Formatted visual {visual_index} on page {page_index}: {', '.join(applied)}"
        ).to_text()
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

    Writes to both BaseThemes and RegisteredResources if the theme file
    exists in RegisteredResources (custom themes used by the report).

    Args:
        alias: The alias of the open file
        theme_json: Complete theme JSON string
        filename: Theme filename (default: CY24SU11.json)
    """
    try:
        info = _ensure_open(alias)
        work_dir = info["work_dir"]

        try:
            theme = json.loads(theme_json)
        except json.JSONDecodeError as e:
            raise LayoutParseError(f"Invalid JSON: {e}")

        written_to = []

        # Write to BaseThemes
        base_dir = os.path.join(work_dir, "Report", "StaticResources", "SharedResources", "BaseThemes")
        os.makedirs(base_dir, exist_ok=True)
        with open(os.path.join(base_dir, filename), "w", encoding="utf-8") as fh:
            json.dump(theme, fh, indent=2, ensure_ascii=False)
        written_to.append("BaseThemes")

        # Also write to RegisteredResources if the file exists there
        reg_dir = os.path.join(work_dir, "Report", "StaticResources", "RegisteredResources")
        reg_path = os.path.join(reg_dir, filename)
        if os.path.exists(reg_path):
            with open(reg_path, "w", encoding="utf-8") as fh:
                json.dump(theme, fh, indent=2, ensure_ascii=False)
            written_to.append("RegisteredResources")

        info["modified"] = True
        return ToolResponse.ok(f"Theme saved to {filename} ({', '.join(written_to)})").to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


def _load_theme_data_colors(work_dir: str) -> list[str]:
    """Load dataColors from the active theme (RegisteredResources first, then BaseThemes)."""
    for subdir in ("RegisteredResources", "SharedResources/BaseThemes"):
        theme_dir = os.path.join(work_dir, "Report", "StaticResources", subdir)
        if os.path.isdir(theme_dir):
            for f in os.listdir(theme_dir):
                if f.endswith(".json"):
                    with open(os.path.join(theme_dir, f)) as fh:
                        try:
                            theme = json.load(fh)
                            if "dataColors" in theme:
                                return [c.upper() for c in theme["dataColors"]]
                        except (json.JSONDecodeError, KeyError):
                            pass
    return []


def _resolve_theme_color(data_colors: list[str], color_id: int, percent: float) -> str:
    """Resolve a ThemeDataColor reference to a hex color string."""
    if color_id < len(data_colors):
        base = data_colors[color_id]
    else:
        base = "#808080"
    r, g, b = int(base[1:3], 16), int(base[3:5], 16), int(base[5:7], 16)
    if percent > 0:
        r = int(r + (255 - r) * percent)
        g = int(g + (255 - g) * percent)
        b = int(b + (255 - b) * percent)
    elif percent < 0:
        r = int(r * (1 + percent))
        g = int(g * (1 + percent))
        b = int(b * (1 + percent))
    return f"#{max(0,min(255,r)):02X}{max(0,min(255,g)):02X}{max(0,min(255,b)):02X}"


@mcp.tool()
def pbix_extract_colors(alias: str) -> str:
    """Extract all colors from the report — theme, visuals, and page backgrounds.

    Scans the theme JSON and every visual's objects/vcObjects for hex color
    values. Also resolves ThemeDataColor references (ColorId + Percent) to
    their actual rendered hex values. Returns a deduplicated list with
    locations so you know what to change for a complete recolor.

    Args:
        alias: The alias of the open file
    """
    import re
    try:
        info = _ensure_open(alias)
        work_dir = info["work_dir"]
        colors: dict[str, list[str]] = {}  # hex -> [locations]
        data_colors = _load_theme_data_colors(work_dir)

        def _add(hex_color: str, location: str):
            h = hex_color.upper()
            colors.setdefault(h, []).append(location)

        # Scan theme files
        for subdir in ("SharedResources/BaseThemes", "RegisteredResources"):
            theme_dir = os.path.join(work_dir, "Report", "StaticResources", subdir)
            if os.path.isdir(theme_dir):
                for f in os.listdir(theme_dir):
                    if f.endswith(".json"):
                        with open(os.path.join(theme_dir, f)) as fh:
                            text = fh.read()
                        for m in re.finditer(r'#[0-9A-Fa-f]{6}\b', text):
                            _add(m.group(), f"theme:{f}")

        # Scan layout — both hex literals AND ThemeDataColor refs
        layout = _get_layout(work_dir)
        if layout:
            for si, sec in enumerate(layout.get("sections", [])):
                page_name = sec.get("displayName", f"Page {si}")
                # Page-level config
                page_cfg_str = sec.get("config", "{}")
                if isinstance(page_cfg_str, str):
                    for m in re.finditer(r"'(#[0-9A-Fa-f]{6})'", page_cfg_str):
                        _add(m.group(1), f"{page_name}:pageConfig")

                for vi, vc in enumerate(sec.get("visualContainers", [])):
                    config_str = vc.get("config", "{}")
                    if isinstance(config_str, dict):
                        config_str = json.dumps(config_str)
                    config = json.loads(config_str) if isinstance(config_str, str) else config_str
                    sv = config.get("singleVisual", {}) if isinstance(config, dict) else {}
                    vtype = sv.get("visualType", "?") if isinstance(sv, dict) else "?"
                    loc = f"{page_name}[{vi}]:{vtype}"

                    # Find hex literals
                    for m in re.finditer(r"'(#[0-9A-Fa-f]{6})'", config_str):
                        _add(m.group(1), loc)

                    # Find ThemeDataColor refs (escaped JSON inside config strings)
                    for m in re.finditer(
                        r'"ThemeDataColor"\s*:\s*\{\s*"ColorId"\s*:\s*(\d+)\s*,\s*"Percent"\s*:\s*([-\d.]+)\s*\}',
                        config_str
                    ):
                        cid, pct = int(m.group(1)), float(m.group(2))
                        resolved = _resolve_theme_color(data_colors, cid, pct)
                        _add(resolved, f"{loc} [ThemeDataColor:{cid},{pct}]")

                    # Also check escaped variants (config stored as JSON string in JSON)
                    for m in re.finditer(
                        r'\\"ThemeDataColor\\"\s*:\s*\{\s*\\"ColorId\\"\s*:\s*(\d+)\s*,\s*\\"Percent\\"\s*:\s*([-\d.]+)\s*\}',
                        config_str
                    ):
                        cid, pct = int(m.group(1)), float(m.group(2))
                        resolved = _resolve_theme_color(data_colors, cid, pct)
                        _add(resolved, f"{loc} [ThemeDataColor:{cid},{pct}]")

        lines = []
        for hex_c in sorted(colors.keys()):
            locs = sorted(set(colors[hex_c]))
            lines.append(f"  {hex_c}  ({len(locs)} refs): {', '.join(locs[:8])}")

        return ToolResponse.ok(
            f"Found {len(colors)} unique colors:\n" + "\n".join(lines)
        ).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_recolor(alias: str, color_map_json: str) -> str:
    """Global find-and-replace of colors across the entire report.

    Replaces hex colors in the theme (BaseThemes + RegisteredResources),
    the report layout (all visuals, pages, config), and page configs.
    Case-insensitive matching.

    Also converts ThemeDataColor references to direct hex Literal values
    when the resolved theme color is in the replacement map. This ensures
    ALL color references are replaced, not just hex literals.

    Args:
        alias: The alias of the open file
        color_map_json: JSON object mapping old hex colors to new ones, e.g.
            {"#0F7C7B": "#C2185B", "#1AA6A5": "#E91E63", "#E7E4D8": "#F5E6F0"}
    """
    import re
    try:
        info = _ensure_open(alias)
        work_dir = info["work_dir"]

        try:
            color_map = json.loads(color_map_json)
        except json.JSONDecodeError as e:
            raise LayoutParseError(f"Invalid color_map_json: {e}")

        # Normalize keys to uppercase
        cmap = {k.upper(): v for k, v in color_map.items()}
        total_replacements = 0

        # Load theme dataColors for resolving ThemeDataColor refs
        data_colors = _load_theme_data_colors(work_dir)

        def _replace_hex(text: str) -> tuple[str, int]:
            count = 0
            for old, new in cmap.items():
                pattern = re.compile(re.escape(old), re.IGNORECASE)
                text, n = pattern.subn(new, text)
                count += n
            return text, count

        def _replace_theme_ref(m) -> str:
            """Replace a ThemeDataColor ref with a Literal hex if it matches the color map."""
            cid, pct = int(m.group(1)), float(m.group(2))
            resolved = _resolve_theme_color(data_colors, cid, pct).upper()
            # Check if this resolved color is in our replacement map
            new_color = cmap.get(resolved)
            if not new_color:
                # Also check close matches (ThemeDataColor percent shifts
                # produce slightly different hex than exact theme colors)
                for old_c, new_c in cmap.items():
                    if cid < len(data_colors) and data_colors[cid].upper() == old_c:
                        new_color = new_c
                        break
            if new_color:
                return f'"Literal":{{"Value":"\'{new_color}\'"}}'
            return m.group(0)  # no match, keep original

        def _replace_theme_ref_escaped(m) -> str:
            """Same but for escaped JSON (config strings inside JSON)."""
            cid, pct = int(m.group(1)), float(m.group(2))
            resolved = _resolve_theme_color(data_colors, cid, pct).upper()
            new_color = cmap.get(resolved)
            if not new_color:
                for old_c, new_c in cmap.items():
                    if cid < len(data_colors) and data_colors[cid].upper() == old_c:
                        new_color = new_c
                        break
            if new_color:
                return f'\\"Literal\\":{{\\"Value\\":\\"\'{new_color}\'\\"}}'
            return m.group(0)

        # Replace in theme files
        for subdir in ("SharedResources/BaseThemes", "RegisteredResources"):
            theme_dir = os.path.join(work_dir, "Report", "StaticResources", subdir)
            if os.path.isdir(theme_dir):
                for f in os.listdir(theme_dir):
                    if f.endswith(".json"):
                        fp = os.path.join(theme_dir, f)
                        with open(fp, "r", encoding="utf-8") as fh:
                            text = fh.read()
                        new_text, n = _replace_hex(text)
                        if n > 0:
                            with open(fp, "w", encoding="utf-8") as fh:
                                fh.write(new_text)
                            total_replacements += n

        # Replace in layout — hex colors + ThemeDataColor refs
        layout = _get_layout(work_dir)
        if layout:
            layout_str = json.dumps(layout, ensure_ascii=False)

            # Replace hex literals
            new_str, n = _replace_hex(layout_str)
            total_replacements += n

            # Replace ThemeDataColor references (non-escaped)
            prev = new_str
            new_str = re.sub(
                r'"ThemeDataColor"\s*:\s*\{\s*"ColorId"\s*:\s*(\d+)\s*,\s*"Percent"\s*:\s*([-\d.]+)\s*\}',
                _replace_theme_ref, new_str
            )
            total_replacements += (len(prev) - len(new_str)) // 10 if len(new_str) != len(prev) else 0

            # Replace ThemeDataColor references (escaped — config strings)
            prev = new_str
            new_str = re.sub(
                r'\\"ThemeDataColor\\"\s*:\s*\{\s*\\"ColorId\\"\s*:\s*(\d+)\s*,\s*\\"Percent\\"\s*:\s*([-\d.]+)\s*\}',
                _replace_theme_ref_escaped, new_str
            )
            total_replacements += (len(prev) - len(new_str)) // 10 if len(new_str) != len(prev) else 0

            # Count actual ThemeDataColor replacements properly
            theme_refs_before = len(re.findall(r'ThemeDataColor', layout_str))
            theme_refs_after = len(re.findall(r'ThemeDataColor', new_str))
            theme_replaced = theme_refs_before - theme_refs_after

            new_layout = json.loads(new_str)
            _set_layout(work_dir, new_layout)

        info["modified"] = True

        parts = [f"Replaced {total_replacements} hex color occurrences"]
        if theme_replaced > 0:
            parts.append(f"{theme_replaced} ThemeDataColor refs converted to hex")
        parts.append(f"({len(cmap)} colors mapped)")

        return ToolResponse.ok(" + ".join(parts)).to_text()
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


def _format_csv_value(val, delimiter: str = ",") -> str:
    """Format a single value for CSV output."""
    if val is None:
        return ""
    if isinstance(val, (int, float)):
        return str(val)
    from datetime import date, datetime
    if isinstance(val, (datetime, date)):
        return val.isoformat()
    s = str(val)
    # Quote if contains delimiter, quote, or newline
    if delimiter in s or '"' in s or "\n" in s or "\r" in s:
        s = s.replace('"', '""')
        return f'"{s}"'
    return s


def _is_system_table(name: str) -> bool:
    """Check if a table is a system/internal table (hidden from users)."""
    return name.startswith(("H$", "R$", "U$", "LocalDateTable", "DateTableTemplate"))


@mcp.tool()
def pbix_export_table_csv(
    alias: str, table_name: str, output_path: str, delimiter: str = ","
) -> str:
    """Export a table's data to a CSV file.

    Writes all rows of the table (no row limit) with headers. Strings are
    quoted when they contain the delimiter, quotes, or newlines. Dates are
    formatted as ISO 8601. Works on Import files only (not DirectQuery).

    Args:
        alias: The alias of the open file
        table_name: Name of the table to export
        output_path: Absolute path for the CSV file
        delimiter: Field delimiter (default ',')
    """
    try:
        info = _ensure_open(alias)
        if info.get("is_directquery"):
            return ToolResponse.error(
                "This file uses DirectQuery — table data is not stored locally.",
                UnsupportedFormatError.code,
            ).to_text()

        from pbix_mcp.formats.model_reader import ModelReader
        model = ModelReader(info["path"], work_dir=info.get("work_dir"))
        table_data = model.get_table(table_name, max_rows=0)

        if not table_data["columns"]:
            return ToolResponse.error(
                f"Table '{table_name}' not found.", "TABLE_NOT_FOUND"
            ).to_text()

        cols = table_data["columns"]
        rows = table_data["rows"]

        with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
            f.write(delimiter.join(_format_csv_value(c, delimiter) for c in cols) + "\n")
            for row in rows:
                f.write(delimiter.join(_format_csv_value(v, delimiter) for v in row) + "\n")

        file_size = os.path.getsize(output_path)
        return ToolResponse.ok(
            f"Exported '{table_name}' to {output_path}\n"
            f"  {len(rows):,} rows × {len(cols)} columns ({file_size:,} bytes)"
        ).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "CSV_EXPORT_ERROR").to_text()


@mcp.tool()
def pbix_export_all_tables_csv(alias: str, output_dir: str) -> str:
    """Export every data table in the model to separate CSV files.

    Creates one CSV per table in the output directory. Skips system tables
    (H$, R$, U$, LocalDateTable, DateTableTemplate). Works on Import files only.

    Args:
        alias: The alias of the open file
        output_dir: Absolute path for the output directory (created if missing)
    """
    try:
        info = _ensure_open(alias)
        if info.get("is_directquery"):
            return ToolResponse.error(
                "This file uses DirectQuery — table data is not stored locally.",
                UnsupportedFormatError.code,
            ).to_text()

        os.makedirs(output_dir, exist_ok=True)

        from pbix_mcp.formats.model_reader import ModelReader
        model = ModelReader(info["path"], work_dir=info.get("work_dir"))
        stats = model.statistics
        data_tables = [t for t in stats if not _is_system_table(t["TableName"])]

        exported = []
        errors = []
        for t in data_tables:
            tname = t["TableName"]
            try:
                safe_name = "".join(c if c.isalnum() or c in "-_. " else "_" for c in tname)
                csv_path = os.path.join(output_dir, f"{safe_name}.csv")
                tdata = model.get_table(tname, max_rows=0)
                if not tdata["columns"]:
                    continue
                with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
                    f.write(",".join(_format_csv_value(c) for c in tdata["columns"]) + "\n")
                    for row in tdata["rows"]:
                        f.write(",".join(_format_csv_value(v) for v in row) + "\n")
                exported.append(f"  {tname}: {len(tdata['rows']):,} rows -> {safe_name}.csv")
            except Exception as e:
                errors.append(f"  {tname}: {e}")

        msg = f"Exported {len(exported)} tables to {output_dir}\n" + "\n".join(exported)
        if errors:
            msg += "\n\nErrors:\n" + "\n".join(errors)
        return ToolResponse.ok(msg).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "CSV_EXPORT_ERROR").to_text()


@mcp.tool()
def pbix_find_value(
    alias: str, search_value: str, case_sensitive: bool = False, max_matches: int = 100
) -> str:
    """Search for a value across all tables and columns in the model.

    Returns all table.column locations where the value appears, with the
    number of matching rows per column. Works on Import files only.

    Args:
        alias: The alias of the open file
        search_value: The value to search for (string comparison)
        case_sensitive: If False (default), compares case-insensitively
        max_matches: Maximum locations to report (default 100)
    """
    try:
        info = _ensure_open(alias)
        if info.get("is_directquery"):
            return ToolResponse.error(
                "This file uses DirectQuery — table data is not stored locally.",
                UnsupportedFormatError.code,
            ).to_text()

        from pbix_mcp.formats.model_reader import ModelReader
        model = ModelReader(info["path"], work_dir=info.get("work_dir"))
        stats = model.statistics
        data_tables = [t for t in stats if not _is_system_table(t["TableName"])]

        needle = search_value if case_sensitive else search_value.lower()
        matches: list[tuple[str, str, int, list]] = []  # (table, col, count, samples)

        for t in data_tables:
            tname = t["TableName"]
            try:
                tdata = model.get_table(tname, max_rows=0)
                cols = tdata["columns"]
                for ci, cname in enumerate(cols):
                    count = 0
                    samples = []
                    for row in tdata["rows"]:
                        val = row[ci]
                        if val is None:
                            continue
                        s = str(val) if case_sensitive else str(val).lower()
                        if needle in s:
                            count += 1
                            if len(samples) < 3:
                                samples.append(str(val))
                    if count > 0:
                        matches.append((tname, cname, count, samples))
                        if len(matches) >= max_matches:
                            break
                if len(matches) >= max_matches:
                    break
            except Exception:
                continue

        if not matches:
            return ToolResponse.ok(f"No matches found for '{search_value}'.").to_text()

        lines = [f"Found '{search_value}' in {len(matches)} location(s):\n"]
        for tname, cname, count, samples in matches:
            sample_str = ", ".join(f"'{s}'" for s in samples[:3])
            lines.append(f"  {tname}.{cname}: {count:,} matches (e.g. {sample_str})")
        return ToolResponse.ok("\n".join(lines)).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "SEARCH_ERROR").to_text()


def _parse_where_clause(where: str) -> list[dict]:
    """Parse a simple SQL-like WHERE clause into conditions.

    Supports: column = 'value', column != 'value', column > N, column < N,
    column >= N, column <= N, column IN ('a', 'b'), column LIKE '%x%'
    joined by AND/OR. Returns list of {col, op, value, connector} dicts.
    """
    import re as _re
    if not where.strip():
        return []

    # Tokenize by AND/OR (preserving connectors)
    parts = _re.split(r'\s+(AND|OR)\s+', where, flags=_re.IGNORECASE)
    conditions = []
    for i, part in enumerate(parts):
        if part.upper() in ("AND", "OR"):
            continue
        connector = "AND"
        if i > 0:
            connector = parts[i - 1].upper()

        # Match: col OP value
        m = _re.match(r"\s*([a-zA-Z_][\w\s\-\.]*?)\s*(IN|LIKE|!=|>=|<=|>|<|=)\s*(.+)\s*$", part, _re.IGNORECASE)
        if not m:
            raise ValueError(f"Can't parse condition: '{part}'")
        col = m.group(1).strip()
        op = m.group(2).upper()
        val = m.group(3).strip()

        # Parse value
        if op == "IN":
            # ('a', 'b', 'c')
            val = val.strip("()")
            items = [x.strip().strip("'\"") for x in val.split(",")]
            parsed_val = items
        elif val.startswith("'") and val.endswith("'"):
            parsed_val = val[1:-1]
        elif val.startswith('"') and val.endswith('"'):
            parsed_val = val[1:-1]
        else:
            # Try number
            try:
                parsed_val = float(val) if "." in val else int(val)
            except ValueError:
                parsed_val = val
        conditions.append({"col": col, "op": op, "value": parsed_val, "connector": connector})
    return conditions


def _eval_condition(row_val, op: str, value) -> bool:
    """Evaluate a single condition against a row value."""
    if row_val is None:
        return False
    try:
        if op == "=":
            return str(row_val) == str(value)
        if op == "!=":
            return str(row_val) != str(value)
        if op == ">":
            return float(row_val) > float(value)
        if op == ">=":
            return float(row_val) >= float(value)
        if op == "<":
            return float(row_val) < float(value)
        if op == "<=":
            return float(row_val) <= float(value)
        if op == "IN":
            return str(row_val) in [str(x) for x in value]
        if op == "LIKE":
            # Convert SQL LIKE to regex
            import re as _re
            pattern = _re.escape(str(value)).replace("%", ".*").replace("_", ".")
            return bool(_re.match(f"^{pattern}$", str(row_val), _re.IGNORECASE))
    except (ValueError, TypeError):
        return False
    return False


@mcp.tool()
def pbix_query_table(
    alias: str,
    table_name: str,
    where: str = "",
    columns: str = "",
    max_rows: int = 100,
    order_by: str = "",
) -> str:
    """Filter table rows with a SQL-like WHERE clause.

    Supports operators: =, !=, >, >=, <, <=, LIKE, IN. Conditions joined
    by AND/OR. Column values can be strings ('USA'), numbers (42), or
    lists for IN (('USA', 'Canada')).

    Args:
        alias: The alias of the open file
        table_name: Name of the table to query
        where: WHERE clause, e.g. "Country = 'USA' AND Amount > 1000"
        columns: Comma-separated columns to return (empty = all)
        max_rows: Maximum rows to return (default 100)
        order_by: Column name to sort by (optional, append ' DESC' for descending)
    """
    try:
        info = _ensure_open(alias)
        if info.get("is_directquery"):
            return ToolResponse.error(
                "This file uses DirectQuery — table data is not stored locally.",
                UnsupportedFormatError.code,
            ).to_text()

        from pbix_mcp.formats.model_reader import ModelReader
        model = ModelReader(info["path"], work_dir=info.get("work_dir"))
        tdata = model.get_table(table_name, max_rows=0)

        if not tdata["columns"]:
            return ToolResponse.error(
                f"Table '{table_name}' not found.", "TABLE_NOT_FOUND"
            ).to_text()

        all_cols = tdata["columns"]
        col_idx = {c: i for i, c in enumerate(all_cols)}

        # Parse WHERE
        conditions = _parse_where_clause(where) if where else []
        for c in conditions:
            if c["col"] not in col_idx:
                return ToolResponse.error(
                    f"Column '{c['col']}' not found in table '{table_name}'", "COLUMN_NOT_FOUND"
                ).to_text()

        # Filter rows
        filtered = []
        for row in tdata["rows"]:
            if not conditions:
                filtered.append(row)
                continue
            # Eval AND/OR — simple left-to-right
            result = _eval_condition(row[col_idx[conditions[0]["col"]]],
                                     conditions[0]["op"], conditions[0]["value"])
            for cond in conditions[1:]:
                val = row[col_idx[cond["col"]]]
                r = _eval_condition(val, cond["op"], cond["value"])
                if cond["connector"] == "AND":
                    result = result and r
                else:
                    result = result or r
            if result:
                filtered.append(row)

        # Column projection
        if columns.strip():
            proj_cols = [c.strip() for c in columns.split(",")]
            for c in proj_cols:
                if c not in col_idx:
                    return ToolResponse.error(
                        f"Column '{c}' not found", "COLUMN_NOT_FOUND"
                    ).to_text()
            proj_idx = [col_idx[c] for c in proj_cols]
            filtered = [[r[i] for i in proj_idx] for r in filtered]
            out_cols = proj_cols
        else:
            out_cols = all_cols

        # ORDER BY
        if order_by.strip():
            ob = order_by.strip()
            reverse = False
            if ob.upper().endswith(" DESC"):
                ob = ob[:-5].strip()
                reverse = True
            elif ob.upper().endswith(" ASC"):
                ob = ob[:-4].strip()
            if ob not in [c for c in out_cols]:
                return ToolResponse.error(
                    f"ORDER BY column '{ob}' not in output", "COLUMN_NOT_FOUND"
                ).to_text()
            ob_idx = out_cols.index(ob)
            filtered.sort(key=lambda r: (r[ob_idx] is None, r[ob_idx]), reverse=reverse)

        total = len(filtered)
        shown = filtered[:max_rows]

        # Format output
        from pbix_mcp.formats.model_reader import format_table_data
        formatted = format_table_data({"columns": out_cols, "rows": shown}, max_rows=max_rows)
        header = f"Query returned {total:,} rows"
        if total > max_rows:
            header += f" (showing first {max_rows})"
        return ToolResponse.ok(f"{header}\n\n{formatted}").to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "QUERY_ERROR").to_text()


@mcp.tool()
def pbix_table_stats(alias: str, table_name: str) -> str:
    """Profile a table — per-column stats (min/max/avg/distinct/nulls).

    For strings: distinct count, null count, min/max length, top 5 values.
    For numbers: min/max/avg/sum/null count.
    For dates: min/max/null count.

    Args:
        alias: The alias of the open file
        table_name: Name of the table to profile
    """
    try:
        info = _ensure_open(alias)
        if info.get("is_directquery"):
            return ToolResponse.error(
                "This file uses DirectQuery — table data is not stored locally.",
                UnsupportedFormatError.code,
            ).to_text()

        from collections import Counter
        from datetime import date, datetime

        from pbix_mcp.formats.model_reader import ModelReader
        model = ModelReader(info["path"], work_dir=info.get("work_dir"))
        tdata = model.get_table(table_name, max_rows=0)

        if not tdata["columns"]:
            return ToolResponse.error(
                f"Table '{table_name}' not found.", "TABLE_NOT_FOUND"
            ).to_text()

        cols = tdata["columns"]
        rows = tdata["rows"]
        total_rows = len(rows)

        lines = [f"# Stats for '{table_name}' ({total_rows:,} rows, {len(cols)} columns)\n"]

        for ci, cname in enumerate(cols):
            values = [r[ci] for r in rows]
            nulls = sum(1 for v in values if v is None)
            non_null = [v for v in values if v is not None]

            if not non_null:
                lines.append(f"## {cname}")
                lines.append(f"  All {total_rows:,} values are null")
                lines.append("")
                continue

            # Detect type from first non-null value
            sample = non_null[0]
            if isinstance(sample, (int, float)) and not isinstance(sample, bool):
                vals = [float(v) for v in non_null]
                mn, mx = min(vals), max(vals)
                avg = sum(vals) / len(vals)
                lines.append(f"## {cname} (numeric)")
                lines.append(f"  count={len(non_null):,}, nulls={nulls:,}")
                lines.append(f"  min={mn:g}, max={mx:g}, avg={avg:.2f}, sum={sum(vals):g}")
                lines.append(f"  distinct={len(set(vals)):,}")
            elif isinstance(sample, (datetime, date)):
                lines.append(f"## {cname} (datetime)")
                lines.append(f"  count={len(non_null):,}, nulls={nulls:,}")
                lines.append(f"  min={min(non_null)}, max={max(non_null)}")
                lines.append(f"  distinct={len(set(non_null)):,}")
            else:
                # String
                strs = [str(v) for v in non_null]
                lens = [len(s) for s in strs]
                distinct = set(strs)
                counter = Counter(strs)
                top = counter.most_common(5)
                lines.append(f"## {cname} (string)")
                lines.append(f"  count={len(non_null):,}, nulls={nulls:,}, distinct={len(distinct):,}")
                lines.append(f"  length: min={min(lens)}, max={max(lens)}, avg={sum(lens)/len(lens):.1f}")
                lines.append(f"  top 5: {', '.join(f'{v!r} ({c})' for v, c in top)}")
            lines.append("")

        return ToolResponse.ok("\n".join(lines)).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "STATS_ERROR").to_text()


@mcp.tool()
def pbix_data_diff(alias_a: str, alias_b: str, table_name: str, key_columns: str) -> str:
    """Diff row data between the same table in two PBIX files.

    Matches rows by key_columns (comma-separated), then reports:
    - Added rows (in B, not in A)
    - Removed rows (in A, not in B)
    - Changed rows (same key, different values)

    Args:
        alias_a: Alias of the first (old) file
        alias_b: Alias of the second (new) file
        table_name: Table to diff (must exist in both files)
        key_columns: Comma-separated columns to match rows by
    """
    try:
        info_a = _ensure_open(alias_a)
        info_b = _ensure_open(alias_b)
        if info_a.get("is_directquery") or info_b.get("is_directquery"):
            return ToolResponse.error(
                "DirectQuery files don't store data locally.",
                UnsupportedFormatError.code,
            ).to_text()

        from pbix_mcp.formats.model_reader import ModelReader
        model_a = ModelReader(info_a["path"], work_dir=info_a.get("work_dir"))
        model_b = ModelReader(info_b["path"], work_dir=info_b.get("work_dir"))

        t_a = model_a.get_table(table_name, max_rows=0)
        t_b = model_b.get_table(table_name, max_rows=0)

        if not t_a["columns"]:
            return ToolResponse.error(f"Table '{table_name}' not in file A", "TABLE_NOT_FOUND").to_text()
        if not t_b["columns"]:
            return ToolResponse.error(f"Table '{table_name}' not in file B", "TABLE_NOT_FOUND").to_text()

        keys = [k.strip() for k in key_columns.split(",")]
        cols_a, cols_b = t_a["columns"], t_b["columns"]

        for k in keys:
            if k not in cols_a:
                return ToolResponse.error(f"Key column '{k}' not in table A", "COLUMN_NOT_FOUND").to_text()
            if k not in cols_b:
                return ToolResponse.error(f"Key column '{k}' not in table B", "COLUMN_NOT_FOUND").to_text()

        key_idx_a = [cols_a.index(k) for k in keys]
        key_idx_b = [cols_b.index(k) for k in keys]

        def row_key(row, key_idx):
            return tuple(str(row[i]) for i in key_idx)

        map_a = {row_key(r, key_idx_a): r for r in t_a["rows"]}
        map_b = {row_key(r, key_idx_b): r for r in t_b["rows"]}

        added_keys = set(map_b) - set(map_a)
        removed_keys = set(map_a) - set(map_b)
        common_keys = set(map_a) & set(map_b)

        # Compare common rows by value
        common_cols = [c for c in cols_a if c in cols_b]
        changed = []
        for k in common_keys:
            ra, rb = map_a[k], map_b[k]
            row_changes = []
            for cname in common_cols:
                va = ra[cols_a.index(cname)]
                vb = rb[cols_b.index(cname)]
                if str(va) != str(vb):
                    row_changes.append((cname, va, vb))
            if row_changes:
                changed.append((k, row_changes))

        lines = [
            f"# Data diff: '{table_name}' (key: {key_columns})",
            "",
            f"File A: {len(t_a['rows']):,} rows",
            f"File B: {len(t_b['rows']):,} rows",
            "",
            f"Summary: {len(added_keys)} added, {len(removed_keys)} removed, {len(changed)} changed",
        ]

        if added_keys:
            lines.append(f"\n## Added ({len(added_keys)}):")
            for k in sorted(list(added_keys))[:20]:
                lines.append(f"  + {' / '.join(k)}")
            if len(added_keys) > 20:
                lines.append(f"  ... and {len(added_keys) - 20} more")

        if removed_keys:
            lines.append(f"\n## Removed ({len(removed_keys)}):")
            for k in sorted(list(removed_keys))[:20]:
                lines.append(f"  - {' / '.join(k)}")
            if len(removed_keys) > 20:
                lines.append(f"  ... and {len(removed_keys) - 20} more")

        if changed:
            lines.append(f"\n## Changed ({len(changed)}):")
            for k, row_changes in changed[:20]:
                lines.append(f"  ~ {' / '.join(k)}")
                for cname, va, vb in row_changes:
                    lines.append(f"      {cname}: {va!r} -> {vb!r}")
            if len(changed) > 20:
                lines.append(f"  ... and {len(changed) - 20} more")

        return ToolResponse.ok("\n".join(lines)).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "DIFF_ERROR").to_text()


@mcp.tool()
def pbix_replace_value(
    alias: str,
    table_name: str,
    column_name: str,
    old_value: str,
    new_value: str,
    case_sensitive: bool = True,
) -> str:
    """Find and replace ALL occurrences of a value in a column.

    Reads the table, replaces all rows where the column matches old_value,
    and writes the updated data back via DataModel rebuild.

    LIMITATION: Uses the full rebuild pipeline — works on builder-created
    files but may break PBI Desktop files with SQL Server imports (destroys
    M expressions). For PBI Desktop files, use with caution.

    Args:
        alias: The alias of the open file
        table_name: Name of the table
        column_name: Name of the column to modify
        old_value: Value to find (exact match)
        new_value: Value to replace with
        case_sensitive: If False, matches strings case-insensitively (default True)
    """
    try:
        info = _ensure_open(alias)
        if info.get("is_directquery"):
            return ToolResponse.error(
                "This file uses DirectQuery — table data is not stored locally.",
                UnsupportedFormatError.code,
            ).to_text()

        from pbix_mcp.formats.model_reader import ModelReader
        model = ModelReader(info["path"], work_dir=info.get("work_dir"))
        tdata = model.get_table(table_name, max_rows=0)

        if not tdata["columns"]:
            return ToolResponse.error(
                f"Table '{table_name}' not found.", "TABLE_NOT_FOUND"
            ).to_text()

        cols = tdata["columns"]
        if column_name not in cols:
            return ToolResponse.error(
                f"Column '{column_name}' not found in table '{table_name}'. "
                f"Available: {', '.join(cols)}",
                "COLUMN_NOT_FOUND"
            ).to_text()

        col_idx = cols.index(column_name)

        # Replace values in rows
        replaced = 0
        if case_sensitive:
            def matches(v):
                return str(v) == old_value
        else:
            needle = old_value.lower()
            def matches(v):
                return str(v).lower() == needle

        # Detect target data type from first non-null sample
        sample = next((r[col_idx] for r in tdata["rows"] if r[col_idx] is not None), None)

        # Coerce new_value to match column type
        def coerce(v):
            if sample is None:
                return v
            if isinstance(sample, bool):
                return v.lower() in ("true", "1", "yes")
            if isinstance(sample, int):
                return int(v)
            if isinstance(sample, float):
                return float(v)
            return v

        try:
            new_val_typed = coerce(new_value)
        except (ValueError, TypeError) as e:
            return ToolResponse.error(
                f"new_value '{new_value}' cannot be converted to column type: {e}",
                "TYPE_MISMATCH"
            ).to_text()

        # Rebuild rows as list-of-dicts (required by _rebuild_datamodel)
        new_rows = []
        for row in tdata["rows"]:
            row_dict = dict(zip(cols, row))
            if matches(row_dict[column_name]):
                row_dict[column_name] = new_val_typed
                replaced += 1
            new_rows.append(row_dict)

        if replaced == 0:
            return ToolResponse.ok(
                f"No matches found — '{old_value}' not in {table_name}.{column_name}"
            ).to_text()

        # Get column definitions from metadata for _rebuild_datamodel
        dm_path = os.path.join(info["work_dir"], "DataModel")
        from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
        from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

        with open(dm_path, "rb") as f:
            dm_bytes = f.read()
        abf = decompress_datamodel(dm_bytes)
        meta_bytes = read_metadata_sqlite(abf)

        fd, tmp_path = tempfile.mkstemp(suffix=".db")
        os.write(fd, meta_bytes)
        os.close(fd)
        try:
            conn = sqlite3.connect(tmp_path)
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
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

        if not col_rows:
            return ToolResponse.error(
                f"Table '{table_name}' has no user columns.", "TABLE_NOT_FOUND"
            ).to_text()

        columns_def = [
            {"name": cr["ExplicitName"],
             "data_type": _AMO_TO_TYPE.get(cr["ExplicitDataType"], "String")}
            for cr in col_rows
        ]

        # Filter new_rows to only include columns that exist in columns_def
        # (tdata may include RowNumber/hidden cols that columns_def excludes)
        valid_names = {c["name"] for c in columns_def}
        filtered_rows = [
            {k: v for k, v in r.items() if k in valid_names}
            for r in new_rows
        ]

        old_size, new_size = _rebuild_datamodel(
            info,
            table_updates={table_name: {"columns": columns_def, "rows": filtered_rows}},
        )
        info["modified"] = True

        return ToolResponse.ok(
            f"Replaced {replaced:,} occurrences of '{old_value}' with '{new_value}' "
            f"in {table_name}.{column_name}\n"
            f"  DataModel: {old_size:,} → {new_size:,} bytes"
        ).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(f"{str(e)}\n{traceback.format_exc()}", "REPLACE_ERROR").to_text()


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

        # Get existing user hierarchies
        user_hierarchies = []
        for hrow in conn.execute(
            "SELECT h.Name, t.Name as TableName FROM Hierarchy h "
            "JOIN [Table] t ON h.TableID = t.ID "
            "WHERE t.ModelID = 1 ORDER BY h.ID"
        ):
            levels = []
            for lrow in conn.execute(
                "SELECT l.Name, c.ExplicitName as ColName FROM Level l "
                "JOIN [Column] c ON l.ColumnID = c.ID "
                "JOIN Hierarchy h ON l.HierarchyID = h.ID "
                "JOIN [Table] t ON h.TableID = t.ID "
                "WHERE h.Name = ? AND t.Name = ? ORDER BY l.Ordinal",
                (hrow["Name"], hrow["TableName"]),
            ):
                levels.append({"name": lrow["Name"], "column": lrow["ColName"]})
            if levels:
                user_hierarchies.append({
                    "table": hrow["TableName"],
                    "name": hrow["Name"],
                    "levels": levels,
                })

        # Get existing RLS roles
        rls_roles = []
        for rrow in conn.execute(
            "SELECT r.Name, r.Description FROM Role r WHERE r.ModelID = 1"
        ):
            perms = conn.execute(
                "SELECT t.Name as TableName, tp.FilterExpression "
                "FROM TablePermission tp JOIN [Table] t ON tp.TableID = t.ID "
                "WHERE tp.RoleID = (SELECT ID FROM Role WHERE Name = ?)",
                (rrow["Name"],),
            ).fetchall()
            for p in perms:
                rls_roles.append({
                    "role_name": rrow["Name"],
                    "description": rrow["Description"] or "",
                    "table_name": p["TableName"],
                    "filter_expression": p["FilterExpression"],
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

    # Add all user hierarchies (existing, preserved across rebuild)
    for uh in user_hierarchies:
        if uh["table"] not in remove_tables:
            builder.add_user_hierarchy(uh["table"], uh["name"], uh["levels"])

    new_pbix = builder.build()

    # Extract new DataModel from builder output
    import io
    import zipfile
    new_z = zipfile.ZipFile(io.BytesIO(new_pbix))
    new_dm = new_z.read("DataModel")

    # Write new DataModel
    with open(dm_path, "wb") as f:
        f.write(new_dm)

    # Re-apply RLS roles (builder doesn't support them, so splice after rebuild)
    if rls_roles:
        def _restore_rls(conn: sqlite3.Connection):
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            maxid_row = c.execute("SELECT Value FROM DBPROPERTIES WHERE Name = 'MAXID'").fetchone()
            max_id = int(maxid_row[0]) if maxid_row else 0

            roles_created = {}  # role_name -> role_id
            for rls in rls_roles:
                rname = rls["role_name"]
                if rname not in roles_created:
                    max_id += 1
                    roles_created[rname] = max_id
                    c.execute(
                        "INSERT INTO Role (ID, ModelID, Name, Description) VALUES (?, 1, ?, ?)",
                        (max_id, rname, rls.get("description") or None),
                    )

                role_id = roles_created[rname]
                trow = c.execute(
                    "SELECT ID FROM [Table] WHERE Name = ? AND ModelID = 1",
                    (rls["table_name"],),
                ).fetchone()
                if trow and rls.get("filter_expression"):
                    max_id += 1
                    c.execute(
                        "INSERT INTO TablePermission (ID, RoleID, TableID, FilterExpression) "
                        "VALUES (?, ?, ?, ?)",
                        (max_id, role_id, trow["ID"], rls["filter_expression"]),
                    )

            c.execute("UPDATE DBPROPERTIES SET Value = ? WHERE Name = 'MAXID'", (str(max_id),))
            conn.commit()

        _modify_metadata_only(dm_path, _restore_rls)

    # Clear DAX cache — rebuild changes data
    _dax_cache.clear()

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

    # Clear DAX cache — metadata changes may affect measure evaluation
    _dax_cache.clear()

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
            # Update MAXID so subsequent adds get a fresh ID
            c.execute(
                "UPDATE DBPROPERTIES SET Value = ? WHERE Name = 'MAXID'",
                (str(new_id),)
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

        # Build row data for the field parameter table
        rows = []
        for i, f in enumerate(fields):
            rows.append({
                parameter_name: f["display"],
                f"{parameter_name} Fields": f["ref"],
                f"{parameter_name} Order": i,
            })

        # Create the table via _rebuild_datamodel (full VertiPaq storage)
        extra_table = {
            "name": parameter_name,
            "columns": [
                {"name": parameter_name, "data_type": "String"},
                {"name": f"{parameter_name} Fields", "data_type": "String"},
                {"name": f"{parameter_name} Order", "data_type": "Int64"},
            ],
            "rows": rows,
        }

        old_size, new_size = _rebuild_datamodel(
            info,
            extra_tables=[extra_table],
        )
        info["modified"] = True

        field_list = ", ".join(f["display"] for f in fields)
        return ToolResponse.ok(
            f"Field parameter '{parameter_name}' created with {len(fields)} fields: {field_list}\n"
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

        # Build row data for the calculation group table
        # Calc groups have 2 columns: Name (text) and Ordinal (int)
        rows = []
        for i, item in enumerate(items):
            rows.append({
                "Name": item["name"],
                "Ordinal": i,
            })

        extra_table = {
            "name": group_name,
            "columns": [
                {"name": "Name", "data_type": "String"},
                {"name": "Ordinal", "data_type": "Int64"},
            ],
            "rows": rows,
        }

        # Create table via _rebuild_datamodel (full VertiPaq storage)
        old_size, new_size = _rebuild_datamodel(
            info,
            extra_tables=[extra_table],
        )

        # Now add CalculationGroup + CalculationItem metadata via splice
        # (these are metadata-only — no VertiPaq impact)
        def _do_calc_group(conn: sqlite3.Connection):
            conn.row_factory = sqlite3.Row
            c = conn.cursor()

            maxid_row = c.execute("SELECT Value FROM DBPROPERTIES WHERE Name = 'MAXID'").fetchone()
            max_id = int(maxid_row[0]) if maxid_row else 0

            # Find the table we just created
            trow = c.execute(
                "SELECT ID FROM [Table] WHERE Name = ? AND ModelID = 1", (group_name,)
            ).fetchone()
            if not trow:
                raise PBIXMCPError(f"Table '{group_name}' not found after rebuild", "INTERNAL_ERROR")
            table_id = trow["ID"]

            # Create CalculationGroup
            max_id += 1
            cg_id = max_id
            c.execute(
                "INSERT INTO CalculationGroup (ID, TableID, Precedence, ModifiedTime) "
                "VALUES (?, ?, ?, ?)",
                (cg_id, table_id, precedence, int(datetime.now().timestamp() * 1e7)),
            )

            # Link table to calculation group
            c.execute(
                "UPDATE [Table] SET CalculationGroupID = ? WHERE ID = ?",
                (cg_id, table_id),
            )

            # Create CalculationItems
            for i, item in enumerate(items):
                max_id += 1
                c.execute(
                    "INSERT INTO CalculationItem (ID, CalculationGroupID, Name, "
                    "Expression, Ordinal, ModifiedTime) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (max_id, cg_id, item["name"], item["expression"], i,
                     int(datetime.now().timestamp() * 1e7)),
                )

            c.execute("UPDATE DBPROPERTIES SET Value = ? WHERE Name = 'MAXID'", (str(max_id),))
            conn.commit()

        dm_path = os.path.join(info["work_dir"], "DataModel")
        _modify_metadata_only(dm_path, _do_calc_group)
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
        fd, tmp_path = tempfile.mkstemp(suffix=".db")
        os.write(fd, db_bytes)
        os.close(fd)
        conn = None
        try:
            conn = sqlite3.connect(tmp_path)
            conn.row_factory = sqlite3.Row

            roles = conn.execute("SELECT * FROM [Role]").fetchall()
            if not roles:
                conn.close()
                conn = None
                return ToolResponse.ok("No RLS roles defined in this file.").to_text()

            lines = [f"RLS Roles ({len(roles)}):\n"]
            for role in roles:
                role_id = role["ID"]
                role_name = role["Name"] if "Name" in role.keys() else f"Role {role_id}"
                lines.append(f"  Role: {role_name} (ID={role_id})")

                perms = conn.execute(
                    "SELECT * FROM [TablePermission] WHERE RoleID = ?",
                    (role_id,)
                ).fetchall()
                for perm in perms:
                    table_id = perm["TableID"]
                    perm_keys = perm.keys()
                    filter_expr = perm["FilterExpression"] if "FilterExpression" in perm_keys else (perm["QueryExpression"] if "QueryExpression" in perm_keys else "")
                    trow = conn.execute(
                        "SELECT Name FROM [Table] WHERE ID = ?", (table_id,)
                    ).fetchone()
                    tname = trow["Name"] if trow else f"Table {table_id}"
                    lines.append(f"    {tname}: {filter_expr}")

                members = conn.execute(
                    "SELECT * FROM [RoleMembership] WHERE RoleID = ?",
                    (role_id,)
                ).fetchall()
                if members:
                    lines.append(f"    Members: {len(members)}")

            conn.close()
            conn = None
            return ToolResponse.ok("\n".join(lines)).to_text()
        finally:
            if conn:
                conn.close()
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
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
            c = conn.cursor()
            # Get MAXID for safe ID allocation
            maxid_row = c.execute(
                "SELECT Value FROM DBPROPERTIES WHERE Name = 'MAXID'"
            ).fetchone()
            max_id = int(maxid_row[0]) if maxid_row else 0

            if role:
                role_id = role["ID"]
            else:
                max_id += 1
                role_id = max_id
                c.execute(
                    "INSERT INTO [Role] (ID, ModelID, Name, Description) VALUES (?, 1, ?, ?)",
                    (role_id, role_name, description),
                )

            table_row = c.execute(
                "SELECT ID FROM [Table] WHERE Name = ? AND ModelID = 1",
                (table_name,)
            ).fetchone()
            if not table_row:
                raise ValueError(f"Table '{table_name}' not found")
            table_id = table_row["ID"]

            existing = c.execute(
                "SELECT ID FROM [TablePermission] WHERE RoleID = ? AND TableID = ?",
                (role_id, table_id)
            ).fetchone()
            if existing:
                c.execute(
                    "UPDATE [TablePermission] SET FilterExpression = ? WHERE ID = ?",
                    (filter_expression, existing["ID"]),
                )
            else:
                max_id += 1
                c.execute(
                    "INSERT INTO [TablePermission] (ID, RoleID, TableID, FilterExpression) VALUES (?, ?, ?, ?)",
                    (max_id, role_id, table_id, filter_expression),
                )

            # Update MAXID
            c.execute(
                "UPDATE DBPROPERTIES SET Value = ? WHERE Name = 'MAXID'",
                (str(max_id),)
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


# ---- Section 10b: Perspectives ----


def _read_metadata_db(alias: str):
    """Helper: decompress DataModel and return a temp SQLite connection + path.

    Caller MUST close conn and os.unlink(tmp_path) when done.
    """
    info = _ensure_open(alias)
    dm_path = os.path.join(info["work_dir"], "DataModel")
    if not os.path.exists(dm_path):
        raise PBIXMCPError("No DataModel found", DataModelCompressionError.code)

    from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
    from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

    with open(dm_path, "rb") as f:
        dm = f.read()
    abf = decompress_datamodel(dm)
    db_bytes = read_metadata_sqlite(abf)

    fd, tmp_path = tempfile.mkstemp(suffix=".db")
    os.write(fd, db_bytes)
    os.close(fd)
    conn = sqlite3.connect(tmp_path)
    conn.row_factory = sqlite3.Row
    return info, conn, tmp_path


@mcp.tool()
def pbix_get_perspectives(alias: str) -> str:
    """Get all perspectives with their included tables, columns, and measures.

    Args:
        alias: The alias of the open file
    """
    try:
        info, conn, tmp_path = _read_metadata_db(alias)
        try:
            perspectives = conn.execute("SELECT ID, Name, Description FROM Perspective ORDER BY ID").fetchall()
            if not perspectives:
                return ToolResponse.ok("No perspectives defined in this file.").to_text()

            lines = [f"Perspectives ({len(perspectives)}):\n"]
            for p in perspectives:
                pid, pname, pdesc = p["ID"], p["Name"], p["Description"]
                lines.append(f"  {pname}" + (f" — {pdesc}" if pdesc else ""))

                ptables = conn.execute(
                    "SELECT pt.ID, pt.IncludeAll, t.Name FROM PerspectiveTable pt "
                    "JOIN [Table] t ON pt.TableID = t.ID "
                    "WHERE pt.PerspectiveID = ? ORDER BY t.Name", (pid,)
                ).fetchall()
                for pt in ptables:
                    ptid, include_all, tname = pt["ID"], pt["IncludeAll"], pt["Name"]
                    if include_all:
                        lines.append(f"    {tname} (all columns/measures)")
                    else:
                        cols = conn.execute(
                            "SELECT c.ExplicitName FROM PerspectiveColumn pc "
                            "JOIN [Column] c ON pc.ColumnID = c.ID "
                            "WHERE pc.PerspectiveTableID = ? ORDER BY c.ExplicitName", (ptid,)
                        ).fetchall()
                        measures = conn.execute(
                            "SELECT m.Name FROM PerspectiveMeasure pm "
                            "JOIN Measure m ON pm.MeasureID = m.ID "
                            "WHERE pm.PerspectiveTableID = ? ORDER BY m.Name", (ptid,)
                        ).fetchall()
                        col_names = [c["ExplicitName"] for c in cols]
                        meas_names = [m["Name"] for m in measures]
                        items = col_names + [f"[M] {m}" for m in meas_names]
                        lines.append(f"    {tname}: {', '.join(items) if items else '(no specific items)'}")
                lines.append("")

            return ToolResponse.ok("\n".join(lines)).to_text()
        finally:
            conn.close()
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_add_perspective(
    alias: str,
    name: str,
    tables_json: str = "[]",
    description: str = "",
) -> str:
    """Add a perspective — a filtered view of the model for different user groups.

    Args:
        alias: The alias of the open file
        name: Name for the perspective (e.g. "Sales Analyst", "Executive View")
        tables_json: JSON array of tables to include, e.g.
            '[{"table": "Sales"}, {"table": "Product", "columns": ["Name", "Category"]}]'
            If columns/measures are omitted for a table, all are included.
            Optional per-table fields: "columns" (list), "measures" (list)
        description: Optional description
    """
    try:
        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")
        tables_spec = json.loads(tables_json) if tables_json else []

        def _do_add(conn: sqlite3.Connection):
            conn.row_factory = sqlite3.Row
            c = conn.cursor()

            # Check if perspective already exists
            existing = c.execute("SELECT ID FROM Perspective WHERE Name = ?", (name,)).fetchone()
            if existing:
                raise PBIXMCPError(f"Perspective '{name}' already exists", "DUPLICATE")

            # Get MAXID
            maxid_row = c.execute("SELECT Value FROM DBPROPERTIES WHERE Name = 'MAXID'").fetchone()
            max_id = int(maxid_row[0]) if maxid_row else 0

            # Create Perspective
            max_id += 1
            persp_id = max_id
            c.execute(
                "INSERT INTO Perspective (ID, ModelID, Name, Description, ModifiedTime) "
                "VALUES (?, 1, ?, ?, ?)",
                (persp_id, name, description or None, int(datetime.now().timestamp() * 1e7)),
            )

            # Add tables
            for tspec in tables_spec:
                tname = tspec.get("table", "")
                trow = c.execute(
                    "SELECT ID FROM [Table] WHERE Name = ? AND ModelID = 1", (tname,)
                ).fetchone()
                if not trow:
                    raise PBIXMCPError(f"Table '{tname}' not found", "TABLE_NOT_FOUND")
                table_id = trow["ID"]

                specific_cols = tspec.get("columns", [])
                specific_meas = tspec.get("measures", [])
                include_all = 1 if (not specific_cols and not specific_meas) else 0

                max_id += 1
                pt_id = max_id
                c.execute(
                    "INSERT INTO PerspectiveTable (ID, PerspectiveID, TableID, IncludeAll, ModifiedTime) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (pt_id, persp_id, table_id, include_all, int(datetime.now().timestamp() * 1e7)),
                )

                if specific_cols:
                    for col_name in specific_cols:
                        crow = c.execute(
                            "SELECT ID FROM [Column] WHERE TableID = ? AND (ExplicitName = ? OR InferredName = ?)",
                            (table_id, col_name, col_name),
                        ).fetchone()
                        if crow:
                            max_id += 1
                            c.execute(
                                "INSERT INTO PerspectiveColumn (ID, PerspectiveTableID, ColumnID, ModifiedTime) "
                                "VALUES (?, ?, ?, ?)",
                                (max_id, pt_id, crow["ID"], int(datetime.now().timestamp() * 1e7)),
                            )

                if specific_meas:
                    for meas_name in specific_meas:
                        mrow = c.execute(
                            "SELECT ID FROM Measure WHERE TableID = ? AND Name = ?",
                            (table_id, meas_name),
                        ).fetchone()
                        if mrow:
                            max_id += 1
                            c.execute(
                                "INSERT INTO PerspectiveMeasure (ID, PerspectiveTableID, MeasureID, ModifiedTime) "
                                "VALUES (?, ?, ?, ?)",
                                (max_id, pt_id, mrow["ID"], int(datetime.now().timestamp() * 1e7)),
                            )

            # Update MAXID
            c.execute("UPDATE DBPROPERTIES SET Value = ? WHERE Name = 'MAXID'", (str(max_id),))
            conn.commit()

        old_size, new_size = _modify_metadata_only(dm_path, _do_add)
        info["modified"] = True
        n_tables = len(tables_spec)
        return ToolResponse.ok(
            f"Perspective '{name}' created with {n_tables} table(s)."
        ).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_remove_perspective(alias: str, name: str) -> str:
    """Remove a perspective and all its included table/column/measure references.

    Args:
        alias: The alias of the open file
        name: Name of the perspective to remove
    """
    try:
        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")

        def _do_remove(conn: sqlite3.Connection):
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            row = c.execute("SELECT ID FROM Perspective WHERE Name = ?", (name,)).fetchone()
            if not row:
                raise PBIXMCPError(f"Perspective '{name}' not found", "NOT_FOUND")
            pid = row["ID"]

            # Get PerspectiveTable IDs for cascade delete
            pt_ids = [r["ID"] for r in c.execute(
                "SELECT ID FROM PerspectiveTable WHERE PerspectiveID = ?", (pid,)
            ).fetchall()]

            for pt_id in pt_ids:
                c.execute("DELETE FROM PerspectiveColumn WHERE PerspectiveTableID = ?", (pt_id,))
                c.execute("DELETE FROM PerspectiveMeasure WHERE PerspectiveTableID = ?", (pt_id,))
                c.execute("DELETE FROM PerspectiveHierarchy WHERE PerspectiveTableID = ?", (pt_id,))

            c.execute("DELETE FROM PerspectiveTable WHERE PerspectiveID = ?", (pid,))
            c.execute("DELETE FROM Perspective WHERE ID = ?", (pid,))
            conn.commit()

        old_size, new_size = _modify_metadata_only(dm_path, _do_remove)
        info["modified"] = True
        return ToolResponse.ok(f"Perspective '{name}' removed.").to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


# ---- Section 10c: User Hierarchies ----


@mcp.tool()
def pbix_get_hierarchies(alias: str) -> str:
    """Get all user hierarchies with their levels and columns.

    Args:
        alias: The alias of the open file
    """
    try:
        info, conn, tmp_path = _read_metadata_db(alias)
        try:
            hierarchies = conn.execute(
                "SELECT h.ID, h.Name, h.IsHidden, h.Description, t.Name as TableName "
                "FROM Hierarchy h JOIN [Table] t ON h.TableID = t.ID "
                "ORDER BY t.Name, h.Name"
            ).fetchall()
            if not hierarchies:
                return ToolResponse.ok("No user hierarchies defined in this file.").to_text()

            lines = [f"Hierarchies ({len(hierarchies)}):\n"]
            for h in hierarchies:
                hid = h["ID"]
                hidden = " (hidden)" if h["IsHidden"] else ""
                desc = f" — {h['Description']}" if h["Description"] else ""
                lines.append(f"  {h['TableName']}.{h['Name']}{hidden}{desc}")

                levels = conn.execute(
                    "SELECT l.Ordinal, l.Name, c.ExplicitName as ColumnName "
                    "FROM Level l LEFT JOIN [Column] c ON l.ColumnID = c.ID "
                    "WHERE l.HierarchyID = ? ORDER BY l.Ordinal", (hid,)
                ).fetchall()
                for lv in levels:
                    lines.append(f"    {lv['Ordinal']}: {lv['Name']} → {lv['ColumnName']}")
                lines.append("")

            return ToolResponse.ok("\n".join(lines)).to_text()
        finally:
            conn.close()
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_add_hierarchy(
    alias: str,
    table_name: str,
    hierarchy_name: str,
    levels_json: str,
) -> str:
    """Create a user hierarchy (drill-down path) on a table.

    Args:
        alias: The alias of the open file
        table_name: Table to add the hierarchy to
        hierarchy_name: Name for the hierarchy (e.g. "Geography", "Date Hierarchy")
        levels_json: JSON array of levels in drill-down order, e.g.
            '[{"name": "Country", "column": "Country"},
              {"name": "State", "column": "State-Province"},
              {"name": "City", "column": "City"}]'
    """
    try:
        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")
        levels = json.loads(levels_json)
        if not levels:
            return ToolResponse.error("levels_json must contain at least one level", "INVALID_INPUT").to_text()

        def _do_add(conn: sqlite3.Connection):
            conn.row_factory = sqlite3.Row
            c = conn.cursor()

            trow = c.execute(
                "SELECT ID FROM [Table] WHERE Name = ? AND ModelID = 1", (table_name,)
            ).fetchone()
            if not trow:
                raise PBIXMCPError(f"Table '{table_name}' not found", "TABLE_NOT_FOUND")
            table_id = trow["ID"]

            # Check duplicate
            existing = c.execute(
                "SELECT ID FROM Hierarchy WHERE TableID = ? AND Name = ?",
                (table_id, hierarchy_name),
            ).fetchone()
            if existing:
                raise PBIXMCPError(f"Hierarchy '{hierarchy_name}' already exists on '{table_name}'", "DUPLICATE")

            # Get MAXID
            maxid_row = c.execute("SELECT Value FROM DBPROPERTIES WHERE Name = 'MAXID'").fetchone()
            max_id = int(maxid_row[0]) if maxid_row else 0

            # Create Hierarchy
            max_id += 1
            hier_id = max_id
            c.execute(
                "INSERT INTO Hierarchy (ID, TableID, Name, IsHidden, State, ModifiedTime, StructureModifiedTime) "
                "VALUES (?, ?, ?, 0, 1, ?, ?)",
                (hier_id, table_id, hierarchy_name,
                 int(datetime.now().timestamp() * 1e7),
                 int(datetime.now().timestamp() * 1e7)),
            )

            # Create Levels and build LevelDefinition string
            level_col_ids = []
            level_def_parts = []
            cumulative_offset = 0
            for ordinal, lspec in enumerate(levels):
                lname = lspec.get("name", f"Level {ordinal}")
                col_name = lspec.get("column", "")

                crow = c.execute(
                    "SELECT ID, ExplicitName FROM [Column] WHERE TableID = ? AND (ExplicitName = ? OR InferredName = ?)",
                    (table_id, col_name, col_name),
                ).fetchone()
                if not crow:
                    raise PBIXMCPError(
                        f"Column '{col_name}' not found in table '{table_name}'",
                        "COLUMN_NOT_FOUND",
                    )
                col_id = crow["ID"]
                col_explicit = crow["ExplicitName"]
                level_col_ids.append(col_id)

                # Build LevelDefinition: $ColumnName (ColumnID)$offset$
                level_def_parts.append(f"${col_explicit} ({col_id})${cumulative_offset}$")

                # Count distinct values for this column to compute next offset
                # Use sorted dictionary cardinality
                distinct = c.execute(
                    "SELECT COUNT(DISTINCT ExplicitName) FROM [Column] WHERE TableID = ? AND ID = ?",
                    (table_id, col_id),
                ).fetchone()[0]
                # Actually need to count distinct VALUES in data, not columns.
                # For now, we can't easily do this from metadata alone.
                # Use a placeholder - PBI Desktop may recompute this.
                # We'll set offset to ordinal position as placeholder.

                max_id += 1
                c.execute(
                    "INSERT INTO Level (ID, HierarchyID, Ordinal, Name, ColumnID, ModifiedTime) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (max_id, hier_id, ordinal, lname, col_id,
                     int(datetime.now().timestamp() * 1e7)),
                )

            # Create HierarchyStorage (unmaterialized — no U$ table needed)
            # PBI Desktop creates U$ tables on first data refresh
            level_def = "".join(
                f"${crow_name} ({cid})$-1"
                for crow_name, cid in zip(
                    [c.execute("SELECT ExplicitName FROM [Column] WHERE ID = ?", (cid,)).fetchone()[0]
                     for cid in level_col_ids],
                    level_col_ids,
                )
            ) + "$"
            max_id += 1
            hier_storage_id = max_id
            c.execute(
                "INSERT INTO HierarchyStorage (ID, HierarchyID, Name, LevelDefinition, "
                "MaterializationType, StructureType, SystemTableID) "
                "VALUES (?, ?, ?, ?, -1, 0, 0)",
                (hier_storage_id, hier_id, f"{hierarchy_name} ({hier_id})",
                 level_def),
            )

            # Update Hierarchy to point to storage, State=4 (unmaterialized)
            c.execute(
                "UPDATE Hierarchy SET HierarchyStorageID = ?, State = 4 WHERE ID = ?",
                (hier_storage_id, hier_id),
            )

            # Set IsAvailableInMDX=1 on columns referenced by hierarchy levels
            for cid in level_col_ids:
                c.execute("UPDATE [Column] SET IsAvailableInMDX = 1 WHERE ID = ?", (cid,))

            c.execute("UPDATE DBPROPERTIES SET Value = ? WHERE Name = 'MAXID'", (str(max_id),))
            conn.commit()

        old_size, new_size = _modify_metadata_only(dm_path, _do_add)
        info["modified"] = True
        return ToolResponse.ok(
            f"Hierarchy '{hierarchy_name}' created on '{table_name}' with {len(levels)} levels."
        ).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_remove_hierarchy(alias: str, table_name: str, hierarchy_name: str) -> str:
    """Remove a user hierarchy and all its levels.

    Args:
        alias: The alias of the open file
        table_name: Table the hierarchy belongs to
        hierarchy_name: Name of the hierarchy to remove
    """
    try:
        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")

        def _do_remove(conn: sqlite3.Connection):
            conn.row_factory = sqlite3.Row
            c = conn.cursor()

            trow = c.execute(
                "SELECT ID FROM [Table] WHERE Name = ? AND ModelID = 1", (table_name,)
            ).fetchone()
            if not trow:
                raise PBIXMCPError(f"Table '{table_name}' not found", "TABLE_NOT_FOUND")

            hrow = c.execute(
                "SELECT ID FROM Hierarchy WHERE TableID = ? AND Name = ?",
                (trow["ID"], hierarchy_name),
            ).fetchone()
            if not hrow:
                raise PBIXMCPError(f"Hierarchy '{hierarchy_name}' not found on '{table_name}'", "NOT_FOUND")

            c.execute("DELETE FROM Level WHERE HierarchyID = ?", (hrow["ID"],))
            c.execute("DELETE FROM Hierarchy WHERE ID = ?", (hrow["ID"],))
            conn.commit()

        old_size, new_size = _modify_metadata_only(dm_path, _do_remove)
        info["modified"] = True
        return ToolResponse.ok(f"Hierarchy '{hierarchy_name}' removed from '{table_name}'.").to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


# ---- Section 10d: Cultures & Translations ----


@mcp.tool()
def pbix_get_cultures(alias: str) -> str:
    """Get all cultures (languages) with translation counts.

    Args:
        alias: The alias of the open file
    """
    try:
        info, conn, tmp_path = _read_metadata_db(alias)
        try:
            cultures = conn.execute("SELECT ID, Name FROM Culture ORDER BY Name").fetchall()
            if not cultures:
                return ToolResponse.ok("No cultures defined in this file.").to_text()

            lines = [f"Cultures ({len(cultures)}):\n"]
            for cu in cultures:
                cid, cname = cu["ID"], cu["Name"]
                count = conn.execute(
                    "SELECT COUNT(*) as cnt FROM ObjectTranslation WHERE CultureID = ?", (cid,)
                ).fetchone()["cnt"]
                lines.append(f"  {cname} — {count} translation(s)")

                # Show sample translations
                samples = conn.execute(
                    "SELECT ot.ObjectType, ot.Property, ot.Value, "
                    "COALESCE(t.Name, c2.ExplicitName, m.Name, h.Name) as ObjName "
                    "FROM ObjectTranslation ot "
                    "LEFT JOIN [Table] t ON ot.ObjectID = t.ID AND ot.ObjectType = 3 "
                    "LEFT JOIN [Column] c2 ON ot.ObjectID = c2.ID AND ot.ObjectType = 4 "
                    "LEFT JOIN Measure m ON ot.ObjectID = m.ID AND ot.ObjectType = 8 "
                    "LEFT JOIN Hierarchy h ON ot.ObjectID = h.ID AND ot.ObjectType = 9 "
                    "WHERE ot.CultureID = ? LIMIT 5", (cid,)
                ).fetchall()
                type_map = {3: "Table", 4: "Column", 8: "Measure", 9: "Hierarchy", 10: "Level"}
                prop_map = {1: "Caption", 2: "Description", 3: "DisplayFolder"}
                for s in samples:
                    otype = type_map.get(s["ObjectType"], f"Type{s['ObjectType']}")
                    prop = prop_map.get(s["Property"], f"Prop{s['Property']}")
                    lines.append(f"    {otype} '{s['ObjName']}' {prop} = \"{s['Value']}\"")
                lines.append("")

            return ToolResponse.ok("\n".join(lines)).to_text()
        finally:
            conn.close()
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_add_culture(alias: str, culture_name: str) -> str:
    """Add a culture (language) for translations.

    Args:
        alias: The alias of the open file
        culture_name: BCP-47 culture code (e.g. "nb-NO", "de-DE", "fr-FR", "ja-JP")
    """
    try:
        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")

        def _do_add(conn: sqlite3.Connection):
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            existing = c.execute("SELECT ID FROM Culture WHERE Name = ?", (culture_name,)).fetchone()
            if existing:
                raise PBIXMCPError(f"Culture '{culture_name}' already exists", "DUPLICATE")

            maxid_row = c.execute("SELECT Value FROM DBPROPERTIES WHERE Name = 'MAXID'").fetchone()
            max_id = int(maxid_row[0]) if maxid_row else 0
            max_id += 1
            c.execute(
                "INSERT INTO Culture (ID, ModelID, Name, ModifiedTime, StructureModifiedTime) "
                "VALUES (?, 1, ?, ?, ?)",
                (max_id, culture_name,
                 int(datetime.now().timestamp() * 1e7),
                 int(datetime.now().timestamp() * 1e7)),
            )
            c.execute("UPDATE DBPROPERTIES SET Value = ? WHERE Name = 'MAXID'", (str(max_id),))
            conn.commit()

        old_size, new_size = _modify_metadata_only(dm_path, _do_add)
        info["modified"] = True
        return ToolResponse.ok(f"Culture '{culture_name}' added.").to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_add_translations(alias: str, culture_name: str, translations_json: str) -> str:
    """Add translated names/descriptions for model objects in a culture.

    Args:
        alias: The alias of the open file
        culture_name: Target culture (e.g. "nb-NO")
        translations_json: JSON array of translations, e.g.
            '[{"object": "Sales", "type": "table", "property": "caption", "value": "Salg"},
              {"object": "Sales.Amount", "type": "column", "property": "caption", "value": "Beloep"}]'
            type: "table", "column", "measure", "hierarchy"
            property: "caption" (display name), "description", "displayFolder"
            For columns/measures: use "Table.Column" or "Table.Measure" dot notation
    """
    try:
        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")
        translations = json.loads(translations_json)

        # TOM ObjectTranslation.ObjectType: 3=Table, 4=Column, 8=Measure, 9=Hierarchy, 10=Level
        type_map = {"table": 3, "column": 4, "measure": 8, "hierarchy": 9, "level": 10}
        # TOM ObjectTranslation.Property enum: 1=Caption, 2=Description, 3=DisplayFolder
        prop_map = {"caption": 1, "description": 2, "displayfolder": 3}

        def _do_add(conn: sqlite3.Connection):
            conn.row_factory = sqlite3.Row
            c = conn.cursor()

            culture_row = c.execute("SELECT ID FROM Culture WHERE Name = ?", (culture_name,)).fetchone()
            if not culture_row:
                raise PBIXMCPError(f"Culture '{culture_name}' not found. Add it first with pbix_add_culture.", "NOT_FOUND")
            culture_id = culture_row["ID"]

            maxid_row = c.execute("SELECT Value FROM DBPROPERTIES WHERE Name = 'MAXID'").fetchone()
            max_id = int(maxid_row[0]) if maxid_row else 0
            added = 0

            for tr in translations:
                obj_ref = tr.get("object", "")
                obj_type_str = tr.get("type", "").lower()
                prop_str = tr.get("property", "caption").lower()
                value = tr.get("value", "")

                obj_type = type_map.get(obj_type_str)
                prop_code = prop_map.get(prop_str, 0)
                if obj_type is None:
                    continue

                # Resolve object name to ID
                obj_id = None
                if obj_type == 3:  # Table
                    row = c.execute("SELECT ID FROM [Table] WHERE Name = ? AND ModelID = 1", (obj_ref,)).fetchone()
                    if row:
                        obj_id = row["ID"]
                elif obj_type == 4:  # Column (Table.Column)
                    parts = obj_ref.split(".", 1)
                    if len(parts) == 2:
                        trow = c.execute("SELECT ID FROM [Table] WHERE Name = ? AND ModelID = 1", (parts[0],)).fetchone()
                        if trow:
                            crow = c.execute(
                                "SELECT ID FROM [Column] WHERE TableID = ? AND (ExplicitName = ? OR InferredName = ?)",
                                (trow["ID"], parts[1], parts[1]),
                            ).fetchone()
                            if crow:
                                obj_id = crow["ID"]
                elif obj_type == 8:  # Measure (Table.Measure)
                    parts = obj_ref.split(".", 1)
                    if len(parts) == 2:
                        trow = c.execute("SELECT ID FROM [Table] WHERE Name = ? AND ModelID = 1", (parts[0],)).fetchone()
                        if trow:
                            mrow = c.execute(
                                "SELECT ID FROM Measure WHERE TableID = ? AND Name = ?",
                                (trow["ID"], parts[1]),
                            ).fetchone()
                            if mrow:
                                obj_id = mrow["ID"]
                elif obj_type == 9:  # Hierarchy (Table.Hierarchy)
                    parts = obj_ref.split(".", 1)
                    if len(parts) == 2:
                        trow = c.execute("SELECT ID FROM [Table] WHERE Name = ? AND ModelID = 1", (parts[0],)).fetchone()
                        if trow:
                            hrow = c.execute(
                                "SELECT ID FROM Hierarchy WHERE TableID = ? AND Name = ?",
                                (trow["ID"], parts[1]),
                            ).fetchone()
                            if hrow:
                                obj_id = hrow["ID"]

                if obj_id is None:
                    continue

                # Upsert: check if translation exists
                existing = c.execute(
                    "SELECT ID FROM ObjectTranslation WHERE CultureID = ? AND ObjectID = ? AND ObjectType = ? AND Property = ?",
                    (culture_id, obj_id, obj_type, prop_code),
                ).fetchone()
                if existing:
                    c.execute(
                        "UPDATE ObjectTranslation SET Value = ?, ModifiedTime = ? WHERE ID = ?",
                        (value, int(datetime.now().timestamp() * 1e7), existing["ID"]),
                    )
                else:
                    max_id += 1
                    c.execute(
                        "INSERT INTO ObjectTranslation (ID, CultureID, ObjectID, ObjectType, Property, Value, ModifiedTime) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (max_id, culture_id, obj_id, obj_type, prop_code, value,
                         int(datetime.now().timestamp() * 1e7)),
                    )
                added += 1

            c.execute("UPDATE DBPROPERTIES SET Value = ? WHERE Name = 'MAXID'", (str(max_id),))
            conn.commit()

        old_size, new_size = _modify_metadata_only(dm_path, _do_add)
        info["modified"] = True
        return ToolResponse.ok(f"Added/updated {len(translations)} translation(s) for culture '{culture_name}'.").to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_remove_culture(alias: str, culture_name: str) -> str:
    """Remove a culture and all its translations.

    Args:
        alias: The alias of the open file
        culture_name: Culture code to remove (e.g. "nb-NO")
    """
    try:
        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")

        def _do_remove(conn: sqlite3.Connection):
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            row = c.execute("SELECT ID FROM Culture WHERE Name = ?", (culture_name,)).fetchone()
            if not row:
                raise PBIXMCPError(f"Culture '{culture_name}' not found", "NOT_FOUND")
            c.execute("DELETE FROM ObjectTranslation WHERE CultureID = ?", (row["ID"],))
            c.execute("DELETE FROM Culture WHERE ID = ?", (row["ID"],))
            conn.commit()

        old_size, new_size = _modify_metadata_only(dm_path, _do_remove)
        info["modified"] = True
        return ToolResponse.ok(f"Culture '{culture_name}' and all its translations removed.").to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


# ---- Section 10e: Partition Management ----


@mcp.tool()
def pbix_get_partitions(alias: str) -> str:
    """Get all partitions with table, type, mode, and M expression.

    Args:
        alias: The alias of the open file
    """
    try:
        info, conn, tmp_path = _read_metadata_db(alias)
        try:
            partitions = conn.execute(
                "SELECT p.Name, p.Type, p.Mode, p.QueryDefinition, t.Name as TableName "
                "FROM Partition p JOIN [Table] t ON p.TableID = t.ID "
                "WHERE t.Name NOT LIKE 'H$%' AND t.Name NOT LIKE 'R$%' AND t.Name NOT LIKE 'U$%' "
                "ORDER BY t.Name, p.Name"
            ).fetchall()
            if not partitions:
                return ToolResponse.ok("No partitions found.").to_text()

            type_map = {1: "Query", 2: "Calculated", 3: "None", 4: "M"}
            mode_map = {0: "Import", 1: "DirectQuery", 2: "Dual"}

            lines = [f"Partitions ({len(partitions)}):\n"]
            current_table = None
            for p in partitions:
                tname = p["TableName"]
                if tname != current_table:
                    current_table = tname
                    lines.append(f"  {tname}:")

                ptype = type_map.get(p["Type"], f"Type{p['Type']}")
                pmode = mode_map.get(p["Mode"], f"Mode{p['Mode']}")
                qd = p["QueryDefinition"]
                expr_preview = (qd[:80] + "...") if qd and len(qd) > 80 else (qd or "(none)")
                lines.append(f"    {p['Name']} [{ptype}/{pmode}]: {expr_preview}")

            return ToolResponse.ok("\n".join(lines)).to_text()
        finally:
            conn.close()
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_add_partition(
    alias: str,
    table_name: str,
    partition_name: str,
    expression: str,
    mode: str = "import",
) -> str:
    """Add a new M (Power Query) partition to a table.

    WARNING: This tool is blocked for PBIX files opened in PBI Desktop.
    Adding partitions requires PartitionStorage objects in VertiPaq which
    cannot be created via metadata-only modification. The partition metadata
    is written correctly but PBI Desktop will reject the file on open.
    Works for PBIP/TMDL export (pbix_export_pbip, pbix_export_tmdl).

    Args:
        alias: The alias of the open file
        table_name: Table to add the partition to
        partition_name: Name for the new partition
        expression: M (Power Query) expression for the partition
        mode: "import" (default) or "directQuery"
    """
    try:
        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")
        mode_code = 1 if mode.lower() == "directquery" else 0

        def _do_add(conn: sqlite3.Connection):
            conn.row_factory = sqlite3.Row
            c = conn.cursor()

            trow = c.execute(
                "SELECT ID FROM [Table] WHERE Name = ? AND ModelID = 1", (table_name,)
            ).fetchone()
            if not trow:
                raise PBIXMCPError(f"Table '{table_name}' not found", "TABLE_NOT_FOUND")

            existing = c.execute(
                "SELECT ID FROM Partition WHERE TableID = ? AND Name = ?",
                (trow["ID"], partition_name),
            ).fetchone()
            if existing:
                raise PBIXMCPError(f"Partition '{partition_name}' already exists on '{table_name}'", "DUPLICATE")

            maxid_row = c.execute("SELECT Value FROM DBPROPERTIES WHERE Name = 'MAXID'").fetchone()
            max_id = int(maxid_row[0]) if maxid_row else 0
            max_id += 1

            c.execute(
                "INSERT INTO Partition (ID, TableID, Name, Type, Mode, State, "
                "ModifiedTime, RefreshedTime, QueryDefinition) "
                "VALUES (?, ?, ?, 4, ?, 1, ?, ?, ?)",
                (max_id, trow["ID"], partition_name, mode_code,
                 int(datetime.now().timestamp() * 1e7),
                 int(datetime.now().timestamp() * 1e7),
                 expression),
            )

            c.execute("UPDATE DBPROPERTIES SET Value = ? WHERE Name = 'MAXID'", (str(max_id),))
            conn.commit()

        old_size, new_size = _modify_metadata_only(dm_path, _do_add)
        info["modified"] = True
        return ToolResponse.ok(f"Partition '{partition_name}' added to '{table_name}'.").to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(str(e), "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_remove_partition(alias: str, table_name: str, partition_name: str) -> str:
    """Remove a partition from a table.

    Will not delete the last remaining partition of a table.

    Args:
        alias: The alias of the open file
        table_name: Table the partition belongs to
        partition_name: Name of the partition to remove
    """
    try:
        info = _ensure_open(alias)
        dm_path = os.path.join(info["work_dir"], "DataModel")

        def _do_remove(conn: sqlite3.Connection):
            conn.row_factory = sqlite3.Row
            c = conn.cursor()

            trow = c.execute(
                "SELECT ID FROM [Table] WHERE Name = ? AND ModelID = 1", (table_name,)
            ).fetchone()
            if not trow:
                raise PBIXMCPError(f"Table '{table_name}' not found", "TABLE_NOT_FOUND")

            prow = c.execute(
                "SELECT ID FROM Partition WHERE TableID = ? AND Name = ?",
                (trow["ID"], partition_name),
            ).fetchone()
            if not prow:
                raise PBIXMCPError(f"Partition '{partition_name}' not found on '{table_name}'", "NOT_FOUND")

            # Guard: don't delete last partition
            count = c.execute(
                "SELECT COUNT(*) as cnt FROM Partition WHERE TableID = ?", (trow["ID"],)
            ).fetchone()["cnt"]
            if count <= 1:
                raise PBIXMCPError(
                    f"Cannot delete the last partition of table '{table_name}'",
                    "LAST_PARTITION",
                )

            c.execute("DELETE FROM Partition WHERE ID = ?", (prow["ID"],))
            conn.commit()

        old_size, new_size = _modify_metadata_only(dm_path, _do_remove)
        info["modified"] = True
        return ToolResponse.ok(f"Partition '{partition_name}' removed from '{table_name}'.").to_text()
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
def pbix_performance(alias: str) -> str:
    """Analyze report for performance issues and optimization opportunities.

    Checks for oversized tables, high column counts, complex measures,
    orphaned tables, inactive relationships, and wide schemas.

    Args:
        alias: The alias of the open file
    """
    import re as _re
    try:
        info = _ensure_open(alias)
        work_dir = info["work_dir"]

        from pbix_mcp.formats.model_reader import ModelReader
        model = ModelReader(info["path"], work_dir=work_dir)

        lines: list[str] = []
        warnings = 0
        infos = 0

        def warn(msg: str):
            nonlocal warnings
            lines.append(f"  WARNING: {msg}")
            warnings += 1

        def info_msg(msg: str):
            nonlocal infos
            lines.append(f"  INFO: {msg}")
            infos += 1

        lines.append("# Performance Analysis\n")

        # --- Table sizes ---
        lines.append("## Table Sizes")
        stats = model.statistics
        data_tables = [t for t in stats if not t["TableName"].startswith(
            ("H$", "R$", "U$", "LocalDateTable", "DateTableTemplate"))]
        data_tables.sort(key=lambda t: t["RowCount"], reverse=True)

        total_rows = sum(t["RowCount"] for t in data_tables)
        total_cols = sum(t["ColumnCount"] for t in data_tables)
        lines.append(f"  Total: {len(data_tables)} tables, {total_rows:,} rows, {total_cols} columns")

        for t in data_tables:
            name, rows, cols = t["TableName"], t["RowCount"], t["ColumnCount"]
            flags = []
            if rows > 1_000_000:
                flags.append("LARGE (>1M rows)")
            elif rows > 100_000:
                flags.append("medium (>100K rows)")
            if cols > 30:
                flags.append(f"wide ({cols} columns)")
            elif cols > 20:
                flags.append(f"moderately wide ({cols} columns)")
            if rows == 0:
                flags.append("empty table")

            if flags:
                warn(f"{name}: {rows:,} rows, {cols} cols — {', '.join(flags)}")
            else:
                lines.append(f"  {name}: {rows:,} rows, {cols} cols")

        # --- Column analysis ---
        lines.append("\n## Column Analysis")
        schema = model.schema
        hidden_count = 0
        calc_count = 0
        by_type: dict[str, int] = {}
        for col in schema:
            if col["TableName"].startswith(("H$", "R$", "U$", "LocalDateTable", "DateTableTemplate")):
                continue
            dt = col["DataType"]
            by_type[dt] = by_type.get(dt, 0) + 1
            if col.get("IsHidden"):
                hidden_count += 1
            if col.get("IsCalculated"):
                calc_count += 1

        lines.append(f"  Types: {', '.join(f'{k}={v}' for k, v in sorted(by_type.items()))}")
        if hidden_count:
            info_msg(f"{hidden_count} hidden columns (keys, internal)")
        if calc_count:
            info_msg(f"{calc_count} calculated columns (evaluated at refresh)")

        # String columns in large tables — high cardinality risk
        for t in data_tables:
            if t["RowCount"] > 50_000:
                str_cols = [c for c in schema
                            if c["TableName"] == t["TableName"]
                            and c["DataType"] == "String"
                            and not c.get("IsHidden")
                            and "RowNumber" not in c["ColumnName"]]
                if len(str_cols) > 5:
                    warn(f"{t['TableName']}: {len(str_cols)} string columns on {t['RowCount']:,} rows — potential high cardinality")

        # --- Measure complexity ---
        lines.append("\n## Measure Complexity")
        measures = model.dax_measures
        if measures:
            for m in measures:
                expr = m["Expression"]
                # Count table references
                table_refs = set(_re.findall(r"'([^']+)'\[", expr))
                table_refs |= set(_re.findall(r"\b([A-Za-z]\w+)\[", expr))
                # Count function calls
                func_calls = len(_re.findall(r"[A-Z]{2,}\s*\(", expr))
                # Count nesting depth (rough — count opening parens)
                max_depth = 0
                depth = 0
                for ch in expr:
                    if ch == '(':
                        depth += 1
                        max_depth = max(max_depth, depth)
                    elif ch == ')':
                        depth -= 1

                flags = []
                if len(table_refs) > 3:
                    flags.append(f"references {len(table_refs)} tables")
                if func_calls > 10:
                    flags.append(f"{func_calls} function calls")
                elif func_calls > 5:
                    flags.append(f"{func_calls} function calls")
                if max_depth > 5:
                    flags.append(f"nesting depth {max_depth}")
                if len(expr) > 500:
                    flags.append(f"{len(expr)} chars")

                if flags:
                    warn(f"Measure '{m['Name']}': {', '.join(flags)}")
                else:
                    lines.append(f"  {m['Name']}: {func_calls} functions, {len(table_refs)} table refs — OK")

            lines.append(f"  {len(measures)} measures total")
        else:
            lines.append("  No measures defined")

        # --- Relationships ---
        lines.append("\n## Relationships")
        rels = model.relationships
        inactive = [r for r in rels if not r.get("IsActive")]
        bidir = [r for r in rels if r.get("CrossFilteringBehavior") == 2]

        lines.append(f"  {len(rels)} relationships total")
        if inactive:
            for r in inactive:
                warn(f"Inactive: {r['FromTableName']}.{r['FromColumnName']} -> {r['ToTableName']}.{r['ToColumnName']}")
        if bidir:
            for r in bidir:
                warn(f"Bidirectional: {r['FromTableName']} <-> {r['ToTableName']} — can cause ambiguity")

        # Orphaned tables (no relationships)
        rel_tables = set()
        for r in rels:
            rel_tables.add(r["FromTableName"])
            rel_tables.add(r["ToTableName"])
        for t in data_tables:
            if t["TableName"] not in rel_tables and t["RowCount"] > 0:
                info_msg(f"Orphaned table '{t['TableName']}' — no relationships, {t['RowCount']:,} rows")

        # --- Summary ---
        lines.insert(1, f"Summary: {warnings} warnings, {infos} info items\n")

        return ToolResponse.ok("\n".join(lines)).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(f"{str(e)}\n{traceback.format_exc()}", "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_diff(alias_a: str, alias_b: str) -> str:
    """Compare two open PBIX files and show what changed.

    Compares tables, columns, measures, relationships, pages, visuals,
    data sources, and theme colors between two files. Both files must
    be open.

    Args:
        alias_a: The alias of the first file (baseline / "old")
        alias_b: The alias of the second file (changed / "new")
    """
    try:
        info_a = _ensure_open(alias_a)
        info_b = _ensure_open(alias_b)

        from pbix_mcp.formats.model_reader import ModelReader
        model_a = ModelReader(info_a["path"], work_dir=info_a.get("work_dir"))
        model_b = ModelReader(info_b["path"], work_dir=info_b.get("work_dir"))

        lines: list[str] = []

        def section(title: str):
            lines.append(f"\n## {title}\n")

        def added(item: str):
            lines.append(f"  + {item}")

        def removed(item: str):
            lines.append(f"  - {item}")

        def changed(item: str, old_val: str, new_val: str):
            lines.append(f"  ~ {item}: {old_val} -> {new_val}")

        name_a = os.path.basename(info_a["path"])
        name_b = os.path.basename(info_b["path"])
        lines.append(f"# Diff: {name_a} vs {name_b}")

        # --- Tables ---
        section("Tables")
        try:
            stats_a = {t["TableName"]: t for t in model_a.statistics
                       if not t["TableName"].startswith(("H$", "R$", "U$", "LocalDateTable", "DateTableTemplate"))}
            stats_b = {t["TableName"]: t for t in model_b.statistics
                       if not t["TableName"].startswith(("H$", "R$", "U$", "LocalDateTable", "DateTableTemplate"))}

            for name in sorted(set(stats_b) - set(stats_a)):
                t = stats_b[name]
                added(f"{name} ({t['ColumnCount']} cols, {t['RowCount']:,} rows)")
            for name in sorted(set(stats_a) - set(stats_b)):
                t = stats_a[name]
                removed(f"{name} ({t['ColumnCount']} cols, {t['RowCount']:,} rows)")
            for name in sorted(set(stats_a) & set(stats_b)):
                a, b = stats_a[name], stats_b[name]
                changes = []
                if a["ColumnCount"] != b["ColumnCount"]:
                    changes.append(f"columns {a['ColumnCount']}->{b['ColumnCount']}")
                if a["RowCount"] != b["RowCount"]:
                    changes.append(f"rows {a['RowCount']:,}->{b['RowCount']:,}")
                if changes:
                    changed(name, "", ", ".join(changes))

            if not any(ln.startswith("  ") for ln in lines if lines.index(ln) > len(lines) - 10):
                lines.append("  (no changes)")
        except Exception as e:
            lines.append(f"  Error: {e}")

        # --- Columns ---
        section("Columns")
        try:
            def _col_set(model):
                result = {}
                for c in model.schema:
                    tn = c["TableName"]
                    if tn.startswith(("H$", "R$", "U$", "LocalDateTable", "DateTableTemplate")):
                        continue
                    if c.get("IsHidden") or "RowNumber" in c["ColumnName"]:
                        continue
                    result[f"{tn}.{c['ColumnName']}"] = c["DataType"]
                return result

            cols_a = _col_set(model_a)
            cols_b = _col_set(model_b)

            for col in sorted(set(cols_b) - set(cols_a)):
                added(f"{col} ({cols_b[col]})")
            for col in sorted(set(cols_a) - set(cols_b)):
                removed(f"{col} ({cols_a[col]})")
            for col in sorted(set(cols_a) & set(cols_b)):
                if cols_a[col] != cols_b[col]:
                    changed(col, cols_a[col], cols_b[col])

            added_count = len(set(cols_b) - set(cols_a))
            removed_count = len(set(cols_a) - set(cols_b))
            if added_count == 0 and removed_count == 0:
                lines.append("  (no changes)")
        except Exception as e:
            lines.append(f"  Error: {e}")

        # --- Measures ---
        section("Measures")
        try:
            meas_a = {m["Name"]: m for m in model_a.dax_measures}
            meas_b = {m["Name"]: m for m in model_b.dax_measures}

            for name in sorted(set(meas_b) - set(meas_a)):
                m = meas_b[name]
                expr = m["Expression"].replace("\n", " ")[:60]
                added(f"{name} = {expr}")
            for name in sorted(set(meas_a) - set(meas_b)):
                removed(name)
            for name in sorted(set(meas_a) & set(meas_b)):
                if meas_a[name]["Expression"] != meas_b[name]["Expression"]:
                    old_expr = meas_a[name]["Expression"].replace("\n", " ")[:40]
                    new_expr = meas_b[name]["Expression"].replace("\n", " ")[:40]
                    changed(name, old_expr, new_expr)

            if not meas_a and not meas_b:
                lines.append("  (no measures in either file)")
            elif len(set(meas_b) - set(meas_a)) == 0 and len(set(meas_a) - set(meas_b)) == 0:
                has_expr_changes = any(meas_a[n]["Expression"] != meas_b[n]["Expression"]
                                       for n in set(meas_a) & set(meas_b))
                if not has_expr_changes:
                    lines.append("  (no changes)")
        except Exception as e:
            lines.append(f"  Error: {e}")

        # --- Relationships ---
        section("Relationships")
        try:
            def _rel_key(r):
                return f"{r['FromTableName']}.{r['FromColumnName']}->{r['ToTableName']}.{r['ToColumnName']}"

            rels_a = {_rel_key(r): r for r in model_a.relationships}
            rels_b = {_rel_key(r): r for r in model_b.relationships}

            for key in sorted(set(rels_b) - set(rels_a)):
                added(key)
            for key in sorted(set(rels_a) - set(rels_b)):
                removed(key)
            for key in sorted(set(rels_a) & set(rels_b)):
                if rels_a[key].get("IsActive") != rels_b[key].get("IsActive"):
                    changed(key, f"active={rels_a[key].get('IsActive')}", f"active={rels_b[key].get('IsActive')}")

            if len(set(rels_b) - set(rels_a)) == 0 and len(set(rels_a) - set(rels_b)) == 0:
                lines.append("  (no changes)")
        except Exception as e:
            lines.append(f"  Error: {e}")

        # --- Pages & Visuals ---
        section("Pages & Visuals")
        try:
            layout_a = _get_layout(info_a.get("work_dir", "")) or {}
            layout_b = _get_layout(info_b.get("work_dir", "")) or {}

            pages_a = {s.get("displayName", f"Page {i}"): s
                       for i, s in enumerate(layout_a.get("sections", []))}
            pages_b = {s.get("displayName", f"Page {i}"): s
                       for i, s in enumerate(layout_b.get("sections", []))}

            for pname in sorted(set(pages_b) - set(pages_a)):
                vc_count = len(pages_b[pname].get("visualContainers", []))
                added(f"Page '{pname}' ({vc_count} visuals)")
            for pname in sorted(set(pages_a) - set(pages_b)):
                removed(f"Page '{pname}'")
            for pname in sorted(set(pages_a) & set(pages_b)):
                vc_a = len(pages_a[pname].get("visualContainers", []))
                vc_b = len(pages_b[pname].get("visualContainers", []))
                if vc_a != vc_b:
                    changed(f"Page '{pname}'", f"{vc_a} visuals", f"{vc_b} visuals")

            if len(set(pages_b) - set(pages_a)) == 0 and len(set(pages_a) - set(pages_b)) == 0:
                has_visual_changes = any(
                    len(pages_a[p].get("visualContainers", [])) != len(pages_b[p].get("visualContainers", []))
                    for p in set(pages_a) & set(pages_b)
                )
                if not has_visual_changes:
                    lines.append("  (no changes)")
        except Exception as e:
            lines.append(f"  Error: {e}")

        # --- Data Sources ---
        section("Data Sources")
        try:
            pq_a = {p["TableName"]: p.get("Expression", "") for p in model_a.power_query}
            pq_b = {p["TableName"]: p.get("Expression", "") for p in model_b.power_query}

            for tname in sorted(set(pq_b) - set(pq_a)):
                added(f"{tname} (new query)")
            for tname in sorted(set(pq_a) - set(pq_b)):
                removed(f"{tname}")
            for tname in sorted(set(pq_a) & set(pq_b)):
                if pq_a[tname] != pq_b[tname]:
                    changed(tname, "M expression modified", "")

            if len(set(pq_b) - set(pq_a)) == 0 and len(set(pq_a) - set(pq_b)) == 0:
                has_pq_changes = any(pq_a[t] != pq_b[t] for t in set(pq_a) & set(pq_b))
                if not has_pq_changes:
                    lines.append("  (no changes)")
        except Exception as e:
            lines.append(f"  Error: {e}")

        # --- Theme ---
        section("Theme Colors")
        try:
            work_a = info_a.get("work_dir", "")
            work_b = info_b.get("work_dir", "")
            colors_a = set(_load_theme_data_colors(work_a))
            colors_b = set(_load_theme_data_colors(work_b))

            for c in sorted(colors_b - colors_a):
                added(c)
            for c in sorted(colors_a - colors_b):
                removed(c)
            if colors_a == colors_b:
                lines.append("  (no changes)")
        except Exception as e:
            lines.append(f"  Error: {e}")

        # --- Summary ---
        adds = sum(1 for ln in lines if ln.startswith("  + "))
        removes = sum(1 for ln in lines if ln.startswith("  - "))
        changes = sum(1 for ln in lines if ln.startswith("  ~ "))
        lines.insert(1, f"\nSummary: {adds} added, {removes} removed, {changes} changed")

        return ToolResponse.ok("\n".join(lines)).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(f"{str(e)}\n{traceback.format_exc()}", "INTERNAL_ERROR").to_text()


@mcp.tool()
def pbix_document(alias: str, output_path: str = "") -> str:
    """Auto-generate a comprehensive report documentation summary.

    Assembles all report metadata into a structured document: tables with
    row counts, column details, DAX measures with expressions, relationships,
    data sources, pages with visual inventories, RLS roles, and theme colors.

    Returns markdown in the response AND saves a .docx file to disk.

    Args:
        alias: The alias of the open file
        output_path: Where to save the .docx file. Default: next to the PBIX.
    """
    import re
    try:
        info = _ensure_open(alias)
        work_dir = info["work_dir"]
        pbix_path = info["path"]

        if not output_path:
            output_path = os.path.splitext(pbix_path)[0] + "_documentation.docx"

        md_lines: list[str] = []

        def md(line: str = ""):
            md_lines.append(line)

        # --- Header ---
        fname = os.path.basename(pbix_path)
        md(f"# Report Documentation: {fname}")
        md(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        md()

        # --- Tables ---
        md("## Data Model — Tables")
        md()
        try:
            from pbix_mcp.formats.model_reader import ModelReader
            model = ModelReader(pbix_path, work_dir=work_dir)
            stats = model.statistics
            data_tables = [t for t in stats if not t["TableName"].startswith(("H$", "R$", "U$", "LocalDateTable", "DateTableTemplate"))]

            md("| Table | Columns | Rows |")
            md("|-------|---------|------|")
            for t in data_tables:
                md(f"| {t['TableName']} | {t['ColumnCount']} | {t['RowCount']:,} |")
            md()
            md(f"*{len(data_tables)} data tables, {sum(t['RowCount'] for t in data_tables):,} total rows*")
            md()
        except Exception as e:
            md(f"*Error reading tables: {e}*")
            md()

        # --- Schema ---
        md("## Column Details")
        md()
        try:
            schema = model.schema
            by_table: dict[str, list] = {}
            for col in schema:
                tname = col["TableName"]
                if tname.startswith(("H$", "R$", "U$", "LocalDateTable", "DateTableTemplate")):
                    continue
                by_table.setdefault(tname, []).append(col)

            for tname in sorted(by_table.keys()):
                cols = by_table[tname]
                visible = [c for c in cols if not c.get("IsHidden") and "RowNumber" not in c["ColumnName"]]
                if not visible:
                    continue
                md(f"### {tname}")
                md()
                md("| Column | Type |")
                md("|--------|------|")
                for c in visible:
                    md(f"| {c['ColumnName']} | {c['DataType']} |")
                md()
        except Exception as e:
            md(f"*Error reading schema: {e}*")
            md()

        # --- Measures ---
        md("## DAX Measures")
        md()
        try:
            measures = model.dax_measures
            if measures:
                md("| Table | Measure | Expression | Format |")
                md("|-------|---------|------------|--------|")
                for m in measures:
                    expr = m["Expression"].replace("\n", " ").replace("|", "\\|")
                    if len(expr) > 80:
                        expr = expr[:77] + "..."
                    fmt = m.get("FormatString", "") or ""
                    md(f"| {m['TableName']} | **{m['Name']}** | `{expr}` | {fmt} |")
                md()
                md(f"*{len(measures)} measures*")
            else:
                md("*No DAX measures defined*")
            md()
        except Exception as e:
            md(f"*Error reading measures: {e}*")
            md()

        # --- Relationships ---
        md("## Relationships")
        md()
        try:
            rels = model.relationships
            if rels:
                md("| From (Many) | | To (One) | Active |")
                md("|-------------|---|----------|--------|")
                for r in rels:
                    active = "Yes" if r.get("IsActive") else "No"
                    md(f"| {r['FromTableName']}.{r['FromColumnName']} | -> | {r['ToTableName']}.{r['ToColumnName']} | {active} |")
                md()
                md(f"*{len(rels)} relationships*")
            else:
                md("*No relationships*")
            md()
        except Exception as e:
            md(f"*Error reading relationships: {e}*")
            md()

        # --- Data Sources ---
        md("## Data Sources")
        md()
        try:
            pq = model.power_query
            if pq:
                md("| Table | M Expression (excerpt) |")
                md("|-------|----------------------|")
                for p in pq:
                    expr = p.get("Expression", "")
                    if expr:
                        # Find the Source = ... line (most informative)
                        lines = expr.split("\n")
                        source_line = next((ln.strip() for ln in lines if "Source" in ln and "=" in ln), lines[0].strip())
                        if len(source_line) > 80:
                            source_line = source_line[:77] + "..."
                        md(f"| {p['TableName']} | `{source_line}` |")
                md()
            else:
                md("*No Power Query expressions found*")
            md()
        except Exception:
            md("*No data source information available*")
            md()

        # --- Pages & Visuals ---
        md("## Report Pages & Visuals")
        md()
        layout = _get_layout(work_dir)
        if layout:
            for si, sec in enumerate(layout.get("sections", [])):
                page_name = sec.get("displayName", f"Page {si}")
                containers = sec.get("visualContainers", [])
                w = sec.get("width", 1280)
                h = sec.get("height", 720)
                md(f"### {page_name} ({w}x{h})")
                md()
                if containers:
                    md("| # | Type | Position | Size |")
                    md("|---|------|----------|------|")
                    for vi, vc in enumerate(containers):
                        config = _parse_visual_config(vc)
                        vtype = _get_visual_type(config)
                        x, y = int(vc.get("x", 0)), int(vc.get("y", 0))
                        vw, vh = int(vc.get("width", 0)), int(vc.get("height", 0))
                        md(f"| {vi} | {vtype} | ({x},{y}) | {vw}x{vh} |")
                else:
                    md("*No visuals on this page*")
                md()
        else:
            md("*No layout found*")
            md()

        # --- RLS Roles ---
        md("## Row-Level Security")
        md()
        try:
            meta = model._read_metadata()
            if meta:
                import sqlite3
                conn = sqlite3.connect(":memory:")
                conn.executescript("BEGIN;" if False else "")
                # Write meta to temp
                import tempfile
                fd, tmp = tempfile.mkstemp(suffix=".db")
                os.write(fd, meta)
                os.close(fd)
                conn = sqlite3.connect(tmp)
                conn.row_factory = sqlite3.Row
                roles = conn.execute("SELECT Name FROM Role").fetchall()
                if roles:
                    for role in roles:
                        rname = role["Name"]
                        perms = conn.execute(
                            "SELECT t.Name as TableName, tp.FilterExpression "
                            "FROM TablePermission tp JOIN Role r ON tp.RoleID=r.ID "
                            "JOIN [Table] t ON tp.TableID=t.ID "
                            "WHERE r.Name=?", (rname,)
                        ).fetchall()
                        md(f"**{rname}**")
                        for p in perms:
                            md(f"- `{p['TableName']}`: `{p['FilterExpression']}`")
                        md()
                else:
                    md("*No RLS roles defined*")
                conn.close()
                os.unlink(tmp)
            else:
                md("*No metadata available for RLS*")
        except Exception:
            md("*No RLS roles defined*")
        md()

        # --- Theme Colors ---
        md("## Theme Colors")
        md()
        data_colors = _load_theme_data_colors(work_dir)
        if data_colors:
            md("Data palette: " + ", ".join(f"`{c}`" for c in data_colors[:10]))
        else:
            md("*Default theme*")
        md()

        # --- Build markdown ---
        markdown = "\n".join(md_lines)

        # --- Build .docx ---
        docx_msg = ""
        try:
            from docx import Document
            from docx.shared import Pt

            doc = Document()
            style = doc.styles["Normal"]
            style.font.name = "Segoe UI"
            style.font.size = Pt(10)

            doc.add_heading(f"Report Documentation: {fname}", level=0)

            # Single-pass: process lines, collecting table rows inline
            i = 0
            while i < len(md_lines):
                line = md_lines[i]

                # Skip title (already added) and empty lines
                if (line.startswith("# ") and not line.startswith("## ")) or not line.strip():
                    i += 1
                    continue

                # Headings
                if line.startswith("### "):
                    doc.add_heading(line[4:], level=2)
                    i += 1
                    continue
                if line.startswith("## "):
                    doc.add_heading(line[3:], level=1)
                    i += 1
                    continue

                # Table block: collect all consecutive | rows (including |--- separators)
                if line.startswith("|"):
                    rows = []
                    while i < len(md_lines) and md_lines[i].startswith("|"):
                        if "---|" not in md_lines[i]:
                            cells = [c.strip() for c in md_lines[i].split("|")[1:-1]]
                            rows.append(cells)
                        i += 1
                    if rows:
                        table = doc.add_table(rows=len(rows), cols=len(rows[0]))
                        table.style = "Light Grid Accent 1"
                        for ri, row in enumerate(rows):
                            for ci, cell in enumerate(row):
                                clean = re.sub(r'[`*]', '', cell)
                                table.rows[ri].cells[ci].text = clean
                        doc.add_paragraph()  # spacing after table
                    continue

                # Italic text
                if line.startswith("*") and line.endswith("*"):
                    p = doc.add_paragraph(line.strip("*"))
                    if p.runs:
                        p.runs[0].italic = True
                    i += 1
                    continue

                # Normal text
                doc.add_paragraph(line)
                i += 1

            doc.save(output_path)
            docx_msg = f"\n\nDocx saved to: {output_path}"
        except ImportError:
            docx_msg = "\n\n(python-docx not installed — skipping .docx generation. Install with: pip install python-docx)"
        except Exception as e:
            docx_msg = f"\n\n(Docx generation error: {e})"

        return ToolResponse.ok(markdown + docx_msg).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(f"{str(e)}\n{traceback.format_exc()}", "INTERNAL_ERROR").to_text()


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
    c.execute(
        "SELECT DefaultPowerBIDataSourceVersion, DiscourageImplicitMeasures, "
        "SourceQueryCulture, DataAccessOptions FROM Model LIMIT 1"
    )
    model_props = c.fetchone()

    with open(os.path.join(output_dir, "model.tmdl"), "w", encoding="utf-8") as f:
        f.write("model Model\n")
        f.write(f"\tculture: {culture}\n")
        if model_props:
            dsv = model_props[0]
            # DefaultPowerBIDataSourceVersion: 2 = powerBI_V3
            dsv_map = {1: "powerBI_V1", 2: "powerBI_V3"}
            if dsv in dsv_map:
                f.write(f"\tdefaultPowerBIDataSourceVersion: {dsv_map[dsv]}\n")
            if model_props[1]:
                f.write("\tdiscourageImplicitMeasures\n")
            sqc = model_props[2]
            if sqc:
                f.write(f"\tsourceQueryCulture: {sqc}\n")
            dao = model_props[3]
            if dao:
                import json as _json
                try:
                    dao_obj = _json.loads(dao)
                    if dao_obj:
                        f.write("\tdataAccessOptions\n")
                        if dao_obj.get("legacyRedirects"):
                            f.write("\t\tlegacyRedirects\n")
                        if dao_obj.get("returnErrorValuesAsNull"):
                            f.write("\t\treturnErrorValuesAsNull\n")
                except Exception:
                    pass

    # ---- expressions.tmdl (shared M parameters) ----
    c.execute(
        "SELECT Name, Expression, Description, LineageTag FROM Expression "
        "WHERE ModelID = 1 ORDER BY ID"
    )
    exprs = c.fetchall()
    if exprs:
        lines = []
        for e_name, e_expr, e_desc, e_tag in exprs:
            if e_expr:
                expr_lines = e_expr.strip().split("\n")
                if len(expr_lines) == 1:
                    lines.append(f"expression {_tmdl_escape(e_name)} =")
                    lines.append(f"\t\t{expr_lines[0]}")
                else:
                    lines.append(f"expression {_tmdl_escape(e_name)} =")
                    for el in expr_lines:
                        lines.append(f"\t\t{el}")
                # Note: TMDL expression objects do not support 'description'
                if e_tag:
                    lines.append(f"\tlineageTag: {e_tag}")
                lines.append("")
        if lines:
            with open(os.path.join(output_dir, "expressions.tmdl"), "w", encoding="utf-8") as f:
                f.write("\n".join(lines) + "\n")

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
        # Note: PBI Desktop's TMDL parser rejects 'description' on tables
        # even though the TOM model supports it — skip to avoid load errors
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
            # Note: PBI Desktop's TMDL parser rejects 'description' on columns
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
            # Note: PBI Desktop's TMDL parser rejects 'description' on measures
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
                if p_type == 2:
                    # Calculated partition (DAX)
                    lines.append(f"\tpartition '{_tmdl_escape(p_name)}' = calculated")
                else:
                    # Type 4 = M (Power Query), default
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
            # TOM CrossFilteringBehavior: 1=OneDirection (default), 2=BothDirections, 3=Automatic
            # Only emit non-default values in TMDL
            cfb_map = {2: "bothDirections", 3: "automatic"}
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
            # Note: PBI Desktop's TMDL parser rejects 'description' on roles

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


def _sanitize_pbir_name(name: str) -> str:
    """Make a name safe for PBIR folder/file naming (word chars or hyphens only)."""
    import re as _re
    sanitized = _re.sub(r'[^\w\-]', '_', name)
    if not sanitized:
        sanitized = "unnamed"
    return sanitized[:50]


def _pbix_config_to_pbir_visual(config: dict, x: float, y: float, w: float, h: float, z: float = 0) -> dict:
    """Convert a PBIX visualContainer config dict to a PBIR visual.json dict."""
    pbir_name = config.get("name", "visual")
    single_visual = config.get("singleVisual", {})
    visual_type = single_visual.get("visualType", "unknown")

    # PBIR visual structure
    visual_obj: dict = {
        "visualType": visual_type,
    }

    # Preserve drillFilterOtherVisuals if present
    if "drillFilterOtherVisuals" in single_visual:
        visual_obj["drillFilterOtherVisuals"] = single_visual["drillFilterOtherVisuals"]

    # Build query structure from prototypeQuery + projections
    proto = single_visual.get("prototypeQuery")
    projections = single_visual.get("projections")
    if proto or projections:
        query: dict = {"queryState": {}}
        if projections:
            # Translate projections to queryState role mappings
            for role_name, role_items in projections.items():
                query["queryState"][role_name] = {"projections": role_items}
        visual_obj["query"] = query
        if proto:
            visual_obj["query"]["sortDefinition"] = {"sort": [], "isDefaultSort": True}
            # Add prototypeQuery as dataViewMappings source
            visual_obj["query"]["queryRef"] = proto
    else:
        visual_obj["query"] = {"queryState": {}}

    # Copy data formatting (objects)
    if "objects" in single_visual:
        visual_obj["objects"] = single_visual["objects"]

    # visualContainerObjects = vcObjects (title, background, border, etc.)
    result: dict = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/1.0.0/schema.json",
        "name": pbir_name,
        "position": {"x": x, "y": y, "z": z, "width": w, "height": h},
        "visual": visual_obj,
    }
    if "vcObjects" in single_visual:
        result["visualContainerObjects"] = single_visual["vcObjects"]

    return result


@mcp.tool()
def pbix_export_pbip(alias: str, output_dir: str = "") -> str:
    """Convert a PBIX to PBIP (Power BI Project) folder structure.

    Creates a PBIP project with:
      - {name}.pbip            (root pointer JSON)
      - {name}.Report/         (report layout + static resources)
      - {name}.SemanticModel/  (semantic model as TMDL)
      - .gitignore             (standard PBIP ignores)

    PBIP is Microsoft's folder-based format for Git version control and CI/CD.

    Args:
        alias: The alias of the open file
        output_dir: Target directory. Defaults to <pbix_dir>/<name>_pbip/
    """
    try:
        info = _ensure_open(alias)
        work_dir = info["work_dir"]
        pbix_path = info["path"]

        base_name = os.path.splitext(os.path.basename(pbix_path))[0]
        # Strip spaces and special chars for safer folder names
        safe_base = "".join(c if c.isalnum() or c in "-_" else "_" for c in base_name)

        if not output_dir:
            pbix_dir = os.path.dirname(pbix_path)
            output_dir = os.path.join(pbix_dir, f"{safe_base}_pbip")

        # Clean or create output directory
        if os.path.exists(output_dir):
            import shutil
            shutil.rmtree(output_dir)
        os.makedirs(output_dir, exist_ok=True)

        report_dir = os.path.join(output_dir, f"{safe_base}.Report")
        model_dir = os.path.join(output_dir, f"{safe_base}.SemanticModel")
        os.makedirs(report_dir, exist_ok=True)
        os.makedirs(model_dir, exist_ok=True)

        # --- 1. Export TMDL to SemanticModel/definition/ ---
        dm_path = os.path.join(work_dir, "DataModel")
        tmdl_stats = {"tables": 0, "columns": 0, "measures": 0, "relationships": 0, "roles": 0}
        if os.path.exists(dm_path):
            from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
            from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

            with open(dm_path, "rb") as f:
                dm_bytes = f.read()
            abf = decompress_datamodel(dm_bytes)
            db_bytes = read_metadata_sqlite(abf)

            tmdl_def_dir = os.path.join(model_dir, "definition")
            os.makedirs(tmdl_def_dir, exist_ok=True)

            fd, tmp_db = tempfile.mkstemp(suffix=".db")
            os.write(fd, db_bytes)
            os.close(fd)
            try:
                conn = sqlite3.connect(tmp_db)
                tmdl_stats = _export_tmdl_from_sqlite(conn, tmdl_def_dir)
                conn.close()
            finally:
                try:
                    os.unlink(tmp_db)
                except OSError:
                    pass

        # --- 2. Create definition.pbism (semantic model descriptor) ---
        pbism_content = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/semanticModel/definitionProperties/1.0.0/schema.json",
            "version": "4.1",
            "settings": {}
        }
        with open(os.path.join(model_dir, "definition.pbism"), "w", encoding="utf-8") as f:
            json.dump(pbism_content, f, indent=2)

        # --- 3. Copy StaticResources to Report folder ---
        pbix_static = os.path.join(work_dir, "Report", "StaticResources")
        if os.path.isdir(pbix_static):
            import shutil
            dest_static = os.path.join(report_dir, "StaticResources")
            shutil.copytree(pbix_static, dest_static)

        # --- 4. Create definition.pbir (report descriptor with byPath ref to model) ---
        pbir_content = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
            "version": "1.0",
            "datasetReference": {
                "byPath": {
                    "path": f"../{safe_base}.SemanticModel"
                }
            }
        }
        with open(os.path.join(report_dir, "definition.pbir"), "w", encoding="utf-8") as f:
            json.dump(pbir_content, f, indent=2)

        # --- 5. Report layout (legacy format) ---
        # Use the original PBIX Layout JSON directly as report.json.
        # PBIR decomposed format (version 4.0) has rendering bugs in PBI Desktop,
        # so we use legacy format (version 1.0) with the full Layout JSON.
        layout = _get_layout(work_dir)
        if not layout:
            return ToolResponse.error("No layout found in PBIX", "LAYOUT_MISSING").to_text()

        sections = layout.get("sections", [])
        with open(os.path.join(report_dir, "report.json"), "w", encoding="utf-8") as f:
            json.dump(layout, f, indent=2, ensure_ascii=False)

        # --- 7. Root .pbip file ---
        pbip_content = {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/pbip/pbipProperties/1.0.0/schema.json",
            "version": "1.0",
            "artifacts": [
                {"report": {"path": f"{safe_base}.Report"}}
            ],
            "settings": {"enableAutoRecovery": True}
        }
        with open(os.path.join(output_dir, f"{safe_base}.pbip"), "w", encoding="utf-8") as f:
            json.dump(pbip_content, f, indent=2)

        # --- 8. .gitignore ---
        with open(os.path.join(output_dir, ".gitignore"), "w", encoding="utf-8") as f:
            f.write("**/.pbi/localSettings.json\n")
            f.write("**/.pbi/cache.abf\n")

        # Count output
        total_pages = len(sections)
        total_visuals = sum(len(s.get("visualContainers", [])) for s in sections)

        summary = (
            f"PBIP project exported to: {output_dir}\n\n"
            f"  {safe_base}.pbip                     (root)\n"
            f"  {safe_base}.Report/\n"
            f"    definition.pbir                    (report descriptor)\n"
            f"    report.json                        ({total_pages} pages, {total_visuals} visuals)\n"
            f"  {safe_base}.SemanticModel/\n"
            f"    definition.pbism                   (model descriptor)\n"
            f"    definition/                        (TMDL)\n"
            f"      {tmdl_stats['tables']} tables, {tmdl_stats['columns']} columns, "
            f"{tmdl_stats['measures']} measures, {tmdl_stats['relationships']} relationships, "
            f"{tmdl_stats['roles']} roles\n"
        )
        return ToolResponse.ok(summary).to_text()
    except PBIXMCPError as e:
        return ToolResponse.error(e.message, e.code).to_text()
    except Exception as e:
        return ToolResponse.error(f"{str(e)}\n{traceback.format_exc()}", "PBIP_EXPORT_ERROR").to_text()


# ---- Section 11: MCP main ----

if __name__ == "__main__":
    mcp.run()
