"""Template-free ABF builder — Step-by-step template elimination.

This module provides functions to progressively eliminate template
dependency from the ABF binary container. Each step is independently
testable and builds on the previous one.

Current step: Step 1 — Strip template VertiPaq data from Class=100069
"""

from __future__ import annotations

import secrets
import xml.etree.ElementTree as ET
from typing import Dict

from pbix_mcp.formats.abf_rebuild import (
    STREAM_STORAGE_SIGNATURE,
    _ABFStructure,
    _HEADER_PAGE_SIZE,
    _SIGNATURE_LEN,
    _xml_to_utf16_bytes,
)


def _rebuild_abf_without_template_vp(
    template_abf: bytes,
    metadata_replacement: bytes,
    new_files: Dict[str, bytes],
) -> bytes:
    """Rebuild ABF from template but STRIP all template VertiPaq files.

    Keeps: ADDITIONAL_LOG, PARTITIONS, db.xml, CryptKey.bin, metadata.sqlitedb
    Strips: ALL 280 template VertiPaq data files (IDF, IDFMETA, dict, hidx)
    Adds: Our new VertiPaq files from new_files dict

    Args:
        template_abf: Decompressed template ABF bytes
        metadata_replacement: Our clean metadata.sqlitedb bytes
        new_files: Dict mapping ABF paths to VertiPaq file bytes
                   Keys like "Regions (1010).tbl\\1013.prt\\0.Regions (1010).RegionID (1027).0.idf"

    Returns:
        Complete ABF bytes ready for XPress9 compression
    """
    s = _ABFStructure(template_abf)
    now = _windows_filetime_now()

    # ── Identify which template files to KEEP vs STRIP ──────────────
    # Class=100002 files (db.xml, CryptKey) → KEEP
    # Class=100069 files → STRIP ALL except metadata.sqlitedb
    # Standalone VDir entries (ADDITIONAL_LOG, PARTITIONS) → KEEP

    keep_storage_paths = set()  # StoragePaths to keep from template

    # Keep Class=100002 (system files)
    for fg in s.backup_log_root.findall("FileGroups/FileGroup"):
        if fg.findtext("Class", "") == "100002":
            for bf in fg.findall("FileList/BackupFile"):
                keep_storage_paths.add(bf.findtext("StoragePath", ""))

    # Find metadata.sqlitedb StoragePath in Class=100069 → KEEP
    metadata_sp = None
    for fg in s.backup_log_root.findall("FileGroups/FileGroup"):
        if fg.findtext("Class", "") == "100069":
            for bf in fg.findall("FileList/BackupFile"):
                path = bf.findtext("Path", "")
                sp = bf.findtext("StoragePath", "")
                if "metadata.sqlitedb" in path:
                    metadata_sp = sp
                    keep_storage_paths.add(sp)

    # Standalone VDir entries (ADDITIONAL_LOG, PARTITIONS) are always kept
    # They're identified by not being in any FileGroup

    # ── Collect file data: kept template files + our new files ──────
    file_entries = []  # List of (vdir_path, data_bytes, is_backuplog_file, fg_class, blog_path)

    # 1. Standalone VDir entries (ADDITIONAL_LOG, PARTITIONS)
    blog_sps = set()
    for fg in s.backup_log_root.findall("FileGroups/FileGroup"):
        for bf in fg.findall("FileList/BackupFile"):
            blog_sps.add(bf.findtext("StoragePath", ""))

    for ve in s.data_entries:
        if ve.path not in blog_sps:
            # Standalone entry — keep as-is
            data = template_abf[ve.m_cbOffsetHeader:ve.m_cbOffsetHeader + ve.size]
            file_entries.append((ve.path, data, False, None, None))

    # 2. Class=100002 files (db.xml, CryptKey) — keep from template
    for fg in s.backup_log_root.findall("FileGroups/FileGroup"):
        if fg.findtext("Class", "") == "100002":
            for bf in fg.findall("FileList/BackupFile"):
                sp = bf.findtext("StoragePath", "")
                path = bf.findtext("Path", "")
                # Read original data from template
                ve = next((v for v in s.data_entries if v.path == sp), None)
                if ve:
                    data = template_abf[ve.m_cbOffsetHeader:ve.m_cbOffsetHeader + ve.size]
                    file_entries.append((sp, data, True, "100002", path))

    # 3. metadata.sqlitedb — our replacement
    if metadata_sp:
        # Find the original BackupLog path for metadata
        for fg in s.backup_log_root.findall("FileGroups/FileGroup"):
            if fg.findtext("Class", "") == "100069":
                for bf in fg.findall("FileList/BackupFile"):
                    if bf.findtext("StoragePath", "") == metadata_sp:
                        blog_path = bf.findtext("Path", "")
                        file_entries.append((metadata_sp, metadata_replacement, True, "100069", blog_path))
                        break

    # 4. Our new VertiPaq files — assign random StoragePaths
    persist_root = ""
    for fg in s.backup_log_root.findall("FileGroups/FileGroup"):
        if fg.findtext("Class", "") == "100069":
            plp = fg.findtext("PersistLocationPath", "")
            if plp:
                persist_root = plp.rstrip("\\") + "\\"
            break

    new_file_mappings = []  # (sp, blog_path, data)
    for rel_path, data in sorted(new_files.items()):
        sp = secrets.token_hex(10).upper()
        blog_path = persist_root + rel_path
        file_entries.append((sp, data, True, "100069", blog_path))
        new_file_mappings.append((sp, blog_path, len(data)))

    # ── Build the binary layout ─────────────────────────────────────
    buf = bytearray(STREAM_STORAGE_SIGNATURE)
    buf.extend(b"\x00" * (_HEADER_PAGE_SIZE - _SIGNATURE_LEN))  # Header placeholder

    # Write file data sequentially
    vdir_entries = []  # (path, offset, size)
    for vdir_path, data, _, _, _ in file_entries:
        offset = len(buf)
        buf.extend(data)
        vdir_entries.append((vdir_path, offset, len(data)))

    # ── Build BackupLog XML ─────────────────────────────────────────
    # Clone template BackupLog but strip Class=100069 VP entries
    new_blog = ET.Element(s.backup_log_root.tag)
    for child in s.backup_log_root:
        if child.tag == "FileGroups":
            new_fgs = ET.SubElement(new_blog, "FileGroups")
            for fg in child.findall("FileGroup"):
                cls = fg.findtext("Class", "")
                if cls == "100002":
                    # Keep Class=100002 as-is
                    new_fgs.append(fg)
                elif cls == "100069":
                    # Rebuild Class=100069 with only our files
                    new_fg = ET.SubElement(new_fgs, "FileGroup")
                    for fg_child in fg:
                        if fg_child.tag == "FileList":
                            new_fl = ET.SubElement(new_fg, "FileList")
                            # Keep metadata.sqlitedb
                            for bf in fg_child.findall("BackupFile"):
                                if "metadata.sqlitedb" in bf.findtext("Path", ""):
                                    # Update size
                                    bf.find("Size").text = str(len(metadata_replacement))
                                    bf.find("LastWriteTime").text = str(now)
                                    new_fl.append(bf)
                            # Add our new VP files
                            for sp, blog_path, size in new_file_mappings:
                                new_bf = ET.SubElement(new_fl, "BackupFile")
                                ET.SubElement(new_bf, "Path").text = blog_path
                                ET.SubElement(new_bf, "StoragePath").text = sp
                                ET.SubElement(new_bf, "LastWriteTime").text = str(now)
                                ET.SubElement(new_bf, "Size").text = str(size)
                        else:
                            new_fg.append(fg_child)
        else:
            new_blog.append(child)

    blog_bytes = _xml_to_utf16_bytes(new_blog)
    blog_offset = len(buf)
    buf.extend(blog_bytes)
    vdir_entries.append(("LOG", blog_offset, len(blog_bytes)))

    # ── Build VirtualDirectory XML ──────────────────────────────────
    vdir_root = ET.Element("VirtualDirectory")
    for vdir_path, offset, size in vdir_entries:
        bf = ET.SubElement(vdir_root, "BackupFile")
        ET.SubElement(bf, "Path").text = vdir_path
        ET.SubElement(bf, "Size").text = str(size)
        ET.SubElement(bf, "m_cbOffsetHeader").text = str(offset)
        ET.SubElement(bf, "Delete").text = "false"
        ET.SubElement(bf, "CreatedTimestamp").text = str(now)
        ET.SubElement(bf, "Access").text = str(now)
        ET.SubElement(bf, "LastWriteTime").text = str(now)

    vdir_bytes = _xml_to_utf16_bytes(vdir_root)
    vdir_offset = len(buf)
    buf.extend(vdir_bytes)

    # ── Patch BackupLogHeader ───────────────────────────────────────
    header_root = ET.Element("BackupLog")
    for child in s.header_root:
        new_child = ET.SubElement(header_root, child.tag)
        if child.tag == "m_cbOffsetHeader":
            new_child.text = str(vdir_offset)
        elif child.tag == "DataSize":
            new_child.text = str(len(vdir_bytes))
        elif child.tag == "Files":
            new_child.text = str(len(vdir_entries))
        else:
            new_child.text = child.text

    header_bytes = _xml_to_utf16_bytes(header_root)
    if len(header_bytes) > _HEADER_PAGE_SIZE - _SIGNATURE_LEN:
        raise ValueError(f"Header XML too large: {len(header_bytes)} bytes")
    buf[_SIGNATURE_LEN:_SIGNATURE_LEN + len(header_bytes)] = header_bytes

    return bytes(buf)


def _windows_filetime_now() -> int:
    """Get current time as Windows FILETIME (100ns ticks since 1601-01-01)."""
    import datetime
    epoch = datetime.datetime(1601, 1, 1)
    now = datetime.datetime.utcnow()
    delta = now - epoch
    return int(delta.total_seconds() * 10_000_000)
