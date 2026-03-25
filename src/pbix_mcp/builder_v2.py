"""Template-free ABF builder — Step-by-step template elimination.

This module provides functions to progressively eliminate template
dependency from the ABF binary container. Each step is independently
testable and builds on the previous one.

Current step: Step 2 — Generate own db.xml + strip template VP
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


# ── Step 3: CryptKey constant ───────────────────────────────────────
# The CryptKey.bin is a 144-byte cryptographic key BLOB that requires
# Microsoft's crypto infrastructure to generate (rskeymgmt / exponent-of-one
# private key packaging). We use a known-valid key extracted from a working
# PBIX. The key is GUID-independent — any valid key works with any db.xml.
CRYPTKEY_BYTES = bytes.fromhex(
    "98bc215d2d8de64ea8e5d038aac94441"
    "04000000300000005000000010000000"
    "0100000007000000ffffffff00000000"
    "010200000366000000a400009270d94a"
    "b3f7014a7f3d8cda8a0b13dc34f880"
    "45ef9e253200a15b7ca339a6f052795f"
    "804bbc5f635463b6f39c4a4de6535c4a"
    "ea8360a9904a3974163dd10200000000"
    "0098bc215d2d8de64ea8e5d038aac94441"
)


# ── Step 2: Generate db.xml XMLA Load document ─────────────────────

# Template db.xml with placeholder GUIDs to substitute
_DB_XML_TEMPLATE = (
    '<Load xmlns="http://schemas.microsoft.com/analysisservices/2003/engine"'
    ' xmlns:ddl2="http://schemas.microsoft.com/analysisservices/2003/engine/2"'
    ' xmlns:ddl2_2="http://schemas.microsoft.com/analysisservices/2003/engine/2/2"'
    ' xmlns:ddl100="http://schemas.microsoft.com/analysisservices/2008/engine/100"'
    ' xmlns:ddl100_100="http://schemas.microsoft.com/analysisservices/2008/engine/100/100"'
    ' xmlns:ddl200="http://schemas.microsoft.com/analysisservices/2010/engine/200"'
    ' xmlns:ddl200_200="http://schemas.microsoft.com/analysisservices/2010/engine/200/200"'
    ' xmlns:ddl300="http://schemas.microsoft.com/analysisservices/2011/engine/300"'
    ' xmlns:ddl300_300="http://schemas.microsoft.com/analysisservices/2011/engine/300/300"'
    ' xmlns:ddl400="http://schemas.microsoft.com/analysisservices/2012/engine/400"'
    ' xmlns:ddl400_400="http://schemas.microsoft.com/analysisservices/2012/engine/400/400"'
    ' xmlns:ddl410="http://schemas.microsoft.com/analysisservices/2012/engine/410"'
    ' xmlns:ddl410_410="http://schemas.microsoft.com/analysisservices/2012/engine/410/410"'
    ' xmlns:ddl500="http://schemas.microsoft.com/analysisservices/2013/engine/500"'
    ' xmlns:ddl500_500="http://schemas.microsoft.com/analysisservices/2013/engine/500/500"'
    ' xmlns:ddl600="http://schemas.microsoft.com/analysisservices/2013/engine/600"'
    ' xmlns:ddl600_600="http://schemas.microsoft.com/analysisservices/2013/engine/600/600"'
    ' xmlns:ddl700="http://schemas.microsoft.com/analysisservices/2018/engine/700"'
    ' xmlns:ddl700_700="http://schemas.microsoft.com/analysisservices/2018/engine/700/700"'
    ' xmlns:ddl800="http://schemas.microsoft.com/analysisservices/2018/engine/800"'
    ' xmlns:ddl800_800="http://schemas.microsoft.com/analysisservices/2018/engine/800/800"'
    ' xmlns:ddl900="http://schemas.microsoft.com/analysisservices/2019/engine/900"'
    ' xmlns:ddl900_900="http://schemas.microsoft.com/analysisservices/2019/engine/900/900"'
    ' xmlns:ddl910="http://schemas.microsoft.com/analysisservices/2020/engine/910"'
    ' xmlns:ddl910_910="http://schemas.microsoft.com/analysisservices/2020/engine/910/910"'
    ' xmlns:ddl920="http://schemas.microsoft.com/analysisservices/2020/engine/920"'
    ' xmlns:ddl920_920="http://schemas.microsoft.com/analysisservices/2020/engine/920/920"'
    ' xmlns:ddl921="http://schemas.microsoft.com/analysisservices/2021/engine/921"'
    ' xmlns:ddl921_921="http://schemas.microsoft.com/analysisservices/2021/engine/921/921"'
    ' xmlns:ddl922="http://schemas.microsoft.com/analysisservices/2022/engine/922"'
    ' xmlns:ddl922_922="http://schemas.microsoft.com/analysisservices/2022/engine/922/922"'
    ' xmlns:xsd="http://www.w3.org/2001/XMLSchema"'
    ' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">'
    "<ObjectDefinition><Database>"
    "<Name>{db_id}</Name>"
    "<ID>{db_id}</ID>"
    "<CreatedTimestamp>{timestamp}</CreatedTimestamp>"
    "<LastSchemaUpdate>{timestamp}</LastSchemaUpdate>"
    "<ObjectVersion>0</ObjectVersion>"
    "<ObjectID>{object_id}</ObjectID>"
    "<Ordinal>0</Ordinal>"
    "<PersistLocation>0</PersistLocation>"
    "<System>false</System>"
    "<DataFileList>0.CryptKey.bin</DataFileList>"
    "<Description/>"
    "<ddl800:DbUniqueId>{db_unique_id}</ddl800:DbUniqueId>"
    "<AggregationPrefix/>"
    "<Language>1033</Language>"
    "<Collation/>"
    "<ddl400:DefaultCollationVersion>100</ddl400:DefaultCollationVersion>"
    "<Visible>true</Visible>"
    "<MasterDataSourceID/>"
    "<ProcessingPriority>0</ProcessingPriority>"
    "<ddl200_200:StorageEngineUsed valuens=\"ddl500_500\">TabularMetadata</ddl200_200:StorageEngineUsed>"
    "<ddl200:CompatibilityLevel>1550</ddl200:CompatibilityLevel>"
    "<Translations/>"
    "<DataSourceImpersonationInfo>"
    "<ImpersonationMode>Default</ImpersonationMode>"
    "<Account/>"
    "<Password/>"
    "</DataSourceImpersonationInfo>"
    "<DataVersion>0</DataVersion>"
    "</Database></ObjectDefinition></Load>"
)


def generate_db_xml(db_id: str, object_id: str, db_unique_id: str) -> bytes:
    """Generate the XMLA Load document (db.xml) for the ABF.

    Args:
        db_id: Database GUID (lowercase, used as Name and ID)
        object_id: Object GUID (uppercase, used as ObjectID)
        db_unique_id: Unique GUID (uppercase, used as DbUniqueId)

    Returns:
        UTF-8 encoded db.xml bytes
    """
    import datetime
    ts = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]

    xml = _DB_XML_TEMPLATE.format(
        db_id=db_id,
        object_id=object_id,
        db_unique_id=db_unique_id,
        timestamp=ts,
    )
    return xml.encode("utf-8")


# ── Steps 4-6: Complete template-free ABF builder ───────────────────

# System file constants
_ADDITIONAL_LOG = '<Property><ProductName>Default</ProductName></Property>'.encode("utf-16")
_PARTITIONS = '<Partitions/>'.encode("utf-16")


def build_abf_clean(
    metadata_sqlite: bytes,
    vertipaq_files: Dict[str, bytes],
    db_id: str | None = None,
) -> bytes:
    """Build a complete ABF archive from scratch — ZERO template dependency.

    Only uses the CRYPTKEY_BYTES constant (144 bytes, crypto format requirement).
    Everything else is generated: db.xml, BackupLog, VDir, Header.

    Args:
        metadata_sqlite: Raw SQLite metadata bytes
        vertipaq_files: Dict mapping relative paths to VertiPaq file bytes
        db_id: Optional database GUID (auto-generated if None)

    Returns:
        Complete ABF binary ready for XPress9 compression
    """
    import uuid

    if db_id is None:
        db_id = str(uuid.uuid4())
    object_id = str(uuid.uuid4()).upper()
    db_unique_id = str(uuid.uuid4()).upper()
    now = _windows_filetime_now()

    # ── Generate system files ───────────────────────────────────────
    db_xml_bytes = generate_db_xml(db_id, object_id, db_unique_id)
    cryptkey_bytes = CRYPTKEY_BYTES

    # ── Assign StoragePaths ─────────────────────────────────────────
    # Each file gets a random 20-char hex ID used in both VDir and BackupLog
    def _sp():
        return secrets.token_hex(10).upper()

    db_xml_sp = _sp()
    cryptkey_sp = _sp()
    metadata_sp = _sp()

    # VP file StoragePaths
    vp_mappings = []  # (sp, rel_path, data)
    for rel_path in sorted(vertipaq_files):
        vp_mappings.append((_sp(), rel_path, vertipaq_files[rel_path]))

    # ── Persist path (synthetic — PBI doesn't validate the actual path) ──
    persist_path = f"\\\\?\\C:\\Sandboxes\\{db_id}.0.db"

    # ── Lay out file data sequentially ──────────────────────────────
    buf = bytearray(STREAM_STORAGE_SIGNATURE)
    buf.extend(b"\x00" * (_HEADER_PAGE_SIZE - _SIGNATURE_LEN))  # Header placeholder

    vdir_entries = []  # (path, offset, size)

    # 1. ADDITIONAL_LOG (standalone VDir entry)
    offset = len(buf)
    buf.extend(_ADDITIONAL_LOG)
    vdir_entries.append(("ADDITIONAL_LOG", offset, len(_ADDITIONAL_LOG)))

    # 2. PARTITIONS (standalone VDir entry)
    offset = len(buf)
    buf.extend(_PARTITIONS)
    vdir_entries.append(("PARTITIONS", offset, len(_PARTITIONS)))

    # 3. db.xml (Class=100002)
    offset = len(buf)
    buf.extend(db_xml_bytes)
    vdir_entries.append((db_xml_sp, offset, len(db_xml_bytes)))

    # 4. CryptKey (Class=100002)
    offset = len(buf)
    buf.extend(cryptkey_bytes)
    vdir_entries.append((cryptkey_sp, offset, len(cryptkey_bytes)))

    # 5. metadata.sqlitedb (Class=100069)
    offset = len(buf)
    buf.extend(metadata_sqlite)
    vdir_entries.append((metadata_sp, offset, len(metadata_sqlite)))

    # 6. VertiPaq files (Class=100069)
    for sp, _rel, data in vp_mappings:
        offset = len(buf)
        buf.extend(data)
        vdir_entries.append((sp, offset, len(data)))

    # ── Build BackupLog XML ─────────────────────────────────────────
    blog_xml = _build_backup_log(
        db_id=db_id,
        object_id=object_id,
        persist_path=persist_path,
        db_xml_sp=db_xml_sp,
        db_xml_size=len(db_xml_bytes),
        cryptkey_sp=cryptkey_sp,
        cryptkey_size=len(cryptkey_bytes),
        metadata_sp=metadata_sp,
        metadata_size=len(metadata_sqlite),
        vp_mappings=vp_mappings,
        now=now,
    )
    blog_bytes = blog_xml.encode("utf-16")
    blog_offset = len(buf)
    buf.extend(blog_bytes)
    vdir_entries.append(("LOG", blog_offset, len(blog_bytes)))

    # ── Build VirtualDirectory XML ──────────────────────────────────
    vdir_parts = ['<VirtualDirectory>']
    for path, off, size in vdir_entries:
        vdir_parts.append(
            f'<BackupFile>'
            f'<Path>{path}</Path>'
            f'<Size>{size}</Size>'
            f'<m_cbOffsetHeader>{off}</m_cbOffsetHeader>'
            f'<Delete>false</Delete>'
            f'<CreatedTimestamp>{now}</CreatedTimestamp>'
            f'<Access>{now}</Access>'
            f'<LastWriteTime>{now}</LastWriteTime>'
            f'</BackupFile>'
        )
    vdir_parts.append('</VirtualDirectory>')
    vdir_xml = ''.join(vdir_parts)
    vdir_bytes = vdir_xml.encode("utf-16")
    vdir_offset = len(buf)
    buf.extend(vdir_bytes)

    # ── Build BackupLogHeader ───────────────────────────────────────
    header_xml = (
        f'<BackupLog>'
        f'<BackupRestoreSyncVersion>140</BackupRestoreSyncVersion>'
        f'<Fault>false</Fault>'
        f'<faultcode>0</faultcode>'
        f'<ErrorCode>false</ErrorCode>'
        f'<EncryptionFlag>false</EncryptionFlag>'
        f'<EncryptionKey>0</EncryptionKey>'
        f'<ApplyCompression>false</ApplyCompression>'
        f'<m_cbOffsetHeader>{vdir_offset}</m_cbOffsetHeader>'
        f'<DataSize>{len(vdir_bytes)}</DataSize>'
        f'<Files>{len(vdir_entries)}</Files>'
        f'<ObjectID>{object_id}</ObjectID>'
        f'<m_cbOffsetData>{_HEADER_PAGE_SIZE}</m_cbOffsetData>'
        f'</BackupLog>'
    )
    header_bytes = header_xml.encode("utf-16")
    if len(header_bytes) > _HEADER_PAGE_SIZE - _SIGNATURE_LEN:
        raise ValueError(f"Header XML too large: {len(header_bytes)} bytes")
    buf[_SIGNATURE_LEN:_SIGNATURE_LEN + len(header_bytes)] = header_bytes

    return bytes(buf)


def _build_backup_log(
    db_id: str,
    object_id: str,
    persist_path: str,
    db_xml_sp: str,
    db_xml_size: int,
    cryptkey_sp: str,
    cryptkey_size: int,
    metadata_sp: str,
    metadata_size: int,
    vp_mappings: list,
    now: int,
) -> str:
    """Build the BackupLog XML string (NOT the header — the full log)."""
    parts = [
        '<BackupLog>',
        '<BackupRestoreSyncVersion>11.53</BackupRestoreSyncVersion>',
        f'<ServerRoot>{persist_path}</ServerRoot>',
        '<SvrEncryptPwdFlag>true</SvrEncryptPwdFlag>',
        '<ServerEnableBinaryXML>false</ServerEnableBinaryXML>',
        '<ServerEnableCompression>false</ServerEnableCompression>',
        '<CompressionFlag>false</CompressionFlag>',
        '<EncryptionFlag>false</EncryptionFlag>',
        f'<ObjectName>{db_id}</ObjectName>',
        f'<ObjectId>{db_id}</ObjectId>',
        '<Write>ReadWrite</Write>',
        '<OlapInfo>false</OlapInfo>',
        '<IsTabular>true</IsTabular>',
        '<Collations />',
        '<Languages><Language>1033</Language></Languages>',
        '<FileGroups>',
    ]

    # FileGroup 0: Class=100002 (database metadata)
    db_xml_path = f'{persist_path}\\{db_id}.0.db.xml'
    cryptkey_path = f'{persist_path}\\0.CryptKey.bin'
    parts.extend([
        '<FileGroup>',
        '<Class>100002</Class>',
        f'<ID>{db_id}</ID>',
        f'<Name>{db_id}</Name>',
        '<ObjectVersion>0</ObjectVersion>',
        '<PersistLocation>0</PersistLocation>',
        f'<PersistLocationPath>{persist_path}</PersistLocationPath>',
        '<StorageLocationPath />',
        f'<ObjectID>{object_id}</ObjectID>',
        '<FileList>',
        f'<BackupFile><Path>{db_xml_path}</Path>'
        f'<StoragePath>{db_xml_sp}</StoragePath>'
        f'<LastWriteTime>{now}</LastWriteTime>'
        f'<Size>{db_xml_size}</Size></BackupFile>',
        f'<BackupFile><Path>{cryptkey_path}</Path>'
        f'<StoragePath>{cryptkey_sp}</StoragePath>'
        f'<LastWriteTime>{now}</LastWriteTime>'
        f'<Size>{cryptkey_size}</Size></BackupFile>',
        '</FileList>',
        '</FileGroup>',
    ])

    # FileGroup 1: Class=100069 (VertiPaq data)
    vp_persist = persist_path
    parts.extend([
        '<FileGroup>',
        '<Class>100069</Class>',
        f'<ID>{db_id}</ID>',
        f'<Name>{db_id}</Name>',
        '<ObjectVersion>-1</ObjectVersion>',
        '<PersistLocation>-1</PersistLocation>',
        f'<PersistLocationPath>{vp_persist}</PersistLocationPath>',
        '<StorageLocationPath />',
        '<ObjectID>00000000-0000-0000-0000-000000000000</ObjectID>',
        '<FileList>',
    ])

    # metadata.sqlitedb
    meta_path = f'{vp_persist}\\metadata.sqlitedb'
    parts.append(
        f'<BackupFile><Path>{meta_path}</Path>'
        f'<StoragePath>{metadata_sp}</StoragePath>'
        f'<LastWriteTime>{now}</LastWriteTime>'
        f'<Size>{metadata_size}</Size></BackupFile>'
    )

    # VertiPaq files
    for sp, rel_path, data in vp_mappings:
        full_path = vp_persist + '\\' + rel_path
        parts.append(
            f'<BackupFile><Path>{full_path}</Path>'
            f'<StoragePath>{sp}</StoragePath>'
            f'<LastWriteTime>{now}</LastWriteTime>'
            f'<Size>{len(data)}</Size></BackupFile>'
        )

    parts.extend([
        '</FileList>',
        '</FileGroup>',
        '</FileGroups>',
        '</BackupLog>',
    ])

    return ''.join(parts)
