"""ABF and PBIX generation — every byte from scratch.

Generates the complete PBIX binary stack from code:
  - ABF binary container (signature, header, VDir, BackupLog, db.xml, CryptKey)
  - PBIX ZIP shell (Version, Content_Types, DiagramLayout, Settings, Metadata)
"""

from __future__ import annotations

import json
import secrets
import xml.etree.ElementTree as ET
from typing import Dict

from pbix_mcp.formats.abf_rebuild import (
    _HEADER_PAGE_SIZE,
    _SIGNATURE_LEN,
    STREAM_STORAGE_SIGNATURE,
    _xml_to_utf16_bytes,
)


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


# ── ABF builder ─────────────────────────────────────────────────────

# System file constants
_ADDITIONAL_LOG = '<Property><ProductName>Default</ProductName></Property>'.encode("utf-16")
_PARTITIONS = '<Partitions/>'.encode("utf-16")


def build_abf_clean(
    metadata_sqlite: bytes,
    vertipaq_files: Dict[str, bytes],
    db_id: str | None = None,
) -> bytes:
    """Build a complete ABF archive from scratch.

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
    # ServerRoot must be the PARENT directory; PersistLocationPath is the .0.db child
    server_root = "\\\\?\\C:\\Sandboxes"
    persist_path = f"{server_root}\\{db_id}.0.db"

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

    # ── Build BackupLog XML (using ET for consistent encoding) ──────
    blog_root = _build_backup_log_et(
        db_id=db_id,
        object_id=object_id,
        server_root=server_root,
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
    blog_bytes = _xml_to_utf16_bytes(blog_root)
    blog_offset = len(buf)
    buf.extend(blog_bytes)
    vdir_entries.append(("LOG", blog_offset, len(blog_bytes)))

    # ── Build VirtualDirectory XML (using ET) ────────────────────────
    vdir_root = ET.Element("VirtualDirectory")
    for path, off, size in vdir_entries:
        bf = ET.SubElement(vdir_root, "BackupFile")
        ET.SubElement(bf, "Path").text = path
        ET.SubElement(bf, "Size").text = str(size)
        ET.SubElement(bf, "m_cbOffsetHeader").text = str(off)
        ET.SubElement(bf, "Delete").text = "false"
        ET.SubElement(bf, "CreatedTimestamp").text = str(now)
        ET.SubElement(bf, "Access").text = str(now)
        ET.SubElement(bf, "LastWriteTime").text = str(now)

    vdir_bytes = _xml_to_utf16_bytes(vdir_root)
    vdir_offset = len(buf)
    buf.extend(vdir_bytes)

    # ── Build BackupLogHeader (using ET) ─────────────────────────────
    header_root = ET.Element("BackupLog")
    ET.SubElement(header_root, "BackupRestoreSyncVersion").text = "140"
    ET.SubElement(header_root, "Fault").text = "false"
    ET.SubElement(header_root, "faultcode").text = "0"
    ET.SubElement(header_root, "ErrorCode").text = "false"
    ET.SubElement(header_root, "EncryptionFlag").text = "false"
    ET.SubElement(header_root, "EncryptionKey").text = "0"
    ET.SubElement(header_root, "ApplyCompression").text = "false"
    ET.SubElement(header_root, "m_cbOffsetHeader").text = str(vdir_offset)
    ET.SubElement(header_root, "DataSize").text = str(len(vdir_bytes))
    ET.SubElement(header_root, "Files").text = str(len(vdir_entries))
    ET.SubElement(header_root, "ObjectID").text = object_id
    ET.SubElement(header_root, "m_cbOffsetData").text = str(_HEADER_PAGE_SIZE)

    header_bytes = _xml_to_utf16_bytes(header_root)
    if len(header_bytes) > _HEADER_PAGE_SIZE - _SIGNATURE_LEN:
        raise ValueError(f"Header XML too large: {len(header_bytes)} bytes")
    buf[_SIGNATURE_LEN:_SIGNATURE_LEN + len(header_bytes)] = header_bytes

    return bytes(buf)


def _build_backup_log_et(
    db_id: str,
    object_id: str,
    server_root: str,
    persist_path: str,
    db_xml_sp: str,
    db_xml_size: int,
    cryptkey_sp: str,
    cryptkey_size: int,
    metadata_sp: str,
    metadata_size: int,
    vp_mappings: list,
    now: int,
) -> ET.Element:
    """Build the BackupLog XML as an ET.Element tree (NOT the header — the full log)."""
    root = ET.Element("BackupLog")

    ET.SubElement(root, "BackupRestoreSyncVersion").text = "11.53"
    ET.SubElement(root, "ServerRoot").text = server_root
    ET.SubElement(root, "SvrEncryptPwdFlag").text = "true"
    ET.SubElement(root, "ServerEnableBinaryXML").text = "false"
    ET.SubElement(root, "ServerEnableCompression").text = "false"
    ET.SubElement(root, "CompressionFlag").text = "false"
    ET.SubElement(root, "EncryptionFlag").text = "false"
    ET.SubElement(root, "ObjectName").text = db_id
    ET.SubElement(root, "ObjectId").text = db_id
    ET.SubElement(root, "Write").text = "ReadWrite"
    ET.SubElement(root, "OlapInfo").text = "false"
    ET.SubElement(root, "IsTabular").text = "true"
    ET.SubElement(root, "Collations")
    langs = ET.SubElement(root, "Languages")
    ET.SubElement(langs, "Language").text = "1033"

    file_groups = ET.SubElement(root, "FileGroups")

    # FileGroup 0: Class=100002 (database metadata)
    fg0 = ET.SubElement(file_groups, "FileGroup")
    ET.SubElement(fg0, "Class").text = "100002"
    ET.SubElement(fg0, "ID").text = db_id
    ET.SubElement(fg0, "Name").text = db_id
    ET.SubElement(fg0, "ObjectVersion").text = "0"
    ET.SubElement(fg0, "PersistLocation").text = "0"
    ET.SubElement(fg0, "PersistLocationPath").text = persist_path
    ET.SubElement(fg0, "StorageLocationPath")
    ET.SubElement(fg0, "ObjectID").text = object_id
    fl0 = ET.SubElement(fg0, "FileList")

    # db.xml entry — file sits at ServerRoot level (sibling of .0.db dir)
    bf_dbxml = ET.SubElement(fl0, "BackupFile")
    ET.SubElement(bf_dbxml, "Path").text = f"{server_root}\\{db_id}.0.db.xml"
    ET.SubElement(bf_dbxml, "StoragePath").text = db_xml_sp
    ET.SubElement(bf_dbxml, "LastWriteTime").text = str(now)
    ET.SubElement(bf_dbxml, "Size").text = str(db_xml_size)

    # CryptKey entry
    bf_ck = ET.SubElement(fl0, "BackupFile")
    ET.SubElement(bf_ck, "Path").text = f"{persist_path}\\0.CryptKey.bin"
    ET.SubElement(bf_ck, "StoragePath").text = cryptkey_sp
    ET.SubElement(bf_ck, "LastWriteTime").text = str(now)
    ET.SubElement(bf_ck, "Size").text = str(cryptkey_size)

    # FileGroup 1: Class=100069 (VertiPaq data)
    fg1 = ET.SubElement(file_groups, "FileGroup")
    ET.SubElement(fg1, "Class").text = "100069"
    ET.SubElement(fg1, "ID").text = db_id
    ET.SubElement(fg1, "Name").text = db_id
    ET.SubElement(fg1, "ObjectVersion").text = "-1"
    ET.SubElement(fg1, "PersistLocation").text = "-1"
    ET.SubElement(fg1, "PersistLocationPath").text = persist_path
    ET.SubElement(fg1, "StorageLocationPath")
    ET.SubElement(fg1, "ObjectID").text = "00000000-0000-0000-0000-000000000000"
    fl1 = ET.SubElement(fg1, "FileList")

    # metadata.sqlitedb
    bf_meta = ET.SubElement(fl1, "BackupFile")
    ET.SubElement(bf_meta, "Path").text = f"{persist_path}\\metadata.sqlitedb"
    ET.SubElement(bf_meta, "StoragePath").text = metadata_sp
    ET.SubElement(bf_meta, "LastWriteTime").text = str(now)
    ET.SubElement(bf_meta, "Size").text = str(metadata_size)

    # VertiPaq files
    for sp, rel_path, data in vp_mappings:
        bf_vp = ET.SubElement(fl1, "BackupFile")
        ET.SubElement(bf_vp, "Path").text = f"{persist_path}\\{rel_path}"
        ET.SubElement(bf_vp, "StoragePath").text = sp
        ET.SubElement(bf_vp, "LastWriteTime").text = str(now)
        ET.SubElement(bf_vp, "Size").text = str(len(data))

    return root


# ── PBIX ZIP shell constants ──────────────────────────────────────────

# Version: "1.28" in UTF-16-LE (8 bytes)
_PBIX_VERSION = "1.28".encode("utf-16-le")

# [Content_Types].xml — standard OOXML content types for PBIX
_CONTENT_TYPES_XML = (
    '<?xml version="1.0" encoding="utf-8"?>'
    '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
    '<Default Extension="json" ContentType="" />'
    '<Override PartName="/Version" ContentType="" />'
    '<Override PartName="/DiagramLayout" ContentType="" />'
    '<Override PartName="/Report/Layout" ContentType="" />'
    '<Override PartName="/Settings" ContentType="application/json" />'
    '<Override PartName="/Metadata" ContentType="application/json" />'
    '<Override PartName="/DataModel" ContentType="" />'
    '</Types>'
).encode("utf-8")

# DiagramLayout — empty diagram
_DIAGRAM_LAYOUT = json.dumps({
    "version": "1.1.0",
    "diagrams": [{
        "ordinal": 0,
        "scrollPosition": {"x": 0, "y": 0},
        "nodes": [],
        "name": "All tables",
        "zoomValue": 100,
        "pinKeyFieldsToTop": False,
        "showExtraHeaderInfo": False,
        "hideKeyFieldsWhenCollapsed": False,
        "tablesLocked": False,
    }],
    "selectedDiagram": "All tables",
    "defaultDiagram": "All tables",
}, separators=(",", ":")).encode("utf-16-le")

# Settings — auto-relationship detection disabled to prevent TMCCollectionObject errors
_SETTINGS = json.dumps({
    "Version": 4,
    "ReportSettings": {},
    "QueriesSettings": {
        "TypeDetectionEnabled": True,
        "RelationshipImportEnabled": False,
        "Version": "2.126.29.0",
    },
}, separators=(",", ":")).encode("utf-16-le")

# Metadata — file-level metadata
_METADATA = json.dumps({
    "Version": 5,
    "AutoCreatedRelationships": [],
    "CreatedFrom": "Cloud",
    "CreatedFromRelease": "2024.03",
}, separators=(",", ":")).encode("utf-16-le")


def build_pbix_clean(
    datamodel_bytes: bytes,
    layout_bytes: bytes,
    theme_json: str | None = None,
) -> bytes:
    """Build a complete PBIX ZIP from scratch.

    Args:
        datamodel_bytes: XPress9-compressed DataModel bytes
        layout_bytes: Report/Layout JSON bytes
        theme_json: Optional theme JSON string. If provided, included as BaseThemes.

    Returns:
        Complete PBIX file bytes (ZIP format)
    """
    import io
    import zipfile

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("Version", _PBIX_VERSION)
        zf.writestr("[Content_Types].xml", _CONTENT_TYPES_XML)
        zf.writestr("DiagramLayout", _DIAGRAM_LAYOUT)
        zf.writestr("Settings", _SETTINGS)
        zf.writestr("Metadata", _METADATA)
        zf.writestr("Report/Layout", layout_bytes)
        zf.writestr("DataModel", datamodel_bytes)
        if theme_json is not None:
            zf.writestr(
                "Report/StaticResources/SharedResources/BaseThemes/CY24SU11.json",
                theme_json.encode("utf-8"),
            )

    return buf.getvalue()
