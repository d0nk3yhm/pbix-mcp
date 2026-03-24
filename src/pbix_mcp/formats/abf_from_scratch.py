"""
Build an ABF (Analysis Backup Format) binary blob entirely from scratch.

NO template dependency. Generates signature, header, system files,
metadata, VertiPaq data, BackupLog, and VirtualDirectory.
"""

import secrets
import uuid
import xml.etree.ElementTree as ET

# ── Constants ────────────────────────────────────────────────────────

STREAM_STORAGE_SIGNATURE = (
    b"\xff\xfe"
    + "STREAM_STORAGE_SIGNATURE_)!@#$%^&*(".encode("utf-16le")
)
_SIGNATURE_LEN = 72
_HEADER_PAGE_SIZE = 0x1000  # 4096

# System file content — exact bytes extracted from a working PBI ABF
_ADDITIONAL_LOG = bytes.fromhex(
    "fffe3c00500072006f00700065007200740079003e003c00500072006f006400"
    "7500630074004e0061006d0065003e00440065006600610075006c0074003c00"
    "2f00500072006f0064007500630074004e0061006d0065003e003c002f005000"
    "72006f00700065007200740079003e00"
)  # UTF-16: <Property><ProductName>Default</ProductName></Property>  112 bytes

_PARTITIONS = bytes.fromhex(
    "fffe3c0050006100720074006900740069006f006e0073002f003e00"
)  # UTF-16: <Partitions/>  28 bytes

_CRYPTKEY = bytes.fromhex(
    "98bc215d2d8de64ea8e5d038aac94441040000003000000050000000"
    "100000000100000007000000ffffffff00000000010200000366000000"
    "a400009270d94ab3f7014a7f3d8cda8a0b13dc34f88045ef9e253200"
    "a15b7ca339a6f052795f804bbc5f635463b6f39c4a4de6535c4aea83"
    "60a9904a3974163dd102000000000098bc215d2d8de64ea8e5d038aa"
    "c94441"
)  # 144 bytes — exact CryptKey.bin from working ABF


def _xml_to_utf16(root: ET.Element) -> bytes:
    """Serialize XML element to UTF-16 bytes (with BOM)."""
    return ET.tostring(root, encoding="unicode", xml_declaration=False).encode("utf-16")


# ── Public API ───────────────────────────────────────────────────────

def build_abf(
    metadata_sqlite: bytes,
    vertipaq_files: dict[str, bytes],
) -> bytes:
    """
    Build a complete ABF blob from scratch — zero template dependency.
    """
    db_guid = str(uuid.uuid4())
    timestamp = 134188169980753163
    persist_path = f"\\\\?\\C:\\Data\\{db_guid}.0.db"

    # ── 1. Assign StoragePaths ──────────────────────────────────────
    # Standalone VDir entries use flat path names (no StoragePath mapping)
    # FileGroup entries use random hex StoragePaths
    dbxml_sp = secrets.token_hex(10).upper()
    crypt_sp = secrets.token_hex(10).upper()

    # Minimal db.xml — just enough to tell AS this is a valid database
    db_xml_content = (
        f'<Load xmlns="http://schemas.microsoft.com/analysisservices/2003/engine">'
        f'<ObjectDefinition><Database xmlns:xsd="http://www.w3.org/2001/XMLSchema"'
        f' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">'
        f'<ID>{db_guid}</ID><Name>{db_guid}</Name>'
        f'<StorageEngineUsed>TabularMetadata</StorageEngineUsed>'
        f'</Database></ObjectDefinition></Load>'
    ).encode("utf-8")

    # VertiPaq files get random StoragePaths
    vp_records: list[tuple[str, str, bytes]] = []
    for fpath, data in vertipaq_files.items():
        sp = secrets.token_hex(10).upper()
        vp_records.append((fpath, sp, data))

    # ── 2. Write data section ───────────────────────────────────────
    buf = bytearray()
    buf.extend(STREAM_STORAGE_SIGNATURE)

    header_start = len(buf)
    buf.extend(b"\x00" * (_HEADER_PAGE_SIZE - _SIGNATURE_LEN))

    # All file offsets/sizes (keyed by VDir path)
    offsets: dict[str, int] = {}
    sizes: dict[str, int] = {}

    # Standalone system entries (flat VDir names, NOT in any FileGroup)
    offsets["ADDITIONAL_LOG"] = len(buf)
    sizes["ADDITIONAL_LOG"] = len(_ADDITIONAL_LOG)
    buf.extend(_ADDITIONAL_LOG)

    offsets["PARTITIONS"] = len(buf)
    sizes["PARTITIONS"] = len(_PARTITIONS)
    buf.extend(_PARTITIONS)

    # db.xml (in FileGroup Class=100002)
    offsets[dbxml_sp] = len(buf)
    sizes[dbxml_sp] = len(db_xml_content)
    buf.extend(db_xml_content)

    # CryptKey.bin (in FileGroup Class=100002)
    offsets[crypt_sp] = len(buf)
    sizes[crypt_sp] = len(_CRYPTKEY)
    buf.extend(_CRYPTKEY)

    # metadata.sqlitedb (standalone VDir entry — raw SQLite)
    meta_vdir_path = secrets.token_hex(10).upper()
    offsets[meta_vdir_path] = len(buf)
    sizes[meta_vdir_path] = len(metadata_sqlite)
    buf.extend(metadata_sqlite)

    # VertiPaq data files
    for fpath, sp, data in vp_records:
        offsets[sp] = len(buf)
        sizes[sp] = len(data)
        buf.extend(data)

    # ── 3. Build BackupLog XML ──────────────────────────────────────
    blog_root = ET.Element("BackupLog")
    ET.SubElement(blog_root, "BackupRestoreSyncVersion").text = "11.53"
    ET.SubElement(blog_root, "ServerRoot").text = persist_path
    ET.SubElement(blog_root, "SvrEncryptPwdFlag").text = "true"
    ET.SubElement(blog_root, "ServerEnableBinaryXML").text = "false"
    ET.SubElement(blog_root, "ServerEnableCompression").text = "false"
    ET.SubElement(blog_root, "CompressionFlag").text = "false"
    ET.SubElement(blog_root, "EncryptionFlag").text = "false"
    ET.SubElement(blog_root, "ObjectName").text = db_guid
    ET.SubElement(blog_root, "ObjectId").text = db_guid
    ET.SubElement(blog_root, "Write").text = "ReadWrite"
    ET.SubElement(blog_root, "OlapInfo").text = "false"
    ET.SubElement(blog_root, "IsTabular").text = "true"
    ET.SubElement(blog_root, "Collations")
    ET.SubElement(blog_root, "Languages")

    file_groups = ET.SubElement(blog_root, "FileGroups")

    # FileGroup 0: Class=100002 (database metadata)
    fg0 = ET.SubElement(file_groups, "FileGroup")
    ET.SubElement(fg0, "Class").text = "100002"
    ET.SubElement(fg0, "ID").text = db_guid
    ET.SubElement(fg0, "Name").text = db_guid
    ET.SubElement(fg0, "ObjectVersion").text = "0"
    ET.SubElement(fg0, "PersistLocation").text = "0"
    ET.SubElement(fg0, "PersistLocationPath").text = persist_path
    ET.SubElement(fg0, "StorageLocationPath")
    ET.SubElement(fg0, "ObjectID").text = str(uuid.uuid4()).upper()
    fl0 = ET.SubElement(fg0, "FileList")

    # db.xml (database definition)
    bf = ET.SubElement(fl0, "BackupFile")
    ET.SubElement(bf, "Path").text = f"{persist_path}\\{db_guid}.0.db.xml"
    ET.SubElement(bf, "StoragePath").text = dbxml_sp
    ET.SubElement(bf, "LastWriteTime").text = str(timestamp)
    ET.SubElement(bf, "Size").text = str(sizes[dbxml_sp])

    # CryptKey.bin
    bf = ET.SubElement(fl0, "BackupFile")
    ET.SubElement(bf, "Path").text = f"{persist_path}\\0.CryptKey.bin"
    ET.SubElement(bf, "StoragePath").text = crypt_sp
    ET.SubElement(bf, "LastWriteTime").text = str(timestamp)
    ET.SubElement(bf, "Size").text = str(sizes[crypt_sp])

    # FileGroup 1: Class=100069 (VertiPaq data)
    fg1 = ET.SubElement(file_groups, "FileGroup")
    ET.SubElement(fg1, "Class").text = "100069"
    ET.SubElement(fg1, "ID").text = db_guid
    ET.SubElement(fg1, "Name").text = db_guid
    ET.SubElement(fg1, "ObjectVersion").text = "-1"
    ET.SubElement(fg1, "PersistLocation").text = "-1"
    ET.SubElement(fg1, "PersistLocationPath").text = persist_path
    ET.SubElement(fg1, "StorageLocationPath")
    ET.SubElement(fg1, "ObjectID").text = "00000000-0000-0000-0000-000000000000"
    fl1 = ET.SubElement(fg1, "FileList")

    # metadata.sqlitedb (in the data FileGroup, NOT standalone)
    bf = ET.SubElement(fl1, "BackupFile")
    ET.SubElement(bf, "Path").text = f"{persist_path}\\metadata.sqlitedb"
    ET.SubElement(bf, "StoragePath").text = meta_vdir_path
    ET.SubElement(bf, "LastWriteTime").text = str(timestamp)
    ET.SubElement(bf, "Size").text = str(len(metadata_sqlite))

    for fpath, sp, data in vp_records:
        bf = ET.SubElement(fl1, "BackupFile")
        ET.SubElement(bf, "Path").text = f"{persist_path}\\{fpath}"
        ET.SubElement(bf, "StoragePath").text = sp
        ET.SubElement(bf, "LastWriteTime").text = str(timestamp)
        ET.SubElement(bf, "Size").text = str(len(data))

    blog_bytes = _xml_to_utf16(blog_root)
    blog_offset = len(buf)
    blog_size = len(blog_bytes)
    buf.extend(blog_bytes)

    # ── 4. Build VirtualDirectory XML ───────────────────────────────
    vdir_root = ET.Element("VirtualDirectory")

    def _add_vdir_entry(path_key: str):
        vf = ET.SubElement(vdir_root, "BackupFile")
        ET.SubElement(vf, "Path").text = path_key
        ET.SubElement(vf, "Size").text = str(sizes[path_key])
        ET.SubElement(vf, "m_cbOffsetHeader").text = str(offsets[path_key])
        ET.SubElement(vf, "Delete").text = "false"
        ET.SubElement(vf, "CreatedTimestamp").text = str(timestamp)
        ET.SubElement(vf, "Access").text = str(timestamp)
        ET.SubElement(vf, "LastWriteTime").text = str(timestamp)

    # Standalone system entries (flat names)
    _add_vdir_entry("ADDITIONAL_LOG")
    _add_vdir_entry("PARTITIONS")

    # FileGroup Class=100002 entries (hex StoragePaths)
    _add_vdir_entry(dbxml_sp)
    _add_vdir_entry(crypt_sp)

    # metadata.sqlitedb (standalone — found by read_metadata_sqlite)
    _add_vdir_entry(meta_vdir_path)

    # VertiPaq files (hex StoragePaths)
    for fpath, sp, data in vp_records:
        vf = ET.SubElement(vdir_root, "BackupFile")
        ET.SubElement(vf, "Path").text = sp
        ET.SubElement(vf, "Size").text = str(sizes[sp])
        ET.SubElement(vf, "m_cbOffsetHeader").text = str(offsets[sp])
        ET.SubElement(vf, "Delete").text = "false"
        ET.SubElement(vf, "CreatedTimestamp").text = str(timestamp)
        ET.SubElement(vf, "Access").text = str(timestamp)
        ET.SubElement(vf, "LastWriteTime").text = str(timestamp)

    # BackupLog (always last)
    vf = ET.SubElement(vdir_root, "BackupFile")
    ET.SubElement(vf, "Path").text = "BackupLog"
    ET.SubElement(vf, "Size").text = str(blog_size)
    ET.SubElement(vf, "m_cbOffsetHeader").text = str(blog_offset)
    ET.SubElement(vf, "Delete").text = "false"
    ET.SubElement(vf, "CreatedTimestamp").text = str(timestamp)
    ET.SubElement(vf, "Access").text = str(timestamp)
    ET.SubElement(vf, "LastWriteTime").text = str(timestamp)

    vdir_bytes = _xml_to_utf16(vdir_root)
    vdir_offset = len(buf)
    vdir_size = len(vdir_bytes)
    buf.extend(vdir_bytes)

    # ── 5. Build and patch header ───────────────────────────────────
    # ADDITIONAL_LOG(1) + PARTITIONS(1) + dbxml(1) + cryptkey(1) + metadata(1) + VP files + BackupLog(1)
    total_files = 5 + len(vp_records) + 1

    hdr = ET.Element("BackupLogHeader")
    ET.SubElement(hdr, "BackupRestoreSyncVersion").text = "140"
    ET.SubElement(hdr, "Fault").text = "false"
    ET.SubElement(hdr, "faultcode").text = "0"
    ET.SubElement(hdr, "ErrorCode").text = "false"
    ET.SubElement(hdr, "EncryptionFlag").text = "false"
    ET.SubElement(hdr, "EncryptionKey").text = "0"
    ET.SubElement(hdr, "ApplyCompression").text = "false"
    ET.SubElement(hdr, "m_cbOffsetHeader").text = str(vdir_offset)
    ET.SubElement(hdr, "DataSize").text = str(vdir_size)
    ET.SubElement(hdr, "Files").text = str(total_files)
    ET.SubElement(hdr, "ObjectID").text = db_guid.upper()
    ET.SubElement(hdr, "m_cbOffsetData").text = "4096"

    hdr_bytes = _xml_to_utf16(hdr)
    available = _HEADER_PAGE_SIZE - _SIGNATURE_LEN
    if len(hdr_bytes) > available:
        raise ValueError(f"Header XML {len(hdr_bytes)} bytes exceeds {available}")
    hdr_padded = hdr_bytes + b"\x00" * (available - len(hdr_bytes))
    buf[header_start : header_start + available] = hdr_padded

    return bytes(buf)
