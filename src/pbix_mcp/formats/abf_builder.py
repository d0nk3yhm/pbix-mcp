"""
ABF (Analysis Backup Format) builder — create from scratch without template.

Generates a complete ABF archive containing:
  - metadata.sqlitedb (Power BI metadata database)
  - VertiPaq column files (IDF, IDFMETA, dictionary, HIDX)
  - BackupLog XML (file-group metadata)
  - VirtualDirectory XML (file index with offsets)

No template required — every byte is generated from the inputs.
"""

import secrets
import xml.etree.ElementTree as ET

from pbix_mcp.formats.abf_rebuild import (
    _HEADER_PAGE_SIZE,
    _SIGNATURE_LEN,
    STREAM_STORAGE_SIGNATURE,
    _xml_to_utf16_bytes,
)


# Fixed timestamp for reproducible builds (2025-03-01 in Windows FILETIME)
_DEFAULT_TIMESTAMP = 133_534_961_699_396_761


def build_abf(
    metadata_sqlite: bytes,
    vertipaq_files: dict[str, bytes],
    persist_root: str = "D:\\Data\\Model",
) -> bytes:
    """Build a complete ABF archive from scratch.

    Parameters
    ----------
    metadata_sqlite : bytes
        Complete Power BI metadata SQLite database bytes.
    vertipaq_files : dict[str, bytes]
        Mapping of ABF-internal paths to binary content.
        Keys are paths like "Sales (100).tbl\\0.prt\\0.Sales (100).Amount (101).0.idf"
    persist_root : str
        Logical root path for the BackupLog FileGroup (default "D:\\Data\\Model").
        Not used at runtime — just metadata.

    Returns
    -------
    bytes
        Complete ABF blob ready for XPress9 compression.
    """
    ts = _DEFAULT_TIMESTAMP

    # Assign random 20-char hex StoragePaths for each file
    sqlite_storage_path = secrets.token_hex(10).upper()
    file_records: list[tuple[str, str, bytes]] = []  # (logical_path, storage_path, data)

    # metadata.sqlitedb is always the first file
    file_records.append(("metadata.sqlitedb", sqlite_storage_path, metadata_sqlite))

    # VertiPaq files
    for fpath, content in vertipaq_files.items():
        storage_path = secrets.token_hex(10).upper()
        file_records.append((fpath, storage_path, content))

    # ---- Build the ABF binary ----
    buf = bytearray()

    # 1. Signature (72 bytes)
    buf.extend(STREAM_STORAGE_SIGNATURE)

    # 2. Header page placeholder (will be patched at the end)
    header_page_start = len(buf)  # == 72
    buf.extend(b"\x00" * (_HEADER_PAGE_SIZE - _SIGNATURE_LEN))
    # buf is now 4096 bytes

    # 3. Write all data files sequentially, recording offsets
    offsets: dict[str, int] = {}   # storage_path -> offset
    sizes: dict[str, int] = {}     # storage_path -> size

    for logical_path, storage_path, data in file_records:
        offsets[storage_path] = len(buf)
        sizes[storage_path] = len(data)
        buf.extend(data)

    # 4. Build and write BackupLog XML
    blog_root = ET.Element("BackupLog")
    file_groups = ET.SubElement(blog_root, "FileGroups")

    # FileGroup 0: System (metadata.sqlitedb)
    fg0 = ET.SubElement(file_groups, "FileGroup")
    ET.SubElement(fg0, "PersistLocationPath").text = "System\\Data Model"
    fl0 = ET.SubElement(fg0, "FileList")

    bf_meta = ET.SubElement(fl0, "BackupFile")
    ET.SubElement(bf_meta, "Path").text = "System\\Data Model\\metadata.sqlitedb"
    ET.SubElement(bf_meta, "StoragePath").text = sqlite_storage_path
    ET.SubElement(bf_meta, "LastWriteTime").text = str(ts)
    ET.SubElement(bf_meta, "Size").text = str(sizes[sqlite_storage_path])

    # FileGroup 1: Database (VertiPaq files)
    if vertipaq_files:
        fg1 = ET.SubElement(file_groups, "FileGroup")
        ET.SubElement(fg1, "PersistLocationPath").text = persist_root
        fl1 = ET.SubElement(fg1, "FileList")

        for logical_path, storage_path, data in file_records:
            if logical_path == "metadata.sqlitedb":
                continue  # Already in FileGroup 0
            bf = ET.SubElement(fl1, "BackupFile")
            ET.SubElement(bf, "Path").text = f"{persist_root}\\{logical_path}"
            ET.SubElement(bf, "StoragePath").text = storage_path
            ET.SubElement(bf, "LastWriteTime").text = str(ts)
            ET.SubElement(bf, "Size").text = str(sizes[storage_path])

    blog_bytes = _xml_to_utf16_bytes(blog_root)
    blog_offset = len(buf)
    blog_size = len(blog_bytes)
    buf.extend(blog_bytes)

    # 5. Build and write VirtualDirectory XML
    vdir_root = ET.Element("VirtualDirectory")

    # All data files
    for logical_path, storage_path, data in file_records:
        bf_elem = ET.SubElement(vdir_root, "BackupFile")
        ET.SubElement(bf_elem, "Path").text = storage_path
        ET.SubElement(bf_elem, "Size").text = str(sizes[storage_path])
        ET.SubElement(bf_elem, "m_cbOffsetHeader").text = str(offsets[storage_path])
        ET.SubElement(bf_elem, "Delete").text = "false"
        ET.SubElement(bf_elem, "CreatedTimestamp").text = str(ts)
        ET.SubElement(bf_elem, "Access").text = str(ts)
        ET.SubElement(bf_elem, "LastWriteTime").text = str(ts)

    # BackupLog entry (MUST be last in VDir)
    blog_storage = "BackupLog.xml"
    bf_elem = ET.SubElement(vdir_root, "BackupFile")
    ET.SubElement(bf_elem, "Path").text = blog_storage
    ET.SubElement(bf_elem, "Size").text = str(blog_size)
    ET.SubElement(bf_elem, "m_cbOffsetHeader").text = str(blog_offset)
    ET.SubElement(bf_elem, "Delete").text = "false"
    ET.SubElement(bf_elem, "CreatedTimestamp").text = str(ts)
    ET.SubElement(bf_elem, "Access").text = str(ts)
    ET.SubElement(bf_elem, "LastWriteTime").text = str(ts)

    vdir_bytes = _xml_to_utf16_bytes(vdir_root)
    vdir_offset = len(buf)
    vdir_size = len(vdir_bytes)
    buf.extend(vdir_bytes)

    # 6. Build and patch BackupLogHeader into the header page
    total_file_count = len(file_records) + 1  # data files + BackupLog

    hdr_root = ET.Element("BackupLogHeader")
    ET.SubElement(hdr_root, "m_cbOffsetHeader").text = str(vdir_offset)
    ET.SubElement(hdr_root, "DataSize").text = str(vdir_size)
    ET.SubElement(hdr_root, "m_cbOffsetData").text = str(_HEADER_PAGE_SIZE)
    ET.SubElement(hdr_root, "ErrorCode").text = "false"
    ET.SubElement(hdr_root, "ApplyCompression").text = "false"
    ET.SubElement(hdr_root, "Files").text = str(total_file_count)

    hdr_bytes = _xml_to_utf16_bytes(hdr_root)
    available = _HEADER_PAGE_SIZE - _SIGNATURE_LEN
    if len(hdr_bytes) > available:
        raise ValueError(
            f"BackupLogHeader XML is {len(hdr_bytes)} bytes, "
            f"exceeds the {available}-byte page limit."
        )
    hdr_padded = hdr_bytes + b"\x00" * (available - len(hdr_bytes))

    # Patch header page
    buf[header_page_start: header_page_start + available] = hdr_padded

    return bytes(buf)
