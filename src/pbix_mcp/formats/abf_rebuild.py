"""
ABF (Analysis Backup Format) reader and rebuilder for PBIX DataModel editing.

The ABF is the decompressed binary blob inside a PBIX ``DataModel`` entry.
Its structure is:

  [0..72)          STREAM_STORAGE_SIGNATURE  (fixed 72 bytes)
  [72..4096)       BackupLogHeader XML       (UTF-16-LE, zero-padded to one 4096-byte page)
  [vdir_off .. vdir_off+vdir_size)
                   VirtualDirectory XML      (UTF-16-LE, lists BackupFiles with offsets/sizes)
  [various offsets] Actual file data          (binary blobs at offsets given in VirtualDirectory)
  [blog_off .. blog_off+blog_size)
                   BackupLog XML             (UTF-16-LE, file-group metadata -- last VDir entry)

The BackupLogHeader contains:
  - m_cbOffsetHeader : offset of the VirtualDirectory
  - DataSize         : size of the VirtualDirectory
  - m_cbOffsetData   : offset where file data begins

This module provides functions to:
  - Parse the file log from an ABF blob
  - Read individual files from the ABF
  - Rebuild the ABF after modifying embedded files (e.g. metadata.sqlitedb)
"""

import os
import sqlite3
import tempfile
import xml.etree.ElementTree as ET
from copy import deepcopy
from typing import Callable, Optional

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STREAM_STORAGE_SIGNATURE = (
    b"\xff\xfe"
    + "STREAM_STORAGE_SIGNATURE_)!@#$%^&*(".encode("utf-16le")
)
_SIGNATURE_LEN = len(STREAM_STORAGE_SIGNATURE)  # 72

_HEADER_PAGE_SIZE = 0x1000  # 4096 bytes -- header is always one page


# ---------------------------------------------------------------------------
# Low-level XML helpers
# ---------------------------------------------------------------------------

def _parse_header_xml(abf: bytes) -> ET.Element:
    """Parse the BackupLogHeader XML from bytes 72..4096."""
    raw = abf[_SIGNATURE_LEN:_HEADER_PAGE_SIZE]
    text = raw.decode("utf-16").rstrip("\x00")
    return ET.fromstring(text)


def _parse_vdir_xml(abf: bytes, offset: int, size: int) -> ET.Element:
    """Parse the VirtualDirectory XML."""
    raw = abf[offset : offset + size]
    # VDir can be UTF-8/ASCII or UTF-16 depending on the file
    # Detect by checking first bytes
    if raw[:2] == b'\xff\xfe':
        text = raw.decode("utf-16")
    elif raw[:1] == b'<':
        # ASCII/UTF-8
        text = raw.decode("utf-8", errors="replace").rstrip("\x00")
    else:
        try:
            text = raw.decode("utf-16-le").rstrip("\x00")
        except Exception:
            text = raw.decode("utf-8", errors="replace").rstrip("\x00")
    return ET.fromstring(text)


def _parse_backup_log_xml(abf: bytes, offset: int, size: int, error_code: bool) -> ET.Element:
    """Parse the BackupLog XML (last entry in the VirtualDirectory)."""
    raw = abf[offset : offset + size]
    if error_code:
        raw = raw[:-4]
    # Detect encoding
    if raw[:2] == b'\xff\xfe':
        text = raw.decode("utf-16")
    elif raw[:1] == b'<':
        text = raw.decode("utf-8", errors="replace").rstrip("\x00")
    else:
        try:
            text = raw.decode("utf-16-le").rstrip("\x00")
        except Exception:
            text = raw.decode("utf-8", errors="replace").rstrip("\x00")
    return ET.fromstring(text)


# ---------------------------------------------------------------------------
# File-log parsing (public)
# ---------------------------------------------------------------------------

def list_abf_files(abf_bytes: bytes) -> list[dict]:
    """
    Parse the ABF and return the file log -- a list of dicts describing every
    embedded file.

    Each dict has keys:
      - Path              : full internal path (with persist root stripped)
      - FileName          : last component of the path
      - StoragePath       : internal storage key used by the VirtualDirectory
      - Size              : byte size from the VirtualDirectory
      - SizeFromLog       : byte size recorded in the BackupLog
      - m_cbOffsetHeader  : absolute byte offset of the file data inside the ABF

    Each dict describes one embedded file in the ABF archive.
    """
    header_root = _parse_header_xml(abf_bytes)

    vdir_offset = int(header_root.findtext("m_cbOffsetHeader"))
    vdir_size = int(header_root.findtext("DataSize"))
    error_code = header_root.findtext("ErrorCode") == "true"

    vdir_root = _parse_vdir_xml(abf_bytes, vdir_offset, vdir_size)

    # Build lookup: StoragePath -> VDir entry
    vdir_files = {}
    backup_log_entry = None
    for bf in vdir_root.findall("BackupFile"):
        path = bf.findtext("Path")
        entry = {
            "Path": path,
            "Size": int(bf.findtext("Size")),
            "m_cbOffsetHeader": int(bf.findtext("m_cbOffsetHeader")),
            "Delete": bf.findtext("Delete") == "true",
            "CreatedTimestamp": int(bf.findtext("CreatedTimestamp")),
            "Access": int(bf.findtext("Access")),
            "LastWriteTime": int(bf.findtext("LastWriteTime")),
        }
        vdir_files[path] = entry
        backup_log_entry = entry  # last one is the backup log

    # Parse the BackupLog to get FileGroups
    blog_offset = backup_log_entry["m_cbOffsetHeader"]
    blog_size = backup_log_entry["Size"]
    blog_root = _parse_backup_log_xml(abf_bytes, blog_offset, blog_size, error_code)

    # Determine persist root (from FileGroup index 1, which is the database group)
    file_groups = blog_root.findall("FileGroups/FileGroup")
    persist_root = ""
    if len(file_groups) > 1:
        persist_root = file_groups[1].findtext("PersistLocationPath", "") + "\\"

    # Match BackupLog files against VirtualDirectory entries
    matched = []
    for fg in file_groups:
        for bf in fg.findall("FileList/BackupFile"):
            storage_path = bf.findtext("StoragePath")
            if storage_path in vdir_files:
                vf = vdir_files[storage_path]
                full_path = bf.findtext("Path")
                if persist_root and full_path.startswith(persist_root):
                    short_path = full_path[len(persist_root):]
                else:
                    short_path = full_path
                matched.append({
                    "Path": short_path,
                    "FileName": short_path.split("\\")[-1],
                    "StoragePath": storage_path,
                    "Size": vf["Size"],
                    "SizeFromLog": int(bf.findtext("Size")),
                    "m_cbOffsetHeader": vf["m_cbOffsetHeader"],
                })

    return matched


def find_abf_file(file_log: list[dict], partial_name: str) -> Optional[dict]:
    """
    Find a file entry by partial path match (case-insensitive).

    Returns the first entry whose ``Path`` or ``FileName`` contains
    *partial_name*, or ``None`` if nothing matches.
    """
    needle = partial_name.lower()
    for entry in file_log:
        if needle in entry["Path"].lower() or needle in entry["FileName"].lower():
            return entry
    return None


def read_abf_file(abf_bytes: bytes, file_entry: dict) -> bytes:
    """
    Read the raw bytes of a file from the ABF using its file-log entry.

    Parameters
    ----------
    abf_bytes : bytes
        The full decompressed ABF blob.
    file_entry : dict
        An entry from :func:`list_abf_files` (must have ``m_cbOffsetHeader``
        and ``Size``).

    Returns
    -------
    bytes
        The raw file content.
    """
    offset = file_entry["m_cbOffsetHeader"]
    size = file_entry["Size"]
    return bytes(abf_bytes[offset : offset + size])


def read_metadata_sqlite(abf_bytes: bytes) -> bytes:
    """
    Convenience function: find and read ``metadata.sqlitedb`` from the ABF.

    Raises ``ValueError`` if the file is not found.
    """
    file_log = list_abf_files(abf_bytes)
    entry = find_abf_file(file_log, "metadata.sqlitedb")
    if entry is None:
        raise ValueError("metadata.sqlitedb not found in ABF file log.")
    return read_abf_file(abf_bytes, entry)


# ---------------------------------------------------------------------------
# Internal: full ABF structure parsing for rebuilding
# ---------------------------------------------------------------------------

class _VDirEntry:
    """Parsed VirtualDirectory BackupFile entry (mutable for rebuilding)."""
    __slots__ = ("path", "size", "m_cbOffsetHeader", "delete",
                 "created_timestamp", "access", "last_write_time")

    def __init__(self, elem: ET.Element):
        self.path = elem.findtext("Path")
        self.size = int(elem.findtext("Size"))
        self.m_cbOffsetHeader = int(elem.findtext("m_cbOffsetHeader"))
        self.delete = elem.findtext("Delete") == "true"
        self.created_timestamp = int(elem.findtext("CreatedTimestamp"))
        self.access = int(elem.findtext("Access"))
        self.last_write_time = int(elem.findtext("LastWriteTime"))


class _ABFStructure:
    """
    Complete parsed ABF structure, holding all the pieces needed for
    a faithful rebuild.
    """

    def __init__(self, abf_bytes: bytes):
        self.original = abf_bytes

        # 1. Parse header
        self.header_root = _parse_header_xml(abf_bytes)
        self.error_code = self.header_root.findtext("ErrorCode") == "true"
        self.apply_compression = self.header_root.findtext("ApplyCompression") == "true"

        vdir_offset = int(self.header_root.findtext("m_cbOffsetHeader"))
        vdir_size = int(self.header_root.findtext("DataSize"))

        # 2. Parse VirtualDirectory
        self.vdir_root = _parse_vdir_xml(abf_bytes, vdir_offset, vdir_size)
        self.vdir_entries: list[_VDirEntry] = []
        for bf in self.vdir_root.findall("BackupFile"):
            self.vdir_entries.append(_VDirEntry(bf))

        # The last VDir entry is the BackupLog
        self.backup_log_entry = self.vdir_entries[-1]
        # All other VDir entries store actual data files
        self.data_entries = self.vdir_entries[:-1]

        # 3. Parse BackupLog
        blog_off = self.backup_log_entry.m_cbOffsetHeader
        blog_size = self.backup_log_entry.size
        self.backup_log_root = _parse_backup_log_xml(
            abf_bytes, blog_off, blog_size, self.error_code
        )

        # 4. Build the matched file log (same logic as list_abf_files)
        file_groups = self.backup_log_root.findall("FileGroups/FileGroup")
        self.persist_root = ""
        if len(file_groups) > 1:
            self.persist_root = file_groups[1].findtext("PersistLocationPath", "") + "\\"

        # Map StoragePath -> VDir entry for quick lookup
        self._vdir_by_path: dict[str, _VDirEntry] = {
            e.path: e for e in self.vdir_entries
        }

        # Build the user-facing file log
        self.file_log: list[dict] = []
        for fg in file_groups:
            for bf in fg.findall("FileList/BackupFile"):
                sp = bf.findtext("StoragePath")
                if sp in self._vdir_by_path:
                    ve = self._vdir_by_path[sp]
                    full_path = bf.findtext("Path")
                    if self.persist_root and full_path.startswith(self.persist_root):
                        short = full_path[len(self.persist_root):]
                    else:
                        short = full_path
                    self.file_log.append({
                        "Path": short,
                        "FileName": short.split("\\")[-1],
                        "StoragePath": sp,
                        "Size": ve.size,
                        "SizeFromLog": int(bf.findtext("Size")),
                        "m_cbOffsetHeader": ve.m_cbOffsetHeader,
                    })

    def read_file_data(self, storage_path: str) -> bytes:
        """Read file bytes by StoragePath."""
        ve = self._vdir_by_path[storage_path]
        return bytes(self.original[ve.m_cbOffsetHeader : ve.m_cbOffsetHeader + ve.size])

    def read_file_data_for_entry(self, entry: dict) -> bytes:
        """Read file bytes using a file_log entry."""
        off = entry["m_cbOffsetHeader"]
        sz = entry["Size"]
        return bytes(self.original[off : off + sz])


# ---------------------------------------------------------------------------
# Internal: ABF serialiser
# ---------------------------------------------------------------------------

def _xml_to_utf16_bytes(root: ET.Element) -> bytes:
    """Serialise an ElementTree root to UTF-16-LE bytes (no BOM, no XML decl)."""
    # ET.tostring with encoding="unicode" gives us a str
    xml_str = ET.tostring(root, encoding="unicode", xml_declaration=False)
    return xml_str.encode("utf-16")


def _pad_to(data: bytes, boundary: int) -> bytes:
    """Zero-pad *data* so its length is a multiple of *boundary*."""
    remainder = len(data) % boundary
    if remainder == 0:
        return data
    return data + b"\x00" * (boundary - remainder)


def _rebuild_abf(
    abf_struct: _ABFStructure,
    replacements: dict[str, bytes],
) -> bytes:
    """
    Rebuild the full ABF blob, substituting any files listed in *replacements*.

    Parameters
    ----------
    abf_struct : _ABFStructure
        Parsed structure of the original ABF.
    replacements : dict[str, bytes]
        Mapping of StoragePath (as found in VirtualDirectory) to new content bytes.
        Partial-name matching is done externally; keys here must be exact StoragePaths.

    Returns
    -------
    bytes
        Complete rebuilt ABF blob.
    """
    # ------------------------------------------------------------------
    # Strategy:
    #   1. Keep the signature (72 bytes) and header page (4096 bytes) as
    #      a skeleton -- we will patch the header XML at the end.
    #   2. Lay out all *data* files (VDir entries except the last / backup log)
    #      sequentially after the header page, recording new offsets/sizes.
    #   3. Write the (possibly updated) BackupLog XML.
    #   4. Write the (updated) VirtualDirectory XML.
    #   5. Patch the BackupLogHeader XML with new offsets/sizes and write
    #      the final header page.
    # ------------------------------------------------------------------

    buf = bytearray()

    # ---- 1. Signature (72 bytes) ----
    buf.extend(STREAM_STORAGE_SIGNATURE)

    # ---- placeholder for header page (will be overwritten) ----
    header_page_start = len(buf)  # == 72
    buf.extend(b"\x00" * (_HEADER_PAGE_SIZE - _SIGNATURE_LEN))
    # buf is now 4096 bytes

    # ---- 2. Data files ----
    # We lay them out in the same order as the original VDir (excluding backup log).
    new_offsets: dict[str, int] = {}   # StoragePath -> new offset
    new_sizes: dict[str, int] = {}     # StoragePath -> new size

    for ve in abf_struct.data_entries:
        if ve.path in replacements:
            data = replacements[ve.path]
        else:
            data = abf_struct.read_file_data(ve.path)

        new_offsets[ve.path] = len(buf)
        new_sizes[ve.path] = len(data)
        buf.extend(data)

    # ---- 3. BackupLog ----
    # Update sizes in the BackupLog XML for replaced files
    blog_root = deepcopy(abf_struct.backup_log_root)
    for fg in blog_root.findall("FileGroups/FileGroup"):
        for bf in fg.findall("FileList/BackupFile"):
            sp = bf.findtext("StoragePath")
            if sp in new_sizes:
                size_elem = bf.find("Size")
                if size_elem is not None:
                    size_elem.text = str(new_sizes[sp])

    blog_bytes = _xml_to_utf16_bytes(blog_root)
    if abf_struct.error_code:
        # Append 4 zero bytes (error-code trailer that gets trimmed on read)
        blog_bytes = blog_bytes + b"\x00\x00\x00\x00"

    blog_offset = len(buf)
    blog_size = len(blog_bytes)
    buf.extend(blog_bytes)

    # ---- 4. VirtualDirectory ----
    # Rebuild VDir XML with updated offsets and sizes for all entries
    vdir_root_new = ET.Element("VirtualDirectory")
    for ve in abf_struct.data_entries:
        bf_elem = ET.SubElement(vdir_root_new, "BackupFile")
        ET.SubElement(bf_elem, "Path").text = ve.path
        ET.SubElement(bf_elem, "Size").text = str(new_sizes.get(ve.path, ve.size))
        ET.SubElement(bf_elem, "m_cbOffsetHeader").text = str(new_offsets.get(ve.path, ve.m_cbOffsetHeader))
        ET.SubElement(bf_elem, "Delete").text = "true" if ve.delete else "false"
        ET.SubElement(bf_elem, "CreatedTimestamp").text = str(ve.created_timestamp)
        ET.SubElement(bf_elem, "Access").text = str(ve.access)
        ET.SubElement(bf_elem, "LastWriteTime").text = str(ve.last_write_time)

    # Append the BackupLog as the last VDir entry
    blog_ve = abf_struct.backup_log_entry
    bf_elem = ET.SubElement(vdir_root_new, "BackupFile")
    ET.SubElement(bf_elem, "Path").text = blog_ve.path
    ET.SubElement(bf_elem, "Size").text = str(blog_size)
    ET.SubElement(bf_elem, "m_cbOffsetHeader").text = str(blog_offset)
    ET.SubElement(bf_elem, "Delete").text = "true" if blog_ve.delete else "false"
    ET.SubElement(bf_elem, "CreatedTimestamp").text = str(blog_ve.created_timestamp)
    ET.SubElement(bf_elem, "Access").text = str(blog_ve.access)
    ET.SubElement(bf_elem, "LastWriteTime").text = str(blog_ve.last_write_time)

    vdir_bytes = _xml_to_utf16_bytes(vdir_root_new)
    vdir_offset = len(buf)
    vdir_size = len(vdir_bytes)
    buf.extend(vdir_bytes)

    # ---- 5. Patch the BackupLogHeader ----
    hdr = deepcopy(abf_struct.header_root)
    hdr.find("m_cbOffsetHeader").text = str(vdir_offset)
    hdr.find("DataSize").text = str(vdir_size)
    # Files count = number of VDir entries (data files + backup log)
    hdr.find("Files").text = str(len(abf_struct.data_entries) + 1)

    hdr_bytes = _xml_to_utf16_bytes(hdr)
    # Must fit in one page (4096 bytes) starting after the signature
    available = _HEADER_PAGE_SIZE - _SIGNATURE_LEN
    if len(hdr_bytes) > available:
        raise ValueError(
            f"BackupLogHeader XML is {len(hdr_bytes)} bytes, "
            f"exceeds the {available}-byte page limit."
        )
    hdr_padded = hdr_bytes + b"\x00" * (available - len(hdr_bytes))

    # Overwrite the header page in the buffer
    buf[header_page_start : header_page_start + available] = hdr_padded

    return bytes(buf)


def _resolve_replacements(
    abf_struct: _ABFStructure,
    partial_replacements: dict[str, bytes],
) -> dict[str, bytes]:
    """
    Convert a dict keyed by partial names to one keyed by exact StoragePaths.
    """
    exact: dict[str, bytes] = {}
    for partial, data in partial_replacements.items():
        # Try exact match first
        if partial in abf_struct._vdir_by_path:
            exact[partial] = data
            continue
        # Partial / case-insensitive match
        entry = find_abf_file(abf_struct.file_log, partial)
        if entry is None:
            raise ValueError(f"No ABF file matching partial name: {partial!r}")
        exact[entry["StoragePath"]] = data
    return exact


# ---------------------------------------------------------------------------
# Public rebuild functions
# ---------------------------------------------------------------------------

def rebuild_abf_with_replacement(
    abf_bytes: bytes,
    replacements: dict[str, bytes],
) -> bytes:
    """
    Rebuild an ABF blob, replacing specified embedded files.

    Parameters
    ----------
    abf_bytes : bytes
        Original decompressed ABF blob.
    replacements : dict[str, bytes]
        Mapping of partial file name/path to new content bytes.  For example::

            {"metadata.sqlitedb": new_sqlite_bytes}

    Returns
    -------
    bytes
        Complete rebuilt ABF blob with updated offsets and sizes.
    """
    abf_struct = _ABFStructure(abf_bytes)
    exact = _resolve_replacements(abf_struct, replacements)
    return _rebuild_abf(abf_struct, exact)


def rebuild_abf_with_modified_sqlite(
    abf_bytes: bytes,
    modifier_fn: Callable[[sqlite3.Connection], None],
) -> bytes:
    """
    Extract ``metadata.sqlitedb``, let *modifier_fn* modify it via a
    ``sqlite3.Connection``, then rebuild the ABF.

    Parameters
    ----------
    abf_bytes : bytes
        Original decompressed ABF blob.
    modifier_fn : callable
        A function that receives a ``sqlite3.Connection`` (to a temporary
        on-disk copy of the metadata database) and applies whatever
        modifications are needed.  It should **not** close the connection.

    Returns
    -------
    bytes
        Rebuilt ABF blob containing the modified SQLite database.

    Example
    -------
    >>> def add_measure(conn):
    ...     conn.execute(
    ...         "INSERT INTO measures (name, expression) VALUES (?, ?)",
    ...         ("Total Sales", "SUM(Sales[Amount])")
    ...     )
    >>> new_abf = rebuild_abf_with_modified_sqlite(abf_bytes, add_measure)
    """
    sqlite_bytes = read_metadata_sqlite(abf_bytes)

    # Write to a temp file so stdlib sqlite3 can open it
    fd, tmp_path = tempfile.mkstemp(suffix=".sqlitedb")
    try:
        os.write(fd, sqlite_bytes)
        os.close(fd)
        fd = None

        conn = sqlite3.connect(tmp_path)
        try:
            modifier_fn(conn)
            conn.commit()
        finally:
            conn.close()

        with open(tmp_path, "rb") as f:
            new_sqlite_bytes = f.read()
    finally:
        if fd is not None:
            os.close(fd)
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    # For metadata-only changes, rebuild_abf_with_replacement works correctly
    # because only metadata.sqlitedb changes (VertiPaq binary data stays the same).
    return rebuild_abf_with_replacement(
        abf_bytes, {"metadata.sqlitedb": new_sqlite_bytes}
    )


# ---------------------------------------------------------------------------
# ABFArchive class
# ---------------------------------------------------------------------------

class ABFArchive:
    """
    High-level wrapper around an ABF blob for inspection and modification.

    Parameters
    ----------
    abf_bytes : bytes
        Decompressed ABF blob (output of :func:`datamodel_roundtrip.decompress_datamodel`).

    Attributes
    ----------
    file_log : list[dict]
        Parsed file log from :func:`list_abf_files`.
    """

    def __init__(self, abf_bytes: bytes):
        self._abf = abf_bytes
        self._struct = _ABFStructure(abf_bytes)
        self.file_log = self._struct.file_log

    @property
    def raw(self) -> bytes:
        """The underlying ABF bytes."""
        return self._abf

    def list_files(self) -> list[dict]:
        """Return the parsed file log."""
        return self.file_log

    def find_file(self, partial_name: str) -> Optional[dict]:
        """Find a file by partial name match."""
        return find_abf_file(self.file_log, partial_name)

    def read_file(self, partial_name: str) -> bytes:
        """
        Read a file's content by partial name.

        Raises ``ValueError`` if no matching file is found.
        """
        entry = self.find_file(partial_name)
        if entry is None:
            raise ValueError(f"No file matching: {partial_name!r}")
        return read_abf_file(self._abf, entry)

    def read_metadata_sqlite(self) -> bytes:
        """Read the metadata.sqlitedb content."""
        return read_metadata_sqlite(self._abf)

    def replace_files(self, replacements: dict[str, bytes]) -> "ABFArchive":
        """
        Return a **new** ABFArchive with specified files replaced.

        Parameters
        ----------
        replacements : dict[str, bytes]
            Partial-name -> new content.

        Returns
        -------
        ABFArchive
            A new archive instance with the replacements applied.
        """
        new_abf = rebuild_abf_with_replacement(self._abf, replacements)
        return ABFArchive(new_abf)

    def modify_sqlite(
        self,
        modifier_fn: Callable[[sqlite3.Connection], None],
    ) -> "ABFArchive":
        """
        Return a **new** ABFArchive with a modified metadata.sqlitedb.

        Parameters
        ----------
        modifier_fn : callable
            Receives a ``sqlite3.Connection`` to modify.

        Returns
        -------
        ABFArchive
            A new archive instance.
        """
        new_abf = rebuild_abf_with_modified_sqlite(self._abf, modifier_fn)
        return ABFArchive(new_abf)


# ---------------------------------------------------------------------------
# Build ABF from scratch (no existing ABF required)
# ---------------------------------------------------------------------------

def build_abf_from_scratch(
    files: dict[str, bytes],
    database_id: str = "00000000-0000-0000-0000-000000000001",
) -> bytes:
    """
    Build a complete ABF archive from scratch -- no existing ABF needed.

    Produces the flat ABF layout that Analysis Services / Power BI Desktop
    expects::

      Page 0 (0..4095):  STREAM_STORAGE_SIGNATURE + BackupLogHeader XML
      Data   (4096..):   Files laid out sequentially
      VirtualDirectory:  File index with flat paths and direct byte offsets

    VirtualDirectory paths are flat names (``ADDITIONAL_LOG``,
    ``PARTITIONS``, ``metadata.sqlitedb``, ``LOG``, ...) -- no
    ``Sandboxes\\`` prefix or ``StoragePath_X`` indirection.

    Parameters
    ----------
    files : dict[str, bytes]
        Internal file paths mapped to their content.
        Must include at least ``metadata.sqlitedb``.
        Example: ``{"metadata.sqlitedb": sqlite_bytes}``
    database_id : str
        A GUID for the database (used in the BackupLog).

    Returns
    -------
    bytes
        A valid ABF blob that can be XPress9-compressed into a DataModel.
    """
    if "metadata.sqlitedb" not in files:
        raise ValueError("files must include 'metadata.sqlitedb'")

    timestamp = 134002835794032078  # Windows FILETIME timestamp

    buf = bytearray()

    # ---- 1. Signature + Header placeholder (first 4096 bytes) ----
    buf.extend(STREAM_STORAGE_SIGNATURE)
    header_page_start = len(buf)
    # Reserve the rest of the header page (will be patched at the end)
    buf.extend(b"\x00" * (_HEADER_PAGE_SIZE - _SIGNATURE_LEN))
    assert len(buf) == _HEADER_PAGE_SIZE

    # ---- 2. Data files sequentially starting at offset 4096 ----
    # Build the ordered list of (flat_name, content) entries.
    # Required system files come first, then user-supplied extras,
    # then the LOG entry is always written last.

    _ADDITIONAL_LOG_CONTENT = (
        b'<Property><ProductName>Default</ProductName></Property>'
    )
    _PARTITIONS_CONTENT = b'<Partitions />'

    # CryptKey.bin — 144-byte crypto key extracted from a valid PBIX.
    # Required by AS when SvrEncryptPwdFlag=true (server default).
    _CRYPTKEY_CONTENT = bytes.fromhex(
        "98bc215d2d8de64ea8e5d038aac94441"
        "040000003000000050000000100000000100000007000000ffffffff00000000"
        "010200000366000000a40000805bf7b37f703bf7ef3b7fb6299d1adab316e67c"
        "80ab58310051a5c7d76097fba0aba4c09cc73a2b165781ea68aa644bcc2bba09"
        "012e44fdfde63ed5221b02000000000098bc215d2d8de64ea8e5d038aac94441"
    )

    # Collect flat-name -> content, preserving required ordering.
    ordered_files: list[tuple[str, bytes]] = []

    # (a) ADDITIONAL_LOG -- always first
    ordered_files.append(("ADDITIONAL_LOG", _ADDITIONAL_LOG_CONTENT))

    # (b) PARTITIONS -- always second
    ordered_files.append(("PARTITIONS", _PARTITIONS_CONTENT))

    # (c) CryptKey.bin -- required by AS for password encryption validation
    ordered_files.append(("2.CryptKey.bin", _CRYPTKEY_CONTENT))

    # (d) metadata.sqlitedb
    ordered_files.append(("metadata.sqlitedb", files["metadata.sqlitedb"]))

    # (d) Any remaining user-supplied files (VertiPaq data, etc.)
    _SKIP = {"metadata.sqlitedb", "ADDITIONAL_LOG", "PARTITIONS", "LOG", "2.CryptKey.bin"}
    for fname, content in files.items():
        if fname not in _SKIP:
            ordered_files.append((fname, content))

    # Write each file and record offset/size for the VirtualDirectory.
    file_records: list[tuple[str, int, int]] = []  # (flat_name, offset, size)
    for flat_name, content in ordered_files:
        file_records.append((flat_name, len(buf), len(content)))
        buf.extend(content)

    # ---- 3. LOG (BackupLog data) -- always last data entry ----
    # Must match Analysis Services backup schema exactly:
    # BackupLog > { BackupRestoreSyncVersion, ServerRoot, flags, ObjectName,
    #               ObjectId, Write, OlapInfo, IsTabular, FileGroups }
    # FileGroup > { Class, ID, Name, ObjectVersion, PersistLocation,
    #               PersistLocationPath, StorageLocationPath, ObjectID, FileList }
    db_path = f"Sandboxes\\{database_id}"
    obj_id = database_id.upper()

    blog_root = ET.Element("BackupLog")
    ET.SubElement(blog_root, "BackupRestoreSyncVersion").text = "11.53"
    ET.SubElement(blog_root, "ServerRoot").text = db_path
    ET.SubElement(blog_root, "SvrEncryptPwdFlag").text = "true"
    ET.SubElement(blog_root, "ServerEnableBinaryXML").text = "false"
    ET.SubElement(blog_root, "ServerEnableCompression").text = "false"
    ET.SubElement(blog_root, "CompressionFlag").text = "false"
    ET.SubElement(blog_root, "EncryptionFlag").text = "false"
    ET.SubElement(blog_root, "ObjectName").text = database_id
    ET.SubElement(blog_root, "ObjectId").text = database_id
    ET.SubElement(blog_root, "Write").text = "ReadWrite"
    ET.SubElement(blog_root, "OlapInfo").text = "false"
    ET.SubElement(blog_root, "IsTabular").text = "true"
    ET.SubElement(blog_root, "Collations")
    ET.SubElement(blog_root, "Languages")
    file_groups = ET.SubElement(blog_root, "FileGroups")

    # FileGroup: Class 100002 = database (contains all our files)
    fg1 = ET.SubElement(file_groups, "FileGroup")
    ET.SubElement(fg1, "Class").text = "100002"
    ET.SubElement(fg1, "ID").text = database_id
    ET.SubElement(fg1, "Name").text = database_id
    ET.SubElement(fg1, "ObjectVersion").text = "2"
    ET.SubElement(fg1, "PersistLocation").text = "2"
    ET.SubElement(fg1, "PersistLocationPath").text = f"{db_path}.2.db"
    ET.SubElement(fg1, "StorageLocationPath")
    ET.SubElement(fg1, "ObjectID").text = obj_id
    file_list = ET.SubElement(fg1, "FileList")
    for flat_name, _offset, size in file_records:
        bf = ET.SubElement(file_list, "BackupFile")
        ET.SubElement(bf, "Path").text = f"{db_path}.2.db\\{flat_name}"
        ET.SubElement(bf, "StoragePath").text = flat_name
        ET.SubElement(bf, "LastWriteTime").text = str(timestamp)
        ET.SubElement(bf, "Size").text = str(size)

    blog_bytes = _xml_to_utf16_bytes(blog_root)
    log_offset = len(buf)
    log_size = len(blog_bytes)
    buf.extend(blog_bytes)

    # Add LOG to file_records so VirtualDirectory includes it as last entry
    file_records.append(("LOG", log_offset, log_size))

    # ---- 4. VirtualDirectory (file index, written after all data) ----
    vdir_root = ET.Element("VirtualDirectory")

    for flat_name, offset, size in file_records:
        bf_elem = ET.SubElement(vdir_root, "BackupFile")
        ET.SubElement(bf_elem, "Path").text = flat_name
        ET.SubElement(bf_elem, "Size").text = str(size)
        ET.SubElement(bf_elem, "m_cbOffsetHeader").text = str(offset)
        ET.SubElement(bf_elem, "Delete").text = "false"
        ET.SubElement(bf_elem, "CreatedTimestamp").text = str(timestamp)
        ET.SubElement(bf_elem, "Access").text = str(timestamp)
        ET.SubElement(bf_elem, "LastWriteTime").text = str(timestamp)

    vdir_bytes = _xml_to_utf16_bytes(vdir_root)
    vdir_offset = len(buf)
    vdir_size = len(vdir_bytes)
    buf.extend(vdir_bytes)

    # ---- 5. Patch the header page with correct offsets ----
    total_files = len(file_records)  # all data files + LOG

    hdr_root = ET.Element("BackupLog")
    ET.SubElement(hdr_root, "BackupRestoreSyncVersion").text = "140"
    ET.SubElement(hdr_root, "Fault").text = "false"
    ET.SubElement(hdr_root, "faultcode").text = "0"
    ET.SubElement(hdr_root, "ErrorCode").text = "false"
    ET.SubElement(hdr_root, "EncryptionFlag").text = "false"
    ET.SubElement(hdr_root, "EncryptionKey").text = "0"
    ET.SubElement(hdr_root, "ApplyCompression").text = "false"
    ET.SubElement(hdr_root, "m_cbOffsetHeader").text = str(vdir_offset)
    ET.SubElement(hdr_root, "DataSize").text = str(vdir_size)
    ET.SubElement(hdr_root, "Files").text = str(total_files)
    ET.SubElement(hdr_root, "ObjectID").text = str(database_id).upper()
    ET.SubElement(hdr_root, "m_cbOffsetData").text = str(_HEADER_PAGE_SIZE)

    hdr_bytes = _xml_to_utf16_bytes(hdr_root)
    available = _HEADER_PAGE_SIZE - _SIGNATURE_LEN
    if len(hdr_bytes) > available:
        raise ValueError(f"Header XML too large: {len(hdr_bytes)} > {available}")

    hdr_padded = hdr_bytes + b"\x00" * (available - len(hdr_bytes))
    buf[header_page_start: header_page_start + available] = hdr_padded

    return bytes(buf)
