"""
Lightweight ABF splice: replace metadata.sqlitedb without rebuilding
the ABF structure. Preserves exact XML bytes for BackupLog,
VirtualDirectory, and BackupLogHeader — only patches the specific
size field and file data bytes.

Works for both builder-generated (UTF-16-LE XML) and PBI Desktop-generated
(UTF-8 XML) ABFs.
"""
from __future__ import annotations

import re

from pbix_mcp.formats.abf_rebuild import list_abf_files


def splice_metadata_in_abf(abf_bytes: bytes, new_sqlite: bytes) -> bytes:
    """Replace metadata.sqlitedb via binary splice — no ABF rebuild.

    1. Finds metadata.sqlitedb offset and size from the file log
    2. Replaces the data bytes at that offset
    3. If size changed: shifts everything after and patches all offset/size
       references in the raw binary (VDir, BackupLog, header)

    For same-size metadata (common for adding a measure to a large file),
    this is a simple byte swap with no structural changes.

    Parameters
    ----------
    abf_bytes : bytes
        Original decompressed ABF blob.
    new_sqlite : bytes
        New metadata.sqlitedb content.

    Returns
    -------
    bytes
        ABF blob with metadata.sqlitedb replaced.
    """
    # Find metadata.sqlitedb in the file log
    file_log = list_abf_files(abf_bytes)
    meta_entry = None
    for entry in file_log:
        if "metadata.sqlitedb" in entry.get("Path", "").lower():
            meta_entry = entry
            break

    if meta_entry is None:
        raise ValueError("metadata.sqlitedb not found in ABF")

    old_offset = meta_entry["m_cbOffsetHeader"]
    old_size = meta_entry["Size"]
    storage_path = meta_entry.get("StoragePath", "")
    size_diff = len(new_sqlite) - old_size

    if size_diff == 0:
        # Same size — simple byte swap, no structural changes needed
        buf = bytearray(abf_bytes)
        buf[old_offset:old_offset + old_size] = new_sqlite
        return bytes(buf)

    # Different size — need to shift data and patch offsets
    buf = bytearray()

    # Everything before metadata
    buf.extend(abf_bytes[:old_offset])

    # New metadata
    buf.extend(new_sqlite)

    # Everything after metadata (shifted by size_diff)
    buf.extend(abf_bytes[old_offset + old_size:])

    # Now patch all offset references in the binary.
    # The VDir and BackupLog contain offset values as text in XML.
    # We need to find and update any offset > old_offset.

    # Strategy: scan for the StoragePath in VDir and update its Size.
    # Then scan for all m_cbOffsetHeader values and shift those > old_offset.

    # The ABF has 3 XML regions: BackupLogHeader (bytes 72-4096),
    # BackupLog (somewhere in the data section), and VirtualDirectory (at the end).
    # All contain size/offset values as decimal text.

    # Detect encoding: PBI Desktop uses UTF-8, builder uses UTF-16-LE
    if b"<VirtualDirectory>" in buf:
        xml_encoding = "utf-8"
    elif "<VirtualDirectory>".encode("utf-16-le") in buf:
        xml_encoding = "utf-16-le"
    else:
        raise ValueError("Cannot find VirtualDirectory in ABF")

    # Helper: find and replace a decimal value in XML text within buf
    def patch_xml_value(region_start, region_end, tag, old_val, new_val):
        """Replace <tag>old_val</tag> with <tag>new_val</tag> in buf[region_start:region_end]."""
        old_str = f"<{tag}>{old_val}</{tag}>".encode(xml_encoding)
        new_str = f"<{tag}>{new_val}</{tag}>".encode(xml_encoding)
        region = bytes(buf[region_start:region_end])
        pos = region.find(old_str)
        if pos >= 0 and len(old_str) == len(new_str):
            # Same byte length — safe in-place replace
            abs_pos = region_start + pos
            buf[abs_pos:abs_pos + len(old_str)] = new_str
            return True
        return False

    # Find VirtualDirectory region in the (shifted) buffer
    vdir_tag = "<VirtualDirectory>".encode(xml_encoding)
    vdir_end_tag = "</VirtualDirectory>".encode(xml_encoding)
    vdir_start = buf.rfind(vdir_tag)
    vdir_end = buf.find(vdir_end_tag, vdir_start) + len(vdir_end_tag) if vdir_start >= 0 else -1

    if vdir_start >= 0 and vdir_end >= 0:
        # Update the Size for metadata's StoragePath entry
        old_size_tag = f"<Size>{old_size}</Size>".encode(xml_encoding)
        new_size_tag = f"<Size>{len(new_sqlite)}</Size>".encode(xml_encoding)

        # Find the specific entry by StoragePath proximity
        sp_marker = storage_path.encode(xml_encoding)
        sp_pos = buf.find(sp_marker, vdir_start, vdir_end)
        if sp_pos >= 0:
            # Find the <Size> tag near this StoragePath
            size_pos = buf.find(old_size_tag, sp_pos - 200, sp_pos + 200)
            if size_pos >= 0:
                # Only replace if the new tag is the same length (pad/truncate if needed)
                if len(old_size_tag) == len(new_size_tag):
                    buf[size_pos:size_pos + len(old_size_tag)] = new_size_tag
                else:
                    # Different length — do string replace in the VDir XML
                    vdir_xml = buf[vdir_start:vdir_end].decode(xml_encoding)
                    # Replace size for this specific entry (near StoragePath)
                    sp_idx = vdir_xml.find(storage_path)
                    if sp_idx >= 0:
                        # Find Size near this StoragePath
                        old_s = f"<Size>{old_size}</Size>"
                        new_s = f"<Size>{len(new_sqlite)}</Size>"
                        near_start = max(0, sp_idx - 100)
                        near_end = min(len(vdir_xml), sp_idx + 200)
                        near = vdir_xml[near_start:near_end]
                        near = near.replace(old_s, new_s, 1)
                        new_vdir_xml = vdir_xml[:near_start] + near + vdir_xml[near_end:]
                        new_vdir_bytes = new_vdir_xml.encode(xml_encoding)
                        # Replace VDir in buffer (may change total size)
                        vdir_size_diff = len(new_vdir_bytes) - (vdir_end - vdir_start)
                        buf[vdir_start:vdir_end] = new_vdir_bytes
                        vdir_end += vdir_size_diff

        # Update offsets for entries that come after metadata
        # Parse all m_cbOffsetHeader values and shift those > old_offset
        vdir_region = buf[vdir_start:vdir_end].decode(xml_encoding)
        for m in re.finditer(r"<m_cbOffsetHeader>(\d+)</m_cbOffsetHeader>", vdir_region):
            offset_val = int(m.group(1))
            if offset_val > old_offset:
                new_offset_val = offset_val + size_diff
                old_tag = m.group(0).encode(xml_encoding)
                new_tag = f"<m_cbOffsetHeader>{new_offset_val}</m_cbOffsetHeader>".encode(xml_encoding)
                # Find in buffer and replace
                tag_pos = buf.find(old_tag, vdir_start)
                if tag_pos >= 0 and len(old_tag) == len(new_tag):
                    buf[tag_pos:tag_pos + len(old_tag)] = new_tag

    # Patch BackupLogHeader (bytes 72-4096) — update VDir offset and size
    # Header is always UTF-16-LE
    hdr_xml = buf[72:4096].decode("utf-16-le", errors="replace").rstrip("\x00")

    # Update VDir offset (m_cbOffsetHeader in header points to VDir)
    hdr_offset_match = re.search(r"<m_cbOffsetHeader>(\d+)</m_cbOffsetHeader>", hdr_xml)
    if hdr_offset_match:
        old_hdr_offset = int(hdr_offset_match.group(1))
        new_hdr_offset = old_hdr_offset + size_diff
        new_hdr_xml = hdr_xml.replace(
            hdr_offset_match.group(0),
            f"<m_cbOffsetHeader>{new_hdr_offset}</m_cbOffsetHeader>", 1
        )

        # Update DataSize if VDir size changed
        vdir_new_size = vdir_end - vdir_start if vdir_start >= 0 else 0
        ds_match = re.search(r"<DataSize>(\d+)</DataSize>", new_hdr_xml)
        if ds_match and vdir_new_size > 0:
            new_hdr_xml = new_hdr_xml.replace(
                ds_match.group(0),
                f"<DataSize>{vdir_new_size}</DataSize>", 1
            )

        new_hdr_bytes = new_hdr_xml.encode("utf-16-le")
        available = 4096 - 72
        if len(new_hdr_bytes) <= available:
            padded = new_hdr_bytes + b"\x00" * (available - len(new_hdr_bytes))
            buf[72:4096] = padded

    # Patch BackupLog — update Size for metadata entry
    blog_tag = "<BackupLog".encode(xml_encoding)
    blog_end_tag = "</BackupLog>".encode(xml_encoding)
    blog_start = buf.rfind(blog_tag, 0, vdir_start if vdir_start >= 0 else len(buf))
    if blog_start >= 0:
        blog_end = buf.find(blog_end_tag, blog_start)
        if blog_end >= 0:
            blog_end += len(blog_end_tag)
            blog_region = buf[blog_start:blog_end].decode(xml_encoding, errors="replace")
            old_s = f"<Size>{old_size}</Size>"
            new_s = f"<Size>{len(new_sqlite)}</Size>"
            if old_s in blog_region and len(old_s) == len(new_s):
                new_blog = blog_region.replace(old_s, new_s, 1)
                buf[blog_start:blog_end] = new_blog.encode(xml_encoding)

    return bytes(buf)
