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

# The BackupLogHeader occupies the first page (signature + header XML). The
# BackupLog *file* lives past it; both use <BackupLog> as their root element.
_HEADER_PAGE = 0x1000


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

    def _replace_size_near(region: str, anchor: str, old_val: int, new_val: int) -> str:
        """Replace the <Size> belonging to `anchor`'s entry, anywhere in `region`.

        Targets the <Size> nearest the anchor (a StoragePath / Path) so the right
        entry is hit even when several files share a byte size."""
        old_s = f"<Size>{old_val}</Size>"
        new_s = f"<Size>{new_val}</Size>"
        if old_s == new_s:
            return region
        idx = region.find(anchor) if anchor else -1
        if idx >= 0:
            lo, hi = max(0, idx - 600), min(len(region), idx + 600)
            near = region[lo:hi]
            if old_s in near:
                return region[:lo] + near.replace(old_s, new_s, 1) + region[hi:]
        return region.replace(old_s, new_s, 1)

    # ---- BackupLog: the metadata entry's <Size> ------------------------------
    # Done BEFORE the VirtualDirectory/header so their offsets can absorb any
    # length change here. Analysis Services sizes the metadata file from THIS
    # value: leaving it stale ships a SQLite image AS truncates, and Desktop
    # reports "The database disk image is malformed. SQLite Error Code=11".
    #
    # The BackupLog has its OWN encoding: Desktop-authored files store the
    # VirtualDirectory as UTF-8 but the BackupLog as UTF-16-LE. Reusing the
    # VDir's encoding here meant the size was never found, so it stayed stale on
    # exactly the files users bring. Skip the BackupLogHeader (bytes 72..4096),
    # whose root element is also <BackupLog>.
    blog_diff = 0
    _vd_probe = buf.rfind("<VirtualDirectory>".encode(xml_encoding))
    _vd_probe = _vd_probe if _vd_probe >= 0 else len(buf)
    blog_start, blog_end, blog_enc = -1, -1, ""
    for enc in ("utf-16-le", "utf-8"):
        s = buf.rfind("<BackupLog".encode(enc), _HEADER_PAGE, _vd_probe)
        if s < 0:
            continue
        e = buf.find("</BackupLog>".encode(enc), s)
        if e < 0:
            continue
        blog_start, blog_end, blog_enc = s, e + len("</BackupLog>".encode(enc)), enc
        break
    if blog_start >= 0 and blog_enc:
        region = buf[blog_start:blog_end].decode(blog_enc, errors="replace")
        new_region = _replace_size_near(region, storage_path, old_size, len(new_sqlite))
        if new_region != region:
            nb = new_region.encode(blog_enc)
            blog_diff = len(nb) - (blog_end - blog_start)
            buf[blog_start:blog_end] = nb

    # Find VirtualDirectory region in the (shifted) buffer
    vdir_tag = "<VirtualDirectory>".encode(xml_encoding)
    vdir_end_tag = "</VirtualDirectory>".encode(xml_encoding)
    vdir_start = buf.rfind(vdir_tag)
    vdir_end = buf.find(vdir_end_tag, vdir_start) + len(vdir_end_tag) if vdir_start >= 0 else -1

    if vdir_start >= 0 and vdir_end >= 0:
        # Rewrite the VirtualDirectory ONE <BackupFile> ENTRY AT A TIME.
        #
        # Each entry's offset and size are recomputed from the entry's own
        # values, so nothing depends on scanning a mutating buffer and a value
        # that gains or loses digits is handled. Two silent-corruption bugs came
        # from the previous per-value approaches:
        #   * a `buf.find(old)+replace` loop re-matched a value it had just
        #     written when two offsets differed by exactly size_diff (near-certain
        #     for page-aligned growth), double-shifting one entry and leaving
        #     another stale -> overlapping segments -> "DBCC failed while checking
        #     the data segments";
        #   * a `len(old)==len(new)` guard skipped any size whose digit count
        #     changed, so the BackupLog kept a stale metadata size -> AS truncated
        #     the SQLite image -> "The database disk image is malformed.
        #     SQLite Error Code=11".
        # `blog_diff` is the byte-length change of the BackupLog patched above;
        # the log's own entry is identified by its offset, not by a name.
        vdir_xml = buf[vdir_start:vdir_end].decode(xml_encoding)
        # The BackupLog is the LAST VirtualDirectory entry — that is how the
        # reader identifies it (abf_rebuild.list_abf_files). Matching it by byte
        # offset instead would miss: a UTF-16 BackupLog starts with a BOM, so its
        # recorded offset is 2 bytes below where "<BackupLog" is found.
        _n_entries = len(re.findall(r"<BackupFile>", vdir_xml))
        _seen = [0]

        def _fix_entry(m: "re.Match[str]") -> str:
            entry = m.group(0)
            _seen[0] += 1
            is_last = _seen[0] == _n_entries
            om = re.search(r"<m_cbOffsetHeader>(\d+)</m_cbOffsetHeader>", entry)
            sm = re.search(r"<Size>(\d+)</Size>", entry)
            if not om or not sm:
                return entry
            off, size = int(om.group(1)), int(sm.group(1))
            new_off = off + size_diff if off > old_offset else off
            if off == old_offset and size == old_size:
                new_size = len(new_sqlite)          # the metadata file itself
            elif blog_diff and is_last:
                new_size = size + blog_diff         # the BackupLog we just resized
            else:
                new_size = size
            entry = entry.replace(om.group(0),
                                  f"<m_cbOffsetHeader>{new_off}</m_cbOffsetHeader>", 1)
            entry = entry.replace(sm.group(0), f"<Size>{new_size}</Size>", 1)
            return entry

        new_vdir_xml = re.sub(r"<BackupFile>.*?</BackupFile>", _fix_entry,
                              vdir_xml, flags=re.DOTALL)
        if new_vdir_xml != vdir_xml:
            nb = new_vdir_xml.encode(xml_encoding)
            buf[vdir_start:vdir_end] = nb
            vdir_end = vdir_start + len(nb)

    # Patch BackupLogHeader (bytes 72-4096) — update VDir offset and size
    # Header is always UTF-16-LE
    hdr_xml = buf[72:4096].decode("utf-16-le", errors="replace").rstrip("\x00")

    # Update VDir offset (m_cbOffsetHeader in header points to VDir)
    hdr_offset_match = re.search(r"<m_cbOffsetHeader>(\d+)</m_cbOffsetHeader>", hdr_xml)
    if hdr_offset_match:
        old_hdr_offset = int(hdr_offset_match.group(1))
        # The VirtualDirectory sits after both the metadata and the BackupLog,
        # so it moves by BOTH deltas.
        new_hdr_offset = old_hdr_offset + size_diff + blog_diff
        new_hdr_xml = hdr_xml.replace(
            hdr_offset_match.group(0),
            f"<m_cbOffsetHeader>{new_hdr_offset}</m_cbOffsetHeader>", 1
        )

        # Update DataSize (= the VirtualDirectory byte length the reader slices
        # off m_cbOffsetHeader). `vdir_start` was located at "<VirtualDirectory>",
        # which for a UTF-16-LE VDir sits AFTER the 2-byte BOM — but the header's
        # m_cbOffsetHeader points AT the BOM. Sizing from the header offset makes
        # DataSize BOM-inclusive so `abf[offset:offset+DataSize]` lands exactly on
        # the closing "</VirtualDirectory>". Using `vdir_end - vdir_start` here
        # dropped the trailing ">" (2 bytes) and corrupted every spliced file
        # whose VDir carries a BOM (CWE: silent data corruption).
        vdir_new_size = vdir_end - new_hdr_offset if vdir_start >= 0 else 0
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

    # (The BackupLog was patched above, before the VirtualDirectory, so its
    # length change could be folded into the VDir entry and the header offset.)
    return bytes(buf)
