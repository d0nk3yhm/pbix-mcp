"""
DataModel round-trip compression/decompression for PBIX files.

PBIX ZIP files contain a 'DataModel' entry that is XPress9 compressed.
This module handles decompressing it to raw ABF bytes and re-compressing
modified ABF bytes back into the DataModel format.

Formats supported:
  - Single-threaded XPress9 (signature: "This backup was created using XPress9 compression.")
  - Multi-threaded XPress9  (signature: "This backup was created using multithreaded XPrs9.")
  - Uncompressed ABF (starts with STREAM_STORAGE_SIGNATURE)
"""

import concurrent.futures
import struct

from xpress9 import Xpress9

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SINGLE_THREAD_SIGNATURE = "This backup was created using XPress9 compression."
MULTI_THREAD_SIGNATURE = "This backup was created using multithreaded XPrs9."

# 50 UTF-16-LE chars = 100 bytes + 2 null bytes padding = 102 bytes total
HEADER_SINGLE = SINGLE_THREAD_SIGNATURE.encode("utf-16-le") + b"\x00\x00"  # 102 bytes
HEADER_MULTI = MULTI_THREAD_SIGNATURE.encode("utf-16-le") + b"\x00\x00"    # 102 bytes

STREAM_STORAGE_SIGNATURE = (
    b"\xff\xfe"
    + "STREAM_STORAGE_SIGNATURE_)!@#$%^&*(".encode("utf-16le")
)

# Chunk size used when compressing (2 MiB, same as Power BI)
COMPRESS_CHUNK_SIZE = 2 * 1024 * 1024


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _detect_format(dm_bytes: bytes) -> str:
    """Return 'single_threaded', 'multi_threaded', or 'uncompressed'."""
    if dm_bytes[:72] == STREAM_STORAGE_SIGNATURE or STREAM_STORAGE_SIGNATURE in dm_bytes[:72]:
        return "uncompressed"
    try:
        sig = dm_bytes[:102].decode("utf-16-le", errors="ignore")
    except Exception:
        sig = ""
    if SINGLE_THREAD_SIGNATURE in sig:
        return "single_threaded"
    if MULTI_THREAD_SIGNATURE in sig:
        return "multi_threaded"
    raise ValueError(
        "Unknown DataModel format: first 102 bytes do not match any known signature."
    )


def _decompress_chunks_sequential(data: bytes, offset: int) -> bytearray:
    """Read (uncompressed_size, compressed_size, payload) pairs and decompress."""
    result = bytearray()
    total = len(data)
    xp = Xpress9()
    try:
        while offset < total:
            if offset + 8 > total:
                break
            uncompressed_size = struct.unpack_from("<I", data, offset)[0]
            compressed_size = struct.unpack_from("<I", data, offset + 4)[0]
            offset += 8
            if compressed_size == 0 or offset + compressed_size > total:
                break
            chunk = xp.decompress(data[offset : offset + compressed_size], uncompressed_size)
            result.extend(chunk)
            offset += compressed_size
    finally:
        del xp
    return result


def _decompress_chunk_group(group: list[tuple[int, bytes]]) -> bytearray:
    """Decompress a list of (uncompressed_size, compressed_data) tuples using one Xpress9 context."""
    xp = Xpress9()
    out = bytearray()
    try:
        for uncompressed_size, compressed_data in group:
            out.extend(xp.decompress(compressed_data, uncompressed_size))
    finally:
        del xp
    return out


def _decompress_multi_threaded(data: bytes) -> bytearray:
    """Handle the multi-threaded XPress9 format."""
    off = 102

    main_chunks_per_thread = struct.unpack_from("<Q", data, off)[0];   off += 8
    prefix_chunks_per_thread = struct.unpack_from("<Q", data, off)[0]; off += 8
    prefix_thread_count = struct.unpack_from("<Q", data, off)[0];      off += 8
    main_thread_count = struct.unpack_from("<Q", data, off)[0];        off += 8
    _chunk_uncompressed_size = struct.unpack_from("<Q", data, off)[0]; off += 8

    result = bytearray()

    def _read_chunks(count):
        nonlocal off
        chunks = []
        for _ in range(count):
            us = struct.unpack_from("<I", data, off)[0]; off += 4
            cs = struct.unpack_from("<I", data, off)[0]; off += 4
            cd = data[off : off + cs]; off += cs
            chunks.append((us, cd))
        return chunks

    def _process_groups(chunks, chunks_per_thread, thread_count):
        groups = [
            chunks[i * chunks_per_thread : (i + 1) * chunks_per_thread]
            for i in range(thread_count)
        ]
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, thread_count)) as pool:
            futures = {pool.submit(_decompress_chunk_group, g): idx for idx, g in enumerate(groups)}
            ordered = [None] * len(groups)
            for fut in concurrent.futures.as_completed(futures):
                ordered[futures[fut]] = fut.result()
        out = bytearray()
        for part in ordered:
            if part:
                out.extend(part)
        return out

    # Prefix chunks
    if prefix_thread_count > 0 and prefix_chunks_per_thread > 0:
        prefix_chunks = _read_chunks(prefix_thread_count * prefix_chunks_per_thread)
        result.extend(_process_groups(prefix_chunks, prefix_chunks_per_thread, prefix_thread_count))

    # Main chunks
    if main_thread_count > 0 and main_chunks_per_thread > 0:
        main_chunks = _read_chunks(main_thread_count * main_chunks_per_thread)
        result.extend(_process_groups(main_chunks, main_chunks_per_thread, main_thread_count))

    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def decompress_datamodel(dm_bytes: bytes) -> bytes:
    """
    Decompress the raw DataModel bytes extracted from a PBIX ZIP file.

    Parameters
    ----------
    dm_bytes : bytes
        Raw bytes of the ``DataModel`` entry inside the PBIX ZIP archive.

    Returns
    -------
    bytes
        The decompressed ABF (Analysis Backup Format) binary blob.
    """
    fmt = _detect_format(dm_bytes)

    if fmt == "uncompressed":
        return bytes(dm_bytes)

    if fmt == "single_threaded":
        return bytes(_decompress_chunks_sequential(dm_bytes, 102))

    if fmt == "multi_threaded":
        return bytes(_decompress_multi_threaded(dm_bytes))

    raise ValueError(f"Unsupported format: {fmt}")


def compress_datamodel(abf_bytes: bytes, chunk_size: int = COMPRESS_CHUNK_SIZE) -> bytes:
    """
    Compress raw ABF bytes into the single-threaded XPress9 DataModel format.

    The produced bytes can be written back into a PBIX ZIP as the ``DataModel``
    entry to create a valid PBIX file.

    Parameters
    ----------
    abf_bytes : bytes
        The raw ABF blob (typically produced by :func:`abf_rebuild.rebuild_abf_with_replacement`
        or similar).
    chunk_size : int, optional
        Chunk size for compression.  Defaults to 2 MiB (matching Power BI).

    Returns
    -------
    bytes
        XPress9-compressed DataModel bytes ready to be stored in a PBIX ZIP.
    """
    parts: list[bytes] = []
    parts.append(HEADER_SINGLE)  # 102-byte header

    xp = Xpress9()
    try:
        offset = 0
        total = len(abf_bytes)
        while offset < total:
            end = min(offset + chunk_size, total)
            raw_chunk = abf_bytes[offset:end]
            # max_compressed_size: worst case is input + overhead
            max_compressed = len(raw_chunk) + (len(raw_chunk) // 4) + 65536
            compressed_chunk = xp.compress(raw_chunk, max_compressed)
            uncompressed_size = len(raw_chunk)
            compressed_size = len(compressed_chunk)
            parts.append(struct.pack("<I", uncompressed_size))
            parts.append(struct.pack("<I", compressed_size))
            parts.append(compressed_chunk)
            offset = end
    finally:
        del xp

    return b"".join(parts)
