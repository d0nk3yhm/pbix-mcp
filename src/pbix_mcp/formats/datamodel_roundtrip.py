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
        # Cap the pool: hybrid-reuse containers declare one thread-group per
        # tail chunk, so thread_count can reach the hundreds on a large model
        # — the declared count is a GROUPING, not a concurrency requirement.
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=max(1, min(thread_count, 16))) as pool:
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


def _compress_single_threaded(abf_bytes: bytes, chunk_size: int) -> bytes:
    """Today's primary path: one XPress9 session over uniform chunks."""
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


def _parse_single_threaded_chunks(dm_bytes: bytes) -> list[tuple[int, bytes]]:
    """The (uncompressed_size, compressed_payload) chunk table of a
    single-threaded DataModel stream."""
    chunks: list[tuple[int, bytes]] = []
    offset = 102
    total = len(dm_bytes)
    while offset + 8 <= total:
        u = struct.unpack_from("<I", dm_bytes, offset)[0]
        c = struct.unpack_from("<I", dm_bytes, offset + 4)[0]
        offset += 8
        if c == 0 or offset + c > total:
            break
        chunks.append((u, dm_bytes[offset : offset + c]))
        offset += c
    return chunks


def _common_prefix_len(a: bytes, b: bytes) -> int:
    n = min(len(a), len(b))
    lo, step = 0, 1 << 20
    # coarse block scan, then byte-exact within the mismatching block
    while lo < n and a[lo : lo + step] == b[lo : lo + step]:
        lo += step
    hi = min(lo + step, n)
    while lo < hi and a[lo] == b[lo]:
        lo += 1
    return lo


def _parse_session_groups(dm_bytes: bytes) -> list[list[tuple[int, bytes]]]:
    """The stream's chunks partitioned into SESSION groups — the units a
    decoder context consumes whole. A single-threaded stream is one session;
    a multi-threaded stream has one session per thread-group."""
    fmt = _detect_format(dm_bytes)
    if fmt == "single_threaded":
        chunks = _parse_single_threaded_chunks(dm_bytes)
        return [chunks] if chunks else []
    if fmt != "multi_threaded":
        raise ValueError(f"no session groups in format {fmt!r}")

    off = 102
    main_cpt = struct.unpack_from("<Q", dm_bytes, off)[0]; off += 8
    prefix_cpt = struct.unpack_from("<Q", dm_bytes, off)[0]; off += 8
    prefix_tc = struct.unpack_from("<Q", dm_bytes, off)[0]; off += 8
    main_tc = struct.unpack_from("<Q", dm_bytes, off)[0]; off += 8
    off += 8  # chunk uncompressed size

    def _read(count):
        nonlocal off
        out = []
        for _ in range(count):
            us = struct.unpack_from("<I", dm_bytes, off)[0]; off += 4
            cs = struct.unpack_from("<I", dm_bytes, off)[0]; off += 4
            out.append((us, dm_bytes[off : off + cs]))
            off += cs
        return out

    groups: list[list[tuple[int, bytes]]] = []
    for _ in range(prefix_tc):
        groups.append(_read(prefix_cpt))
    for _ in range(main_tc):
        groups.append(_read(main_cpt))
    return [g for g in groups if g]


def _compress_hybrid_reuse(abf_bytes: bytes, original_dm: bytes,
                           chunk_size: int) -> bytes:
    """Multi-threaded container that REUSES the original stream's unchanged
    session prefix verbatim and re-encodes only the changed tail.

    Why this shape ('Compression failed or not effective'): the xpress9
    binding's encoder cannot emit incompressible chunks (its output lands a
    few hundred bytes OVER the input and the wrapper raises), while Power
    BI's own encoder stores them slightly UNDER — so a full re-encode of a
    real-world model fails on already-compressed VertiPaq chunks. Those
    chunks are almost always in the UNCHANGED part of a metadata edit (the
    sqlite lives near the end of the ABF), so reusing them verbatim
    sidesteps the encoder entirely and keeps Power BI's superior window-22
    encoding.

    Format notes, all empirically verified against Desktop-authored streams:
    XPress9 blocks chain their LZ77 window across a session, so only a
    session PREFIX is reusable, in order (a mid-session chunk decoded in a
    fresh context hangs the codec). The multi-threaded container decodes
    each thread-group with an independent codec context — i.e. each group is
    its own session — which legally mixes the original's sessions (reused
    groups, byte-verbatim) with ours (one chunk per main group, each a
    fresh session). Repeated edits stay in this family: a hybrid output's
    groups are reusable by the NEXT edit the same way.
    """
    groups = _parse_session_groups(original_dm)
    if not groups:
        raise ValueError("original stream has no parsable session groups")
    orig_plain = decompress_datamodel(original_dm)
    prefix_len = _common_prefix_len(bytes(orig_plain), abf_bytes)

    # Reusable = leading WHOLE groups inside the unchanged prefix; within
    # the first group that doesn't fully fit, a chunk-prefix is still a
    # valid (truncated) session.
    reused: list[list[tuple[int, bytes]]] = []
    cum = 0
    for gi, g in enumerate(groups):
        g_size = sum(u for u, _ in g)
        if cum + g_size <= prefix_len:
            reused.append(g)
            cum += g_size
            continue
        part: list[tuple[int, bytes]] = []
        for u, payload in g:
            if cum + u <= prefix_len:
                part.append((u, payload))
                cum += u
            else:
                break
        if part:
            reused.append(part)
        break

    # Rectangle assembly: the container has exactly two uniform rectangles.
    # First reused group -> the prefix rectangle (1 thread x its length);
    # further reused groups join the main rectangle only if single-chunk
    # (the shape our own hybrids emit). Anything else is re-encoded.
    prefix_group = reused[0] if reused else []
    reused_singles: list[tuple[int, bytes]] = []
    for g in reused[1:]:
        if len(g) == 1:
            reused_singles.append(g[0])
        else:
            break
    reused_plain = sum(u for u, _ in prefix_group) + \
        sum(u for u, _ in reused_singles)

    # encode the tail: one chunk per thread-group, each its own session
    tail = abf_bytes[reused_plain:]
    tail_chunks: list[tuple[int, bytes]] = []
    o = 0
    while o < len(tail):
        piece = tail[o : o + chunk_size]
        xp = Xpress9()
        try:
            payload = xp.compress(
                piece, len(piece) + (len(piece) // 4) + 65536)
        finally:
            del xp
        tail_chunks.append((len(piece), payload))
        o += len(piece)

    main_chunks = reused_singles + tail_chunks
    parts: list[bytes] = [HEADER_MULTI]
    parts.append(struct.pack("<Q", 1))                      # main chunks/thread
    parts.append(struct.pack("<Q", len(prefix_group)))      # prefix chunks/thread
    parts.append(struct.pack("<Q", 1 if prefix_group else 0))  # prefix threads
    parts.append(struct.pack("<Q", len(main_chunks)))       # main threads
    parts.append(struct.pack("<Q", chunk_size))
    for u, payload in prefix_group + main_chunks:
        parts.append(struct.pack("<I", u))
        parts.append(struct.pack("<I", len(payload)))
        parts.append(payload)
    out = b"".join(parts)

    # Safety: the produced container must round-trip to exactly the input.
    if decompress_datamodel(out) != abf_bytes:
        raise ValueError("hybrid container failed round-trip verification")
    return out


def compress_datamodel(abf_bytes: bytes, chunk_size: int = COMPRESS_CHUNK_SIZE,
                       original_dm: bytes | None = None) -> bytes:
    """
    Compress raw ABF bytes into an XPress9 DataModel stream.

    Primary path: the single-threaded format (one session, uniform chunks) —
    byte-compatible with everything this library ever produced. When the
    encoder fails on incompressible chunks ("Compression failed or not
    effective" — already-compressed VertiPaq data, present in any real-world
    model), two fallbacks engage in order:

    1. With ``original_dm`` (the pre-edit DataModel bytes): a multi-threaded
       container reusing the original's unchanged chunk prefix VERBATIM and
       re-encoding only the changed tail (see :func:`_compress_hybrid_reuse`).
    2. The uncompressed ABF format (``STREAM_STORAGE_SIGNATURE``), which every
       reader of this format — including this library — accepts; the PBIX ZIP
       layer still deflates it.

    Parameters
    ----------
    abf_bytes : bytes
        The raw ABF blob.
    chunk_size : int, optional
        Chunk size for compression.  Defaults to 2 MiB (matching Power BI).
    original_dm : bytes, optional
        The DataModel bytes this ABF was decompressed from, when the caller
        is round-tripping an edit. Enables chunk reuse.

    Returns
    -------
    bytes
        DataModel bytes ready to be stored in a PBIX ZIP.
    """
    try:
        return _compress_single_threaded(abf_bytes, chunk_size)
    except ValueError as primary_err:
        if "not effective" not in str(primary_err):
            raise

        if original_dm is not None:
            try:
                return _compress_hybrid_reuse(abf_bytes, original_dm, chunk_size)
            except ValueError:
                pass  # fall through to the uncompressed format

        # Uncompressed ABF: recognized by every reader of this entry
        # (see _detect_format), and what unblocks the file when even the
        # changed chunks are incompressible.
        head = abf_bytes[:72]
        if head.startswith(STREAM_STORAGE_SIGNATURE) or \
                STREAM_STORAGE_SIGNATURE in head:
            return bytes(abf_bytes)

        raise ValueError(
            "XPress9 re-encode failed on an incompressible chunk (the "
            "xpress9 binding cannot emit literal blocks as compactly as "
            "Power BI's encoder), no reusable original stream was provided, "
            "and the ABF blob does not carry STREAM_STORAGE_SIGNATURE for "
            "the uncompressed fallback. Original error: " + str(primary_err)
        ) from primary_err
