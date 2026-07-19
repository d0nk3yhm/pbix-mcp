"""Regression: nullable + all-NULL columns must encode a segment / dictionary /
attribute-hierarchy triple that Power BI's DBCC consistency check accepts.

Two failure modes are guarded here:

1. NULLABLE columns (some values, some NULLs). In this codebase's convention the
   BLANK member is reserved data_id 2 (= dictionary BaseId) and real values occupy
   data_ids 3.. . The segment stats (Statistics_MinDataID/MaxDataID) describe the
   REAL-value range and exclude the blank; POS_TO_ID includes the blank at data_id
   2 plus the reals. The hierarchy data-ids must match the segment domain or DBCC
   rejects the model on open.

2. ALL-NULL columns (rows exist but every value is blank -> zero real dictionary
   entries). The ONLY data_id physically stored is the blank (2). Declaring the
   normal has_nulls range [3, 3] would claim a real value at data_id 3 that the
   empty dictionary does not provide -> DBCC "off-by-one"
   (PFE_XM_DBCC_COLUMN_DICTIONARY_FAILED, model won't open). The encoder must
   instead declare min == max == 2 (LastId == BaseId == 2), matching the sole
   stored data_id and the empty dictionary.

Verified end-to-end against Power BI Desktop: before the fix an all-null column
produced "Something went wrong" on open; after, the model loads clean and the
data round-trips (all-null -> all blanks, partial-null preserved).
"""

import os
import sqlite3
import struct
import tempfile
import zipfile

import pytest

from pbix_mcp.builder import PBIXBuilder
from pbix_mcp.formats.abf_rebuild import list_abf_files, read_abf_file, read_metadata_sqlite
from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel
from pbix_mcp.formats.vertipaq_decoder import decode_dictionary, decode_idf, decode_idfmeta

pytestmark = pytest.mark.unit

BASE_ID = 2  # blank / dictionary base


def _build(tmp_path):
    """Build a model exercising: non-null Int, partial-null Double, partial-null
    String, and all-null String/Int/DateTime columns."""
    out = str(tmp_path / "nullable_hierarchy.pbix")
    ages = [22, 38, 26, None, 35, None, 54, 2, 27, 14]
    cats = ["A", "B", None, "A", "C", None, "B", "A", None, "C"]
    rows = [
        {
            "Id": i,
            "Age": ages[i],
            "Cat": cats[i],
            "AllNullStr": None,
            "AllNullInt": None,
            "AllNullDate": None,
        }
        for i in range(len(ages))
    ]
    b = PBIXBuilder("NullHier")
    b.add_table(
        "T",
        [
            {"name": "Id", "data_type": "Int64"},
            {"name": "Age", "data_type": "Int64"},
            {"name": "Cat", "data_type": "String"},
            {"name": "AllNullStr", "data_type": "String"},
            {"name": "AllNullInt", "data_type": "Int64"},
            {"name": "AllNullDate", "data_type": "DateTime"},
        ],
        rows=rows,
    )
    b.add_page("Page 1")
    b.save(out, validate=True)
    return out


def _load(pbix_path):
    abf = decompress_datamodel(zipfile.ZipFile(pbix_path).read("DataModel"))
    files = list_abf_files(abf)
    meta = read_metadata_sqlite(abf)
    return abf, files, meta


def _decode_nosplit32(buf, records_per_seg):
    """Decode a NoSplit<32> IDF (R$/H$ format), segment-aware: each segment is a
    u64 word count then that many u64 words (two little-endian u32 values each);
    only the first records_per_seg[i] values of segment i are real, the rest are
    word padding. Returns the trimmed logical value list."""
    out = []
    pos = 0
    n = len(buf)
    for seg_records in records_per_seg:
        if pos + 8 > n:
            break
        (wc,) = struct.unpack_from("<Q", buf, pos)
        pos += 8
        seg = []
        for _ in range(wc):
            if pos + 8 > n:
                break
            (w,) = struct.unpack_from("<Q", buf, pos)
            pos += 8
            seg.append(w & 0xFFFFFFFF)
            seg.append((w >> 32) & 0xFFFFFFFF)
        out.extend(seg[:seg_records])
    return out


def _segments(record_count, records_per_segment):
    """Split record_count into per-segment chunks of records_per_segment."""
    segs, rem = [], record_count
    rps = records_per_segment or record_count or 1
    while rem > 0:
        seg = min(rps, rem)
        segs.append(seg)
        rem -= seg
    return segs


def _decode_h(abf, files, col_name, col_id, tag, record_count, records_per_segment):
    """Decode an H$ POS_TO_ID / ID_TO_POS value array for a column (or None)."""
    idf = None
    for e in files:
        p = e["Path"]
        if "H$" not in p or f"{col_name} ({col_id})" not in p or f".{tag}." not in p:
            continue
        if p.endswith(".idf"):
            idf = e
    if not idf or not record_count:
        return None
    return _decode_nosplit32(read_abf_file(abf, idf), _segments(record_count, records_per_segment))


def _column_facts(pbix_path):
    abf, files, meta = _load(pbix_path)

    # segment file lookup by "<name> (<colid>)" + suffix (final ABF naming)
    def seg(col_name, col_id, suffix):
        tag = f"{col_name} ({col_id})"
        for e in files:
            p = e["Path"]
            if p.startswith(("H$", "R$")) or tag not in p:
                continue
            if suffix == "idf" and p.endswith(".idf"):
                return e
            if suffix == "meta" and p.endswith(".idfmeta"):
                return e
            if suffix == "dict" and p.endswith(".dictionary"):
                return e
        return None

    fd, tp = tempfile.mkstemp(suffix=".db")
    os.write(fd, meta)
    os.close(fd)
    facts = {}
    try:
        con = sqlite3.connect(tp)
        con.row_factory = sqlite3.Row
        rows = con.execute(
            """SELECT c.ID cid, c.ExplicitName nm,
                      cs.Statistics_MinDataID mn, cs.Statistics_MaxDataID mx,
                      cs.Statistics_HasNulls hn,
                      ds.BaseId base, ds.LastId last,
                      ahs.MaterializationType mat, ahs.DistinctDataCount ddc,
                      sms.RecordCount hrc, sms.RecordsPerSegment hrps
               FROM [Column] c
               JOIN [Table] t ON c.TableID = t.ID
               JOIN ColumnStorage cs ON c.ColumnStorageID = cs.ID
               JOIN DictionaryStorage ds ON ds.ColumnStorageID = cs.ID
               JOIN AttributeHierarchy ah ON ah.ColumnID = c.ID
               JOIN AttributeHierarchyStorage ahs ON ah.AttributeHierarchyStorageID = ahs.ID
               LEFT JOIN [Partition] hp ON hp.TableID = ahs.SystemTableID
               LEFT JOIN PartitionStorage hps ON hps.PartitionID = hp.ID
               LEFT JOIN SegmentMapStorage sms ON sms.PartitionStorageID = hps.ID
               WHERE t.Name = 'T' AND c.ExplicitName NOT LIKE 'RowNumber%'"""
        ).fetchall()
        con.close()
    finally:
        os.unlink(tp)

    for r in rows:
        nm = r["nm"]
        meta_e = seg(nm, r["cid"], "meta")
        idf_e = seg(nm, r["cid"], "idf")
        dict_e = seg(nm, r["cid"], "dict")
        mi = decode_idfmeta(read_abf_file(abf, meta_e)) if meta_e else {}
        raw = (
            decode_idf(read_abf_file(abf, idf_e), mi["bit_width"], mi["row_count"])
            if idf_e and meta_e
            else []
        )
        try:
            _, dvals = decode_dictionary(read_abf_file(abf, dict_e)) if dict_e else (None, [])
        except Exception:
            dvals = []
        facts[nm] = {
            "cid": r["cid"],
            "min": r["mn"],
            "max": r["mx"],
            "has_nulls": bool(r["hn"]),
            "base": r["base"],
            "last": r["last"],
            "mat": r["mat"],
            "ddc": r["ddc"],
            "n_real": len(dvals),
            "raw": raw,
            "pos_to_id": _decode_h(abf, files, nm, r["cid"], "POS_TO_ID", r["hrc"], r["hrps"]),
            "id_to_pos": _decode_h(abf, files, nm, r["cid"], "ID_TO_POS", r["hrc"], r["hrps"]),
        }
    return facts


def test_nullable_and_allnull_columns_are_dbcc_consistent(tmp_path):
    facts = _column_facts(_build(tmp_path))

    # Sanity: we exercised the intended column shapes.
    assert set(facts) == {"Id", "Age", "Cat", "AllNullStr", "AllNullInt", "AllNullDate"}
    allnull = {"AllNullStr", "AllNullInt", "AllNullDate"}
    problems = []

    for nm, f in facts.items():
        null_off = 1 if f["has_nulls"] else 0

        # ---- dictionary <-> declared real range must agree (the #6 core guard) ----
        if f["n_real"] == 0:
            # No real values: the only data state is the blank. Declaring any real
            # value (max >= 3) with an empty dictionary is the DBCC "off-by-one".
            if not (f["min"] == BASE_ID and f["max"] == BASE_ID):
                problems.append(
                    f"{nm}: empty dict but declared range [{f['min']},{f['max']}] "
                    f"(expected [{BASE_ID},{BASE_ID}] — phantom real value)"
                )
            if f["last"] != BASE_ID:
                problems.append(f"{nm}: empty dict but LastId={f['last']} (expected {BASE_ID})")
        else:
            # n_real real values occupy data_ids 3 .. 3+n_real-1.
            exp_min, exp_max = 3, 3 + f["n_real"] - 1
            if f["min"] != exp_min or f["max"] != exp_max:
                problems.append(
                    f"{nm}: {f['n_real']} dict entries but range [{f['min']},{f['max']}] "
                    f"(expected [{exp_min},{exp_max}])"
                )
            if f["last"] != f["max"]:
                problems.append(f"{nm}: LastId={f['last']} != max_data_id={f['max']}")

        if f["base"] != BASE_ID:
            problems.append(f"{nm}: BaseId={f['base']} (expected {BASE_ID})")

        # ---- every physically stored data_id must be in-range (blank allowed) ----
        # data_id = raw_index + 3 - null_off  (blank raw 0 -> BASE_ID when nullable)
        allowed = set(range(f["min"], f["max"] + 1)) | ({BASE_ID} if f["has_nulls"] else set())
        stored = {ri + 3 - null_off for ri in f["raw"]}
        if not stored <= allowed:
            problems.append(
                f"{nm}: segment stores data_ids {sorted(stored)} outside "
                f"allowed {sorted(allowed)}"
            )

        # ---- all-null columns: no materialized hierarchy, degenerate stats ----
        if nm in allnull:
            if f["n_real"] != 0:
                problems.append(f"{nm}: expected all-null (0 real) but n_real={f['n_real']}")
            if f["mat"] != 2 or f["ddc"] != 0:
                problems.append(
                    f"{nm}: all-null expected MaterializationType=2/DistinctDataCount=0, "
                    f"got {f['mat']}/{f['ddc']}"
                )
            if f["pos_to_id"] is not None or f["id_to_pos"] is not None:
                problems.append(f"{nm}: all-null column must NOT have a materialized H$ hierarchy")
            continue

        # ---- materialized columns: hierarchy matches segment domain ----
        p2i_raw, i2p = f["pos_to_id"], f["id_to_pos"]
        if p2i_raw is None or i2p is None:
            problems.append(f"{nm}: expected a materialized H$ hierarchy but none found")
            continue
        # POS_TO_ID entries are all >= BASE_ID; trailing zeros are word padding.
        n_pos = f["n_real"] + null_off  # blank member + reals
        p2i = list(p2i_raw)[:n_pos]
        # DistinctDataCount counts distinct hierarchy members (blank included).
        if f["ddc"] != n_pos:
            problems.append(f"{nm}: DistinctDataCount={f['ddc']} != distinct+blank={n_pos}")
        # POS_TO_ID data-ids = {blank if nullable} U {real value data_ids}.
        exp_pos = ([BASE_ID] if f["has_nulls"] else []) + list(range(3, 3 + f["n_real"]))
        if p2i != exp_pos:
            problems.append(f"{nm}: POS_TO_ID {p2i} != expected {exp_pos}")
        # ID_TO_POS must invert POS_TO_ID at each hierarchy data-id.
        for pos, did in enumerate(p2i):
            if did < len(i2p) and i2p[did] != pos:
                problems.append(
                    f"{nm}: ID_TO_POS[{did}]={i2p[did]} not inverse of POS_TO_ID (pos {pos})"
                )

    assert not problems, "DBCC-inconsistent columns:\n  " + "\n  ".join(problems)


def test_allnull_column_declares_no_phantom_real_value(tmp_path):
    """Focused guard for OpenBI #6: an all-null column must declare min==max==2
    (blank only), never the [3,3] range that claims a nonexistent real value."""
    facts = _column_facts(_build(tmp_path))
    for nm in ("AllNullStr", "AllNullInt", "AllNullDate"):
        f = facts[nm]
        assert f["n_real"] == 0, f"{nm} should have zero real dictionary entries"
        assert (f["min"], f["max"]) == (BASE_ID, BASE_ID), (
            f"{nm}: all-null must be [2,2], got [{f['min']},{f['max']}] "
            f"(regression: phantom real value -> Power BI DBCC failure)"
        )
        assert f["last"] == BASE_ID, f"{nm}: LastId must equal BaseId for empty dict"
