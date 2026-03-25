# VertiPaq Binary Format Specification

Independent documentation of the Power BI VertiPaq columnar storage format, derived through clean-room analysis of file structures for interoperability purposes.

## PBIX File Structure

A `.pbix` file is a standard ZIP archive containing:

| Entry | Format | Description |
|-------|--------|-------------|
| `DataModel` | XPress9 compressed | ABF archive containing all VertiPaq data |
| `Report/Layout` | JSON | Report pages, visuals, filters, bindings |
| `Settings` | JSON | Report settings |
| `Metadata` | JSON | Package metadata |
| `SecurityBindings` | Binary | Encrypted credential references |
| `[Content_Types].xml` | XML | OPC package manifest |

## ABF Archive

The DataModel entry, after XPress9 decompression, is an ABF (Analysis Backup Format) archive:

```
ABF Structure:
  BackupLog.xml    — File manifest with offsets and sizes
  VirtualDirectory — XML index of all contained files
  metadata.sqlitedb — SQLite database with all metadata
  *.idf            — Column data (bit-packed + RLE)
  *.idfmeta        — Column segment statistics
  *.dictionary     — Column dictionary encoding
  *.hidx           — Hash index files
```

### BackupLog XML

Contains `<FileGroup>` and `<File>` elements with absolute byte offsets into the ABF binary:

```xml
<FileGroup Name="metadata.sqlitedb" Offset="1234" Size="315392" EndOfFile="315392" />
```

### VirtualDirectory XML

Maps logical paths to file entries, establishing the folder hierarchy:

```xml
<File Name="tablename (id).tbl\partition.prt\0.colname.0.idf" ... />
```

## SQLite Metadata Schema

The `metadata.sqlitedb` contains the complete tabular model definition:

### Core Tables

| Table | Purpose | Key Fields |
|-------|---------|------------|
| `[Table]` | User and system tables | ID, Name, TableStorageID, SystemFlags |
| `[Column]` | Column definitions | ID, TableID, Type, DataType, ColumnStorageID |
| `[Partition]` | Data partitions | ID, TableID, Type, Mode, QueryDefinition |
| `[Relationship]` | Table relationships | ID, FromTableID, FromColumnID, ToTableID, ToColumnID |
| `[Measure]` | DAX measures | ID, TableID, Name, Expression |
| `[AttributeHierarchy]` | Column hierarchies | ID, ColumnID |

### Storage Tables

| Table | Purpose |
|-------|---------|
| `TableStorage` | Links Table → StorageFolder |
| `PartitionStorage` | Links Partition → SegmentMapStorage + StorageFolder |
| `ColumnStorage` | Column statistics and dictionary reference |
| `DictionaryStorage` | Dictionary type, BaseId, LastId, IsOperatingOn32 |
| `ColumnPartitionStorage` | Links column to IDF/IDFMETA files |
| `SegmentStorage` | Segment count and IDFMETA file reference |
| `SegmentMapStorage` | RecordCount, SegmentCount, RecordsPerSegment |
| `StorageFolder` | ABF path for table/partition directories |
| `StorageFile` | ABF path for individual data files |
| `AttributeHierarchyStorage` | MaterializationType, SystemTableID, statistics |

### Partition Types and Modes

| Partition.Type | Meaning |
|---------------|---------|
| 3 | System partition (H$, R$ tables) |
| 4 | M/Power Query partition (Import and DirectQuery) |
| 6 | PolicyRange (incremental refresh) |

| Partition.Mode | Meaning |
|---------------|---------|
| 0 | Import (data stored in VertiPaq) |
| 1 | DirectQuery (live database queries) |
| 2 | System (H$, R$ tables) |

## IDF Format (Column Data)

### XMHybridRLECompressionInfo (u32_a = 0xABA5A)

Used for regular data columns. Hybrid RLE + bit-packed encoding:

```
Primary Segment:
  u64: primary_entry_count (always 16 slots)
  16 × (u32 data_value, u32 repeat_count):
    - data_value = 0xFFFFFFFF → next repeat_count values in sub-segment
    - Otherwise → repeat data_value exactly repeat_count times

Sub Segment:
  u64: sub_segment_size (in u64 words)
  N × u64: bit-packed values at specified bit_width
```

### XMRENoSplitCompressionInfo<N> (u32_a = 0xABA36 + N)

Used for H$ and R$ system table columns. Raw N-bit values:

```
  u64: header (bit width indicator)
  Segments: N-bit values packed into u64 words
  Padding to 8-byte alignment between segments
```

Valid N values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 16, 21, 32

### XM123CompressionInfo (u32_b = 0xABA5B)

Used for RowNumber columns. Sequential integer encoding.

## IDFMETA Format (Segment Statistics)

Tagged block structure with XML-like open/close tags:

```
<1:CP\0  (6 bytes) — Column Partition open
  u64: version (1 for single segment, 2+ for multi-segment)

  <1:CS\0  (6 bytes) — Column Segment open
    u64: record_count
    u64: one (1 for data columns, 0 for system/RowNumber)
    u32: u32_a (compression family selector)
    u32: u32_b (compression class selector)
    ... compression-specific fields ...

    <1:SS\0  (6 bytes) — Segment Statistics open
      u64: distinct_states
      u32: min_data_id
      u32: max_data_id
      u32: original_min_segment_data_id
      i64: rle_sort_order (-1 = unsorted)
      u64: row_count
      u8:  has_nulls
      u64: rle_runs
      u64: others_rle_runs
    SS:1>\0  (6 bytes) — close

    u8: has_bit_packed_sub_segment
    [optional sub-CS block if has_bit_packed=1]

  CS:1>\0  (6 bytes) — close
  [repeat CS blocks for multi-segment columns]

CP:1>\0  (6 bytes) — close

<1:SDOs\0  — Segment Data Offsets
  <1:CSDOs\0  — per-segment byte offsets into IDF
    u64: sub_segment_size (byte offset)
    u64: count
  CSDOs:1>\0
  [repeat per segment]
SDOs:1>\0
```

## Compression Class ID Selectors

Determined through binary format analysis:

```
u32_a (compression family):
  0xABA5A → XMHybridRLECompressionInfo (standard data columns)
  0xABA36 + N → XMRENoSplitCompressionInfo<N> (system columns)

u32_b (inner compression class):
  0xABA36 + aligned_bw → XMRENoSplitCompressionInfo<aligned_bw>
  0xABA5B → XM123CompressionInfo (RowNumber)
  0xABA57 → XMREGeneralCompressionInfo

aligned_bw = bit width rounded to valid N: {1,2,3,4,5,6,7,8,9,10,12,16,21,32}
```

## Dictionary Format

### Integer Dictionary (dict_type = 0, LONG)

```
u32: dict_type (0)
u32: entry_count
u32[]: values (4 bytes each if IsOperatingOn32=1, 8 bytes if =0)
```

### Float Dictionary (dict_type = 1, REAL)

```
u32: dict_type (1)
u32: entry_count
f64[]: values (8 bytes each, IEEE 754)
```

### String Dictionary (dict_type = 2, STRING)

```
u32: dict_type (2)
u8: f_store_compressed (1 = compressed)
u32: store_longest_string (max string length excluding null)
u32: char_count (total UTF-16LE characters)
u16[]: char_data (UTF-16LE encoded, null-separated)
u32: record_count
RecordHandle[]: handles
  u32: char_offset (into char_data)
  u32: page_id (always 0)
HashInfo:
  u32: hash_table_size
  u32[]: hash_entries (chain pointers, -1 = empty)
  u32[]: chain_next (-1 = end of chain)
```

## H$ System Tables (Attribute Hierarchies)

Each user column with MaterializationType=0 has an H$ system table:

```
Table name: H$<table> (<tid>)$<column> (<cid>)
Columns: POS_TO_ID (sorted position → data_id)
          ID_TO_POS (data_id → sorted position)
Encoding: NoSplit<32> (u32_a = 0xABA56)
SMS: RecordCount = distinct + 3, SegmentCount = ceil(RecordCount/RecPerSeg)
```

## R$ System Tables (Relationships)

Each relationship has an R$ system table:

```
Table name: R$<from_table> (<tid>)$<guid> (<rid>)
Columns: INDEX (from-table row → to-table row mapping)
Encoding: NoSplit<N> where N = bits_needed(max_to_row_index)
Data: 0-based row indices (no dictionary offset)
IDFMETA: one=0, min_data_id=0
```

## XPress9 Compression

The DataModel entry uses Microsoft XPress9 block compression:

- Block size: typically 65536 bytes
- Header: 4-byte magic + metadata
- Byte-exact round-trip verified

## Key Constants

| Constant | Value | Meaning |
|----------|-------|---------|
| DATA_ID_OFFSET | 3 | First dictionary entry gets data_id = 3 |
| BaseId | 2 | DictionaryStorage.BaseId for data columns |
| HYBRID_RLE_FAMILY | 0xABA5A | XMHybridRLECompressionInfo |
| NOSPLIT_BASE | 0xABA36 | XMRENoSplitCompressionInfo base |
| XM123_CLASS | 0xABA5B | XM123CompressionInfo (RowNumber) |

---

*This specification was independently derived through analysis of file structures for interoperability purposes, in accordance with applicable reverse engineering laws (EU Directive 2009/24/EC Article 6, US DMCA §1201(f)). No Microsoft source code was used.*
