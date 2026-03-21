#!/usr/bin/env python3
"""Test: inject EXACT real IDF+IDFMETA bytes from template for a new table."""
import os
import sys
import uuid
import zipfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel, compress_datamodel
from pbix_mcp.formats.abf_rebuild import (
    list_abf_files, read_abf_file, read_metadata_sqlite,
    _parse_header_xml, _parse_vdir_xml, _parse_backup_log_xml,
)
from pbix_mcp.builder import _rebuild_abf_with_new_files
import sqlite3
import struct
import tempfile

# Load template
with open('src/pbix_mcp/templates/minimal_datamodel.bin', 'rb') as f:
    dm = f.read()
abf = decompress_datamodel(dm)
files = list_abf_files(abf)

# Get REAL IDF+IDFMETA from # Measures RowNumber (0 rows)
real_rn_idf = real_rn_meta = None
for f in files:
    p = f['Path']
    if 'Measures' in p and 'RowNumber' in p:
        data = read_abf_file(abf, f)
        if p.endswith('.idf') and 'meta' not in p:
            real_rn_idf = data
        elif p.endswith('.idfmeta'):
            real_rn_meta = data

print(f'Real RN IDF: {len(real_rn_idf)} bytes, IDFMETA: {len(real_rn_meta)} bytes')

# Modify metadata: add new table with only RowNumber
meta_entry = [f for f in files if 'metadata' in f['Path'].lower()][0]
meta_bytes = read_abf_file(abf, meta_entry)

tmp = tempfile.mktemp(suffix='.db')
with open(tmp, 'wb') as f:
    f.write(meta_bytes)
conn = sqlite3.connect(tmp)
c = conn.cursor()

# Find max ID
all_tables = ['Table','Column','Partition','Measure','TableStorage','ColumnStorage',
              'PartitionStorage','StorageFolder','SegmentMapStorage','ColumnPartitionStorage',
              'SegmentStorage','StorageFile']
max_id = 0
for t in all_tables:
    try:
        r = c.execute(f'SELECT COALESCE(MAX(ID),0) FROM [{t}]').fetchone()[0]
        if r > max_id: max_id = r
    except:
        pass
gid = max_id + 1
ts = 133534961699396761

# Allocate IDs
sf_id = gid; gid += 1
ts_id = gid; gid += 1
table_id = gid; gid += 1
part_id = gid; gid += 1
ps_sf_id = gid; gid += 1
ps_id = gid; gid += 1
sms_id = gid; gid += 1
rn_col_id = gid; gid += 1
rn_cs_id = gid; gid += 1
rn_cps_id = gid; gid += 1
rn_seg_id = gid; gid += 1
rn_idf_sf = gid; gid += 1
rn_meta_sf = gid; gid += 1

tbl_path = f'OnlyRN ({table_id}).tbl'
rn_name = f'RowNumber-{uuid.uuid4()}'
prt_path = f'{tbl_path}\\{part_id}.prt'
# File names use spaces instead of dashes (matching ColumnStorage.Name format)
rn_file_name = rn_name.replace('-', ' ')
idf_fname = f'0.OnlyRN ({table_id}).{rn_file_name} ({rn_col_id}).0.idf'
meta_fname = f'0.OnlyRN ({table_id}).{rn_file_name} ({rn_col_id}).0.idfmeta'

# Insert everything
c.execute('INSERT INTO StorageFolder (ID, OwnerID, OwnerType, Path) VALUES (?, ?, 18, ?)', (sf_id, ts_id, tbl_path))
c.execute('INSERT INTO StorageFolder (ID, OwnerID, OwnerType, Path) VALUES (?, ?, 20, ?)', (ps_sf_id, ps_id, prt_path))
c.execute('INSERT INTO TableStorage (ID, TableID, Name, Version, Settings, RIViolationCount, StorageFolderID) VALUES (?, ?, ?, 1, 4353, 0, ?)', (ts_id, table_id, f'OnlyRN ({table_id})', sf_id))
c.execute('''INSERT INTO [Table] (ID, ModelID, Name, IsHidden, TableStorageID, ModifiedTime, StructureModifiedTime, SystemFlags, ShowAsVariationsOnly, IsPrivate, DefaultDetailRowsDefinitionID, AlternateSourcePrecedence, RefreshPolicyID, CalculationGroupID, ExcludeFromModelRefresh, LineageTag, SystemManaged, ExcludeFromAutomaticAggregations) VALUES (?, 1, 'OnlyRN', 0, ?, ?, ?, 0, 0, 0, 0, 0, 0, 0, 0, ?, 0, 0)''', (table_id, ts_id, ts, ts, str(uuid.uuid4())))
c.execute('INSERT INTO SegmentMapStorage (ID, PartitionStorageID, Type, RecordCount, SegmentCount, RecordsPerSegment) VALUES (?, ?, 3, 0, 1, 0)', (sms_id, ps_id))
c.execute('''INSERT INTO PartitionStorage (ID, PartitionID, Name, StoragePosition, SegmentMapStorageID, DataObjectId, StorageFolderID, DeltaTableMetadataStorageID) VALUES (?, ?, ?, 0, ?, 0, ?, 0)''', (ps_id, part_id, f'OnlyRN ({part_id})', sms_id, ps_sf_id))
c.execute('''INSERT INTO [Partition] (ID, TableID, Name, Type, State, Mode, DataView, ModifiedTime, RefreshedTime, PartitionStorageID) VALUES (?, ?, 'OnlyRN_part', 2, 1, 0, 3, ?, ?, ?)''', (part_id, table_id, ts, ts, ps_id))
# ColumnStorage.Name must match the column name format used in file paths
# RowNumber: dashes replaced with spaces + " (ColumnID)"
rn_storage_name = rn_name.replace('-', ' ') + f' ({rn_col_id})'
c.execute('INSERT INTO ColumnStorage (ID, ColumnID, Name, StoragePosition, Settings) VALUES (?, ?, ?, 0, 0)', (rn_cs_id, rn_col_id, rn_storage_name))
c.execute('''INSERT INTO [Column] (ID, TableID, ExplicitName, ExplicitDataType, InferredDataType, IsHidden, State, IsUnique, IsKey, IsNullable, Alignment, TableDetailPosition, IsDefaultLabel, IsDefaultImage, SummarizeBy, ColumnStorageID, Type, IsAvailableInMDX, SortByColumnID, ModifiedTime, StructureModifiedTime, RefreshedTime, SystemFlags, KeepUniqueRows, DisplayOrdinal, EncodingHint, RelatedColumnDetailsID, AlternateOfID, EvaluationBehavior) VALUES (?, ?, ?, 6, 19, 1, 1, 1, 1, 0, 1, -1, 0, 0, 1, ?, 3, 1, 0, ?, ?, 31240512000000000, 0, 0, 0, 0, 0, 0, 1)''', (rn_col_id, table_id, rn_name, rn_cs_id, ts, ts))
c.execute('INSERT INTO ColumnPartitionStorage (ID, ColumnStorageID, PartitionStorageID, SegmentStorageID, StorageFileID) VALUES (?, ?, ?, ?, ?)', (rn_cps_id, rn_cs_id, ps_id, rn_seg_id, rn_idf_sf))
c.execute('INSERT INTO SegmentStorage (ID, ColumnPartitionStorageID, SegmentCount, StorageFileID) VALUES (?, ?, 1, ?)', (rn_seg_id, rn_cps_id, rn_meta_sf))
c.execute('INSERT INTO StorageFile (ID, OwnerID, OwnerType, StorageFolderID, FileName) VALUES (?, ?, 23, ?, ?)', (rn_idf_sf, rn_cps_id, ps_sf_id, idf_fname))
c.execute('INSERT INTO StorageFile (ID, OwnerID, OwnerType, StorageFolderID, FileName) VALUES (?, ?, 24, ?, ?)', (rn_meta_sf, rn_seg_id, ps_sf_id, meta_fname))

# Measure
max_mid = c.execute('SELECT COALESCE(MAX(ID),0) FROM [Measure]').fetchone()[0]
c.execute('''INSERT INTO [Measure] (ID, TableID, Name, Expression, DataType, IsHidden, State, ModifiedTime, StructureModifiedTime, LineageTag) VALUES (?, ?, 'Test', '42', 6, 0, 1, ?, ?, ?)''', (max_mid + 1, table_id, ts, ts, str(uuid.uuid4())))

conn.commit()
conn.close()

with open(tmp, 'rb') as f:
    new_meta = f.read()
os.unlink(tmp)

# New files to inject into ABF
new_files_dict = {
    f'{prt_path}\\{idf_fname}': real_rn_idf,
    f'{prt_path}\\{meta_fname}': real_rn_meta,
}

print(f'Files to inject:')
for p, d in new_files_dict.items():
    print(f'  {p}: {len(d)} bytes')

# Parse ABF structure for _rebuild_abf_with_new_files
hdr = _parse_header_xml(abf)
vdir_offset = int(hdr.findtext('m_cbOffsetHeader', '0'))
vdir_size = int(hdr.findtext('DataSize', '0'))
error_code = hdr.findtext('ErrorCode', 'false').lower() == 'true'
vdir_root = _parse_vdir_xml(abf, vdir_offset, vdir_size)


class _VE:
    def __init__(self, path, size, offset, delete, ct, acc, lwt):
        self.path = path
        self.size = size
        self.m_cbOffsetHeader = offset
        self.delete = delete
        self.created_timestamp = ct
        self.access = acc
        self.last_write_time = lwt


data_entries = []
for bf in vdir_root.findall('BackupFile'):
    data_entries.append(_VE(
        bf.findtext('Path', ''),
        int(bf.findtext('Size', '0')),
        int(bf.findtext('m_cbOffsetHeader', '0')),
        bf.findtext('Delete', 'false').lower() == 'true',
        int(bf.findtext('CreatedTimestamp', '0')),
        int(bf.findtext('Access', '0')),
        int(bf.findtext('LastWriteTime', '0')),
    ))

# Find LOG entry (last in vdir)
log_entry = data_entries[-1]
blog_root = _parse_backup_log_xml(abf, log_entry.m_cbOffsetHeader, log_entry.size, error_code)


class _ABF:
    pass


# Use rebuild_abf_with_replacement for metadata, then manually inject new files
from pbix_mcp.formats.abf_rebuild import rebuild_abf_with_replacement

# First, replace just the metadata
replacements = {meta_entry['StoragePath']: new_meta}
new_abf = rebuild_abf_with_replacement(abf, replacements)

# Now manually inject the new IDF/IDFMETA files into the ABF
# by appending them and updating VDir + header
from pbix_mcp.formats.abf_rebuild import _xml_to_utf16_bytes, STREAM_STORAGE_SIGNATURE, _HEADER_PAGE_SIZE, _SIGNATURE_LEN
import xml.etree.ElementTree as ET
from copy import deepcopy

# Re-parse the new_abf to inject files
hdr2 = _parse_header_xml(new_abf)
vdir_offset2 = int(hdr2.findtext('m_cbOffsetHeader', '0'))
vdir_size2 = int(hdr2.findtext('DataSize', '0'))
error_code2 = hdr2.findtext('ErrorCode', 'false').lower() == 'true'
vdir_root2 = _parse_vdir_xml(new_abf, vdir_offset2, vdir_size2)

# Build new ABF: signature + header + old data + new files + updated vdir
buf = bytearray()
buf.extend(STREAM_STORAGE_SIGNATURE)
header_start = len(buf)
buf.extend(b'\x00' * (_HEADER_PAGE_SIZE - _SIGNATURE_LEN))

timestamp = 134002835794032078

# We need to update the LOG entry to include our new files
# First, find the LOG entry in the VDir
log_vdir_entry = None
for bf in vdir_root2.findall('BackupFile'):
    if bf.findtext('Path') == 'LOG':
        log_offset = int(bf.findtext('m_cbOffsetHeader', '0'))
        log_size = int(bf.findtext('Size', '0'))
        log_data = new_abf[log_offset:log_offset + log_size]
        # Parse the LOG XML
        log_text = log_data.decode('utf-16-le', errors='replace').lstrip('\ufeff')
        log_xml = ET.fromstring(log_text)

        # Add new files to the BackupLog FileGroup
        file_groups = log_xml.findall('FileGroups/FileGroup')
        if file_groups:
            db_fg = file_groups[0]  # First FileGroup (database)
            file_list = db_fg.find('FileList')
            if file_list is None:
                file_list = ET.SubElement(db_fg, 'FileList')
            persist_path = db_fg.findtext('PersistLocationPath', '')
            for fpath, content in new_files_dict.items():
                bf_new = ET.SubElement(file_list, 'BackupFile')
                ET.SubElement(bf_new, 'Path').text = f'{persist_path}\\{fpath}'
                ET.SubElement(bf_new, 'StoragePath').text = fpath
                ET.SubElement(bf_new, 'LastWriteTime').text = str(timestamp)
                ET.SubElement(bf_new, 'Size').text = str(len(content))

        # Serialize updated LOG as UTF-16LE with BOM
        log_data = b'\xff\xfe' + ET.tostring(log_xml, encoding='unicode').encode('utf-16-le')
        break

# Copy all existing data EXCEPT the old LOG (we'll append updated one)
data_start = _HEADER_PAGE_SIZE
# Copy data up to the LOG entry
buf.extend(new_abf[data_start:log_offset])
# Skip old LOG data, copy remaining data up to VDir
buf.extend(new_abf[log_offset + log_size:vdir_offset2])

# Append updated LOG
log_new_offset = len(buf)
buf.extend(log_data)

# Append new files
timestamp = 134002835794032078
new_file_records = []
for fpath, content in new_files_dict.items():
    offset = len(buf)
    size = len(content)
    new_file_records.append((fpath, offset, size))
    buf.extend(content)

# Rebuild VDir with new entries (update LOG offset/size)
vdir_new = ET.Element("VirtualDirectory")
for bf in vdir_root2.findall('BackupFile'):
    bf_copy = deepcopy(bf)
    if bf_copy.findtext('Path') == 'LOG':
        bf_copy.find('m_cbOffsetHeader').text = str(log_new_offset)
        bf_copy.find('Size').text = str(len(log_data))
    vdir_new.append(bf_copy)

for fpath, offset, size in new_file_records:
    bf = ET.SubElement(vdir_new, "BackupFile")
    ET.SubElement(bf, "Path").text = fpath
    ET.SubElement(bf, "Size").text = str(size)
    ET.SubElement(bf, "m_cbOffsetHeader").text = str(offset)
    ET.SubElement(bf, "Delete").text = "false"
    ET.SubElement(bf, "CreatedTimestamp").text = str(timestamp)
    ET.SubElement(bf, "Access").text = str(timestamp)
    ET.SubElement(bf, "LastWriteTime").text = str(timestamp)

vdir_bytes = _xml_to_utf16_bytes(vdir_new)
new_vdir_offset = len(buf)
new_vdir_size = len(vdir_bytes)
buf.extend(vdir_bytes)

# Patch header
hdr_new = deepcopy(hdr2)
hdr_new.find('m_cbOffsetHeader').text = str(new_vdir_offset)
hdr_new.find('DataSize').text = str(new_vdir_size)
files_elem = hdr_new.find('Files')
if files_elem is not None:
    files_elem.text = str(int(files_elem.text) + len(new_file_records))

hdr_bytes = _xml_to_utf16_bytes(hdr_new)
available = _HEADER_PAGE_SIZE - _SIGNATURE_LEN
hdr_padded = hdr_bytes + b'\x00' * (available - len(hdr_bytes))
buf[header_start:header_start + available] = hdr_padded

final_abf = bytes(buf)
new_dm = compress_datamodel(final_abf)

with zipfile.ZipFile('src/pbix_mcp/templates/minimal_template.pbix') as zf_in:
    with zipfile.ZipFile('test_real_bytes.pbix', 'w', zipfile.ZIP_DEFLATED) as zf_out:
        for item in zf_in.namelist():
            if item == 'DataModel':
                zf_out.writestr(item, new_dm)
            else:
                zf_out.writestr(item, zf_in.read(item))

print(f'test_real_bytes.pbix ({os.path.getsize("test_real_bytes.pbix")} bytes)')
print('Uses EXACT real IDF+IDFMETA bytes from template RowNumber')
