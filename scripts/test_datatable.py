#!/usr/bin/env python3
"""Test creating a calculated DATATABLE with full Storage layer."""
import os
import sys
import uuid
import zipfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel, compress_datamodel
from pbix_mcp.formats.abf_rebuild import rebuild_abf_with_modified_sqlite

with open('src/pbix_mcp/templates/minimal_datamodel.bin', 'rb') as f:
    dm = f.read()
abf = decompress_datamodel(dm)


def add_calc_table(conn):
    c = conn.cursor()
    all_tables = [
        'Table', 'Column', 'Partition', 'Measure', 'TableStorage', 'ColumnStorage',
        'PartitionStorage', 'StorageFolder', 'SegmentMapStorage', 'ColumnPartitionStorage',
        'DictionaryStorage', 'SegmentStorage', 'StorageFile', 'AttributeHierarchy',
        'AttributeHierarchyStorage',
    ]
    max_id = 0
    for t in all_tables:
        try:
            r = c.execute(f'SELECT COALESCE(MAX(ID),0) FROM [{t}]').fetchone()[0]
            if r > max_id:
                max_id = r
        except Exception:
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
    col1_id = gid; gid += 1
    cs1_id = gid; gid += 1
    col2_id = gid; gid += 1
    cs2_id = gid; gid += 1

    tbl_path = f'MyProducts ({table_id}).tbl'

    # StorageFolders
    c.execute(
        'INSERT INTO StorageFolder (ID, OwnerID, OwnerType, Path) VALUES (?, ?, 18, ?)',
        (sf_id, ts_id, tbl_path),
    )
    c.execute(
        'INSERT INTO StorageFolder (ID, OwnerID, OwnerType, Path) VALUES (?, ?, 20, ?)',
        (ps_sf_id, ps_id, f'{tbl_path}\\{part_id}.prt'),
    )

    # TableStorage
    c.execute(
        'INSERT INTO TableStorage (ID, TableID, Name, Version, Settings, RIViolationCount, StorageFolderID) '
        'VALUES (?, ?, ?, 1, 4353, 0, ?)',
        (ts_id, table_id, f'MyProducts ({table_id})', sf_id),
    )

    # Table
    c.execute(
        '''INSERT INTO [Table] (ID, ModelID, Name, IsHidden, TableStorageID,
        ModifiedTime, StructureModifiedTime, SystemFlags, ShowAsVariationsOnly,
        IsPrivate, DefaultDetailRowsDefinitionID, AlternateSourcePrecedence,
        RefreshPolicyID, CalculationGroupID, ExcludeFromModelRefresh,
        LineageTag, SystemManaged, ExcludeFromAutomaticAggregations)
        VALUES (?, 1, 'MyProducts', 0, ?, ?, ?, 0, 0, 0, 0, 0, 0, 0, 0, ?, 0, 0)''',
        (table_id, ts_id, ts, ts, str(uuid.uuid4())),
    )

    # SegmentMapStorage
    c.execute(
        'INSERT INTO SegmentMapStorage (ID, PartitionStorageID, Type, RecordCount, SegmentCount, RecordsPerSegment) '
        'VALUES (?, ?, 3, 0, 0, 0)',
        (sms_id, ps_id),
    )

    # PartitionStorage
    c.execute(
        '''INSERT INTO PartitionStorage (ID, PartitionID, Name, StoragePosition,
        SegmentMapStorageID, DataObjectId, StorageFolderID, DeltaTableMetadataStorageID)
        VALUES (?, ?, ?, 0, ?, 0, ?, 0)''',
        (ps_id, part_id, f'MyProducts ({part_id})', sms_id, ps_sf_id),
    )

    # DATATABLE expression
    datatable_expr = (
        'DATATABLE(\n'
        '    "Name", STRING,\n'
        '    "Price", INTEGER,\n'
        '    {\n'
        '        {"Widget", 100},\n'
        '        {"Gadget", 200},\n'
        '        {"Gizmo", 50}\n'
        '    }\n'
        ')'
    )

    # Partition (Type=2 calculated)
    c.execute(
        '''INSERT INTO [Partition] (ID, TableID, Name, QueryDefinition, Type, State, Mode, DataView,
        ModifiedTime, RefreshedTime, PartitionStorageID)
        VALUES (?, ?, 'MyProducts_part', ?, 2, 1, 0, 3, ?, ?, ?)''',
        (part_id, table_id, datatable_expr, ts, ts, ps_id),
    )

    # ColumnStorage entries
    c.execute('INSERT INTO ColumnStorage (ID, ColumnID, StoragePosition, Settings) VALUES (?, ?, 0, 0)', (rn_cs_id, rn_col_id))
    c.execute('INSERT INTO ColumnStorage (ID, ColumnID, StoragePosition, Settings) VALUES (?, ?, 1, 0)', (cs1_id, col1_id))
    c.execute('INSERT INTO ColumnStorage (ID, ColumnID, StoragePosition, Settings) VALUES (?, ?, 2, 0)', (cs2_id, col2_id))

    # RowNumber column
    rn_name = f'RowNumber-{uuid.uuid4()}'
    c.execute(
        '''INSERT INTO [Column] (
        ID, TableID, ExplicitName, ExplicitDataType, InferredDataType,
        IsHidden, State, IsUnique, IsKey, IsNullable, Alignment,
        TableDetailPosition, IsDefaultLabel, IsDefaultImage,
        SummarizeBy, ColumnStorageID, Type,
        IsAvailableInMDX, SortByColumnID,
        ModifiedTime, StructureModifiedTime, RefreshedTime,
        SystemFlags, KeepUniqueRows, DisplayOrdinal,
        EncodingHint, RelatedColumnDetailsID, AlternateOfID, EvaluationBehavior
        ) VALUES (
        ?, ?, ?, 6, 19, 1, 1, 1, 1, 0, 1, -1, 0, 0,
        1, ?, 3, 1, 0, ?, ?, 31240512000000000, 0, 0, 0, 0, 0, 0, 1
        )''',
        (rn_col_id, table_id, rn_name, rn_cs_id, ts, ts),
    )

    # Name column (String)
    c.execute(
        '''INSERT INTO [Column] (
        ID, TableID, ExplicitName, InferredName, ExplicitDataType, InferredDataType,
        IsHidden, State, IsUnique, IsKey, IsNullable, Alignment,
        TableDetailPosition, IsDefaultLabel, IsDefaultImage,
        SummarizeBy, ColumnStorageID, Type, SourceColumn, ColumnOriginID,
        IsAvailableInMDX, SortByColumnID,
        ModifiedTime, StructureModifiedTime, RefreshedTime,
        SystemFlags, KeepUniqueRows, DisplayOrdinal,
        EncodingHint, RelatedColumnDetailsID, AlternateOfID,
        LineageTag, EvaluationBehavior
        ) VALUES (
        ?, ?, 'Name', 'Name', 2, 2, 0, 1, 0, 0, 1, 1, -1, 0, 0,
        2, ?, 1, 'Name', 0, 1, 0, ?, ?, 31240512000000000,
        1, 0, 0, 0, 0, 0, ?, 1
        )''',
        (col1_id, table_id, cs1_id, ts, ts, str(uuid.uuid4())),
    )

    # Price column (Int64)
    c.execute(
        '''INSERT INTO [Column] (
        ID, TableID, ExplicitName, InferredName, ExplicitDataType, InferredDataType,
        IsHidden, State, IsUnique, IsKey, IsNullable, Alignment,
        TableDetailPosition, IsDefaultLabel, IsDefaultImage,
        SummarizeBy, ColumnStorageID, Type, SourceColumn, ColumnOriginID,
        IsAvailableInMDX, SortByColumnID,
        ModifiedTime, StructureModifiedTime, RefreshedTime,
        SystemFlags, KeepUniqueRows, DisplayOrdinal,
        EncodingHint, RelatedColumnDetailsID, AlternateOfID,
        LineageTag, EvaluationBehavior
        ) VALUES (
        ?, ?, 'Price', 'Price', 6, 6, 0, 1, 0, 0, 1, 1, -1, 0, 0,
        2, ?, 1, 'Price', 0, 1, 0, ?, ?, 31240512000000000,
        1, 0, 1, 0, 0, 0, ?, 1
        )''',
        (col2_id, table_id, cs2_id, ts, ts, str(uuid.uuid4())),
    )

    # Measure
    max_mid = c.execute('SELECT COALESCE(MAX(ID),0) FROM [Measure]').fetchone()[0]
    c.execute(
        '''INSERT INTO [Measure] (ID, TableID, Name, Expression, DataType, IsHidden,
        State, ModifiedTime, StructureModifiedTime, LineageTag)
        VALUES (?, ?, 'Total Price', 'SUM(MyProducts[Price])', 6, 0, 1, ?, ?, ?)''',
        (max_mid + 1, table_id, ts, ts, str(uuid.uuid4())),
    )

    conn.commit()
    print(f'Table={table_id}, all columns have ColumnStorageID')


new_abf = rebuild_abf_with_modified_sqlite(abf, add_calc_table)
new_dm = compress_datamodel(new_abf)

with zipfile.ZipFile('src/pbix_mcp/templates/minimal_template.pbix') as zf_in:
    with zipfile.ZipFile('test_datatable2.pbix', 'w', zipfile.ZIP_DEFLATED) as zf_out:
        for item in zf_in.namelist():
            if item == 'DataModel':
                zf_out.writestr(item, new_dm)
            else:
                zf_out.writestr(item, zf_in.read(item))

print(f'test_datatable2.pbix ({os.path.getsize("test_datatable2.pbix")} bytes)')
