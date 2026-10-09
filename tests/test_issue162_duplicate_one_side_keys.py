"""Issue #162: a key the one side of a relationship holds more than once
reaches its LAST row, as Desktop builds the relationship index.

PBIXBuilder's R$ index took the first row. A key can repeat in exact copies,
in spellings the column store folds into one value (ASCII case, #43) or that
Desktop's import strips to one (a trailing space, #159); the index now also
uses the dictionary's identity for text keys, so its slots follow the folded
dictionary (#160).

Expected: Power BI Desktop 2.152 over ADOMD after its own refresh
(build_b160r.py): RELATED(D[name]) of F's rows, by F[v]."""
from __future__ import annotations

import importlib

import pytest

pytestmark = pytest.mark.unit

_t81 = importlib.import_module("tests.test_issue81_orphan_keys_blank_member")

D = [('k1', 'A'), ('k2', 'B'), ('k1', 'C'), ('K1', 'D'), ('k3', 'E'), ('k2 ', 'F'), ('k3', 'G'), ('k4', 'H')]
F = [('k1', 1), ('k2', 10), ('k3', 100), ('K1', 1000), ('k1 ', 10000), ('k4', 100000), ('k5', 1000000)]
DESKTOP_RELATED = [['k1', '1', 'D'], ['k2', '10', 'F'], ['k3', '100', 'G'], ['k1', '1000', 'D'], ['k1', '10000', 'D'], ['k4', '100000', 'H'], ['k5', '1000000', None]]   # (F[k], F[v], RELATED(D[name])) after the refresh


@pytest.fixture(scope="module")
def index(tmp_path_factory):
    import sqlite3
    import tempfile
    import zipfile
    from pathlib import Path

    from pbix_mcp.builder import PBIXBuilder
    from pbix_mcp.formats.abf_rebuild import list_abf_files, read_abf_file, read_metadata_sqlite
    from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

    path = tmp_path_factory.mktemp("i162") / "b160r.pbix"
    b = PBIXBuilder("b160r")
    b.add_table("D", [{"name": "k", "data_type": "String"}, {"name": "name", "data_type": "String"}],
                rows=[{"k": k, "name": n} for k, n in D])
    b.add_table("F", [{"name": "k", "data_type": "String"}, {"name": "v", "data_type": "Int64"}],
                rows=[{"k": k, "v": v} for k, v in F])
    b.add_relationship("F", "k", "D", "k")
    b.add_page("Page 1")
    b.save(str(path))
    abf = decompress_datamodel(zipfile.ZipFile(path).read("DataModel"))
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "meta.db"
        db.write_bytes(read_metadata_sqlite(abf))
        con = sqlite3.connect(db)
        (count,) = con.execute("SELECT RecordCount FROM RelationshipIndexStorage").fetchone()
        con.close()
    files = {f["Path"]: f for f in list_abf_files(abf)}
    (idf,) = [p for p in files if p.startswith("R$F (") and p.endswith(".idf")]
    (meta,) = [p for p in files if p.startswith("R$F (") and p.endswith(".idfmeta")]
    return _t81._decode_nosplit(read_abf_file(abf, files[idf]), read_abf_file(abf, files[meta]), count)


def test_each_key_reaches_the_row_desktop_joins(index):
    from pbix_mcp.formats.vertipaq_encoder import column_text_key, import_text

    # the FK dictionary: F's keys, stored and folded, in insertion order
    fk = []
    for k, _v in F:
        key = column_text_key(import_text(k))
        if key not in fk:
            fk.append(key)
    names = [n for _k, n in D]
    want = {}
    for k, v, name in DESKTOP_RELATED:
        want[column_text_key(k)] = names.index(name) + 1 if name is not None else 0
    assert index == [0, 0, 0] + [want[k] for k in fk]
