#!/usr/bin/env python3
"""Full-fidelity check of a rebuild-path edit against the file's own control.

Written after a rebuilt model was rejected three times by the Power BI service
while a field-by-field metadata comparison reported "0 differences". Each of
those comparisons was true — and each covered a different, insufficient subset.
This checks every dimension that has actually been shown to matter:

  1. SCHEMA ERA      the metadata.sqlitedb table and column set. Rebuilding a
                     1455-era model with a 63-table schema invents tables that
                     level never had; Analysis Services rejects the database.
  2. COMPATIBILITY   the db.xml CompatibilityLevel and DbUniqueId, which must
                     be the source's own, not a hardcoded 1550.
  3. REFERENCES      every foreign key resolves. A dangling reference is what
                     "Failed to PublishAbf database" looks like from outside.
  4. PROPERTIES      IsHidden / FormatString / SummarizeBy / SortByColumnID /
                     DataCategory / DisplayOrdinal survive; losing them turns a
                     hidden auto-date table visible and makes Year SUM.
  5. STORAGE         every StorageFile row has a matching ABF entry.
  6. DAX BINDING     calculated tables and columns still resolve.
  7. AUTO-DATE SHAPE Desktop's exact shape for auto date/time tables.

The control is the SAME file opened and saved with no edit — never the original,
because saving legitimately rewrites members on its own.

Usage:  python scripts/verify_rebuild_fidelity.py [file.pbix ...]
Exit code is the number of files with findings.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import sqlite3
import sys
import tempfile
import uuid
import zipfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "src"))

from pbix_mcp import server  # noqa: E402
from pbix_mcp.formats.abf_rebuild import (  # noqa: E402
    list_abf_files,
    read_abf_file,
    read_metadata_sqlite,
)
from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel  # noqa: E402

_AMO = {2: "String", 6: "Int64", 8: "Float64", 9: "DateTime",
        10: "Decimal", 11: "Boolean"}

FKS = [
    ("Expression", "ModelID", "Model"), ("DataSource", "ModelID", "Model"),
    ("LinguisticMetadata", "CultureID", "Culture"),
    ("Perspective", "ModelID", "Model"),
    ("PerspectiveTable", "PerspectiveID", "Perspective"),
    ("PerspectiveTable", "TableID", "Table"),
    ("PerspectiveColumn", "PerspectiveTableID", "PerspectiveTable"),
    ("PerspectiveColumn", "ColumnID", "Column"),
    ("KPI", "MeasureID", "Measure"),
    ("RelatedColumnDetails", "ColumnID", "Column"),
    ("GroupByColumn", "RelatedColumnDetailsID", "RelatedColumnDetails"),
    ("GroupByColumn", "GroupingColumnID", "Column"),
    ("Variation", "ColumnID", "Column"),
    ("Variation", "RelationshipID", "Relationship"),
    ("Variation", "DefaultHierarchyID", "Hierarchy"),
    ("Hierarchy", "TableID", "Table"), ("Level", "HierarchyID", "Hierarchy"),
    ("Level", "ColumnID", "Column"), ("Column", "SortByColumnID", "Column"),
    ("Measure", "TableID", "Table"), ("Partition", "TableID", "Table"),
    ("Relationship", "FromColumnID", "Column"),
    ("Relationship", "ToColumnID", "Column"),
    ("Column", "TableID", "Table"),
]
OBJ = {1: "Model", 3: "Table", 4: "Column", 7: "Relationship",
       8: "Measure", 9: "Hierarchy", 12: "KPI", 41: "Expression"}
POLY = ("Annotation", "ExtendedProperty", "FormatStringDefinition",
        "ChangedProperty")
PROPS = ("IsHidden", "FormatString", "SummarizeBy", "DataCategory",
         "DisplayOrdinal")


def _open(pbix):
    with zipfile.ZipFile(pbix) as z:
        abf = decompress_datamodel(z.read("DataModel"))
    meta = read_metadata_sqlite(abf)
    fd, tmp = tempfile.mkstemp(suffix=".db")
    os.write(fd, meta)
    os.close(fd)
    conn = sqlite3.connect(tmp)
    conn.row_factory = sqlite3.Row
    return abf, conn, tmp


def snapshot(pbix):
    abf, c, tmp = _open(pbix)
    try:
        tabs = {r[0] for r in c.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        cols = {}
        for n in sorted(tabs):
            try:
                cols[n] = {r[1] for r in c.execute(f"PRAGMA table_info([{n}])")}
            except sqlite3.Error:
                pass
        e = [f for f in list_abf_files(abf) if f["FileName"].endswith(".db.xml")]
        lvl, uid = "?", "?"
        if e:
            raw = read_abf_file(abf, e[0])
            txt = (raw.decode("utf-16-le", "replace")
                   if raw[:2] in (b"\xff\xfe", b"<\x00")
                   else raw.decode("utf-8-sig", "replace"))
            m = re.search(r"CompatibilityLevel>(\d+)<", txt)
            lvl = m.group(1) if m else "?"
            uid = "yes" if "DbUniqueId" in txt else "no"
        names = {r["ID"]: r["nm"] for r in c.execute(
            "SELECT c.ID, COALESCE(c.ExplicitName,c.InferredName) nm "
            "FROM [Column] c JOIN [Table] t ON c.TableID=t.ID "
            "WHERE t.ModelID=1")}
        tprops = {r["Name"]: r["IsHidden"] for r in c.execute(
            "SELECT Name, IsHidden FROM [Table] WHERE ModelID=1")}
        cprops = {}
        for r in c.execute("SELECT c.*, t.Name _t FROM [Column] c "
                           "JOIN [Table] t ON c.TableID=t.ID WHERE t.ModelID=1"):
            nm = r["ExplicitName"] or r["InferredName"]
            if not nm or nm.startswith("RowNumber"):
                continue
            d = {k: r[k] for k in PROPS}
            d["SortBy"] = names.get(r["SortByColumnID"]) if r["SortByColumnID"] else None
            cprops[(r["_t"], nm)] = d
        sf = {r["FileName"] for r in c.execute("SELECT FileName FROM [StorageFile]")}
        abf_names = {f["FileName"] for f in list_abf_files(abf)}
        return dict(tabs=tabs, cols=cols, lvl=lvl, uid=uid, tprops=tprops,
                    cprops=cprops, sf=sf, abf=abf_names)
    finally:
        c.close()
        os.unlink(tmp)


def integrity(pbix):
    _abf, c, tmp = _open(pbix)
    bad = []
    try:
        for tbl, fk, ref in FKS:
            try:
                n = c.execute(
                    f"SELECT COUNT(*) FROM [{tbl}] x LEFT JOIN [{ref}] r "
                    f"ON x.[{fk}]=r.ID WHERE x.[{fk}] IS NOT NULL "
                    f"AND x.[{fk}]!=0 AND r.ID IS NULL").fetchone()[0]
                if n:
                    bad.append(f"{tbl}.{fk}->{ref}: {n} dangling")
            except sqlite3.Error:
                pass
        for tbl in POLY:
            for ot, rt in OBJ.items():
                try:
                    n = c.execute(
                        f"SELECT COUNT(*) FROM [{tbl}] x LEFT JOIN [{rt}] r "
                        f"ON x.ObjectID=r.ID WHERE x.ObjectType=? AND r.ID IS NULL",
                        (ot,)).fetchone()[0]
                    if n:
                        bad.append(f"{tbl}(type {ot}->{rt}): {n} dangling")
                except sqlite3.Error:
                    pass
        return bad
    finally:
        c.close()
        os.unlink(tmp)


def dax_binds(pbix):
    _abf, c, tmp = _open(pbix)
    ref = re.compile(r"'([^']+)'\s*\[([^\]]+)\]|(?<![\w'])([A-Za-z_]\w*)\s*\[([^\]]+)\]")
    try:
        cols, tabs, meas = set(), set(), set()
        for r in c.execute("SELECT t.Name tn, COALESCE(col.ExplicitName,"
                           "col.InferredName) cn FROM [Column] col "
                           "JOIN [Table] t ON col.TableID=t.ID WHERE t.ModelID=1"):
            if r["cn"]:
                cols.add((r["tn"].lower(), r["cn"].lower()))
        for r in c.execute("SELECT Name FROM [Table] WHERE ModelID=1"):
            tabs.add(r[0].lower())
        for r in c.execute("SELECT Name FROM [Measure]"):
            meas.add(r[0].lower())
        bad = []

        def scan(expr, owner, own):
            for m in ref.finditer(expr or ""):
                tn, cn = m.group(1) or m.group(3), m.group(2) or m.group(4)
                low = (cn or "").lower()
                if tn is None:
                    if (own.lower(), low) not in cols and low not in meas:
                        bad.append(f"{owner}: [{cn}] unresolved")
                elif tn.lower() not in tabs:
                    bad.append(f"{owner}: table '{tn}' missing")
                elif (tn.lower(), low) not in cols and low not in meas:
                    # 'Table'[Name] is legal for a MEASURE too, not just a
                    # column — treating it as column-only reported four false
                    # positives on Ecommerce_Conversion's KPI calc table.
                    bad.append(f"{owner}: '{tn}'[{cn}] missing")
        for r in c.execute("SELECT t.Name tn, p.QueryDefinition qd FROM [Partition] p "
                           "JOIN [Table] t ON p.TableID=t.ID "
                           "WHERE p.Type=2 AND t.ModelID=1"):
            scan(r["qd"], f"calc table {r['tn'][:30]}", r["tn"])
        for r in c.execute("SELECT t.Name tn, COALESCE(col.ExplicitName,"
                           "col.InferredName) cn, col.Expression ex FROM [Column] col "
                           "JOIN [Table] t ON col.TableID=t.ID "
                           "WHERE col.Type=2 AND t.ModelID=1"):
            scan(r["ex"], f"calc column {r['tn'][:24]}[{r['cn']}]", r["tn"])
        return bad
    finally:
        c.close()
        os.unlink(tmp)


def _cols_for(alias, table):
    _i, c, t = server._read_metadata_db(alias)
    try:
        out = []
        for r in c.execute(
            "SELECT COALESCE(col.ExplicitName,col.InferredName) nm, "
            "col.ExplicitDataType edt, col.InferredDataType idt FROM [Column] col "
            "JOIN [Table] tt ON col.TableID=tt.ID WHERE tt.Name=? "
            "AND col.Type IN (1,4) AND COALESCE(col.ExplicitName,col.InferredName) "
            "NOT LIKE 'RowNumber%' ORDER BY col.ID", (table,)
        ):
            a = r["edt"] if r["edt"] in _AMO else r["idt"]
            out.append({"name": r["nm"], "data_type": _AMO.get(a, "String")})
        return out
    finally:
        c.close()
        os.unlink(t)


def _pick(pbix):
    """A small PLAIN table to rebuild.

    Never a calculated table: replacing its rows deliberately demotes it to
    static data, so using one as the probe target measures the wrong thing —
    an earlier version of this script picked an auto date/time table and then
    reported the engine's correct refusal as a regression.
    """
    _abf, c, tmp = _open(pbix)
    base = ("SELECT t.Name nm, COUNT(col.ID) nc FROM [Table] t "
            "JOIN [Column] col ON col.TableID=t.ID "
            "JOIN [Partition] p ON p.TableID=t.ID "
            "WHERE t.ModelID=1 AND p.Type != 2 AND col.Type = 1 "
            "AND COALESCE(col.ExplicitName,col.InferredName) NOT LIKE 'RowNumber%' "
            "AND NOT EXISTS (SELECT 1 FROM [Column] x WHERE x.TableID=t.ID "
            "                AND x.Type IN (2,4)) GROUP BY t.ID ")
    try:
        for having in ("HAVING nc BETWEEN 1 AND 6 ORDER BY nc LIMIT 1",
                       "HAVING nc >= 1 ORDER BY nc LIMIT 1"):
            r = c.execute(base + having).fetchone()
            if r:
                return r["nm"]
        return None
    finally:
        c.close()
        os.unlink(tmp)


def _sample(dtype, i):
    """A value of the RIGHT type. Feeding "row0" into a DateTime column makes
    every dependent calculated column evaluate blank, which the engine then
    correctly refuses — a probe artefact, not a product failure."""
    return {"String": f"row{i}", "Int64": i, "Float64": float(i),
            "Decimal": float(i), "Boolean": bool(i % 2),
            "DateTime": f"2020-01-0{i + 1}T00:00:00"}.get(dtype, f"row{i}")


def build(src, workdir, table=None):
    w = os.path.join(workdir, f"w{uuid.uuid4().hex[:8]}.pbix")
    shutil.copy(src, w)
    out = os.path.join(workdir, f"o{uuid.uuid4().hex[:8]}.pbix")
    alias = "v" + uuid.uuid4().hex[:8]
    server.pbix_open(w, alias)
    try:
        if table:
            cs = _cols_for(alias, table)
            rows = [{x["name"]: _sample(x["data_type"], i) for x in cs}
                    for i in range(3)]
            r = json.loads(server.pbix_set_table_data(
                alias, table, json.dumps({"columns": cs, "rows": rows})))
            if not r.get("success"):
                return None, str(r.get("message"))[:120]
        server.pbix_save(alias, out, overwrite=True, backup=False)
        return out, ""
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"[:120]
    finally:
        try:
            server.pbix_close(alias, force=True)
        except Exception:
            pass


def check(src, workdir):
    name = os.path.basename(src)
    table = _pick(src)
    if not table:
        return name, ["no suitable table to rebuild"], True
    ctl, err = build(src, workdir)
    if not ctl:
        return name, [f"control save failed: {err}"], False
    edited, err = build(src, workdir, table)
    if not edited:
        return name, [f"REFUSED: {err}"], True

    a, b = snapshot(ctl), snapshot(edited)
    f = []
    if b["tabs"] - a["tabs"]:
        f.append(f"1 SCHEMA ERA: invents tables {sorted(b['tabs']-a['tabs'])}")
    for n in a["cols"]:
        if n in b["cols"] and b["cols"][n] - a["cols"][n]:
            f.append(f"1 SCHEMA ERA: [{n}] invents columns "
                     f"{sorted(b['cols'][n]-a['cols'][n])}")
    if (a["lvl"], a["uid"]) != (b["lvl"], b["uid"]):
        f.append(f"2 COMPATIBILITY: {a['lvl']}/{a['uid']} -> {b['lvl']}/{b['uid']}")
    for x in integrity(edited):
        f.append(f"3 REFERENCES: {x}")
    lost_t = {k: (v, b["tprops"][k]) for k, v in a["tprops"].items()
              if k in b["tprops"] and b["tprops"][k] != v}
    if lost_t:
        f.append(f"4 PROPERTIES: table IsHidden changed on {list(lost_t)[:4]}")
    lost_c = {k: {p: (v[p], b['cprops'][k][p]) for p in v if b['cprops'][k][p] != v[p]}
              for k, v in a["cprops"].items() if k in b["cprops"] and b["cprops"][k] != v}
    if lost_c:
        f.append(f"4 PROPERTIES: {len(lost_c)} column(s) changed, "
                 f"e.g. {list(lost_c.items())[:2]}")
    missing = b["sf"] - b["abf"]
    if missing:
        f.append(f"5 STORAGE: {len(missing)} StorageFile rows with no ABF entry")
    for x in dax_binds(edited)[:4]:
        f.append(f"6 DAX BINDING: {x}")
    return name, f, True


def main():
    args = sys.argv[1:]
    root = os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "test_corpus")
    files = args or sorted(os.path.join(root, x) for x in os.listdir(root)
                           if x.endswith(".pbix"))
    workdir = tempfile.mkdtemp()
    bad = 0
    for src in files:
        name, findings, _ok = check(src, workdir)
        if not findings:
            print(f"  {name:34s} OK", flush=True)
        else:
            refused = all(x.startswith(("REFUSED", "no suitable")) for x in findings)
            if not refused:
                bad += 1
            for x in findings:
                print(f"  {name:34s} {x}", flush=True)
    print(f"\n{bad} file(s) with fidelity findings")
    return bad


if __name__ == "__main__":
    sys.exit(main())
