"""Issue #128: PBIXBuilder stores a DatePartOnly relationship as Desktop does --
RelationshipStorage.DefinitionType 2, index flag 8, and an R$ join index built
by each key's date part.

It wrote JoinOnDateBehavior = 2 but stored the relationship like any other
(DefinitionType 0, an exact index). Desktop reads the join from that storage,
so a departure at 12:30 joined no calendar row, before and after a full
refresh. With the fix Power BI Desktop 2.152 answers January 3, February 2,
the blank member 1 as built and after a full refresh -- what it answers once
its own TOM sets DatePartOnly. Desktop's own auto date/time relationships
carry DefinitionType 2 and flag 8 (the community Briqlab file).
"""
from __future__ import annotations

import datetime as dt
import json

import pytest

from pbix_mcp import server as S
from pbix_mcp.builder import PBIXBuilder
from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

DEPS = [(1, dt.datetime(2024, 1, 12, 12, 30)), (2, dt.datetime(2024, 1, 15, 11, 15)),
        (3, dt.datetime(2024, 2, 3, 0, 0)), (4, dt.datetime(2024, 2, 9, 18, 45)),
        (5, dt.datetime(2024, 1, 12, 23, 59, 59)), (6, dt.datetime(2024, 3, 9, 8, 0))]


@pytest.fixture()
def built(tmp_path):
    b = PBIXBuilder("b128")
    cal = [dt.datetime(2024, 1, 1) + dt.timedelta(days=i) for i in range(60)]
    for t, f, jod in (("CalA", "Flights", 2), ("CalB", "FlightsB", 1)):
        b.add_table(t, [{"name": "Date", "data_type": "DateTime"}, {"name": "Month", "data_type": "String"}],
                    rows=[{"Date": d, "Month": d.strftime("%B")} for d in cal])
        b.add_table(f, [{"name": "Flight_ID", "data_type": "Int64"}, {"name": "Dep", "data_type": "DateTime"}],
                    rows=[{"Flight_ID": i, "Dep": d} for i, d in DEPS])
        b.add_relationship(f, "Dep", t, "Date", join_on_date_behavior=jod, auto_orient=False)
    b.add_measure("Flights", "nA", "COUNT(Flights[Flight_ID])")
    b.add_measure("Flights", "nB", "COUNT(FlightsB[Flight_ID])")
    p = str(tmp_path / "b128.pbix")
    with pytest.warns(UserWarning):          # the orphan-key note: times match no calendar row exactly
        b.save(p)
    alias = "b128_" + tmp_path.name[-6:]
    assert json.loads(S.pbix_open(p, alias))["success"]
    yield alias
    S.pbix_close(alias)


def test_storage_is_desktops_date_part_join(built):
    sql = ("SELECT ft.Name AS t, r.JoinOnDateBehavior AS jod, rs.DefinitionType AS def, ris.Flags AS flags "
           "FROM [Relationship] r JOIN [Table] ft ON r.FromTableID = ft.ID "
           "JOIN [RelationshipStorage] rs ON rs.ID = r.RelationshipStorageID "
           "JOIN [RelationshipIndexStorage] ris ON ris.ID = rs.RelationshipIndexStorageID ORDER BY ft.Name")
    lines = json.loads(S.pbix_datamodel_query_metadata(built, sql))["message"].splitlines()[2:]
    rows = {c[0]: c[1:] for c in ([x.strip() for x in ln.split("|")] for ln in lines if ln.strip())}
    jod, def_type, flags = rows["Flights"]
    assert (jod, def_type) == ("2", "2") and int(flags) & 8         # 0.9.121: DefinitionType 0, no flag 8
    jod_b, def_b, flags_b = rows["FlightsB"]
    assert (jod_b, def_b) == ("1", "0") and not int(flags_b) & 8


def test_read_back_joins_on_the_date(built):
    ctx = S._get_dax_context(built)
    rels = {(r["FromTable"], r["ToTable"]): r.get("JoinOnDateBehavior") for r in ctx["relationships"]}
    assert rels[("Flights", "CalA")] == 2 and rels[("FlightsB", "CalB")] == 1
    got = {(cal, m): de.evaluate_measures_smart([meas], ctx["tables"], ctx["measure_defs"], {f"{cal}.Month": [m]},
                                                 relationships=ctx["relationships"], simulate_row_context=False,
                                                 group_by={f"{cal}.Month"}, selected_filters={})[meas]
           for cal, meas in (("CalA", "nA"), ("CalB", "nB")) for m in ("January", "February")}
    assert (got[("CalA", "January")], got[("CalA", "February")]) == (3, 2)     # Desktop, built and refreshed
    assert (got[("CalB", "January")], got[("CalB", "February")]) == (None, 1)
