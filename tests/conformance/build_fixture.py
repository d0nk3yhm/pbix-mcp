"""Build the conformance fixture .pbix with pbix-mcp's own builder.

Run from the repo root:  python tools/dax_conformance/build_fixture.py
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from pbix_mcp.builder import PBIXBuilder  # noqa: E402
from tests.conformance.fixture_def import (  # noqa: E402
    BUILDER_TABLES,
    FIXTURE_PBIX,
    FIXTURE_RELATIONSHIPS,
    PC_CALC_DAX,
)


def main() -> str:
    b = PBIXBuilder("ConformanceFixture")
    for name, columns, rows in BUILDER_TABLES:
        dict_rows = [dict(zip([c["name"] for c in columns], r)) for r in rows]
        b.add_table(name, columns, rows=dict_rows)
    for rel in FIXTURE_RELATIONSHIPS:
        b.add_relationship(rel["FromTable"], rel["FromColumn"],
                           rel["ToTable"], rel["ToColumn"])
    # one measure so the model is not measure-less
    b.add_measure("F", "Total V", "SUM(F[v])")
    data = b.build()
    out = os.path.abspath(FIXTURE_PBIX)
    with open(out, "wb") as fh:
        fh.write(data)
    # PC rides in as a CALCULATED table so Desktop processes its hierarchy
    # support structures at open, making it PATH-queryable (import tables
    # from the builder are not — the known H$ processing gap).
    from pbix_mcp import server  # noqa: E402
    server.pbix_open(out, "__fixture__")
    server.pbix_datamodel_add_calculated_table("__fixture__", "PC",
                                               PC_CALC_DAX)
    server.pbix_save("__fixture__")
    server.pbix_close("__fixture__")
    size = os.path.getsize(out)
    print(f"built {out} ({size:,} bytes, PC as calculated table)")
    return out


if __name__ == "__main__":
    main()
