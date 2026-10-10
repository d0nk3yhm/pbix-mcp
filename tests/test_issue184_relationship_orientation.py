"""Issue #184: when both columns of a relationship are unique, the builder keeps
the caller's From (Many) -> To (One); it swaps only when From is unique and To
repeats a key.

The larger table was taken for the Many side: a sample fact whose dates were
all different, related Sales -> Date, was written Date (Many) -> Sales (One),
and Power BI Desktop 2.152 then left all 12 sales under a March filter on the
dates, where Sales -> Date leaves 2 (the Desktop oracle toolkit's
build_b184.py, INFO.RELATIONSHIPS and COUNTROWS under CALCULATETABLE). Which
side contains the other decides nothing either: a fact with an orphan key
contains its small dimension's keys."""
from __future__ import annotations

import datetime as dt
import os
import sqlite3
import tempfile
import warnings
import zipfile

import pytest

from pbix_mcp.builder import PBIXBuilder

pytestmark = pytest.mark.unit

DAYS = [dt.datetime(2023, 12, 1) + dt.timedelta(days=i) for i in range(183)]


def _relationship(tmp_path, build):
    """(from table, to table) of the one relationship the builder wrote."""
    from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
    from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

    b = PBIXBuilder("orient")
    build(b)
    b.add_page("Page 1")
    path = str(tmp_path / "orient.pbix")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        b.save(path)
    abf = decompress_datamodel(zipfile.ZipFile(path).read("DataModel"))
    fd, tmp = tempfile.mkstemp(suffix=".db")
    os.write(fd, read_metadata_sqlite(abf))
    os.close(fd)
    try:
        con = sqlite3.connect(tmp)
        names = dict(con.execute("SELECT ID, Name FROM [Table]").fetchall())
        rows = con.execute("SELECT FromTableID, ToTableID FROM Relationship").fetchall()
        con.close()
    finally:
        os.remove(tmp)
    assert len(rows) == 1
    return names[rows[0][0]], names[rows[0][1]]


def _dates_and_sales(b, repeat=False):
    b.add_table("Date", [{"name": "Date", "data_type": "DateTime"}, {"name": "Month", "data_type": "Int64"}],
                rows=[{"Date": d, "Month": d.month} for d in DAYS])
    b.add_table("Sales", [{"name": "SDate", "data_type": "DateTime"}, {"name": "Amount", "data_type": "Int64"}],
                rows=[{"SDate": d, "Amount": 1} for d in DAYS if d.day in (5, 15) for _ in range(2 if repeat else 1)])


@pytest.mark.parametrize("reverse", [False, True], ids=["fact_to_dates", "dates_to_fact"])
def test_both_unique_keeps_the_callers_order(tmp_path, reverse):
    def build(b):
        _dates_and_sales(b)
        if reverse:
            b.add_relationship("Date", "Date", "Sales", "SDate")
        else:
            b.add_relationship("Sales", "SDate", "Date", "Date")
    assert _relationship(tmp_path, build) == (("Date", "Sales") if reverse else ("Sales", "Date"))


def test_an_orphan_key_does_not_turn_it_around(tmp_path):
    def build(b):
        b.add_table("Fact", [{"name": "K", "data_type": "Int64"}], rows=[{"K": 1}, {"K": 7}])
        b.add_table("Dim", [{"name": "K", "data_type": "Int64"}], rows=[{"K": 1}])
        b.add_relationship("Fact", "K", "Dim", "K")
    assert _relationship(tmp_path, build) == ("Fact", "Dim")


@pytest.mark.parametrize("reverse", [False, True], ids=["fact_to_dates", "dates_to_fact"])
def test_repeated_keys_decide_as_before(tmp_path, reverse):
    def build(b):
        _dates_and_sales(b, repeat=True)
        if reverse:
            b.add_relationship("Date", "Date", "Sales", "SDate")
        else:
            b.add_relationship("Sales", "SDate", "Date", "Date")
    assert _relationship(tmp_path, build) == ("Sales", "Date")


@pytest.mark.parametrize("reverse", [False, True], ids=["a_to_b", "b_to_a"])
def test_the_smaller_table_is_no_evidence(tmp_path, reverse):
    def build(b):
        b.add_table("A", [{"name": "k", "data_type": "Int64"}], rows=[{"k": 1}, {"k": 2}, {"k": 3}])
        b.add_table("B", [{"name": "k", "data_type": "Int64"}], rows=[{"k": 3}, {"k": 4}, {"k": 5}, {"k": 6}])
        if reverse:
            b.add_relationship("B", "k", "A", "k")
        else:
            b.add_relationship("A", "k", "B", "k")
    assert _relationship(tmp_path, build) == (("B", "A") if reverse else ("A", "B"))
