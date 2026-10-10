"""Issue #166: a measure that reads no filter of an iteration's table is
evaluated once for the iteration, not once per row.

Agents Performance guards its time intelligence with [_ShowValueForDates]:
CALCULATE(MAXX({ MAX(FactSales[DateKey]) }, ''[Value]), REMOVEFILTERS()) and
MIN('Date'[Date]). Under FILTER(ALL(DimEmployee[EmployeeKey]), ...) its value
is the same for every employee -- REMOVEFILTERS() clears the employee's
filter, and no filter path leads from the employees to the dates -- but the
measure memo keyed it by the employee, so FactSales was scanned once per
employee: [Number of Employees with Positive Change] took 44.6 s (30 s, the
census cap, before it answered) where Desktop answers 137 at once.

The memo's shield (_shielded_tables) now also proves that a measure reads no
filter on a table at all (_shield_noread): constants, pure functions, a
CALCULATE that clears every filter before its body runs, and an aggregation
over a table the shielded table's filters cannot reach."""
from __future__ import annotations

import datetime as dt
import random

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

random.seed(7)
DAYS = [dt.datetime(2023, 1, 1) + dt.timedelta(days=i) for i in range(120)]
EMPS = list(range(1, 41))
FACT = []
for _ in range(4000):
    d = random.choice(DAYS)
    FACT.append([random.choice(EMPS), d, int(d.strftime("%Y%m%d")), round(random.uniform(10, 500), 2)])
TABLES = {
    "Date": {"columns": ["Date", "Year", "Month"], "rows": [[d, d.year, d.month] for d in DAYS]},
    "Emp": {"columns": ["EmployeeKey"], "rows": [[e] for e in EMPS]},
    "Sales": {"columns": ["EmployeeKey", "OrderDate", "DateKey", "Amount"], "rows": FACT},
}
RELS = [{"FromTable": "Sales", "FromColumn": "EmployeeKey", "ToTable": "Emp", "ToColumn": "EmployeeKey",
         "IsActive": 1},
        {"FromTable": "Sales", "FromColumn": "OrderDate", "ToTable": "Date", "ToColumn": "Date", "IsActive": 1}]
MEASURES = {
    "Total": "SUM(Sales[Amount])",
    "Plain": "CALCULATE(MAX(Sales[DateKey]), REMOVEFILTERS()) > 0",
    "Guard": "VAR __Last = CALCULATE(MAXX({ MAX(Sales[DateKey]) }, ''[Value]), REMOVEFILTERS()) "
             "VAR __First = MIN('Date'[Date]) RETURN __First <= __Last",
    "Guarded": "IF([Guard], [Total])",
    "CountPlain": "COUNTX(FILTER(ALL(Emp[EmployeeKey]), [Plain]), Emp[EmployeeKey])",
    "CountGuard": "COUNTX(FILTER(ALL(Emp[EmployeeKey]), [Guard]), Emp[EmployeeKey])",
    "CountGuarded": "COUNTX(FILTER(ALL(Emp[EmployeeKey]), [Guarded] > 0), Emp[EmployeeKey])",
}
FC = {"Date.Year": [2023], "Date.Month": [2]}


def _eval(names, monkeypatch, noread=True):
    if not noread:
        monkeypatch.setattr(de.DAXEngine, "_shield_noread", lambda self, *a, **k: False)
    try:
        return de.evaluate_measures_batch(list(names), TABLES, dict(MEASURES), filter_context=dict(FC),
                                          date_table="Date", date_column="Date", relationships=RELS,
                                          date_tables={"Date": "Date"})
    finally:
        if not noread:
            monkeypatch.undo()


def _computed(name, monkeypatch):
    """How many times a measure's body runs (memo misses) while CountX runs."""
    text = MEASURES[name]
    seen = []
    orig = de.DAXEngine._expand_variation_refs

    def spy(self, expr, ctx):
        if expr == text:
            seen.append(1)
        return orig(self, expr, ctx)

    monkeypatch.setattr(de.DAXEngine, "_expand_variation_refs", spy)
    return seen


def test_the_guard_reads_no_filter_on_the_employees():
    eng = de._engine._get()
    ctx = de.DAXContext(TABLES, MEASURES, relationships=RELS, date_table="Date", date_column="Date")
    assert "Emp" in eng._shielded_tables("Plain", ctx)
    assert "Emp" in eng._shielded_tables("Guard", ctx)
    assert "Date" not in eng._shielded_tables("Guard", ctx)   # MIN('Date'[Date]) reads it
    assert "Emp" not in eng._shielded_tables("Total", ctx)    # Emp filters Sales


@pytest.mark.parametrize("count,measure", [("CountPlain", "Plain"), ("CountGuard", "Guard")])
def test_one_evaluation_per_iteration(count, measure, monkeypatch):
    seen = _computed(measure, monkeypatch)
    got = _eval([count], monkeypatch)[count]
    assert got == len(EMPS)
    assert len(seen) == 1


def test_values_are_unchanged(monkeypatch):
    names = ["Plain", "Guard", "Guarded", "CountPlain", "CountGuard", "CountGuarded"]
    assert _eval(names, monkeypatch) == _eval(names, monkeypatch, noread=False)


def test_a_measure_the_filter_reaches_still_runs_per_row(monkeypatch):
    seen = _computed("Total", monkeypatch)
    _eval(["CountGuarded"], monkeypatch)
    assert len(seen) == len(EMPS)
