"""Per-function conformance probes.

Each entry: FUNCTION -> list of DAX expressions exercising it against the
fixture model (tables N, D, F, K -- see fixture_def.py). The expression is run
as ``EVALUATE ROW("<name>", <expr>)`` against live Power BI Desktop; whatever
Desktop returns (a value, BLANK, or an error) IS the ground truth, captured
into ``tests/conformance/golden.json``.

Rules:
- literals chosen inside every function's domain, plus one edge case where the
  edge is cheap (negative input, zero, BLANK());
- a probe that Desktop itself refuses marks the function out-of-authorable-
  scope (recorded, not counted against parity);
- keep expressions deterministic: no NOW/TODAY/RAND family here (they are
  listed in VOLATILE below and tested by shape, not value).
"""
from __future__ import annotations

PROBES: dict[str, list[str]] = {
    # ----------------------------------------------------------------- trig
    "ACOS": ["ACOS(0.5)", "ACOS(-1)"],
    "ACOSH": ["ACOSH(1)", "ACOSH(2.5)"],
    "ACOT": ["ACOT(1)", "ACOT(-2)"],
    "ACOTH": ["ACOTH(2)", "ACOTH(-3)"],
    "ASIN": ["ASIN(0.5)", "ASIN(-1)"],
    "ASINH": ["ASINH(0)", "ASINH(-2.5)"],
    "ATAN": ["ATAN(1)", "ATAN(-0.5)"],
    "ATANH": ["ATANH(0.5)", "ATANH(-0.9)"],
    "COS": ["COS(0)", "COS(PI()/3)"],
    "COSH": ["COSH(0)", "COSH(1.5)"],
    "COT": ["COT(PI()/4)", "COT(1)"],
    "COTH": ["COTH(1)", "COTH(-2)"],
    "SIN": ["SIN(0)", "SIN(PI()/6)"],
    "SINH": ["SINH(0)", "SINH(1.5)"],
    "TAN": ["TAN(0)", "TAN(PI()/4)"],
    "TANH": ["TANH(0)", "TANH(1.5)"],
    "DEGREES": ["DEGREES(PI())", "DEGREES(1)"],
    "RADIANS": ["RADIANS(180)", "RADIANS(90)"],
    "SQRTPI": ["SQRTPI(1)", "SQRTPI(4)"],
    "COMBIN": ["COMBIN(8, 2)", "COMBIN(10, 10)"],
    "COMBINA": ["COMBINA(4, 3)", "COMBINA(10, 3)"],
    "PERMUT": ["PERMUT(8, 2)", "PERMUT(5, 0)"],
    "QUOTIENT": ["QUOTIENT(10, 3)", "QUOTIENT(-10, 3)"],
    "EXPON.DIST": ["EXPON.DIST(0.2, 10, TRUE)", "EXPON.DIST(0.2, 10, FALSE)"],
    # ---------------------------------------------------------- statistical
    "BETA.DIST": ["BETA.DIST(0.4, 8, 10, TRUE)", "BETA.DIST(0.4, 8, 10, FALSE)"],
    "BETA.INV": ["BETA.INV(0.685470581, 8, 10)"],
    "CHISQ.DIST": ["CHISQ.DIST(0.5, 1, TRUE)", "CHISQ.DIST(2, 3, FALSE)"],
    "CHISQ.DIST.RT": ["CHISQ.DIST.RT(3, 4)"],
    "CHISQ.INV": ["CHISQ.INV(0.93, 1)"],
    "CHISQ.INV.RT": ["CHISQ.INV.RT(0.05, 10)"],
    "CONFIDENCE.NORM": ["CONFIDENCE.NORM(0.05, 2.5, 50)"],
    "CONFIDENCE.T": ["CONFIDENCE.T(0.05, 1, 50)"],
    "GEOMEAN": ["GEOMEAN(N[i])"],
    "GEOMEANX": ["GEOMEANX(N, N[i])", "GEOMEANX(N, N[i] * 2)"],
    "NORM.DIST": ["NORM.DIST(42, 40, 1.5, TRUE)", "NORM.DIST(42, 40, 1.5, FALSE)"],
    "NORM.INV": ["NORM.INV(0.908789, 40, 1.5)"],
    "NORM.S.DIST": ["NORM.S.DIST(1.333333, TRUE)", "NORM.S.DIST(1.333333, FALSE)"],
    "NORM.S.INV": ["NORM.S.INV(0.908789)"],
    "PERCENTILE.EXC": ["PERCENTILE.EXC(N[i], 0.25)", "PERCENTILE.EXC(N[i], 0.5)"],
    "PERCENTILE.INC": ["PERCENTILE.INC(N[i], 0.25)", "PERCENTILE.INC(N[i], 0.9)"],
    "PERCENTILEX.EXC": ["PERCENTILEX.EXC(N, N[i], 0.5)"],
    "PERCENTILEX.INC": ["PERCENTILEX.INC(N, N[i], 0.5)", "PERCENTILEX.INC(F, F[v], 0.75)"],
    "POISSON.DIST": ["POISSON.DIST(2, 5, TRUE)", "POISSON.DIST(2, 5, FALSE)"],
    "RANK.EQ": ["RANK.EQ(3, N[i])", "RANK.EQ(2, F[k])"],
    "STDEVX.P": ["STDEVX.P(N, N[i])"],
    "STDEVX.S": ["STDEVX.S(N, N[i])"],
    "VARX.P": ["VARX.P(N, N[i])"],
    "VARX.S": ["VARX.S(N, N[i])"],
    "T.DIST": ["T.DIST(1.5, 10, TRUE)", "T.DIST(1.5, 10, FALSE)"],
    "T.DIST.2T": ["T.DIST.2T(1.959999998, 60)"],
    "T.DIST.RT": ["T.DIST.RT(1.959999998, 60)"],
    "T.INV": ["T.INV(0.75, 2)"],
    "T.INV.2T": ["T.INV.2T(0.546449, 60)"],
    # ------------------------------------------------------------- bit ops
    "BITAND": ["BITAND(13, 25)", "BITAND(0, 7)"],
    "BITOR": ["BITOR(13, 25)"],
    "BITXOR": ["BITXOR(13, 25)"],
    "BITLSHIFT": ["BITLSHIFT(4, 2)", "BITLSHIFT(16, -2)"],
    "BITRSHIFT": ["BITRSHIFT(13, 2)", "BITRSHIFT(4, -2)"],
    # ------------------------------------------------------- misc scalars
    "AVERAGEA": ["AVERAGEA(N[i])", "AVERAGEA(N[b])"],
    "DATEVALUE": ["DATEVALUE(\"2024-03-15\")", "DATEVALUE(\"1/8/2009\")"],
    "TIMEVALUE": ["TIMEVALUE(\"14:30:00\")"],
    "GCD": ["GCD(24, 36)"],
    "LCM": ["LCM(4, 6)"],
    "EVEN": ["EVEN(1.5)", "EVEN(-1)"],
    "ODD": ["ODD(1.5)", "ODD(-2)"],
    "ISO.CEILING": ["ISO.CEILING(4.3)", "ISO.CEILING(-4.3, 2)"],
    "CEILING.MATH": ["CEILING.MATH(24.3, 5)", "CEILING.MATH(-8.1, 2)"],
    "FLOOR.MATH": ["FLOOR.MATH(24.3, 5)", "FLOOR.MATH(-8.1, 2)"],
    "MROUND": ["MROUND(10, 3)", "MROUND(-10, -3)"],
    "CURRENCY": ["CURRENCY(1234.56789)"],
    "FIXED": ["FIXED(1234.567, 1)", "FIXED(1234.567, -2, TRUE)"],

    # ================================================== batch 2
    # ---- week-grain time intelligence (fixture D spans Jan-Jun 2024)
    "STARTOFWEEK": ["COUNTROWS(STARTOFWEEK(D[Date]))",
                     "CALCULATE(MAX(D[Date]), STARTOFWEEK(D[Date]))"],
    "ENDOFWEEK": ["CALCULATE(MAX(D[Date]), ENDOFWEEK(D[Date]))"],
    "NEXTDAY": ["CALCULATE(MAX(D[Date]), NEXTDAY(D[Date]))",
                 "COUNTROWS(NEXTDAY(D[Date]))"],
    "PREVIOUSDAY": ["COUNTROWS(PREVIOUSDAY(D[Date]))"],
    "NEXTWEEK": ["COUNTROWS(NEXTWEEK(D[Date]))"],
    "PREVIOUSWEEK": ["COUNTROWS(PREVIOUSWEEK(D[Date]))"],
    "DATESWTD": ["COUNTROWS(DATESWTD(D[Date]))",
                  "CALCULATE(SUM(F[v]), DATESWTD(D[Date]))"],
    "CLOSINGBALANCEWEEK": ["CLOSINGBALANCEWEEK(SUM(F[v]), D[Date])"],
    "OPENINGBALANCEWEEK": ["OPENINGBALANCEWEEK(SUM(F[v]), D[Date])"],
    "NETWORKDAYS": ["NETWORKDAYS(DATE(2024,1,1), DATE(2024,1,31))",
                     "NETWORKDAYS(DATE(2024,2,1), DATE(2024,3,1), 1)"],
    "ISDATETIME": ["ISDATETIME(DATE(2024,1,1))", "ISDATETIME(5)"],
    # ---- table machinery
    "CONTAINSROW": ["CONTAINSROW(VALUES(K[grp]), \"X\")",
                     "CONTAINSROW(VALUES(K[grp]), \"Q\")"],
    "ALLNOBLANKROW": ["COUNTROWS(ALLNOBLANKROW(K))",
                       "COUNTROWS(ALLNOBLANKROW(K[grp]))"],
    "FILTERS": ["COUNTROWS(FILTERS(K[grp]))",
                 "CALCULATE(COUNTROWS(FILTERS(K[grp])), K[grp] = \"X\")"],
    "TOPNSKIP": ["SUMX(TOPNSKIP(2, 1, F, F[v]), F[v])",
                  "COUNTROWS(TOPNSKIP(2, 0, F, F[v]))"],
    "NATURALINNERJOIN": ["COUNTROWS(NATURALINNERJOIN(F, K))"],
    "NATURALLEFTOUTERJOIN": ["COUNTROWS(NATURALLEFTOUTERJOIN(F, K))"],
    "GROUPBY": ["COUNTROWS(GROUPBY(F, F[k]))",
                 "SUMX(GROUPBY(F, F[k], \"S\", SUMX(CURRENTGROUP(), F[v])), [S])"],
    "ISONORAFTER": ["ISONORAFTER(3, 2, ASC)", "ISONORAFTER(1, 2, ASC)"],
    "ALLCROSSFILTERED": ["CALCULATE(COUNTROWS(F), ALLCROSSFILTERED(F))",
                          "CALCULATE(CALCULATE(COUNTROWS(F), ALLCROSSFILTERED(F)), K[grp] = \"X\")"],
    # ---- classification probes: expected to ERROR standalone
    "IGNORE": ["COUNTROWS(SUMMARIZECOLUMNS(K[grp], \"m\", IGNORE(SUM(F[v]))))"],
    "ROLLUPADDISSUBTOTAL": ["COUNTROWS(SUMMARIZECOLUMNS(ROLLUPADDISSUBTOTAL(K[grp], \"T\")))"],
    "NONVISUAL": ["COUNTROWS(SUMMARIZECOLUMNS(NONVISUAL(VALUES(K[grp]))))"],
    "DETAILROWS": ["COUNTROWS(DETAILROWS([Total V]))"],
    "SUBSTITUTEWITHINDEX": ["COUNTROWS(SUBSTITUTEWITHINDEX(F, \"i\", K, K[k], ASC))"],
    # ROWNUMBER/ORDERBY: window-function batch, deliberately deferred
}

# Deterministic-by-shape only: capture records the TYPE, not the value.
VOLATILE: set[str] = set()
