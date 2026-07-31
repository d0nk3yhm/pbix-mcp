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

    # ================================================== batch 3: financial
    # ---- annuity family
    "PMT": ["PMT(0.08/12, 10, 10000)", "PMT(0.08/12, 10, 10000, 0, 1)"],
    "FV": ["FV(0.06/12, 10, -200, -500, 1)", "FV(0.005, 10, -200)"],
    "PV": ["PV(0.08/12, 240, 500)", "PV(0.05, 10, -100, -1000, 1)"],
    "NPER": ["NPER(0.12/12, -100, -1000, 10000, 1)", "NPER(0.01, -100, -1000)"],
    "RATE": ["RATE(48, -200, 8000)", "RATE(24, -250, 5000, 0, 1)"],
    "IPMT": ["IPMT(0.1/12, 1, 36, 8000)", "IPMT(0.1, 3, 3, 8000)"],
    "PPMT": ["PPMT(0.1/12, 1, 24, 2000)", "PPMT(0.08, 10, 10, 200000)"],
    "CUMIPMT": ["CUMIPMT(0.09/12, 360, 125000, 13, 24, 0)"],
    "CUMPRINC": ["CUMPRINC(0.09/12, 360, 125000, 13, 24, 0)"],
    "ISPMT": ["ISPMT(0.1/12, 1, 36, 8000000)"],
    # ---- depreciation
    "SLN": ["SLN(30000, 7500, 10)"],
    "SYD": ["SYD(30000, 7500, 10, 1)", "SYD(30000, 7500, 10, 10)"],
    "DDB": ["DDB(2400, 300, 10, 1, 2)", "DDB(2400, 300, 10, 2)"],
    "DB": ["DB(1000000, 100000, 6, 1, 7)", "DB(1000000, 100000, 6, 2, 7)"],
    "VDB": ["VDB(2400, 300, 10, 0, 1)", "VDB(2400, 300, 10, 1, 3)"],
    "AMORDEGRC": ["AMORDEGRC(2400, DATE(2008,8,19), DATE(2008,12,31), 300, 1, 0.15, 1)"],
    "AMORLINC": ["AMORLINC(2400, DATE(2008,8,19), DATE(2008,12,31), 300, 1, 0.15, 1)"],
    # ---- rates / misc
    "EFFECT": ["EFFECT(0.0525, 4)"],
    "NOMINAL": ["NOMINAL(0.053543, 4)"],
    "RRI": ["RRI(96, 10000, 11000)"],
    "PDURATION": ["PDURATION(0.025, 2000, 2200)"],
    "DOLLARDE": ["DOLLARDE(1.02, 16)", "DOLLARDE(1.1, 32)"],
    "DOLLARFR": ["DOLLARFR(1.125, 16)", "DOLLARFR(1.09375, 32)"],
    # ---- cash flows on the fixture (F has 4 rows; use literals via DATATABLE-free shapes)
    "XNPV": ["XNPV(F, F[v], F[d], 0.09)"],
    "XIRR": ["XIRR(SELECTCOLUMNS(F, \"d\", DATE(2019 + F[k], 1, 1), \"cf\", IF(F[k] = 1, -F[v], F[v])), [cf], [d])"],
    # ---- day-count / bond family (settlement 2008-2-15, maturity varies)
    "ACCRINT": ["ACCRINT(DATE(2008,3,1), DATE(2008,8,31), DATE(2008,5,1), 0.1, 1000, 2, 0)"],
    "ACCRINTM": ["ACCRINTM(DATE(2008,4,1), DATE(2008,6,15), 0.1, 1000, 3)"],
    "COUPDAYBS": ["COUPDAYBS(DATE(2011,1,25), DATE(2011,11,15), 2, 1)",
                   "COUPDAYBS(DATE(2011,1,25), DATE(2011,11,15), 2, 0)"],
    "COUPDAYS": ["COUPDAYS(DATE(2011,1,25), DATE(2011,11,15), 2, 1)",
                  "COUPDAYS(DATE(2011,1,25), DATE(2011,11,15), 2, 0)"],
    "COUPDAYSNC": ["COUPDAYSNC(DATE(2011,1,25), DATE(2011,11,15), 2, 1)"],
    "COUPNCD": ["COUPNCD(DATE(2011,1,25), DATE(2011,11,15), 2, 1)"],
    "COUPNUM": ["COUPNUM(DATE(2007,1,25), DATE(2008,11,15), 2, 1)"],
    "COUPPCD": ["COUPPCD(DATE(2011,1,25), DATE(2011,11,15), 2, 1)"],
    "DISC": ["DISC(DATE(2018,7,1), DATE(2048,1,1), 97.975, 100, 1)"],
    "DURATION": ["DURATION(DATE(2008,1,1), DATE(2016,1,1), 0.08, 0.09, 2, 1)"],
    "MDURATION": ["MDURATION(DATE(2008,1,1), DATE(2016,1,1), 0.08, 0.09, 2, 1)"],
    "INTRATE": ["INTRATE(DATE(2008,2,15), DATE(2008,5,15), 1000000, 1014420, 2)"],
    "PRICE": ["PRICE(DATE(2008,2,15), DATE(2017,11,15), 0.0575, 0.065, 100, 2, 0)"],
    "PRICEDISC": ["PRICEDISC(DATE(2008,2,16), DATE(2008,3,1), 0.0525, 100, 2)"],
    "PRICEMAT": ["PRICEMAT(DATE(2008,2,15), DATE(2008,4,13), DATE(2007,11,11), 0.061, 0.061, 0)"],
    "RECEIVED": ["RECEIVED(DATE(2008,2,15), DATE(2008,5,15), 1000000, 0.0575, 2)"],
    "TBILLEQ": ["TBILLEQ(DATE(2008,3,31), DATE(2008,6,1), 0.0914)"],
    "TBILLPRICE": ["TBILLPRICE(DATE(2008,3,31), DATE(2008,6,1), 0.09)"],
    "TBILLYIELD": ["TBILLYIELD(DATE(2008,3,31), DATE(2008,6,1), 98.45)"],
    "YIELD": ["YIELD(DATE(2008,2,15), DATE(2016,11,15), 0.0575, 95.04287, 100, 2, 0)"],
    "YIELDDISC": ["YIELDDISC(DATE(2008,2,16), DATE(2008,3,1), 99.795, 100, 2)"],
    "YIELDMAT": ["YIELDMAT(DATE(2008,3,15), DATE(2008,11,3), DATE(2007,11,8), 0.0625, 100.0123, 0)"],
    "ODDFPRICE": ["ODDFPRICE(DATE(2008,11,11), DATE(2021,3,1), DATE(2008,10,15), DATE(2009,3,1), 0.0785, 0.0625, 100, 2, 1)"],
    "ODDFYIELD": ["ODDFYIELD(DATE(2008,11,11), DATE(2021,3,1), DATE(2008,10,15), DATE(2009,3,1), 0.0575, 84.5, 100, 2, 0)"],
    "ODDLPRICE": ["ODDLPRICE(DATE(2008,2,7), DATE(2008,6,15), DATE(2007,10,15), 0.0375, 0.0405, 100, 2, 0)"],
    "ODDLYIELD": ["ODDLYIELD(DATE(2008,4,20), DATE(2008,6,15), DATE(2007,12,24), 0.0375, 99.875, 100, 2, 0)"],
}

# Deterministic-by-shape only: capture records the TYPE, not the value.
VOLATILE: set[str] = set()
