"""Issue #171: UPPER and LOWER map each character to exactly one, through
Desktop's own casing table; characters beyond the BMP keep their case.

The engine used Python's full Unicode case mapping, which turns one character
into several (UPPER("ß") was "SS", "ﬁ" "FI") and maps characters Desktop
leaves alone (the micro sign, the Kelvin sign, later Greek and Georgian
letters, the Deseret alphabet). Desktop's table is older and narrower; where
both map a character, they agree.

Expected values: Power BI Desktop 2.152 over ADOMD, one EVALUATE ROW per probe (the Desktop oracle toolkit's build_b166.py, build_b166b.py, build_b174.py), and build_b170.py over every
BMP code point."""
from __future__ import annotations

import datetime as dt

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

MODEL = {
    "T": {"columns": ["c"], "rows": [["x"]]},
    # build_b175.py's N
    "N": {"columns": ["k", "d", "dt", "b", "i"],
          "rows": [["a", 1.0, dt.datetime(2024, 1, 2), True, 1],
                   ["b", 2.5, dt.datetime(2024, 3, 4, 15, 30), False, 2]]},
}


def _engine(dax):
    """The engine's answer as ADOMD prints it; "ERROR" for an error."""
    eng = de.DAXEngine()
    try:
        got = eng._eval_expr(dax, de.DAXContext(MODEL, {}))
    except Exception:
        return "ERROR"
    if got is None:
        return None
    if isinstance(got, bool):
        return "True" if got else "False"
    if isinstance(got, float) and got.is_integer():
        return str(int(got))
    return str(got)


def _want(want):
    """A date of the current year (measured in 2026) as its serial now."""
    if isinstance(want, str) and want.startswith("THIS_YEAR "):
        _, m, d = want.split()
        return str((dt.date(dt.date.today().year, int(m), int(d)) - dt.date(1899, 12, 30)).days)
    return want


def _same(got, want):
    if got == want:
        return True
    try:
        return abs(float(got) - float(want)) <= 1e-9 * max(1.0, abs(float(want)))
    except (TypeError, ValueError):
        return False

DESKTOP = [
    ('b166:upper_sz_len', 'LEN(UPPER("ß"))', '1'),
    ('b166:lower_cap_i', 'LEN(LOWER(UNICHAR(304)))', '1'),
    ('b166b:lower_i_dot', 'UNICODE(LOWER(UNICHAR(304)))', '105'),
    ('b166b:upper_dotless', 'UNICODE(UPPER(UNICHAR(305)))', '73'),
    ('b166b:upper_fi', 'LEN(UPPER(UNICHAR(64257)))', '1'),
    ('b166b:upper_fi_cp', 'UNICODE(UPPER(UNICHAR(64257)))', '64257'),
    ('b166b:upper_n_apos', 'UNICODE(UPPER(UNICHAR(329)))', '329'),
    ('b166b:upper_final_sigma', 'UNICODE(LOWER(UNICHAR(931)))', '963'),
    ('b166b:upper_micro', 'UNICODE(UPPER(UNICHAR(181)))', '181'),
    ('b166b:upper_y_diaer', 'UNICODE(UPPER(UNICHAR(255)))', '376'),
    ('b166b:lower_kelvin', 'UNICODE(LOWER(UNICHAR(8490)))', '8490'),
    ('b174:upper_deseret', 'UNICODE(UPPER(UNICHAR(66600)))', '66600'),
    ('b174:lower_deseret', 'UNICODE(LOWER(UNICHAR(66560)))', '66560'),
]


@pytest.mark.parametrize("probe,dax,want", DESKTOP, ids=[p[0] for p in DESKTOP])
def test_matches_desktop(probe, dax, want):
    assert _same(_engine(dax), _want(want))


# build_b170.py: UPPER and LOWER of every BMP code point (made with UNICHAR in
# DAX). Desktop changes 887 characters under UPPER and 881 under LOWER,
# each to exactly one character; the digests are of its "cp:mapped" pairs.
UPPER_CHANGED, UPPER_DIGEST = 887, "188cff2d87f99a514ef79d7f06524e92cc56dcaf526a8a6c960a21e36ddae710"
LOWER_CHANGED, LOWER_DIGEST = 881, "3d29b2b0da947080e873b24fb88f6ac8f386354737d8f83dbb52558ea9378d2d"
SURROGATES = range(0xD800, 0xE000)


def _mapped(fn):
    """{cp: mapped} of the engine's UPPER / LOWER over the BMP, one DAX call."""
    import hashlib

    cps = [c for c in range(1, 0x10000) if c not in SURROGATES]
    text = "".join(chr(c) for c in cps)
    eng = de._engine._get()
    ctx = de.DAXContext(MODEL, {})
    got = getattr(eng, fn)('"' + text.replace('"', '""') + '"', ctx)
    assert len(got) == len(cps)                      # one character to one
    m = {c: ord(g) for c, g in zip(cps, got) if ord(g) != c}
    return m, hashlib.sha256(",".join(f"{a}:{b}" for a, b in sorted(m.items())).encode()).hexdigest()


@pytest.mark.parametrize("fn,changed,digest", [("_fn_upper", UPPER_CHANGED, UPPER_DIGEST),
                                                ("_fn_lower", LOWER_CHANGED, LOWER_DIGEST)])
def test_every_bmp_character_maps_as_in_desktop(fn, changed, digest):
    m, d = _mapped(fn)
    assert len(m) == changed
    assert d == digest


@pytest.mark.parametrize("cp,upper,lower", [
    (0x00DF, 0x00DF, 0x00DF),     # sharp s: no capital in Desktop's table (Python: "SS")
    (0xFB01, 0xFB01, 0xFB01),     # fi ligature (Python: "FI")
    (0x0149, 0x0149, 0x0149),     # n preceded by apostrophe (Python: two characters)
    (0x00B5, 0x00B5, 0x00B5),     # micro sign (Python: Greek capital mu)
    (0x212A, 0x212A, 0x212A),     # Kelvin sign (Python: "k")
    (0x0130, 0x0130, 0x0069),     # capital I with dot: LOWER is "i" (Python: two characters)
    (0x0131, 0x0049, 0x0131),     # dotless i: UPPER is "I"
    (0x00FF, 0x0178, 0x00FF),     # y with diaeresis
    (0x03A3, 0x03A3, 0x03C3),     # capital sigma
    (0x0390, 0x03AA, 0x0390),     # iota with dialytika and tonos: UPPER drops the tonos (Python: three characters)
])
def test_characters_python_maps_otherwise(cp, upper, lower):
    assert _engine(f"UNICODE(UPPER(UNICHAR({cp})))") == str(upper)
    assert _engine(f"UNICODE(LOWER(UNICHAR({cp})))") == str(lower)
    assert _engine(f"LEN(UPPER(UNICHAR({cp}))) + LEN(LOWER(UNICHAR({cp})))") == "2"


# build_b174.py: the 520 characters beyond the BMP whose case Python changes;
# Desktop leaves every one as it is
SUPPLEMENTARY = [66560, 66561, 66562, 66563, 66564, 66565, 66566, 66567, 66568, 66569, 66570, 66571, 66572, 66573, 66574, 66575, 66576, 66577, 66578, 66579, 66580, 66581, 66582, 66583, 66584, 66585, 66586, 66587, 66588, 66589, 66590, 66591, 66592, 66593, 66594, 66595, 66596, 66597, 66598, 66599, 66600, 66601, 66602, 66603, 66604, 66605, 66606, 66607, 66608, 66609, 66610, 66611, 66612, 66613, 66614, 66615, 66616, 66617, 66618, 66619, 66620, 66621, 66622, 66623, 66624, 66625, 66626, 66627, 66628, 66629, 66630, 66631, 66632, 66633, 66634, 66635, 66636, 66637, 66638, 66639, 66736, 66737, 66738, 66739, 66740, 66741, 66742, 66743, 66744, 66745, 66746, 66747, 66748, 66749, 66750, 66751, 66752, 66753, 66754, 66755, 66756, 66757, 66758, 66759, 66760, 66761, 66762, 66763, 66764, 66765, 66766, 66767, 66768, 66769, 66770, 66771, 66776, 66777, 66778, 66779, 66780, 66781, 66782, 66783, 66784, 66785, 66786, 66787, 66788, 66789, 66790, 66791, 66792, 66793, 66794, 66795, 66796, 66797, 66798, 66799, 66800, 66801, 66802, 66803, 66804, 66805, 66806, 66807, 66808, 66809, 66810, 66811, 66928, 66929, 66930, 66931, 66932, 66933, 66934, 66935, 66936, 66937, 66938, 66940, 66941, 66942, 66943, 66944, 66945, 66946, 66947, 66948, 66949, 66950, 66951, 66952, 66953, 66954, 66956, 66957, 66958, 66959, 66960, 66961, 66962, 66964, 66965, 66967, 66968, 66969, 66970, 66971, 66972, 66973, 66974, 66975, 66976, 66977, 66979, 66980, 66981, 66982, 66983, 66984, 66985, 66986, 66987, 66988, 66989, 66990, 66991, 66992, 66993, 66995, 66996, 66997, 66998, 66999, 67000, 67001, 67003, 67004, 68736, 68737, 68738, 68739, 68740, 68741, 68742, 68743, 68744, 68745, 68746, 68747, 68748, 68749, 68750, 68751, 68752, 68753, 68754, 68755, 68756, 68757, 68758, 68759, 68760, 68761, 68762, 68763, 68764, 68765, 68766, 68767, 68768, 68769, 68770, 68771, 68772, 68773, 68774, 68775, 68776, 68777, 68778, 68779, 68780, 68781, 68782, 68783, 68784, 68785, 68786, 68800, 68801, 68802, 68803, 68804, 68805, 68806, 68807, 68808, 68809, 68810, 68811, 68812, 68813, 68814, 68815, 68816, 68817, 68818, 68819, 68820, 68821, 68822, 68823, 68824, 68825, 68826, 68827, 68828, 68829, 68830, 68831, 68832, 68833, 68834, 68835, 68836, 68837, 68838, 68839, 68840, 68841, 68842, 68843, 68844, 68845, 68846, 68847, 68848, 68849, 68850, 71840, 71841, 71842, 71843, 71844, 71845, 71846, 71847, 71848, 71849, 71850, 71851, 71852, 71853, 71854, 71855, 71856, 71857, 71858, 71859, 71860, 71861, 71862, 71863, 71864, 71865, 71866, 71867, 71868, 71869, 71870, 71871, 71872, 71873, 71874, 71875, 71876, 71877, 71878, 71879, 71880, 71881, 71882, 71883, 71884, 71885, 71886, 71887, 71888, 71889, 71890, 71891, 71892, 71893, 71894, 71895, 71896, 71897, 71898, 71899, 71900, 71901, 71902, 71903, 93760, 93761, 93762, 93763, 93764, 93765, 93766, 93767, 93768, 93769, 93770, 93771, 93772, 93773, 93774, 93775, 93776, 93777, 93778, 93779, 93780, 93781, 93782, 93783, 93784, 93785, 93786, 93787, 93788, 93789, 93790, 93791, 93792, 93793, 93794, 93795, 93796, 93797, 93798, 93799, 93800, 93801, 93802, 93803, 93804, 93805, 93806, 93807, 93808, 93809, 93810, 93811, 93812, 93813, 93814, 93815, 93816, 93817, 93818, 93819, 93820, 93821, 93822, 93823, 125184, 125185, 125186, 125187, 125188, 125189, 125190, 125191, 125192, 125193, 125194, 125195, 125196, 125197, 125198, 125199, 125200, 125201, 125202, 125203, 125204, 125205, 125206, 125207, 125208, 125209, 125210, 125211, 125212, 125213, 125214, 125215, 125216, 125217, 125218, 125219, 125220, 125221, 125222, 125223, 125224, 125225, 125226, 125227, 125228, 125229, 125230, 125231, 125232, 125233, 125234, 125235, 125236, 125237, 125238, 125239, 125240, 125241, 125242, 125243, 125244, 125245, 125246, 125247, 125248, 125249, 125250, 125251]


def test_characters_beyond_the_bmp_keep_their_case():
    text = "".join(chr(c) for c in SUPPLEMENTARY)
    eng = de._engine._get()
    ctx = de.DAXContext(MODEL, {})
    assert eng._fn_upper('"' + text + '"', ctx) == text
    assert eng._fn_lower('"' + text + '"', ctx) == text
