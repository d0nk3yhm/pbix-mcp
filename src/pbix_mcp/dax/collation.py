"""Power BI Desktop's text collation: the order its engine sorts text in and
the comparison DAX's text operators make (issues #156, #157).

Desktop compares text the way Windows' NLS sorting does for the model's
locale (en-US for a model this package builds), at the NLS version of its
sort tables: by each character's primary weight (case-insensitively, and an
accented letter with its base letter); then, left to right, by the accents;
then by the "word sort" punctuation -- hyphens and ASCII apostrophes weigh
nothing at first and only order the strings they alone tell apart, earliest
first. Case, width and kana type play no part: "É" = "é", "æ" = "ae",
"ß" = "ss", "²" = "2"; "_x" < "1x" < "a10" < "a2" < "Americas" < "APAC";
"coop" < "co-op" < "cop"; "cote" < "coté" < "côte".

The weights are Windows NLS 6.1's, measured per character in context, with
the differences Desktop's older table makes measured over ADOMD (the Desktop
oracle toolkit's build_b157*.py): characters it predates are fully ignorable
(U+1E9E among them), and CYRILLIC SHORT I is a letter of its own. Characters
beyond the BMP are weighted by their UTF-16 surrogates, as Windows weighs
them. Context rules -- kana iteration marks, Hangul jamo sequences, Tibetan
composed vowels -- are compared character by character.

``sort_key(s)`` gives a key whose order is Desktop's and whose equality is
DAX's text equality in expressions. (The column store is stricter: it folds
ASCII case only, so "æ" and "ae" are two values of a column.)
"""
from __future__ import annotations

import bisect
import lzma
import struct

_STARTS: list = []
_RUNS: list = []
_CHARS: dict = {}
_KEYS: dict[str, tuple] = {}
_KEYS_MAX = 200_000
_IGNORABLE = (b"", b"", 0, None, None)


def _load() -> None:
    if _STARTS:
        return
    from pbix_mcp.dax import _collation_data as data

    raw = lzma.decompress(bytes.fromhex("".join(data.DATA)))
    starts: list = []
    runs: list = []
    i = 0
    while i < len(raw):
        cp, count, plen = struct.unpack_from("<IHB", raw, i)
        i += 7
        p = raw[i:i + plen]
        i += plen
        dlen = raw[i]
        i += 1
        d = raw[i:i + dlen]
        i += dlen
        adv, mark = struct.unpack_from("<BH", raw, i)
        i += 3
        slen = raw[i]
        i += 1
        sp = raw[i:i + slen]
        i += slen
        starts.append(cp)
        runs.append((count, p, d, adv, None if mark == 0xFFFF else mark,
                     int.from_bytes(sp, "big") if sp else None))
    _RUNS[:] = runs
    _STARTS[:] = starts


def _unit(cp: int) -> tuple:
    """(primary, diacritics, advance, mark, special) of a BMP code point or
    a UTF-16 surrogate."""
    k = bisect.bisect_right(_STARTS, cp) - 1
    if k < 0:
        return _IGNORABLE
    count, p, d, adv, mark, sp = _RUNS[k]
    off = cp - _STARTS[k]
    if off >= count:
        return _IGNORABLE
    if off and p:
        p = (int.from_bytes(p, "big") + off).to_bytes(len(p), "big")
    return (p, d, adv, mark, sp)


def _char(ch: str) -> tuple:
    hit = _CHARS.get(ch)
    if hit is None:
        _load()
        cp = ord(ch)
        if cp < 0x10000:
            hit = _unit(cp)
        else:
            v = cp - 0x10000
            hp, hd, ha, _hm, _hs = _unit(0xD800 + (v >> 10))
            lp, ld, la, _lm, _ls = _unit(0xDC00 + (v & 0x3FF))
            hit = (hp + lp, hd + ld, ha + la, None, None)
        _CHARS[ch] = hit
    return hit


def sort_key(s: str) -> tuple:
    """Desktop's key for a text: (primary weights, trailing punctuation,
    diacritic weights, word-sort punctuation as (position, weight) pairs).
    Keys order as Desktop sorts, and compare equal exactly when DAX finds the
    texts equal.

    Hyphens and apostrophes weigh last of all ("a-e" < "aé", "coop" <
    "co-op"), except at the end of a text, where they weigh more than any
    accent: a text ending in them sorts after one that does not -- "é" <
    "e-", "a-é" < "ae-" -- those with no others first, by all of them in
    order ("a--b" < "-ab-" < "a'b'" < "a-b-"), then those with them only at
    the end, by the run's length, then its weights ("a-b-" < "ab'" < "ab-"
    < "ab–" < "ab--"). Desktop 2.152, build_b157f.py / build_b157g.py."""
    hit = _KEYS.get(s)
    if hit is not None:
        return hit
    prim: list = []
    dia: list = []
    spec: list = []
    pos = 0
    trailing = 0            # specials since the last character with a primary weight
    for ch in s:
        p, d, adv, mark, sp = _char(ch)
        if sp is not None:
            spec.append((pos, sp))
            trailing += 1
        if p:
            prim.append(p)
            dia.extend(d)
            pos += adv
            trailing = 0
        elif mark is not None:
            # a nonspacing mark adds to the accent of what it follows
            if dia:
                dia[-1] = (dia[-1] + mark) & 0xFF
            else:
                dia.append(mark)
    while dia and dia[-1] == 2:
        dia.pop()
    tail: tuple = ()
    if prim and trailing:
        if trailing == len(spec):
            # only after the last letter: by the run, before the accents
            tail = (1, len(spec)) + tuple(w for _p, w in spec)
            spec = []
        else:
            tail = (0,)     # at the end and elsewhere: by all of them, last
    key = (b"".join(prim), tail, bytes(dia), tuple(spec))
    if len(_KEYS) >= _KEYS_MAX:
        _KEYS.clear()
    _KEYS[s] = key
    return key


def column_order_key(s: str) -> tuple:
    """The order of a text column's values in its attribute hierarchy, which
    Desktop's ORDER BY, MIN / MAX and sorted visuals read: the collation,
    then -- for values it finds equal, which a column keeps apart ("æ" /
    "ae", "é" / "É") -- their code points (Desktop's own order among those
    follows no rule we can see) (issue #156)."""
    return (sort_key(s), s)


def compare(a: str, b: str) -> int:
    """-1, 0 or 1 as Desktop orders ``a`` and ``b``."""
    ka, kb = sort_key(a), sort_key(b)
    return (ka > kb) - (ka < kb)


def equal(a: str, b: str) -> bool:
    """DAX's text equality in an expression: "É" = "é", "æ" = "ae"."""
    return a == b or sort_key(a) == sort_key(b)
