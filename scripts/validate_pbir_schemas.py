#!/usr/bin/env python3
"""Validate PBIR definition files against Microsoft's published JSON schemas.

Every file under ``Report/definition/`` in a PBIR report carries a ``$schema``
pointing at ``developer.microsoft.com``.  This script downloads those schemas
(caching them under ``.pbir_schema_cache/``) and validates each file, so the
writer can be checked against the format owner's own contract rather than
against our reader.

Usage::

    python scripts/validate_pbir_schemas.py report.pbix [more.pbix ...]
    python scripts/validate_pbir_schemas.py --refresh report.pbix
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import urllib.error
import urllib.request
import zipfile
from pathlib import Path
from typing import Any

CACHE_DIR = Path(__file__).resolve().parent.parent / ".pbir_schema_cache"
SCHEMA_HOST = "https://developer.microsoft.com/json-schemas/"
TIMEOUT = 30


def _cache_path(url: str) -> Path:
    return CACHE_DIR / (hashlib.sha256(url.encode()).hexdigest()[:24] + ".json")


def _fetch_exact(url: str, refresh: bool) -> dict[str, Any]:
    path = _cache_path(url)
    if path.exists() and not refresh:
        return json.loads(path.read_text())
    with urllib.request.urlopen(url, timeout=TIMEOUT) as resp:  # noqa: S310
        raw = resp.read()
    CACHE_DIR.mkdir(exist_ok=True)
    path.write_bytes(raw)
    return json.loads(raw)


def fetch_schema(url: str, refresh: bool = False) -> dict[str, Any]:
    """Download ``url`` (or read it from the local cache).

    The Power BI service stamps schema versions that are newer than the ones
    published under developer.microsoft.com (e.g. ``visualContainer/2.11.0``
    when only ``2.9.0`` is indexed).  When the exact version 404s we walk back
    to the highest published minor of the same major, which is the strictest
    contract that is actually available.
    """
    if not url.startswith(SCHEMA_HOST):
        raise ValueError(f"refusing to fetch off-host schema: {url}")
    try:
        return _fetch_exact(url, refresh)
    except urllib.error.HTTPError as exc:
        if exc.code != 404:
            raise
    resolved = _downgrade(url, refresh)
    if resolved is None:
        raise FileNotFoundError(f"no published schema for {url}")
    return resolved


def _downgrade(url: str, refresh: bool) -> dict[str, Any] | None:
    """Find the highest published ``major.*.0`` schema below ``url``."""
    parts = url.rsplit("/", 2)
    if len(parts) != 3:
        return None
    stem, version, tail = parts
    try:
        major, minor, _patch = (int(x) for x in version.split("."))
    except ValueError:
        return None
    for candidate in range(minor - 1, -1, -1):
        alt = f"{stem}/{major}.{candidate}.0/{tail}"
        try:
            doc = _fetch_exact(alt, refresh)
        except Exception:
            continue
        DOWNGRADES[url] = alt
        return doc
    return None


DOWNGRADES: dict[str, str] = {}


def _make_registry(refresh: bool):
    from referencing import Registry, Resource

    def retrieve(uri: str) -> Any:
        return Resource.from_contents(fetch_schema(uri, refresh=refresh))

    return Registry(retrieve=retrieve)  # type: ignore[call-arg]


def validate_pbix(pbix: Path, refresh: bool = False) -> tuple[int, list[str]]:
    """Return ``(files_checked, errors)`` for one .pbix."""
    from jsonschema import Draft202012Validator

    registry = _make_registry(refresh)
    errors: list[str] = []
    checked = 0

    with zipfile.ZipFile(pbix) as zf:
        names = [
            n
            for n in zf.namelist()
            if n.startswith("Report/definition") and n.endswith(".json")
        ]
        if not names:
            return 0, [f"{pbix.name}: not a PBIR report (no Report/definition/*.json)"]

        for name in sorted(names):
            try:
                doc = json.loads(zf.read(name).decode("utf-8-sig"))
            except Exception as exc:  # pragma: no cover - corrupt input
                errors.append(f"{name}: unparseable JSON: {exc}")
                continue

            url = doc.get("$schema")
            if not url:
                errors.append(f"{name}: no $schema declared")
                continue

            try:
                schema = fetch_schema(url, refresh=refresh)
            except Exception as exc:
                errors.append(f"{name}: cannot fetch {url}: {exc}")
                continue

            validator = Draft202012Validator(schema, registry=registry)
            found = sorted(validator.iter_errors(doc), key=lambda e: list(e.path))
            checked += 1
            downgraded = url in DOWNGRADES
            for err in found:
                path = list(err.absolute_path)
                # When we validated against an older published schema, the
                # `$schema` const necessarily disagrees.  That is an artifact
                # of the downgrade, not a defect in the document.
                if downgraded and path[:1] == ["$schema"]:
                    continue
                loc = "/".join(str(p) for p in path) or "<root>"
                errors.append(f"{name}: at {loc}: {err.message}")

    return checked, errors


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("pbix", nargs="+", type=Path)
    ap.add_argument(
        "--refresh", action="store_true", help="re-download cached schemas"
    )
    args = ap.parse_args()

    total_files = 0
    total_errors = 0
    for pbix in args.pbix:
        checked, errors = validate_pbix(pbix, refresh=args.refresh)
        total_files += checked
        total_errors += len(errors)
        status = "OK" if not errors else f"{len(errors)} ERROR(S)"
        print(f"\n=== {pbix.name}: {checked} file(s) validated -> {status}")
        for err in errors:
            print(f"  ! {err}")

    print(f"\nTotal: {total_files} file(s), {total_errors} error(s)")
    return 1 if total_errors else 0


if __name__ == "__main__":
    sys.exit(main())
