#!/usr/bin/env python3
"""Download the public PBIX test corpus for integration testing.

Two MIT-licensed sources:

  1. https://github.com/Dashboard-Design/Power-BI-Design-Files
     Copyright (c) 2024 Sajjad Ahmadi — four community dashboards, two of
     which are stored in the service's PBIR format.

  2. https://github.com/microsoft/powerbi-desktop-samples
     Copyright (c) Microsoft Corporation — the official sample reports.
     These cover the feature range the community dashboards do not: AI
     visuals (key influencers, decomposition tree), large DAX models,
     drillthrough, bookmarks, R/Python visuals, and every visual type
     Desktop ships.

The `--extended` set is what makes the persistence and fidelity suites
meaningful: a converter bug that only shows up on a decomposition tree or a
1000-measure model is invisible against four dashboards.

Usage:
  python scripts/download_test_corpus.py                # core 4 + extended
  python scripts/download_test_corpus.py --core-only    # just the original 4
  python scripts/download_test_corpus.py --all-samples  # every MS sample

After downloading:
  PBIX_TEST_SAMPLES=test_corpus pytest -v
"""

import argparse
import sys
import zipfile
from pathlib import Path
from urllib.request import urlopen, urlretrieve

COMMUNITY_REPO = "https://github.com/Dashboard-Design/Power-BI-Design-Files"
COMMUNITY_ZIP = f"{COMMUNITY_REPO}/archive/refs/heads/main.zip"

MS_REPO = "https://github.com/microsoft/powerbi-desktop-samples"
MS_RAW = "https://raw.githubusercontent.com/microsoft/powerbi-desktop-samples/main/"

# The original four. Two are PBIR, two are classic Report/Layout.
DASHBOARDS = {
    "GeoSales_Dashboard.pbix": "Full Dashboards/GeoSales Dashboard - Azure Map/GeoSales Dashboard - Azure Map.pbix",
    "Agents_Performance.pbix": "Full Dashboards/Agents Performance - Dashboard/Agents Performance - Dashboard.pbix",
    "Ecommerce_Conversion.pbix": "Full Dashboards/Ecommerce Conversion Dashboard/Ecommerce Conversion Dashboard.pbix",
    "IT_Support.pbix": "Full Dashboards/IT Support Performance Dashboard/IT_Support_Ticket_Desk.pbix",
}

# Microsoft samples, chosen for feature coverage rather than volume. The
# comment on each says what it is here to exercise.
MS_SAMPLES = {
    # AI visuals: key influencers + decomposition tree
    "MS_AI_Sample.pbix": "Sample Reports/Artificial Intelligence Sample.pbix",
    # Microsoft's showcase report — most visual types, bookmarks, drillthrough,
    # tooltip pages, custom shapes
    "MS_Sales_Returns.pbix": "Sample Reports/Sales & Returns Sample v201912.pbix",
    # Large DAX model, many measures and calculated columns
    "MS_AdventureWorks_DW.pbix": "DAX/Adventure Works DW 2020.pbix",
    # Canonical star schema
    "MS_AdventureWorks_Sales.pbix": "AdventureWorks Sales Sample/AdventureWorks Sales.pbix",
    # Service samples — different authoring lineage from Desktop
    "MS_Regional_Sales.pbix": "new-power-bi-service-samples/Regional Sales Sample.pbix",
    "MS_Corporate_Spend.pbix": "new-power-bi-service-samples/Corporate Spend.pbix",
    "MS_Revenue_Opportunities.pbix": "new-power-bi-service-samples/Revenue Opportunities.pbix",
    "MS_Competitive_Marketing.pbix": "new-power-bi-service-samples/Competitive Marketing Analysis.pbix",
    "MS_Employee_Hiring.pbix": "new-power-bi-service-samples/Employee Hiring and History.pbix",
    "MS_Store_Sales.pbix": "new-power-bi-service-samples/Store Sales.pbix",
    # Varied real-world shapes
    "MS_Supply_Chain.pbix": "Sample Reports/Supply Chain Sample.pbix",
    "MS_Human_Resources.pbix": "Sample Reports/Human Resources Sample PBIX.pbix",
    "MS_Covid_Tracking.pbix": "Sample Reports/COVID-19 US Tracking Sample.pbix",
    "MS_Life_Expectancy.pbix": "Sample Reports/Life expectancy v202009.pbix",
    # Small but unusual: exported performance-analyzer report
    "MS_Perf_Analyzer.pbix": "Performance Analyzer/PerformanceAnalyzerExportReport.pbix",
    # Feature demos from the monthly blog series
    "MS_Blog_DataProfiling.pbix": "Monthly Desktop Blog Samples/2018/2018SU10 Data Profiling Demo - October.pbix",
    "MS_Blog_FuzzyMatching.pbix": "Monthly Desktop Blog Samples/2018/2018SU10 Fuzzy Matching Demo - October.pbix",
    "MS_Blog_CustomerFeedback.pbix": "Monthly Desktop Blog Samples/2019/customerfeedback.pbix",
    "MS_Blog_2020_Nov.pbix": "Monthly Desktop Blog Samples/2020/2020SU11 Blog Demo - November.pbix",
    "MS_Blog_2020_Sep.pbix": "Monthly Desktop Blog Samples/2020/2020SU09 Blog Demo - September.pbix",
}


def _download_community(out: Path) -> int:
    missing = [n for n in DASHBOARDS if not (out / n).exists()]
    if not missing:
        print(f"community: all {len(DASHBOARDS)} present")
        return 0
    print(f"community: downloading {COMMUNITY_REPO} ...")
    zip_path = out / "_repo.zip"
    urlretrieve(COMMUNITY_ZIP, zip_path)
    got = 0
    with zipfile.ZipFile(zip_path) as zf:
        tops = {n.split("/")[0] for n in zf.namelist() if "/" in n}
        prefix = tops.pop() if len(tops) == 1 else "Power-BI-Design-Files-main"
        for local, repo_path in DASHBOARDS.items():
            if (out / local).exists():
                continue
            try:
                data = zf.read(f"{prefix}/{repo_path}")
            except KeyError:
                print(f"  x {local} — not found")
                continue
            (out / local).write_bytes(data)
            print(f"  + {local} ({len(data)/1e6:.1f} MB)")
            got += 1
    zip_path.unlink()
    return got


def _list_all_ms_samples() -> dict:
    """Every .pbix in the Microsoft repo, keyed by a flattened local name."""
    import json

    api = ("https://api.github.com/repos/microsoft/"
           "powerbi-desktop-samples/git/trees/main?recursive=1")
    with urlopen(api, timeout=60) as r:  # noqa: S310
        tree = json.load(r).get("tree", [])
    out = {}
    seen = set()
    for node in tree:
        p = node.get("path", "")
        if not p.lower().endswith(".pbix"):
            continue
        stem = Path(p).stem
        if stem in seen:      # the service samples are duplicated in two dirs
            continue
        seen.add(stem)
        local = "MS_" + "".join(
            c if c.isalnum() else "_" for c in stem).strip("_") + ".pbix"
        out[local] = p
    return out


def _download_ms(out: Path, samples: dict) -> int:
    got = 0
    for local, repo_path in samples.items():
        target = out / local
        if target.exists():
            continue
        url = MS_RAW + repo_path.replace(" ", "%20").replace("&", "%26")
        try:
            with urlopen(url, timeout=300) as r:  # noqa: S310
                data = r.read()
        except Exception as exc:
            print(f"  x {local} — {type(exc).__name__}: {str(exc)[:60]}")
            continue
        if not data.startswith(b"PK"):
            print(f"  x {local} — not a zip ({len(data)} bytes)")
            continue
        target.write_bytes(data)
        print(f"  + {local} ({len(data)/1e6:.1f} MB)")
        got += 1
    return got


def download_corpus(output_dir: str = "test_corpus", core_only: bool = False,
                    all_samples: bool = False) -> None:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    _download_community(out)
    if not core_only:
        samples = _list_all_ms_samples() if all_samples else MS_SAMPLES
        print(f"\nmicrosoft: {len(samples)} sample(s) from {MS_REPO}")
        _download_ms(out, samples)

    files = sorted(p for p in out.glob("*.pbix"))
    total = sum(p.stat().st_size for p in files)
    (out / "README.md").write_text(
        "# Test Corpus\n\n"
        f"{len(files)} Power BI reports, {total/1e6:.0f} MB.\n\n"
        "## Sources\n\n"
        f"- [{COMMUNITY_REPO}]({COMMUNITY_REPO}) — MIT, "
        "Copyright (c) 2024 Sajjad Ahmadi\n"
        f"- [{MS_REPO}]({MS_REPO}) — MIT, Copyright (c) Microsoft Corporation\n\n"
        "Downloaded by `python scripts/download_test_corpus.py`. "
        "Not committed — this directory is gitignored.\n\n"
        "```bash\nPBIX_TEST_SAMPLES=test_corpus pytest -v\n```\n"
    )
    print(f"\n{len(files)} report(s), {total/1e6:.0f} MB in {out}/")
    print(f"Set PBIX_TEST_SAMPLES={output_dir} to run the integration suites.")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--output-dir", default="test_corpus")
    ap.add_argument("--core-only", action="store_true",
                    help="only the four community dashboards")
    ap.add_argument("--all-samples", action="store_true",
                    help="every .pbix in the Microsoft repo (~200 MB)")
    args = ap.parse_args()
    download_corpus(args.output_dir, args.core_only, args.all_samples)
    return 0


if __name__ == "__main__":
    sys.exit(main())
