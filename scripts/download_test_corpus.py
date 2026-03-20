#!/usr/bin/env python3
"""Download the public PBIX test corpus for integration testing.

All 4 dashboards come from one MIT-licensed public repository:
  https://github.com/Dashboard-Design/Power-BI-Design-Files
  Copyright (c) 2024 Sajjad Ahmadi — MIT License

Usage:
  python scripts/download_test_corpus.py [--output-dir test_corpus]

After downloading, run integration tests:
  PBIX_TEST_SAMPLES=test_corpus pytest tests/test_cross_report.py -v
"""

import argparse
import os
import sys
import zipfile
from pathlib import Path
from urllib.request import urlretrieve

REPO_URL = "https://github.com/Dashboard-Design/Power-BI-Design-Files"
ARCHIVE_URL = f"{REPO_URL}/archive/refs/heads/main.zip"

# Paths inside the zip archive (relative to the repo root)
DASHBOARDS = {
    "GeoSales_Dashboard.pbix": "Full Dashboards/GeoSales Dashboard - Azure Map/GeoSales Dashboard - Azure Map.pbix",
    "Agents_Performance.pbix": "Full Dashboards/Agents Performance - Dashboard/Agents Performance - Dashboard.pbix",
    "Ecommerce_Conversion.pbix": "Full Dashboards/Ecommerce Conversion Dashboard/Ecommerce Conversion Dashboard.pbix",
    "IT_Support.pbix": "Full Dashboards/IT Support Performance Dashboard/IT_Support_Ticket_Desk.pbix",
}


def download_corpus(output_dir: str = "test_corpus") -> None:
    """Download and extract the 4 test dashboards."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # Check if already downloaded
    existing = [name for name in DASHBOARDS if (out / name).exists()]
    if len(existing) == len(DASHBOARDS):
        print(f"All {len(DASHBOARDS)} dashboards already present in {out}/")
        return

    print(f"Downloading from {REPO_URL} ...")
    zip_path = out / "repo.zip"
    urlretrieve(ARCHIVE_URL, zip_path)
    print(f"Downloaded {zip_path.stat().st_size / 1024 / 1024:.1f} MB")

    print("Extracting dashboards...")
    with zipfile.ZipFile(zip_path, "r") as zf:
        # The zip contains a top-level folder like "Power-BI-Design-Files-main/"
        top_dirs = {name.split("/")[0] for name in zf.namelist() if "/" in name}
        prefix = top_dirs.pop() if len(top_dirs) == 1 else "Power-BI-Design-Files-main"

        for local_name, repo_path in DASHBOARDS.items():
            zip_entry = f"{prefix}/{repo_path}"
            try:
                data = zf.read(zip_entry)
                target = out / local_name
                target.write_bytes(data)
                print(f"  ✓ {local_name} ({len(data) / 1024:.0f} KB)")
            except KeyError:
                print(f"  ✗ {local_name} — not found at {zip_entry}")

    # Clean up zip
    zip_path.unlink()

    # Write a README for the corpus
    readme = out / "README.md"
    readme.write_text(
        f"# Test Corpus\n\n"
        f"4 public Power BI dashboards from [{REPO_URL}]({REPO_URL}).\n\n"
        f"**License:** MIT (Copyright (c) 2024 Sajjad Ahmadi)\n\n"
        f"**Downloaded by:** `python scripts/download_test_corpus.py`\n\n"
        f"**Usage:**\n```bash\n"
        f"PBIX_TEST_SAMPLES={output_dir} pytest tests/test_cross_report.py -v\n"
        f"```\n"
    )

    print(f"\nDone. Set PBIX_TEST_SAMPLES={output_dir} to run integration tests.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default="test_corpus", help="Directory to save dashboards")
    args = parser.parse_args()
    download_corpus(args.output_dir)


if __name__ == "__main__":
    main()
