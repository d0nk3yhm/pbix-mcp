"""Issue #130 (reported by @allanon2 in PR #123): PBIXBuilder writes the report
setting every Desktop-saved report carries, useStylableVisualContainerHeader
("Use the modern visual header with updated styling options").

All 26 Desktop-authored community reports of the local corpus have it. Power
BI Desktop 2.152 draws a report built without it in the legacy visual
container: the same built report with only the setting added renders its
visuals' content ~13-18 screen px higher.
"""
from __future__ import annotations

import json
import zipfile

import pytest

from pbix_mcp.builder import PBIXBuilder

pytestmark = pytest.mark.unit


def test_built_report_has_the_modern_visual_header(tmp_path):
    b = PBIXBuilder("hdr")
    b.add_table("T", [{"name": "v", "data_type": "Int64"}], rows=[{"v": 1}])
    b.add_page("Page 1")
    p = tmp_path / "hdr.pbix"
    b.save(str(p))
    with zipfile.ZipFile(p) as z:
        layout = json.loads(z.read("Report/Layout").decode("utf-16-le"))
    settings = json.loads(layout["config"])["settings"]
    assert settings == {"useNewFilterPaneExperience": True, "allowChangeFilterTypes": True,
                        "useStylableVisualContainerHeader": True}
