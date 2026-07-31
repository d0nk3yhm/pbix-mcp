"""Guards against two report-render failures found by looking at Desktop (not
just the model): a missing themeCollection crashes Desktop's renderer, and a
mis-shaped visual config silently produces an empty visual.

Desktop root cause: with no ``config.themeCollection`` the renderer throws
``Cannot read properties of undefined (reading 'customTheme')`` and the whole
report fails to render (the model still loads, which is why a model-only
ADOMD check missed it). A builder-produced report carrying the base-theme
reference renders the bar chart with data (Desktop-verified by screenshot).
"""
from __future__ import annotations

import io
import json
import warnings
import zipfile

from pbix_mcp.builder import PBIXBuilder


def _layout(data: bytes) -> dict:
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        return json.loads(z.read("Report/Layout").decode("utf-16-le"))


def _report_with_bar(cfg: dict) -> bytes:
    b = PBIXBuilder("RenderTest")
    b.add_table("Sales",
                [{"name": "Category", "data_type": "String"},
                 {"name": "Amount", "data_type": "Double"}],
                rows=[{"Category": "Bikes", "Amount": 1200.0},
                      {"Category": "Tires", "Amount": 450.0}])
    b.add_measure("Sales", "Total Sales", "SUM(Sales[Amount])")
    b.add_page("Overview", visuals=[{"name": "v", "type": "barChart",
                                     "config": cfg}])
    return b.build()


class TestReportRenders:
    def test_layout_carries_theme_collection(self):
        """The render-crash guard: config MUST have themeCollection."""
        cfg = {"category": {"table": "Sales", "column": "Category"},
               "measure": "Total Sales"}
        layout = _layout(_report_with_bar(cfg))
        report_cfg = json.loads(layout["config"])
        assert "themeCollection" in report_cfg, report_cfg.keys()
        assert report_cfg["themeCollection"]["baseTheme"]["name"]

    def test_correct_config_binds_the_visual(self):
        cfg = {"category": {"table": "Sales", "column": "Category"},
               "measure": "Total Sales"}
        layout = _layout(_report_with_bar(cfg))
        vc = layout["sections"][0]["visualContainers"][0]
        sv = json.loads(vc["config"])["singleVisual"]
        assert sv["projections"]["Category"], sv.get("projections")
        assert sv["projections"]["Y"], sv.get("projections")
        assert "query" in vc and "dataTransforms" in vc

    def test_misshaped_config_warns_not_silent(self):
        """A field-naming config that matches no shape must WARN, not ship an
        empty visual silently."""
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            # the exact wrong shape that produced the empty visual in Desktop
            _report_with_bar({"category": "Sales.Category",
                              "values": ["Sales.Amount"]})
        msgs = [str(w.message) for w in caught]
        assert any("matched no binding shape" in m for m in msgs), msgs
