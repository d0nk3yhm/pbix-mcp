#!/usr/bin/env python3
"""Create a demo PBIX report using the template's financials data.

The template PBIX has a 'financials' table with columns:
  Segment, Country, Product, Discount Band, Units Sold,
  Manufacturing Price, Sale Price, Gross Sales, Discounts,
  Sales, COGS, Profit, Date

This script creates custom measures and a 2-page layout that
references these existing columns. The result opens in Power BI
Desktop with actual data and working visuals.

Usage:
    python scripts/create_demo_report.py [output_path]
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from pbix_mcp.builder import PBIXBuilder


def create_demo_report(output_path: str = "demo_report.pbix") -> str:
    builder = PBIXBuilder(name="Financial Analytics")

    # =========================================================================
    # Measures — reference the template's financials table columns
    # =========================================================================
    measures = [
        ("financials", "Total Revenue", "SUM(financials[ Sales])"),
        ("financials", "Total Profit", "SUM(financials[Profit])"),
        ("financials", "Total Units", "SUM(financials[Units Sold])"),
        ("financials", "Profit Margin", "DIVIDE([Total Profit], [Total Revenue], 0)"),
        ("financials", "Avg Sale Price", "AVERAGE(financials[Sale Price])"),
        ("financials", "Order Count", "COUNTROWS(financials)"),
        ("financials", "Avg Discount", "AVERAGE(financials[Discounts])"),
        ("financials", "Revenue per Unit", "DIVIDE([Total Revenue], [Total Units], 0)"),
        ("financials", "Total COGS", "SUM(financials[COGS])"),
        ("financials", "Gross Margin", "DIVIDE([Total Revenue] - [Total COGS], [Total Revenue], 0)"),
    ]
    for table, name, expr in measures:
        builder.add_measure(table, name, expr)

    # =========================================================================
    # Page 1: Financial Overview
    # =========================================================================
    builder.add_page("Financial Overview", [
        {
            "type": "textbox",
            "x": 20, "y": 10, "width": 600, "height": 50,
            "config": {"text": "Financial Performance Dashboard"},
        },
        # KPI Cards
        {
            "type": "card",
            "x": 20, "y": 70, "width": 200, "height": 100,
            "config": {"measure": "Total Revenue"},
        },
        {
            "type": "card",
            "x": 240, "y": 70, "width": 200, "height": 100,
            "config": {"measure": "Total Profit"},
        },
        {
            "type": "card",
            "x": 460, "y": 70, "width": 200, "height": 100,
            "config": {"measure": "Order Count"},
        },
        {
            "type": "card",
            "x": 680, "y": 70, "width": 200, "height": 100,
            "config": {"measure": "Profit Margin"},
        },
        # Bar chart: Revenue by Country
        {
            "type": "clusteredBarChart",
            "x": 20, "y": 190, "width": 420, "height": 300,
            "config": {
                "category": {"table": "financials", "column": "Country"},
                "measure": "Total Revenue",
            },
        },
        # Table: Products
        {
            "type": "tableEx",
            "x": 460, "y": 190, "width": 420, "height": 300,
            "config": {
                "columns": [
                    {"table": "financials", "column": "Product"},
                    {"measure": "Total Revenue"},
                    {"measure": "Total Profit"},
                ],
            },
        },
        # Slicer: Segment
        {
            "type": "slicer",
            "x": 900, "y": 70, "width": 150, "height": 100,
            "config": {"column": {"table": "financials", "column": "Segment"}},
        },
    ])

    # Page 2: Regional Analysis
    builder.add_page("Regional Analysis", [
        {
            "type": "textbox",
            "x": 20, "y": 10, "width": 600, "height": 50,
            "config": {"text": "Regional Performance"},
        },
        {
            "type": "clusteredBarChart",
            "x": 20, "y": 70, "width": 500, "height": 350,
            "config": {
                "category": {"table": "financials", "column": "Country"},
                "measure": "Total Profit",
            },
        },
        {
            "type": "donutChart",
            "x": 540, "y": 70, "width": 400, "height": 350,
            "config": {
                "category": {"table": "financials", "column": "Segment"},
                "measure": "Total Revenue",
            },
        },
    ])

    builder.save(output_path)
    return output_path


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "demo_report.pbix"
    path = create_demo_report(out)
    size = os.path.getsize(path)
    print(f"Created: {path} ({size:,} bytes)")
    print(f"\nContents:")
    print(f"  - Template: financials table (Segment, Country, Product, Sales, Profit, etc.)")
    print(f"  - 10 custom DAX measures (Revenue, Profit, Margin, Units, etc.)")
    print(f"  - 2 pages: Financial Overview (8 visuals), Regional Analysis (3 visuals)")
    print(f"\nOpen in Power BI Desktop to view with actual data.")
