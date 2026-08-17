# Capability parity: pbix-mcp vs Power BI

An audit of what Power BI can author against what pbix-mcp exposes as tools,
written for consumers (OpenBI) that need to know which operations are a single
call, which need raw-JSON escape hatches, and which are not supported at all.

Audited at **0.9.39 / 126 tools**. Re-run the inventory with:

```bash
python -c "from pbix_mcp import server; print(len([n for n in dir(server) if n.startswith('pbix_')]))"
```

Both storage formats are covered by the same tools: classic `Report/Layout` and
the PBIR `Report/definition/` tree the service produces. Tools go through
`_get_layout`/`_set_layout`, which normalize the format differences (see
"Format normalization" below), so callers do not branch on format. Check with
`pbix_report_format`.

## Report authoring

| Power BI operation | pbix-mcp | Notes |
|---|---|---|
| Add / remove page | ✅ `pbix_add_page`, `pbix_remove_page` | |
| Rename page | ✅ `pbix_rename_page` | Internal `name` preserved — bookmarks and page navigation keep working |
| Reorder pages | ✅ `pbix_reorder_pages` | Unlisted pages keep their relative order |
| Hide / show page | ✅ `pbix_set_page_visibility` | |
| Duplicate page | ✅ `pbix_duplicate_page` | Page and every visual get fresh identities |
| Add / remove visual | ✅ `pbix_add_visual`, `pbix_remove_visual` | |
| Move / resize visual | ✅ `pbix_move_visual` | Geometry lives on the container, not in config — `pbix_set_visual_property` cannot reach it |
| Duplicate / copy visual across pages | ✅ `pbix_duplicate_visual` | |
| Format a visual | ✅ `pbix_format_visual`, `pbix_set_visual_property`, `pbix_update_visual_json` | |
| Sort a visual | ✅ `pbix_set_visual_sort` | |
| Bookmarks | ✅ `pbix_add_bookmark`, `pbix_get_bookmarks`, `pbix_remove_bookmark` | Persisted to `definition/bookmarks/` on PBIR. Captures visual data state (slicer selections, visual filters); `clear_selections` authors a Clear-all bookmark. Both tools return the internal `name` a button's bookmark action needs |
| Report / page filters | ✅ `pbix_get_filters`, `pbix_set_filters` | |
| Themes | ✅ `pbix_get_theme`, `pbix_set_theme`, `pbix_recolor`, `pbix_extract_colors` | |
| Images and resources | ✅ `pbix_add_image`, `pbix_set_image`, `pbix_register_resource`, `pbix_list_resources` | |
| Custom visuals | ✅ `pbix_add_custom_visual`, `pbix_remove_custom_visual`, `pbix_reference_public_visual` | |
| HTML / SVG visuals | ✅ `pbix_add_html_visual`, `pbix_set_html_visual`, `pbix_html_template`, `pbix_svg_measure` | |
| Report settings | ✅ `pbix_get_settings`, `pbix_set_settings` | |
| Group visuals | ⚠️ read-only | `pbix_get_visual_positions` resolves group offsets; creating a group needs `pbix_update_visual_json` |
| Container formatting (title, background, border) | ✅ `pbix_format_visual`, `pbix_set_visual_property` | Round-trips on both formats (`vcObjects` ↔ `visual.visualContainerObjects`) |
| Edit visual interactions | ⚠️ raw JSON | PBIR models this as `page.visualInteractions`; no dedicated tool |
| Mark page as tooltip / drillthrough | ⚠️ raw JSON | Readable (`section["type"]`), no dedicated setter |
| Sync slicers | ⚠️ raw JSON | Sync groups are read and round-tripped, not authored |

## Model authoring

| Power BI operation | pbix-mcp | Notes |
|---|---|---|
| Measures | ✅ add / modify / remove / set category | |
| Calculated columns | ✅ `pbix_datamodel_add_calculated_column` | Values materialized |
| Calculated tables | ✅ `pbix_datamodel_add_calculated_table` | Rows materialized |
| Relationships | ✅ add / modify / remove | |
| Hierarchies | ✅ `pbix_add_hierarchy`, `pbix_remove_hierarchy` | |
| Calculation groups | ✅ `pbix_datamodel_add_calculation_group` | |
| Field parameters | ✅ `pbix_datamodel_add_field_parameter` | |
| Perspectives | ✅ add / get / remove | |
| Translations / cultures | ✅ add / get / remove | |
| RLS | ✅ `pbix_set_rls_role`, `pbix_get_rls_roles`, `pbix_evaluate_rls` | |
| Incremental refresh | ✅ get / set | |
| Partitions | ✅ `pbix_add_partition`, `pbix_remove_partition`, `pbix_get_partitions` | |
| Power Query (M) | ✅ `pbix_get_m_code`, `pbix_set_m_code`, `pbix_update_data_source` | |
| Table data | ✅ `pbix_set_table_data`, `pbix_update_table_rows`, `pbix_replace_value` | |
| Column properties | ✅ `pbix_datamodel_modify_column` | Any metadata property by name |
| Remove table | ✅ `pbix_datamodel_remove_table` | |
| Add a data table to an existing model | ✅ `pbix_set_table_data` | Creates the table when it doesn't exist. Runs the rebuild path — see "Rebuild-path edits" |
| Sort-by-column | ✅ `pbix_set_sort_by_column`, `pbix_get_sort_by_columns` | Resolves names to IDs; rejects self-sorts and cycles |
| **Rename table / column / measure** | ❌ | No rename tool; renaming must not orphan the DAX and layout references to the old name |
| Report-level measures (live connect) | ❌ | PBIR `reportExtensions.json` is neither read nor written |

## Format normalization

`_get_layout` always returns the classic shape, whichever format the file uses.
Fields where the two formats disagree are converted at both boundaries, so a
caller never branches on format:

| Field | Classic | PBIR | Exposed as |
|---|---|---|---|
| `displayOption` | int (`1`) | enum name (`"FitToPage"`) | int |
| page `visibility` | `config.visibility` int | `visibility` enum name | `config.visibility` int |
| bookmarks | `config.bookmarks` | `definition/bookmarks/*.bookmark.json` | `config.bookmarks` |
| container formatting | `singleVisual.vcObjects` | `visual.visualContainerObjects` | `singleVisual.vcObjects` |
| visual sort | `prototypeQuery.OrderBy` | `visual.query.sortDefinition` | `prototypeQuery.OrderBy` |
| resources, custom visuals, theme, report filters, settings | Layout top level / `config` | `definition/report.json` | Layout shape |

### Diagnostics

`pbix_doctor` checks report-definition integrity alongside the model and file
layers. Each check corresponds to a defect class this audit found — state a tool
wrote that never reached disk, a reference that stopped resolving, or a
classic-shaped value in a PBIR document:

| check | catches |
|---|---|
| Registered resources | a file under `RegisteredResources/` with no `report.json` entry (it never renders), or an entry whose file is gone |
| Custom visual registration | a visual type registered in neither `publicCustomVisuals` nor `Report/CustomVisuals/` (warns — it may come from the org store) |
| Page / visual naming | duplicate page names, unnamed visuals, visual names duplicated **within** a page (across pages is legitimate) |
| Bookmark references | bookmark steps pointing at a deleted page or visual (warns — Power BI tolerates it) |
| PBIR page tree | `pageOrder` / `activePageName` / folder / `page.json` name disagreement |
| PBIR visual tree | `visual.json` name vs its folder |
| PBIR naming convention | folder names Desktop would ignore as private files |
| PBIR classic-shape leaks | `singleVisual` / `vcObjects` / `prototypeQuery` written into a PBIR file |
| PBIR enum fields | the `displayOption` class — a classic int where PBIR needs the enum name |

Failures (❌) are things Power BI will reject; warnings (⚠️) are things it
tolerates but a human probably wants to know about. The split is calibrated
against the corpus: across 24 real reports the checks produce **0 failures and
5 warnings**, all of them genuine stale bookmark references in Microsoft's own
samples.

### How this is verified

The report-affecting tools present at the 0.9.39 audit are tested on both formats
by applying the tool, saving, reopening and checking the **saved bytes**, with a
negative control proving each check fails on an untouched file. As of 0.9.39, of
the tools categorized: 50 persist, 59 are read-only, 2 are not applicable to the
fixtures, and 0 lose their change.

Beyond that, the corpus of **24 real reports** is exercised directly: a no-op
read/write cycle must leave every report definition byte-identical (24/24), and
a matrix of 10 editing tools x 24 reports must persist every change (240/240).

`scripts/validate_pbir_schemas.py` is a necessary but **not sufficient** check —
a file can be perfectly schema-valid and still be missing state a tool claimed
to write. Persistence is verified separately, in `tests/test_pbir_roundtrip.py`.

**Known coverage gap.** `pbix_set_m_code` and `pbix_set_incremental_refresh`
operate on the `DataMashup` part, and no file in the public test corpus carries
one — so they are exercised by unit tests but not by the end-to-end persistence
sweep. They are reported as *not applicable* rather than *passing*, because an
untested tool should not be counted as verified.

Validate what the writer emits against Microsoft's published schemas with
`scripts/validate_pbir_schemas.py` (see `docs/development.md`).

The writer also self-checks every PBIR document before writing it, offline: the
fields PBIR types as string enums must carry the enum NAME, and required fields
must be present. This exists because a page written with the classic integer
`displayOption` produced a .pbix that the service **imported successfully and
then refused to open** — the failure surfaced only on upload, with nothing in
the local pipeline flagging it.

## Rebuild-path edits

Some model edits (adding or replacing a table, adding or removing a
relationship, removing a table) reconstruct the whole model rather than
splicing metadata. A from-scratch rebuild loses Type=2 calculated columns and
demotes calculated tables to plain data, so before 0.9.37 these edits were
**refused outright** on any model containing either — three of the four
reports in the public corpus.

They now re-materialize the calculated objects as part of the edit: calculated
columns are re-evaluated from their DAX, calculated tables keep their rows and
their `Type=2` partition + `QueryDefinition`, so Power BI still recomputes both
on refresh. All four corpus reports accept these edits with their calculated
objects byte-identical afterwards.

The refusal remains where it is the right answer: if the engine cannot
reproduce an existing calculated column or table, the edit is refused rather
than written with wrong values. The surgical tools
(`pbix_datamodel_add_measure` / `modify_measure` / `remove_measure` /
`modify_column` / `set_sort_by_column`) never rebuild and always work.

## Known gaps, in priority order

1. **Rename model objects.** A rename has to rewrite every DAX expression and
   layout binding that references the old name, or it silently breaks the
   report — that dependency rewrite is the actual work, not the metadata edit.
2. **Report-level measures.** Needed for live-connect reports, where measures
   live in `reportExtensions.json` rather than the model.
3. **Visual interactions and visual groups.** Both are reachable today through
   `pbix_update_visual_json`; dedicated tools would remove the raw-JSON step.

Nothing in this list is blocked — each is scoped work, listed in the order that
buys a report editor the most.
