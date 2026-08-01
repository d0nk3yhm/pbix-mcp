# OpenBI findings ledger

Every engine issue OpenBI has reported lives in that project's
`docs/pbix-mcp-issues-<N>.md` files. This ledger is the audited status of all
of them, so an item cannot quietly sit unimplemented -- which is exactly what
happened to findings #18 (fixed in 0.9.59, after being live for four releases).

**Audited 2026-07-30 against 0.9.59.** 132 actionable items across docs 1-19:
**85 implemented**, 27 not actionable (pure
confirmation, nothing to do), **17 still open**. A second,
adversarial pass refuted 3 of the 20 items the first pass
flagged, so the open list is verified rather than merely suspected. Docs #18/#19
were audited unlabelled as a control and correctly came back closed.

Re-run the audit before claiming the queue is clear. `gh issue list` covers only
the GitHub tracker; these docs are a separate stream.

## Open: silently wrong output

These produce a plausible answer with no error, which is why they survived so long.

- [x] **issues-9** -- CALCULATE boolean filter args other than `Col = value` were silently dropped (fixed 0.9.60, Desktop-verified). The `IN` operator is
      implemented and used by CALCULATE but deliberately NOT enabled for
      general expressions.
      **CORRECTION (2026-07-30): the stated reason was wrong.** It was recorded
      here, and in `TestInMachinery`, that enabling `IN` "unmasks an inaccurate
      RANKX/TOPN chain". Measured against Desktop on Agents_Performance, the
      chain is NOT inaccurate: `[MTD Total Sales] @ StoreType=Catalog` is
      1783540.7792 in Desktop and identical here, and the non-blank-MTD employee
      count is 1 in both. What is wrong is REVERSE FILTER PROPAGATION -- our
      single-hop relationship index is symmetric, so filtering the many side
      restricts the one side, and `SELECTEDVALUE(DimEmployee[EmployeeKey])`
      answers 213 where Desktop answers BLANK. That is what makes the measure
      return 1 instead of 0. RESOLVED in 0.9.63: propagation now follows TABLE
      EXPANSION (a column filter propagates one -> many only; a table filter
      argument's keys may also ride many -> one), and BOTH anchors hold --
      Agents_Performance is 408/408 with [Actives] = 32,401. Whether `IN` can
      now be enabled generally is a SEPARATE question, still untested.
- [x] **issues-3** -- set_visual_property / update_visual_json on a CLASSIC layout now recompile query+dataTransforms (0.9.61-dev, shipped in 0.9.62), so Desktop no longer renders the old field
- [x] **issues-7** -- CLOSED (0.9.68). Two layers: unknown TOP-LEVEL names
      already raised DAXMeasureNotFoundError with close-match hints; the
      remaining hole was IN-EXPRESSION references -- `[Nope] + 1` answered 1
      with status "ok". A bare [Name] that is neither a measure, a
      row/extension-column key, nor a model column now raises (same rule as
      the qualified Table[Name] path), and pbix_evaluate_dax reports status
      "error" with the message. Corpus re-verified (22/22, 41/41, 102/102).
- [x] **issues-9** -- default-filter behaviour differs between pbix_evaluate_dax and pbix_evaluate_dax_per_dimension.
      NOT A DEFECT: the divergence is a deliberate contract (a per-dimension
      sweep is normally asked against the raw model) and is pinned by tests.
      The real gap was documentation -- `pbix_evaluate_dax_grouped` carried no
      NOTE. Added in the 0.9.61-dev cycle (shipped in 0.9.62); all three tools now say so in their own docstring.
- [x] **issues-15** -- residual COALESCE(ExplicitName, InferredName) instances (fixed in the 0.9.61-dev cycle, shipped in 0.9.62). Four sites audited and corrected: `pbix_get_hierarchies`'
      level query, `_report_type_resolver`'s Type IN (1,2,4) scan,
      `_detect_field_parameter_shape` (whose `ExplicitName NOT LIKE` also
      dropped the NULL rows outright), and the perspective column list.
- [x] **issues-5** -- CLOSED (0.9.68). Latitude/Longitude field wells and
      lat/long-named numeric columns in value/X roles now compile to
      Aggregation Function=1 (Avg), matching Desktop's geographic default
      summarization; all other value-role behavior unchanged.

- [x] **issues-9 (follow-on)** -- CLOSED. Agents_Performance matches Desktop on
      all 102 measures, `CALCULATETABLE` is implemented, and the `IN` operator is
      enabled generally. The corpus-wide Desktop diff that followed (0.9.61)
      took every other file to 1:1 as well.

## Open: Desktop fidelity

Our output loads and renders, but differs field-for-field from a Desktop-authored file.

- [x] **issues-3** -- CLOSED (0.9.69). add_visual, add_html_visual and the
      builder now stamp Desktop's 1000-step `z` + `tabOrder = z + 1000` on
      the container AND config.layouts position (add_image's verified
      pattern). Desktop-load + query verified on a freshly built file.
- [x] **issues-4** -- CLOSED (0.9.69). The builder's Report/Layout now
      carries a report-level config with `version` (5.61, the corpus-era
      schema version), `activeSectionIndex`, `linguisticSchemaSyncVersion`,
      `defaultDrillFilterOtherVisuals` and filter-pane settings, matching
      Desktop-authored files (ground truth MS_AI_Sample / GeoSales).
      Desktop-load verified.
- [x] **issues-8 (table audit)** -- CLOSED (0.9.70-dev). Field-for-field vs
      the GeoSales Desktop-authored tableEx: container keys match (incl.
      tabOrder, query, dataTransforms, filters); the two structural gaps --
      config.layouts written only for image visuals, and
      drillFilterOtherVisuals not defaulted -- are fixed in pbix_add_visual
      for every type. The remaining differences (objects.columnFormatting /
      columnHeaders / grid / total, vcObjects styling, columnProperties
      display names, hasDefaultSort) are user/theme content, accepted via
      config_json / pbix_format_visual, not defaults to invent.
- [x] **issues-8 (offline behaviour)** -- CLOSED as documentation
      (0.9.70-dev). Desktop resolves `publicCustomVisuals` GUIDs from
      AppSource at report open and keeps a per-machine cache
      (`%LOCALAPPDATA%\Microsoft\Power BI Desktop\ExtensionCache`,
      hashed entries; `CertifiedExtensions` holds connector .pqx, not
      visuals). Offline with a cold cache the report still OPENS -- the
      referenced visual's container renders Desktop's unavailable-visual
      placeholder while every other visual and the model work normally;
      once the visual has been fetched on any online open, the cache
      serves it offline. NOT verified by a live network-isolated run on
      this machine (blocking Desktop's network needs firewall changes
      out of scope for this environment); the cache-directory facts are
      verified locally, the placeholder behaviour is Power BI's
      documented/standard handling. pbix_reference_public_visual's
      docstring now carries the offline note.

## Open: feature asks

Capability requests rather than defects.

- [x] **issues-12 (partition-M setter)** -- CLOSED (0.9.71-dev).
      `pbix_set_partition_m(alias, table, m_expression)` writes
      Partition.QueryDefinition verbatim, metadata-only (cached rows
      untouched; Power BI runs the new M on next Refresh).
- [x] **issues-12 (source on set_table_data)** -- CLOSED (0.9.71-dev).
      Optional `source_json` (same format as pbix_update_data_source)
      applies the partition source right after the rows are written --
      snapshot + repoint in one call; a failed source update reports
      loudly that the rows ARE written.
- [x] **issues-14 (predicates + Top-N)** -- CLOSED (0.9.72). Half A
      (predicate objects: comparison / between / relative-date) was already
      live via make_value_matcher; half B lands now: a filter value of
      {"top_n": {"n": 5, "by": "<measure or Table.Column>",
      "direction": "desc"}} is materialized SERVER-SIDE into a concrete
      In-set (ranked under the other filters, blanks last, stable ties)
      before evaluation, in all three evaluate tools -- the same
      materialization OpenBI performed client-side, moved server-side.
- [x] **issues-17 (matrix)** -- CLOSED as covered-by-composition (0.9.72).
      pbix_evaluate_dax_grouped already takes a COMPOSITE group_by
      ("RowDim.Col,ColDim.Col") and returns one structured row per (row,
      column) combination -- a matrix or series is a client-side pivot of
      that flat result. Recipe documented in the tool docstring.
- [x] **issues-17 (propagation reuse)** -- CLOSED as measured-unnecessary
      (0.9.72). On Agents_Performance (200K fact rows), 50 repeated
      pbix_evaluate_dax calls sharing a filter set cost 6.0 ms/call
      (2.6 ms/call for a second measure under the same set): the shared
      filter-index cache plus the per-context measure memo already make
      repeats near-free. No workload evidence justifies another cache
      layer.
- [x] **issues-13 (auto date/time)** -- CLOSED as wont-do-now (0.9.72).
      Rationale: LocalDateTable_<guid> + Variations are HIDDEN tables the
      user did not author; Desktop generates its own the moment its auto
      date/time option is on (and our built models verify clean in Desktop
      and the service without them -- the whole DAX-parity program ran on
      models that carry none); no downstream user has asked for them
      (checked the downstream-usage record). Date hierarchies that users DO
      author are covered by add_user_hierarchy + a real date table
      (pbix_datamodel_add_calculated_table over CALENDAR()).
- [x] **issues-14 (auto date/time, re-ask)** -- same wont-do-now close as
      issues-13 above.

## Refuted (reported open, actually implemented)

Kept so the same items are not re-litigated:

- **pbix-mcp-issues-10.md** -- §4 Copy the bridge's security posture into the engine tools: magic-byte type sniffing (never extension/header claims), 5
  - implemented at: D:/dependency_tracker/pbix-mcp/src/pbix_mcp/server.py -- magic-byte sniffing `_sniff_image_ext` 3645-3711 + `_sniff_resource_ext` 3714-3731; 5 MB cap 
- **pbix-mcp-issues-13.md** -- Item 4: let filter_context carry structured predicates (comparison / between / relative-date) and a Top-N spec applied n
  - implemented at: Predicates (half A): D:\dependency_tracker\pbix-mcp\src\pbix_mcp\dax\engine.py:506-564 `make_value_matcher` (+ helpers `_compare`:458, `_relative_date
- **pbix-mcp-issues-14.md** -- Item 1.2: extend calculated-COLUMN authoring beyond row context (aggregation / CALCULATE / RELATED), or keep an explicit
  - implemented at: Refusal branch (explicitly permitted by the item): D:\dependency_tracker\pbix-mcp\src\pbix_mcp\server.py:10840-10845 + docstring 10815-10820; test loc

## Recently closed

- **findings-21** -- CLOSED (0.9.77). `pbix_set_table_data` left the report
  unqueryable in Desktop ("Error fetching data for this visual"). Root cause: a
  column type passed under `dataType` (camelCase) or a lowercase name (`int64`)
  was ignored -- only `data_type` was read -- so every column silently defaulted
  to `String`. A numeric column then shipped as text, `SUM()` over it returned
  BLANK, and every measure-bound visual errored while the tool reported success
  and `pbix_query_table` read the (text) rows. Engine-reproduced: `[Val] =
  SUM(S[V])` went 550 -> blank on the call, and returns the correct 3000 with the
  fix. `PBIXBuilder.add_table` now normalizes the type key (`data_type`/`dataType`
  /`type`) and value (case-insensitive) and refuses an unrecognized type rather
  than defaulting to String; the malformed-`columns` payloads that used to leak a
  raw `"string indices must be integers"` TypeError now give a clear shape error.
  `tests/test_set_table_data_typing.py` (10). The secondary "contradictory error
  messages" note in the report is resolved by the same clear-error path.
- **findings-20** -- CLOSED (0.9.73). `RELATED()` inside an iterator ignored
  row context: it resolved to the first *visible* row of the related table for
  every iterated row, so `SUMX(Sales, Sales[Qty] * RELATED(Products[UnitPrice]))`
  gave 839.72 instead of 599.72 (grouped output masked it; grand totals and
  cards were wrong). `_fn_related` now navigates from the current row's FK
  through active relationships (single/multi-hop); `RELATED`/`RELATEDTABLE`
  removed from the FILTER aggregation guard so `FILTER(T, RELATED(...) = v)`
  binds the current row. Desktop-pinned probes (RELATED -> 350/7/1) +
  `tests/test_related_rowcontext.py` (9 cases). Reported by OpenBI's bridge
  suite after the 0.9.55 -> 0.9.72 upgrade.
- **issues-19** -- `pbix_bind_field_parameter` (0.9.57), plus the dangling-OrderBy
  and wells-less-visual fixes (0.9.58). All 8 pieces of the binding diffed
  IDENTICAL against the Desktop-authored artifact in that doc.
- **issues-18** -- assigning `filter_context` now clears the column memo (0.9.59).

