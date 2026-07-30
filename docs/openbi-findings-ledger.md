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
      general expressions -- it unmasks an inaccurate RANKX/TOPN chain that
      then returns confidently wrong values instead of BLANK. See below.
- [x] **issues-3** -- set_visual_property / update_visual_json on a CLASSIC layout now recompile query+dataTransforms (0.9.61), so Desktop no longer renders the old field
- [ ] **issues-7** -- "measure not found" is indistinguishable from a genuine BLANK; needs a typed DAXError
- [x] **issues-9** -- default-filter behaviour differs between pbix_evaluate_dax and pbix_evaluate_dax_per_dimension.
      NOT A DEFECT: the divergence is a deliberate contract (a per-dimension
      sweep is normally asked against the raw model) and is pinned by tests.
      The real gap was documentation -- `pbix_evaluate_dax_grouped` carried no
      NOTE. Added in 0.9.61; all three tools now say so in their own docstring.
- [x] **issues-15** -- residual COALESCE(ExplicitName, InferredName) instances (fixed 0.9.61). Four sites audited and corrected: `pbix_get_hierarchies`'
      level query, `_report_type_resolver`'s Type IN (1,2,4) scan,
      `_detect_field_parameter_shape` (whose `ExplicitName NOT LIKE` also
      dropped the NULL rows outright), and the perspective column list.
- [ ] **issues-5** -- lat/long in X/Y value roles must AVERAGE, not Sum

- [x] **issues-9 (follow-on)** -- CLOSED. Agents_Performance matches Desktop on
      all 102 measures, `CALCULATETABLE` is implemented, and the `IN` operator is
      enabled generally. The corpus-wide Desktop diff that followed (0.9.61)
      took every other file to 1:1 as well.

## Open: Desktop fidelity

Our output loads and renders, but differs field-for-field from a Desktop-authored file.

- [ ] **issues-3** -- `tabOrder` is never written on the visualContainer by add_visual / add_html_visual / builder (add_image does write it)
- [ ] **issues-4** -- report-level config keys `version` / `activeSectionIndex` / `linguisticSchema` are not filled
- [ ] **issues-8** -- verify + document table-visual properties against a Desktop-authored table
- [ ] **issues-8** -- record Desktop's OFFLINE behaviour for a reference-only public custom visual

## Open: feature asks

Capability requests rather than defects.

- [ ] **issues-12** -- a table-scoped partition-M setter, e.g. `pbix_set_partition_m`
- [ ] **issues-12** -- a `source` parameter on `pbix_set_table_data`
- [ ] **issues-14** -- `FilterContext.filters` accepting predicate objects + an optional per-key mode
- [ ] **issues-17** -- a grouped entry point for the matrix (rows x columns) and series
- [ ] **issues-17** -- reuse the relationship-propagation result across calls that share a filter set
- [ ] **issues-13** -- synthesize Desktop's AUTO date/time hierarchy (LocalDateTable_<guid> + Variations)
- [ ] **issues-14** -- same AUTO date-hierarchy generator, asked again

## Refuted (reported open, actually implemented)

Kept so the same items are not re-litigated:

- **pbix-mcp-issues-10.md** -- §4 Copy the bridge's security posture into the engine tools: magic-byte type sniffing (never extension/header claims), 5
  - implemented at: D:/dependency_tracker/pbix-mcp/src/pbix_mcp/server.py -- magic-byte sniffing `_sniff_image_ext` 3645-3711 + `_sniff_resource_ext` 3714-3731; 5 MB cap 
- **pbix-mcp-issues-13.md** -- Item 4: let filter_context carry structured predicates (comparison / between / relative-date) and a Top-N spec applied n
  - implemented at: Predicates (half A): D:\dependency_tracker\pbix-mcp\src\pbix_mcp\dax\engine.py:506-564 `make_value_matcher` (+ helpers `_compare`:458, `_relative_date
- **pbix-mcp-issues-14.md** -- Item 1.2: extend calculated-COLUMN authoring beyond row context (aggregation / CALCULATE / RELATED), or keep an explicit
  - implemented at: Refusal branch (explicitly permitted by the item): D:\dependency_tracker\pbix-mcp\src\pbix_mcp\server.py:10840-10845 + docstring 10815-10820; test loc

## Recently closed

- **issues-19** -- `pbix_bind_field_parameter` (0.9.57), plus the dangling-OrderBy
  and wells-less-visual fixes (0.9.58). All 8 pieces of the binding diffed
  IDENTICAL against the Desktop-authored artifact in that doc.
- **issues-18** -- assigning `filter_context` now clears the column memo (0.9.59).

