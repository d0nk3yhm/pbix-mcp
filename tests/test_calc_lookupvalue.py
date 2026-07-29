"""LOOKUPVALUE and the VAR/RETURN gate fix, for authored calculated columns.

Ground truth for the LOOKUPVALUE semantics here is Power BI Desktop's own
stored values: the three LOOKUPVALUE columns in the corpus
(MS_AI_Sample Accounts[Industry Lookup] and Cases[Topic], MS_Store_Sales
Item[Category (clusters) 2]) all match ours exactly, 374,484 rows in total.
"""
from __future__ import annotations

from pbix_mcp.dax.calc_tables import (
    calc_column_unsupported_reason,
    evaluate_row_context_column,
    lookupvalue_table_names,
)

ACC_COLS = ["IndustrySeq", "Name"]
ACC_ROWS = [[1, "A"], [2, "B"], [9, "C"]]
IND = {"columns": ["IndustrySeq", "Industry"],
       "rows": [[1, "Tech"], [2, "Retail"]]}


def _snap():
    return {"Accounts": {"columns": list(ACC_COLS),
                         "rows": [list(r) for r in ACC_ROWS]},
            "Industries": {"columns": list(IND["columns"]),
                           "rows": [list(r) for r in IND["rows"]]}}


def _known(snap):
    return {t: v["columns"] for t, v in snap.items()}


class TestLookupValue:
    def test_matches_and_blanks_when_absent(self):
        e = ("LOOKUPVALUE(Industries[Industry], Industries[IndustrySeq], "
             "Accounts[IndustrySeq])")
        snap = _snap()
        assert calc_column_unsupported_reason(
            e, "Accounts", ACC_COLS, _known(snap)) is None
        vals, err = evaluate_row_context_column(
            ACC_COLS, ACC_ROWS, e, "Accounts", snap, [])
        assert err is None
        assert vals == ["Tech", "Retail", None]

    def test_alternate_result_is_used_for_a_miss(self):
        e = ('LOOKUPVALUE(Industries[Industry], Industries[IndustrySeq], '
             'Accounts[IndustrySeq], "none")')
        snap = _snap()
        vals, err = evaluate_row_context_column(
            ACC_COLS, ACC_ROWS, e, "Accounts", snap, [])
        assert err is None
        assert vals == ["Tech", "Retail", "none"]

    def test_string_search_is_case_insensitive(self):
        """Power BI models use a case-insensitive collation by default."""
        snap = {"Item": {"columns": ["Cat"], "rows": [["abc"], ["XYZ"]]},
                "Map": {"columns": ["Category", "ClusterId"],
                        "rows": [["ABC", 1], ["xyz", 2]]}}
        e = "LOOKUPVALUE(Map[ClusterId], Map[Category], Item[Cat])"
        assert calc_column_unsupported_reason(
            e, "Item", ["Cat"], _known(snap)) is None
        vals, err = evaluate_row_context_column(
            ["Cat"], [["abc"], ["XYZ"]], e, "Item", snap, [])
        assert err is None
        assert vals == [1, 2]

    def test_self_lookup_with_bare_references(self):
        """Events[ParentIndex] = LOOKUPVALUE([Index], [id], [parentId])."""
        cols = ["Index", "id", "parentId"]
        rows = [[1, "a", None], [2, "b", "a"], [3, "c", "b"]]
        snap = {"Events": {"columns": cols, "rows": [list(r) for r in rows]}}
        e = "LOOKUPVALUE('Events'[Index], 'Events'[id], 'Events'[parentId])"
        assert calc_column_unsupported_reason(
            e, "Events", cols, {"Events": cols}) is None
        vals, err = evaluate_row_context_column(
            cols, rows, e, "Events", snap, [])
        assert err is None
        assert vals == [None, 1, 2]

    def test_ambiguous_match_is_refused_not_guessed(self):
        """Several matching rows with DIFFERENT values is an error in DAX."""
        snap = {"T": {"columns": ["k"], "rows": [[1]]},
                "D": {"columns": ["k", "v"], "rows": [[1, "x"], [1, "y"]]}}
        e = "LOOKUPVALUE(D[v], D[k], T[k])"
        vals, err = evaluate_row_context_column(
            ["k"], [[1]], e, "T", snap, [])
        assert vals is None
        assert "ambiguous" in err

    def test_several_matching_rows_agreeing_is_fine(self):
        snap = {"T": {"columns": ["k"], "rows": [[1]]},
                "D": {"columns": ["k", "v"], "rows": [[1, "x"], [1, "x"]]}}
        e = "LOOKUPVALUE(D[v], D[k], T[k])"
        vals, err = evaluate_row_context_column(["k"], [[1]], e, "T", snap, [])
        assert err is None
        assert vals == ["x"]

    def test_without_known_tables_the_strict_refusal_stands(self):
        """A caller that cannot supply other tables' rows must still refuse."""
        e = ("LOOKUPVALUE(Industries[Industry], Industries[IndustrySeq], "
             "Accounts[IndustrySeq])")
        why = calc_column_unsupported_reason(e, "Accounts", ACC_COLS)
        assert why and "another table 'Industries'" in why

    def test_mixed_tables_are_refused(self):
        snap = _snap()
        snap["Other"] = {"columns": ["IndustrySeq"], "rows": [[1]]}
        e = ("LOOKUPVALUE(Industries[Industry], Other[IndustrySeq], "
             "Accounts[IndustrySeq])")
        why = calc_column_unsupported_reason(
            e, "Accounts", ACC_COLS, _known(snap))
        assert why and "mixes tables" in why

    def test_unknown_column_is_refused(self):
        snap = _snap()
        e = ("LOOKUPVALUE(Industries[Nope], Industries[IndustrySeq], "
             "Accounts[IndustrySeq])")
        why = calc_column_unsupported_reason(
            e, "Accounts", ACC_COLS, _known(snap))
        assert why and "does not have" in why

    def test_unavailable_table_is_refused(self):
        e = ("LOOKUPVALUE(Missing[Industry], Missing[IndustrySeq], "
             "Accounts[IndustrySeq])")
        why = calc_column_unsupported_reason(
            e, "Accounts", ACC_COLS, {"Accounts": ACC_COLS})
        assert why and "not available" in why

    def test_table_names_are_discoverable_for_lazy_loading(self):
        e = ("LOOKUPVALUE(Industries[Industry], Industries[IndustrySeq], "
             "Accounts[IndustrySeq]) & LOOKUPVALUE(P[A], P[B], Accounts[Name])")
        assert lookupvalue_table_names(e) == {"Industries", "Accounts", "P"}


class TestVarReturnIsNotATableReference:
    """`VAR _x = 1 RETURN [Col]` was refused as "references another table
    'RETURN'" -- the bare word before `[` was read as a table name, which
    refused the most common modern DAX idiom outright."""

    def test_bare_column_after_return_is_not_a_table(self):
        """The BARE bracket is the failing shape.

        `RETURN 'E'[Index]` never tripped this -- the quoted branch matches
        first -- so the bug only bit the un-qualified spelling Desktop actually
        stores, e.g. Events[TreeLabel]'s `... RETURN [Index] & _Indent & ...`.
        """
        cols = ["Index", "PathLength"]
        assert calc_column_unsupported_reason(
            "VAR _x = [PathLength] RETURN [Index] & _x", "E", cols) is None

    def test_var_return_evaluates_once_references_are_qualified(self):
        cols = ["Index", "PathLength"]
        rows = [[1, 2], [2, 0]]
        snap = {"E": {"columns": cols, "rows": [list(r) for r in rows]}}
        e = "VAR _i = REPT(\"-\", 'E'[PathLength]) RETURN 'E'[Index] & _i"
        assert calc_column_unsupported_reason(e, "E", cols) is None
        vals, err = evaluate_row_context_column(cols, rows, e, "E", snap, [])
        assert err is None
        assert vals == ["1--", "2"]

    def test_a_real_cross_table_reference_inside_var_is_still_refused(self):
        why = calc_column_unsupported_reason(
            "VAR _x = Other[Col] RETURN _x", "E", ["Index"])
        assert why and "another table 'Other'" in why

    def test_a_quoted_table_named_return_is_still_seen(self):
        """DAX forces reserved words to be quoted, so the quoted branch must
        keep catching them -- skipping the BARE word cannot hide a real ref."""
        why = calc_column_unsupported_reason(
            "'Return'[Col] + 1", "E", ["Index"])
        assert why and "another table 'Return'" in why


REL = [{"FromTable": "Product", "FromColumn": "Category",
        "ToTable": "Table", "ToColumn": "Category", "IsActive": 1}]
PROD = {"columns": ["Category", "Name"],
        "rows": [["Bikes", "B1"], ["Parts", "P1"], ["Zzz", "Q"]]}
DIM = {"columns": ["Category", "Sorting"],
       "rows": [["Bikes", 1], ["Parts", 2]]}


def _rel_snap():
    return {"Product": {"columns": list(PROD["columns"]),
                        "rows": [list(r) for r in PROD["rows"]]},
            "Table": {"columns": list(DIM["columns"]),
                      "rows": [list(r) for r in DIM["rows"]]}}


class TestRelated:
    """RELATED walks many-to-one only, and refuses anything ambiguous.

    Ground truth: MS_AdventureWorks_Sales Product[Sorting] =
    RELATED('Table'[Sorting]) matches Desktop's stored values on all 397 rows.
    """

    def test_single_hop_resolves_and_misses_are_blank(self):
        snap = _rel_snap()
        e = "RELATED('Table'[Sorting])"
        assert calc_column_unsupported_reason(
            e, "Product", PROD["columns"], _known(snap), REL) is None
        vals, err = evaluate_row_context_column(
            PROD["columns"], [list(r) for r in PROD["rows"]], e, "Product",
            snap, REL)
        assert err is None
        assert vals == [1, 2, None]

    def test_multi_hop_chain_resolves(self):
        rels = REL + [{"FromTable": "Table", "FromColumn": "GroupId",
                       "ToTable": "Grp", "ToColumn": "Id", "IsActive": 1}]
        snap = _rel_snap()
        snap["Table"] = {"columns": ["Category", "Sorting", "GroupId"],
                         "rows": [["Bikes", 1, 7], ["Parts", 2, 8]]}
        snap["Grp"] = {"columns": ["Id", "Label"],
                       "rows": [[7, "Wheels"], [8, "Bits"]]}
        e = "RELATED(Grp[Label])"
        assert calc_column_unsupported_reason(
            e, "Product", PROD["columns"], _known(snap), rels) is None
        vals, err = evaluate_row_context_column(
            PROD["columns"], [list(r) for r in PROD["rows"]], e, "Product",
            snap, rels)
        assert err is None
        assert vals == ["Wheels", "Bits", None]

    def test_two_paths_are_refused_not_picked(self):
        rels = REL + [{"FromTable": "Product", "FromColumn": "Name",
                       "ToTable": "Table", "ToColumn": "Sorting",
                       "IsActive": 1}]
        why = calc_column_unsupported_reason(
            "RELATED('Table'[Sorting])", "Product", PROD["columns"],
            _known(_rel_snap()), rels)
        assert why and "more than one active relationship path" in why

    def test_inactive_relationship_is_not_a_path(self):
        rels = [dict(REL[0], IsActive=0)]
        why = calc_column_unsupported_reason(
            "RELATED('Table'[Sorting])", "Product", PROD["columns"],
            _known(_rel_snap()), rels)
        assert why and "no active many-to-one relationship path" in why

    def test_wrong_direction_is_not_a_path(self):
        """RELATED goes many->one; the one side cannot reach the many side."""
        why = calc_column_unsupported_reason(
            "RELATED(Product[Name])", "Table", DIM["columns"],
            _known(_rel_snap()), REL)
        assert why and "no active many-to-one relationship path" in why

    def test_bare_column_is_refused_rather_than_guessed(self):
        why = calc_column_unsupported_reason(
            "RELATED([Sorting])", "Product", PROD["columns"],
            _known(_rel_snap()), REL)
        assert why and "does not name its table" in why

    def test_without_the_relationship_graph_it_stays_refused(self):
        why = calc_column_unsupported_reason(
            "RELATED('Table'[Sorting])", "Product", PROD["columns"],
            _known(_rel_snap()))
        assert why and "another table 'Table'" in why

    def test_table_names_are_discoverable_for_lazy_loading(self):
        from pbix_mcp.dax.calc_tables import related_table_names
        assert related_table_names(
            "RELATED('Table'[Sorting]) + RELATED(Grp[X])") == {"Table", "Grp"}
