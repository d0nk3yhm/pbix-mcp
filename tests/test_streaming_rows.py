"""Issue #46: streaming/append row load.

pbix_create's single tables_json string forced ~3x the data size in RAM
(source rows + row dicts + the serialized JSON text). Two additions:

- per-table "rows_path" in pbix_create: an NDJSON file (one JSON object per
  line) as the row source — callers write batches and free them, then pass
  the path; the tool holds ONE in-memory copy (what the encoder needs).
- pbix_append_table_rows: ADDS rows to an existing table (both row APIs
  were replace-only, making batching impossible). Schema inferred from the
  existing table; accepts rows_json or an NDJSON rows_path.
"""
import json
import uuid

import pytest

from pbix_mcp import server

pytestmark = pytest.mark.unit


def _mk_ndjson(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


@pytest.fixture()
def streamed(tmp_path):
    alias = "st_" + uuid.uuid4().hex[:8]
    nd = str(tmp_path / "rows.ndjson")
    _mk_ndjson(nd, [{"K": f"k{i}", "V": float(i)} for i in range(100)])
    p = str(tmp_path / "t.pbix")
    tables = [{"name": "T",
               "columns": [{"name": "K", "data_type": "String"},
                           {"name": "V", "data_type": "Double"}],
               "rows_path": nd}]
    r = json.loads(server.pbix_create(p, alias, json.dumps(tables)))
    assert r.get("success"), r
    yield alias, p
    server._open_files.pop(alias, None)
    server._dax_cache.pop(alias, None)


class TestNdjsonRowsPath:
    def test_create_from_ndjson(self, streamed):
        alias, _p = streamed
        st = json.loads(server.pbix_table_stats(alias, "T"))
        assert "(100 rows" in st["message"]

    def test_rows_and_rows_path_mutually_exclusive(self, tmp_path):
        nd = str(tmp_path / "r.ndjson")
        _mk_ndjson(nd, [{"K": "a"}])
        r = json.loads(server.pbix_create(
            str(tmp_path / "x.pbix"), "st_excl", json.dumps(
                [{"name": "T", "columns": [{"name": "K", "data_type": "String"}],
                  "rows": [{"K": "b"}], "rows_path": nd}])))
        assert not r.get("success")
        assert "mutually exclusive" in r["message"]

    def test_missing_ndjson_file_errors(self, tmp_path):
        r = json.loads(server.pbix_create(
            str(tmp_path / "x.pbix"), "st_miss", json.dumps(
                [{"name": "T", "columns": [{"name": "K", "data_type": "String"}],
                  "rows_path": str(tmp_path / "nope.ndjson")}])))
        assert not r.get("success")
        assert "not found" in r["message"]

    def test_malformed_line_reports_line_number(self, streamed, tmp_path):
        alias, _p = streamed
        bad = str(tmp_path / "bad.ndjson")
        with open(bad, "w", encoding="utf-8") as f:
            f.write('{"K": "ok", "V": 1}\nnot json at all\n')
        r = json.loads(server.pbix_append_table_rows(alias, "T", rows_path=bad))
        assert not r.get("success")
        assert ":2:" in r["message"]


class TestAppendTableRows:
    def test_append_adds_not_replaces(self, streamed):
        alias, _p = streamed
        r = json.loads(server.pbix_append_table_rows(
            alias, "T", rows_json=json.dumps(
                [{"K": "x1", "V": 9.5}, {"K": "x2", "V": 10.5}])))
        assert r.get("success"), r
        assert "100 -> 102" in r["message"].replace(",", "")
        # values from BOTH the original load and the batch survive
        assert json.loads(server.pbix_datamodel_add_measure(
            alias, "T", "S", "SUM(T[V])"))["success"]
        out = json.loads(server.pbix_evaluate_dax(alias, "S"))
        want = sum(float(i) for i in range(100)) + 9.5 + 10.5
        assert out["results"][0]["value"] == pytest.approx(want)

    def test_batched_ndjson_appends_accumulate_and_persist(self, streamed,
                                                           tmp_path):
        alias, p = streamed
        for b in range(2):
            nd = str(tmp_path / f"batch{b}.ndjson")
            _mk_ndjson(nd, [{"K": f"b{b}_{i}", "V": 1.0} for i in range(50)])
            r = json.loads(server.pbix_append_table_rows(
                alias, "T", rows_path=nd))
            assert r.get("success"), r
        out_p = str(tmp_path / "saved.pbix")
        assert json.loads(server.pbix_save(
            alias, output_path=out_p, overwrite=True))["success"]
        server._open_files.pop(alias, None)
        server._dax_cache.pop(alias, None)

        alias2 = alias + "_r"
        assert json.loads(server.pbix_open(out_p, alias2))["success"]
        try:
            assert json.loads(server.pbix_datamodel_add_measure(
                alias2, "T", "N", "COUNTROWS(T)"))["success"]
            out = json.loads(server.pbix_evaluate_dax(alias2, "N"))
            assert out["results"][0]["value"] == 200  # 100 + 50 + 50
        finally:
            server._open_files.pop(alias2, None)
            server._dax_cache.pop(alias2, None)

    def test_missing_keys_store_null(self, streamed):
        alias, _p = streamed
        r = json.loads(server.pbix_append_table_rows(
            alias, "T", rows_json=json.dumps([{"K": "nullv"}])))
        assert r.get("success"), r
        assert json.loads(server.pbix_datamodel_add_measure(
            alias, "T", "NB", "COUNTBLANK(T[V])"))["success"]
        out = json.loads(server.pbix_evaluate_dax(alias, "NB"))
        assert out["results"][0]["value"] == 1

    def test_error_paths(self, streamed):
        alias, _p = streamed
        r = json.loads(server.pbix_append_table_rows(alias, "T"))
        assert not r.get("success") and "exactly one" in r["message"]
        r = json.loads(server.pbix_append_table_rows(
            alias, "T", rows_json="[]"))
        assert not r.get("success")
        r = json.loads(server.pbix_append_table_rows(
            alias, "Nope", rows_json='[{"K": "x"}]'))
        assert not r.get("success") and "not found" in r["message"]
