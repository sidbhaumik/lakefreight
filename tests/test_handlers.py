"""The reconcile handler against a fake Snowflake connection that behaves like
connector 5.x, found in the live run: cursor.description stays None after
get_results_from_sfqid until the rows are fetched."""

import json

from lakefreight import handlers
from lakefreight.models import ColumnSpec, TableSpec

SPEC = TableSpec("TRAVEL", "WEB", "CLICK_EVENTS", "INTERNAL", (ColumnSpec("EVENT_ID", "TEXT"),))
ITEM = {
    "DATABASE_NAME": "TRAVEL",
    "SCHEMA_NAME": "WEB",
    "TABLE_NAME": "CLICK_EVENTS",
    "CHUNK_ID": "0001_202501_202501",
    "PREDICATE": None,
    "SNAPSHOT_TS": None,
}


class FakeCursor:
    def __init__(self, copy_rows, source_rows):
        self.copy_rows, self.source_rows = copy_rows, source_rows
        self.description, self._pending, self.updates = None, None, []

    def get_results_from_sfqid(self, qid):
        self.description, self._pending = None, ("copy", self.copy_rows)

    def execute(self, sql, params=None):
        if sql.startswith("UPDATE"):
            self.updates.append(sql)
        self.description = None
        self._pending = ("scalar", [(self.source_rows if "COUNT" in sql else 2,)])

    def fetchall(self):
        kind, rows = self._pending
        if kind == "copy":
            self.description = [("FILE_NAME",), ("FILE_SIZE",), ("ROW_COUNT",)] if rows else []
        return rows

    def fetchone(self):
        return self._pending[1][0]


class FakeConn:
    def __init__(self, cur):
        self.cur = cur

    def cursor(self):
        return self.cur

    def close(self):
        pass


def run(monkeypatch, copy_rows, source_rows):
    cur = FakeCursor(copy_rows, source_rows)
    written = {}
    monkeypatch.setattr(handlers, "_connect_for", lambda *a: FakeConn(cur))
    monkeypatch.setattr(handlers, "get_work_item", lambda *a: ITEM)
    monkeypatch.setattr(handlers, "load_table_spec", lambda *a: SPEC)
    monkeypatch.setattr(
        handlers,
        "_s3",
        lambda: type(
            "S3", (), {"put_object": lambda self, **kw: written.update({kw["Key"]: kw})}
        )(),
    )
    event = {
        "work_item_id": "x",
        "query_id": "q",
        "run_id": "r1",
        "zone": "INTERNAL",
        "lane": "SMALL",
    }
    return handlers.reconcile(event), written


def test_reconcile_reads_results_after_fetch(monkeypatch):
    out, written = run(monkeypatch, [("data_0_0_0.snappy.parquet", 1000, 4482)], 4482)
    assert out["state"] == "RECONCILED"
    manifest = next(
        json.loads(v["Body"]) for k, v in written.items() if k.endswith("manifest.json")
    )
    assert (
        manifest["row_count"] == 4482
        and manifest["files"][0]["name"] == "data_0_0_0.snappy.parquet"
    )


def test_empty_chunk_reconciles_with_no_files(monkeypatch):
    out, written = run(monkeypatch, [], 0)
    assert out["state"] == "RECONCILED"
    manifest = next(
        json.loads(v["Body"]) for k, v in written.items() if k.endswith("manifest.json")
    )
    assert manifest["file_count"] == 0 and manifest["row_count"] == 0


def test_row_mismatch_fails_and_writes_no_manifest(monkeypatch):
    out, written = run(monkeypatch, [("data_0_0_0.snappy.parquet", 1000, 10)], 11)
    assert out["state"] == "FAILED"
    assert not any(k.endswith("manifest.json") for k in written)


class SummaryCursor:
    def __init__(self, rows):
        self.rows, self.params = rows, None

    def execute(self, sql, params=None):
        self.params = params

    def fetchall(self):
        return self.rows


def summarize(monkeypatch, ids, status_rows):
    cur = SummaryCursor(status_rows)
    body = json.dumps([{"work_item_id": i} for i in ids]).encode()
    monkeypatch.setattr(handlers, "_connect_for", lambda *a: FakeConn(cur))
    monkeypatch.setattr(
        handlers,
        "_s3",
        lambda: type(
            "S3",
            (),
            {"get_object": lambda self, **kw: {"Body": type("B", (), {"read": lambda s: body})()}},
        )(),
    )
    event = {"lane": "SMALL", "count": len(ids), "items": {"bucket": "b", "key": "k"}}
    return handlers.summarize(event), cur


def test_summarize_counts_items_not_reconciled(monkeypatch):
    # Found in the live AWS run: every item failed, yet the lane reported success.
    out, cur = summarize(monkeypatch, ["a", "b", "c"], [("RECONCILED", 1), ("FAILED", 2)])
    assert out["not_reconciled"] == 2 and out["statuses"] == {"RECONCILED": 1, "FAILED": 2}
    assert json.loads(cur.params[0]) == ["a", "b", "c"]


def test_summarize_all_reconciled(monkeypatch):
    out, _ = summarize(monkeypatch, ["a", "b"], [("RECONCILED", 2)])
    assert out["not_reconciled"] == 0


def test_state_machine_fails_lane_with_unreconciled_items():
    from pathlib import Path

    asl = json.loads(
        (Path(__file__).parent.parent / "stepfunctions" / "extract_lane.asl.json").read_text()
    )
    states = asl["States"]
    assert states["ExtractItems"]["ResultPath"] is None  # keep list_items' output for Summarize
    assert states["ExtractItems"]["Next"] == "Summarize"
    choice = states["AllReconciled"]
    assert choice["Choices"][0]["Variable"] == "$.not_reconciled"
    assert states[choice["Default"]]["Type"] == "Fail"
