"""The reconcile handler against a fake Snowflake connection that behaves like
connector 5.x, found in the live run: cursor.description stays None after
get_results_from_sfqid until the rows are fetched."""

import dataclasses
import json

from lakefreight import handlers
from lakefreight.models import ColumnSpec, TableSpec

SPEC = TableSpec(
    "TRAVEL",
    "WEB",
    "CLICK_EVENTS",
    "INTERNAL",
    (ColumnSpec("EVENT_ID", "TEXT"), ColumnSpec("EVENT_TS", "TIMESTAMP_NTZ(9)")),
)
ITEM = {
    "DATABASE_NAME": "TRAVEL",
    "SCHEMA_NAME": "WEB",
    "TABLE_NAME": "CLICK_EVENTS",
    "CHUNK_ID": "0001_202501_202501",
    "PREDICATE": None,
    "SNAPSHOT_TS": None,
}


class FakeCursor:
    balance = (100, 100, 0, 0)  # ROWS_FROM, ROWS_TO, INSERTS, DELETES

    def __init__(self, copy_rows, source_rows, values=None):
        self.copy_rows, self.source_rows = copy_rows, source_rows
        rows = sum(r[2] for r in copy_rows)
        # (source hash, sub-millisecond count, staged rows, staged hash)
        self.values = values or (77, 0, rows, 77)
        self.description, self._pending, self.updates, self.sql = None, None, [], []

    def get_results_from_sfqid(self, qid):
        self.description, self._pending = None, ("copy", self.copy_rows)

    def execute(self, sql, params=None):
        self.sql.append(sql)
        if sql.startswith("UPDATE"):
            self.updates.append((sql, params))
        self.description = None
        source_hash, sub_ms, staged_rows, staged_hash = self.values
        if "ROWS_FROM" in sql:
            row = self.balance
        elif "HASH_AGG" in sql and "FROM @" in sql:
            row = (staged_rows, staged_hash)
        elif "HASH_AGG" in sql:
            row = (source_hash, sub_ms)
        else:
            row = (self.source_rows if "COUNT" in sql else 2,)
        self._pending = ("scalar", [row])

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


def run(monkeypatch, copy_rows, source_rows, values=None, allow_loss=False):
    cur = FakeCursor(copy_rows, source_rows, values)
    if allow_loss:
        settings = handlers.load_settings()
        monkeypatch.setattr(
            handlers,
            "load_settings",
            lambda: dataclasses.replace(settings, allow_timestamp_precision_loss=True),
        )
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


def manifest_of(written):
    return next(json.loads(v["Body"]) for k, v in written.items() if k.endswith("manifest.json"))


def failure(out, written):
    assert out["state"] == "FAILED"
    assert not any(k.endswith("manifest.json") for k in written)
    return out["reason"]


FILE = [("data_0_0_0.snappy.parquet", 1000, 50)]


def test_manifest_records_the_value_fingerprint(monkeypatch):
    out, written = run(monkeypatch, FILE, 50, values=(-123, 0, 50, -123))
    assert out["state"] == "RECONCILED" and manifest_of(written)["value_hash"] == -123


def test_values_that_differ_fail_even_when_counts_match(monkeypatch):
    out, written = run(monkeypatch, FILE, 50, values=(1, 0, 50, 2))
    assert "values differ" in failure(out, written)


def test_files_holding_other_rows_than_the_copy_reported_fail(monkeypatch):
    out, written = run(monkeypatch, FILE, 50, values=(1, 0, 49, 1))
    assert "files hold 49 rows" in failure(out, written)


def test_sub_millisecond_timestamps_fail_by_default(monkeypatch):
    out, written = run(monkeypatch, FILE, 50, values=(1, 12, 50, 1))
    reason = failure(out, written)
    assert "EVENT_TS (12 values)" in reason and "allow_timestamp_precision_loss" in reason


def test_precision_loss_can_be_accepted(monkeypatch):
    out, written = run(monkeypatch, FILE, 50, values=(1, 12, 50, 1), allow_loss=True)
    assert out["state"] == "RECONCILED"


def test_staged_check_reads_this_attempts_files(monkeypatch):
    cur_holder = {}
    original = FakeCursor.__init__

    def keep(self, *a, **kw):
        original(self, *a, **kw)
        cur_holder["cur"] = self

    monkeypatch.setattr(FakeCursor, "__init__", keep)
    run(monkeypatch, FILE, 50)
    staged = [q for q in cur_holder["cur"].sql if "FROM @" in q]
    assert len(staged) == 1
    assert "/CLICK_EVENTS/chunk=0001_202501_202501/run_id=r1/" in staged[0]
    assert "LAKEFREIGHT_PARQUET" in staged[0]


def test_pinned_item_reads_every_query_at_its_snapshot(monkeypatch):
    from datetime import datetime

    holder = {}
    original = FakeCursor.__init__

    def keep(self, *a, **kw):
        original(self, *a, **kw)
        holder["cur"] = self

    monkeypatch.setattr(FakeCursor, "__init__", keep)
    monkeypatch.setitem(ITEM, "SNAPSHOT_TS", datetime(2026, 11, 27, 20, 0, 0))
    out, written = run(monkeypatch, FILE, 50)
    assert out["state"] == "RECONCILED"
    source_reads = [q for q in holder["cur"].sql if "CLICK_EVENTS" in q and "FROM @" not in q]
    assert source_reads and all(
        "AT(TIMESTAMP => '2026-11-27 20:00:00.000000 +00:00'::TIMESTAMP_TZ)" in q
        for q in source_reads
    )
    assert manifest_of(written)["snapshot_ts"] == "2026-11-27 20:00:00"


def test_submit_keeps_the_planned_snapshot(monkeypatch):
    executed, updates = [], {}

    class Cur:
        sfqid = "q1"

        def execute_async(self, sql):
            executed.append(sql)

    monkeypatch.setattr(
        handlers,
        "_connect_for",
        lambda *a: type("C", (), {"cursor": lambda s: Cur(), "close": lambda s: None})(),
    )
    monkeypatch.setattr(
        handlers,
        "get_work_item",
        lambda *a: {
            **ITEM,
            "ZONE": "INTERNAL",
            "LANE": "SMALL",
            "ATTEMPTS": 0,
            "SNAPSHOT_TS": "2026-11-27 20:00:00",
        },
    )
    monkeypatch.setattr(handlers, "load_table_spec", lambda *a: SPEC)
    monkeypatch.setattr(
        handlers, "update_work_item", lambda cur, settings, wid, **f: updates.update(f)
    )
    handlers.submit({"work_item_id": "x"})
    assert "AT(TIMESTAMP => '2026-11-27 20:00:00 +00:00'::TIMESTAMP_TZ)" in executed[0]
    assert "SNAPSHOT_TS" not in updates


def _keep_cursor(monkeypatch):
    holder = {}
    original = FakeCursor.__init__

    def keep(self, *a, **kw):
        original(self, *a, **kw)
        holder["cur"] = self

    monkeypatch.setattr(FakeCursor, "__init__", keep)
    return holder


CHANGE_ITEM = {
    "CHUNK_ID": "changes_20260131T120000_20260201T090000",
    "KIND": "CHANGES",
    "CHANGES_FROM_TS": "2026-01-31 12:00:00",
    "SNAPSHOT_TS": "2026-02-01 09:00:00",
}


def test_change_set_reconciles_into_the_changes_folder(monkeypatch):
    holder = _keep_cursor(monkeypatch)
    for key, value in CHANGE_ITEM.items():
        monkeypatch.setitem(ITEM, key, value)
    monkeypatch.setattr(FakeCursor, "balance", (1000, 1002, 17, 15))
    out, written = run(monkeypatch, FILE, 50)
    assert out["state"] == "RECONCILED"
    key = next(k for k in written if k.endswith("manifest.json"))
    assert key == (
        "internal/_changes/TRAVEL/WEB/CLICK_EVENTS/"
        "chunk=changes_20260131T120000_20260201T090000/run_id=r1/manifest.json"
    )
    manifest = manifest_of(written)
    assert manifest["changes_from_ts"] == "2026-01-31 12:00:00"
    assert manifest["snapshot_ts"] == "2026-02-01 09:00:00"
    assert manifest["chunks_in_table"] is None
    sql = holder["cur"].sql
    assert not any(q.startswith("SELECT COUNT(*)") and "EXTRACT_WORK_ITEM" in q for q in sql)
    assert any("ROWS_FROM" in q for q in sql)
    source_reads = [q for q in sql if "CLICK_EVENTS" in q and "FROM @" not in q]
    assert all("CHANGES(INFORMATION => DEFAULT)" in q for q in source_reads)


def test_change_set_that_does_not_balance_fails(monkeypatch):
    for key, value in CHANGE_ITEM.items():
        monkeypatch.setitem(ITEM, key, value)
    monkeypatch.setattr(FakeCursor, "balance", (1000, 1002, 17, 16))
    out, written = run(monkeypatch, FILE, 50)
    assert out["state"] == "FAILED" and "does not balance" in out["reason"]
    assert not any(k.endswith("manifest.json") for k in written)


def test_bulk_chunk_count_leaves_out_change_sets(monkeypatch):
    holder = _keep_cursor(monkeypatch)
    out, _ = run(monkeypatch, FILE, 50)
    assert out["state"] == "RECONCILED"
    count = next(
        q for q in holder["cur"].sql if q.startswith("SELECT COUNT(*)") and "EXTRACT_WORK_ITEM" in q
    )
    assert "COALESCE(KIND, 'BULK') = 'BULK'" in count
