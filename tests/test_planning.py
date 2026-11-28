import dataclasses
from datetime import date, datetime

import pytest

from lakefreight import planning
from lakefreight.config import load_settings

GB = 1024**3


SNAP = datetime(2026, 11, 27, 20, 0, 0, 123000)


class FakeCursor:
    """Answers the config query with `config`, each profile query with `profile`,
    the retention lookup with `retention` days, and SYSDATE() with SNAP."""

    def __init__(self, config, profile, retention=1):
        self.config, self.profile, self.retention = config, profile, retention
        self.sql, self.inserted, self._rows = [], [], []

    def execute(self, sql, params=None):
        self.sql.append(sql)
        if "EXTRACT_CONFIG" in sql:
            self._rows = self.config
        elif "RETENTION_TIME" in sql:
            self._rows = [] if self.retention is None else [(self.retention,)]
        elif "SYSDATE" in sql:
            self._rows = [(SNAP,)]
        else:
            self._rows = self.profile

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def executemany(self, sql, rows):
        self.inserted += rows


class FakeConn:
    def __init__(self, cur):
        self.cur = cur

    def cursor(self):
        return self.cur


def config_row(chunk_col, data_type, strategy="CHUNKED", still_written=False, table="LEDGER"):
    return (
        "SHOP",
        "SALES",
        table,
        300 * GB,
        "INTERNAL",
        "LARGE",
        strategy,
        chunk_col,
        None,
        data_type,
        still_written,
    )


def test_integer_chunk_column_gets_key_ranges():
    settings = load_settings()
    profile = [(i * 1000, 100) for i in range(10)] + [(None, 3)]
    cur = FakeCursor([config_row("ENTRY_ID", "NUMBER(38,0)")], profile)
    created = planning.build_work_items(FakeConn(cur), settings)
    assert 'MIN("ENTRY_ID")' in cur.sql[1]
    ids = [row[4] for row in cur.inserted]
    assert created == len(ids) and ids[-1] == "null" and ids[0].startswith("0001_k0_k")
    assert "EXTRACT_COLUMN_CONFIG" in cur.sql[0]


def test_date_chunk_column_gets_day_profile():
    cur = FakeCursor([config_row("BOOKED_AT", "TIMESTAMP_NTZ(9)")], [(date(2024, 1, 1), 10)])
    planning.build_work_items(FakeConn(cur), load_settings())
    assert "DATE_TRUNC('DAY', \"BOOKED_AT\")" in cur.sql[1]


def test_text_chunk_column_is_refused():
    cur = FakeCursor([config_row("NOTE", "VARCHAR")], [])
    with pytest.raises(ValueError, match="must be a date, timestamp or integer"):
        planning.build_work_items(FakeConn(cur), load_settings())


def test_full_table_needs_no_profile():
    cur = FakeCursor([config_row(None, None, strategy="FULL")], [])
    assert planning.build_work_items(FakeConn(cur), load_settings()) == 1
    assert len(cur.sql) == 1 and cur.inserted[0][4] == "full"


# --- Snapshots ---------------------------------------------------------------


def settings_with(**kw):
    return dataclasses.replace(load_settings(), **kw)


def test_still_written_tables_share_one_snapshot_and_profile_at_it():
    rows = [
        config_row("ENTRY_ID", "NUMBER(38,0)", still_written=True),
        config_row(None, None, strategy="FULL", still_written=True, table="ACCOUNTS"),
        config_row(None, None, strategy="FULL", table="REGIONS"),
    ]
    cur = FakeCursor(rows, [(0, 10), (1000, 10)])
    planning.build_work_items(FakeConn(cur), settings_with(snapshot="still_written"))
    snaps = {row[3]: row[9] for row in cur.inserted}
    assert snaps == {"LEDGER": SNAP, "ACCOUNTS": SNAP, "REGIONS": None}
    profile = next(q for q in cur.sql if "MIN(" in q)
    assert "AT(TIMESTAMP => '2026-11-27 20:00:00.123000 +00:00'::TIMESTAMP_TZ)" in profile
    assert sum("SYSDATE" in q for q in cur.sql) == 1


def test_snapshot_all_and_none():
    rows = [config_row(None, None, strategy="FULL")]
    cur = FakeCursor(rows, [])
    planning.build_work_items(FakeConn(cur), settings_with(snapshot="all"))
    assert cur.inserted[0][9] == SNAP
    cur = FakeCursor([config_row(None, None, strategy="FULL", still_written=True)], [])
    planning.build_work_items(FakeConn(cur), settings_with(snapshot="none"))
    assert cur.inserted[0][9] is None and not any("SYSDATE" in q for q in cur.sql)


def test_short_retention_is_refused_before_anything_is_planned():
    rows = [
        config_row(None, None, strategy="FULL", table="REGIONS"),
        config_row(None, None, strategy="FULL", still_written=True),
    ]
    cur = FakeCursor(rows, [], retention=0)
    with pytest.raises(ValueError, match="LEDGER is read as of a snapshot but has no time travel"):
        planning.build_work_items(FakeConn(cur), settings_with(snapshot_window_hours=12))
    assert cur.inserted == []
    cur = FakeCursor(rows, [], retention=1)
    with pytest.raises(ValueError, match="1 day"):
        planning.build_work_items(FakeConn(cur), settings_with(snapshot_window_hours=36))
    cur = FakeCursor(rows, [], retention=2)
    assert planning.build_work_items(FakeConn(cur), settings_with(snapshot_window_hours=36)) == 2


BULK_SNAP = datetime(2026, 11, 27, 10, 0, 0)


def table_row(table="LEDGER", items=4, done=4, bulk=BULK_SNAP, open_=0, last=None, clone=None):
    return ("SHOP", "SALES", table, "INTERNAL", "LARGE", clone, items, done, bulk, open_, last)


class ChangeCursor:
    """Answers the work-item summary with `tables`, SYSDATE() with SNAP, the
    retention lookup with `retention` days, and fails the change-tracking probe
    for tables named in `untracked`."""

    def __init__(self, tables, retention=1, untracked=()):
        self.tables, self.retention, self.untracked = tables, retention, untracked
        self.sql, self.inserted, self._rows = [], [], []

    def execute(self, sql, params=None):
        self.sql.append(sql)
        if "BULK_ITEMS" in sql:
            self._rows = self.tables
        elif "SYSDATE" in sql:
            self._rows = [(SNAP,)]
        elif "RETENTION_TIME" in sql:
            self._rows = [(self.retention,)]
        elif "LIMIT 0" in sql:
            if any(f'"{t}"' in sql for t in self.untracked):
                raise RuntimeError(
                    "091930 (2F003): SQL compilation error: Change tracking is not enabled "
                    "or has been missing for the time range requested\nmore detail"
                )
            self._rows = []

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def executemany(self, sql, rows):
        self.inserted += rows


def test_change_set_runs_from_the_bulk_snapshot_to_a_new_one():
    cur = ChangeCursor([table_row()])
    created, skipped = planning.build_change_items(FakeConn(cur), load_settings())
    assert created == 1 and skipped == []
    (row,) = cur.inserted
    assert row[0] == "SHOP.SALES.LEDGER#changes_20261127T100000_20261127T200000"
    assert row[7:] == ("CHANGES", BULK_SNAP, SNAP)
    probe = next(q for q in cur.sql if "LIMIT 0" in q)
    assert "CHANGES(INFORMATION => DEFAULT)" in probe and "2026-11-27 10:00:00" in probe


def test_next_change_set_starts_where_the_last_one_ended():
    last = datetime(2026, 11, 27, 14, 0, 0)
    cur = ChangeCursor([table_row(last=last)])
    planning.build_change_items(FakeConn(cur), load_settings())
    assert cur.inserted[0][8] == last


def test_tables_not_ready_for_a_delta_pass_are_skipped():
    cur = ChangeCursor(
        [
            table_row("A", done=3),
            table_row("B", bulk=None),
            table_row("C", clone="SHOP_CLONE"),
            table_row("D", open_=1),
        ]
    )
    created, skipped = planning.build_change_items(FakeConn(cur), load_settings())
    assert created == 0 and not cur.inserted
    assert [s.split(":")[0] for s in skipped] == [f"SHOP.SALES.{t}" for t in "ABCD"]
    assert "3 of 4 reconciled" in skipped[0] and "read live" in skipped[1]
    assert not any("SYSDATE" in q for q in cur.sql)


def test_missing_change_tracking_refuses_every_change_set():
    cur = ChangeCursor([table_row("A"), table_row("B")], untracked=("B",))
    with pytest.raises(ValueError, match="SHOP.SALES.B: 091930") as err:
        planning.build_change_items(FakeConn(cur), load_settings())
    assert "more detail" not in str(err.value) and "SET CHANGE_TRACKING = TRUE" in str(err.value)
    assert not cur.inserted


def test_change_set_beyond_retention_is_refused():
    # 10 hours since the bulk snapshot plus a 12-hour window needs more than 0 days.
    cur = ChangeCursor([table_row()], retention=0)
    with pytest.raises(ValueError, match="has no time travel"):
        planning.build_change_items(FakeConn(cur), load_settings())
    assert not cur.inserted
