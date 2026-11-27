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
