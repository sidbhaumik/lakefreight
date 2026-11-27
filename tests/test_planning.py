from datetime import date

import pytest

from lakefreight import planning
from lakefreight.config import load_settings

GB = 1024**3


class FakeCursor:
    """Answers the config query with `config`, then each profile query with `profile`."""

    def __init__(self, config, profile):
        self.config, self.profile = config, profile
        self.sql, self.inserted, self._rows = [], [], []

    def execute(self, sql, params=None):
        self.sql.append(sql)
        self._rows = self.config if "EXTRACT_CONFIG" in sql else self.profile

    def fetchall(self):
        return self._rows

    def executemany(self, sql, rows):
        self.inserted += rows


class FakeConn:
    def __init__(self, cur):
        self.cur = cur

    def cursor(self):
        return self.cur


def config_row(chunk_col, data_type, strategy="CHUNKED"):
    return (
        "SHOP",
        "SALES",
        "LEDGER",
        300 * GB,
        "INTERNAL",
        "LARGE",
        strategy,
        chunk_col,
        None,
        data_type,
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
