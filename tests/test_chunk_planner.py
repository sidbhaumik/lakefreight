from datetime import date

from lakefreight.chunk_planner import NULL_CHUNK_ID, oversized, plan_chunks

GB = 1024**3


def months(year, counts):
    return [(date(year, m + 1, 1), n) for m, n in enumerate(counts)]


def test_packs_months_to_target_and_covers_everything():
    # 12 months, 10 rows each, 120 GB total -> 10 GB per month; target 30 GB.
    chunks = plan_chunks("TXN_DT", months(2020, [10] * 12), 120 * GB, 30 * GB)
    data, null = chunks[:-1], chunks[-1]

    assert len(data) == 4
    assert [c.est_rows for c in data] == [30, 30, 30, 30]
    assert data[0].predicate == """t."TXN_DT" < '2020-04-01'"""
    assert data[1].predicate == """t."TXN_DT" >= '2020-04-01' AND t."TXN_DT" < '2020-07-01'"""
    assert data[-1].predicate == """t."TXN_DT" >= '2020-10-01'"""
    assert null.chunk_id == NULL_CHUNK_ID
    assert null.predicate == 't."TXN_DT" IS NULL'


def test_ranges_are_contiguous_across_gaps():
    # No rows between Jan 2019 and Jun 2021: the boundary must still leave no gap.
    counts = [(date(2019, 1, 1), 100), (date(2021, 6, 1), 100)]
    data = plan_chunks("D", counts, 200 * GB, 100 * GB)[:-1]
    assert data[0].predicate == """t."D" < '2021-06-01'"""
    assert data[1].predicate == """t."D" >= '2021-06-01'"""


def test_null_rows_get_their_own_chunk_and_rows_sum():
    counts = months(2020, [50, 50]) + [(None, 20)]
    chunks = plan_chunks("D", counts, 120 * GB, 1000 * GB)
    assert sum(c.est_rows for c in chunks) == 120
    assert chunks[-1].est_rows == 20
    # One data chunk with no bounds must still exclude nulls.
    assert chunks[0].predicate == 't."D" IS NOT NULL'


def test_single_huge_month_is_flagged():
    counts = [(date(2020, 1, 1), 10), (date(2020, 2, 1), 900), (date(2020, 3, 1), 10)]
    chunks = plan_chunks("D", counts, 920 * GB, 50 * GB)
    big = oversized(chunks, 50 * GB)
    assert [c.months for c in big] == [(date(2020, 2, 1),)]


def test_empty_profile_still_covers_table():
    chunks = plan_chunks("D", [], 0, 50 * GB)
    assert [c.predicate for c in chunks] == ['t."D" IS NOT NULL', 't."D" IS NULL']


# --- Day splits, integer keys and clustering keys ------------------------------

from decimal import Decimal  # noqa: E402

import pytest  # noqa: E402

from lakefreight.chunk_planner import (  # noqa: E402
    chunk_kind,
    leading_cluster_column,
    profile_sql,
)


def days(year, month, counts):
    return [(date(year, month, d + 1), n) for d, n in enumerate(counts)]


def test_month_too_big_for_one_chunk_is_split_by_day():
    # Jan and Mar are small; Feb holds 28 days of 10 rows. 1 GB per row, target 50 GB.
    counts = [(date(2020, 1, 15), 20), *days(2020, 2, [10] * 28), (date(2020, 3, 3), 20)]
    chunks = plan_chunks("D", counts, 320 * GB, 50 * GB)
    data = chunks[:-1]
    assert [c.chunk_id for c in data] == [
        "0001_202001_202001",
        "0002_20200201_20200205",
        "0003_20200206_20200210",
        "0004_20200211_20200215",
        "0005_20200216_20200220",
        "0006_20200221_20200225",
        "0007_20200226_20200228",
        "0008_202003_202003",
    ]
    assert data[0].predicate == """t."D" < '2020-02-01'"""
    assert data[1].predicate == """t."D" >= '2020-02-01' AND t."D" < '2020-02-06'"""
    assert data[6].predicate == """t."D" >= '2020-02-26' AND t."D" < '2020-03-01'"""
    assert data[7].predicate == """t."D" >= '2020-03-01'"""
    assert sum(c.est_rows for c in chunks) == 320
    assert not oversized(chunks, 50 * GB)


def test_day_profile_packs_whole_months_as_before():
    # A day-level profile of months that fit gives the same chunks as a month profile.
    by_day = [(date(2020, m, d), 5) for m in range(1, 13) for d in (1, 15)]
    assert [(c.chunk_id, c.predicate) for c in plan_chunks("D", by_day, 120 * GB, 30 * GB)] == [
        (c.chunk_id, c.predicate)
        for c in plan_chunks("D", months(2020, [10] * 12), 120 * GB, 30 * GB)
    ]


def test_integer_key_ranges_cover_everything():
    # Buckets of width 1000 from -2000, 10 rows each; target fits 3 buckets.
    buckets = [(Decimal(-2000 + 1000 * i), 10) for i in range(7)] + [(None, 4)]
    chunks = plan_chunks("ORDER_ID", buckets, 74 * GB, 30 * GB)
    assert [c.chunk_id for c in chunks] == [
        "0001_k-2000_k1000",
        "0002_k1000_k4000",
        "0003_k4000_kmax",
        NULL_CHUNK_ID,
    ]
    assert [c.predicate for c in chunks] == [
        't."ORDER_ID" < 1000',
        't."ORDER_ID" >= 1000 AND t."ORDER_ID" < 4000',
        't."ORDER_ID" >= 4000',
        't."ORDER_ID" IS NULL',
    ]
    assert sum(c.est_rows for c in chunks) == 74


def test_profile_mixing_dates_and_numbers_is_refused():
    with pytest.raises(ValueError, match="mixes"):
        plan_chunks("D", [(date(2020, 1, 1), 1), (5, 1)], GB, GB)


def test_chunk_kind():
    assert chunk_kind("TIMESTAMP_NTZ(9)") == chunk_kind("DATE") == "date"
    assert chunk_kind("NUMBER(38,0)") == chunk_kind("number( 18, 0 )") == chunk_kind("INT") == "key"
    assert chunk_kind("NUMBER(10,2)") is None
    assert chunk_kind("VARCHAR") is None and chunk_kind(None) is None


def test_profile_sql_by_kind():
    assert "DATE_TRUNC('DAY', \"D\")::DATE AS BUCKET_START" in profile_sql("DB.S.T", "D", "date")
    key = profile_sql("DB.S.T", "ORDER_ID", "key")
    assert 'MIN("ORDER_ID")' in key and "/ 10000)" in key and "AS BUCKET_START" in key
    with pytest.raises(ValueError):
        profile_sql("DB.S.T", "X", "text")


@pytest.mark.parametrize(
    ("key", "columns", "expected"),
    [
        ("LINEAR(LOAD_TS)", ["ID", "LOAD_TS"], "LOAD_TS"),
        ("LINEAR(TO_DATE(EVENT_TS), REGION)", ["REGION", "EVENT_TS"], "EVENT_TS"),
        ("LINEAR(DATE_TRUNC('MONTH', EVENT_TS))", ["MONTH", "EVENT_TS"], "EVENT_TS"),
        ("LINEAR(ORDER_ID)", ["ID", "ORDER_ID"], "ORDER_ID"),
        ('LINEAR("Mixed Key", ID)', ["ID", "Mixed Key"], "Mixed Key"),
        ("linear(load_ts)", ["LOAD_TS"], "LOAD_TS"),
        ("LINEAR(SUBSTRING(CODE, 1, 2))", ["ID"], None),
        (None, ["ID"], None),
    ],
)
def test_leading_cluster_column(key, columns, expected):
    assert leading_cluster_column(key, columns) == expected
