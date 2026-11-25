from datetime import date

from migrator.chunk_planner import NULL_CHUNK_ID, oversized, plan_chunks

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
