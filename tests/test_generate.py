import csv
from datetime import date
from pathlib import Path

from lakefreight import generate
from lakefreight.config import load_settings

ROOT = Path(__file__).resolve().parents[1]
METADATA = ROOT / "examples" / "metadata_sample.csv"
PROFILES = ROOT / "examples" / "profiles_sample.csv"
settings = load_settings()


def run(tmp_path, with_profiles=True):
    tables = generate.read_metadata(METADATA)
    profiles = generate.read_profiles(PROFILES) if with_profiles else {}
    return generate.generate(tables, profiles, settings, tmp_path)


def by_table(summary, table):
    return [r for r in summary if r["table"] == table]


def test_zones_and_strategies(tmp_path):
    summary = run(tmp_path)
    assert {r["table"]: r["zone"] for r in summary} == {
        "HOTEL_DIM": "INTERNAL",
        "TRAVELER": "PII",
        "RESERVATION": "RESTRICTED",
        "CLICK_EVENTS": "INTERNAL",
        "MISC_NOTES": "PII",  # flags missing: fails closed
    }
    assert by_table(summary, "HOTEL_DIM")[0]["strategy"] == "FULL"
    assert "treated as sensitive" in by_table(summary, "MISC_NOTES")[0]["notes"]


def test_large_restricted_table_is_chunked_with_xref_join(tmp_path):
    rows = by_table(run(tmp_path), "RESERVATION")
    # 24 months of 37.5 GB against a 75 GiB target: two months per chunk, plus the null chunk.
    assert len(rows) == 13
    assert rows[-1]["chunk_id"] == "null"
    assert all(r["status"] == "READY" and r["lane"] == "RESTRICTED" for r in rows)

    sql = (tmp_path / rows[0]["file"]).read_text()
    assert "EGRESS_RESTRICTED_STAGE/TRAVEL/BOOKINGS/RESERVATION/" in sql
    assert "LEFT JOIN SECURE.PUBLIC.TOKEN_XREF x1" in sql
    assert 'x1."CLEAR_VALUE" AS "PASSPORT_NUMBER"' in sql
    assert "-- 3. Tokens with no xref match" in sql


def test_large_table_without_profile_gets_profile_sql(tmp_path):
    rows = by_table(run(tmp_path), "CLICK_EVENTS")
    assert [r["status"] for r in rows] == ["NEEDS_PROFILE"]
    profile = (tmp_path / rows[0]["file"]).read_text()
    assert """DATE_TRUNC('DAY', "EVENT_TS")::DATE AS BUCKET_START""" in profile


def test_summary_file_matches_returned_rows(tmp_path):
    summary = run(tmp_path)
    with open(tmp_path / "work_items.csv", newline="") as f:
        assert len(list(csv.DictReader(f))) == len(summary)


def test_column_flag_outranks_table_flag(tmp_path):
    src = tmp_path / "m.csv"
    src.write_text(
        "table_database,table_schema,table_name,table_size_bytes,table_pii_flag,table_restricted_flag,"
        "column_name,column_datatype,column_pii_flag,column_restricted_flag\n"
        "D,S,T,100,N,N,PASSPORT,VARCHAR,N,Y\n"
    )
    (meta,) = generate.read_metadata(src)
    assert meta.spec.zone == "RESTRICTED"
    assert meta.spec.columns[0].use_xref


def test_missing_required_column_is_reported(tmp_path):
    src = tmp_path / "m.csv"
    src.write_text("table_database,table_schema\nD,S\n")
    try:
        generate.read_metadata(src)
    except ValueError as exc:
        assert "missing columns" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_chunk_column_heuristic_prefers_load_dates():
    meta = {m.spec.table: m for m in generate.read_metadata(METADATA)}
    assert meta["RESERVATION"].chunk_column == "LOAD_TS"
    assert meta["HOTEL_DIM"].chunk_column == "LISTED_AT"
    assert meta["MISC_NOTES"].chunk_column is None


def test_unquoted_comma_in_datatype_is_reported(tmp_path):
    src = tmp_path / "m.csv"
    src.write_text(
        "table_database,table_schema,table_name,table_size_bytes,table_pii_flag,table_restricted_flag,"
        "column_name,column_datatype,column_pii_flag,column_restricted_flag\n"
        "D,S,T,100,N,N,AMT,NUMBER(18,2),N,N\n"
    )
    try:
        generate.read_metadata(src)
    except ValueError as exc:
        assert "line 2" in str(exc) and "quote" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def write_csv(path, header, rows):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    return path


META_HEADER = [
    "table_database",
    "table_schema",
    "table_name",
    "table_size_bytes",
    "table_pii_flag",
    "table_restricted_flag",
    "column_name",
    "column_ordinal",
    "column_datatype",
    "column_pii_flag",
    "column_restricted_flag",
    "table_clustering_key",
]


def ledger_metadata(tmp_path, clustering_key, chunk_column=""):
    rows = [
        [
            "SHOP",
            "SALES",
            "LEDGER",
            600 * 1024**3,
            "N",
            "N",
            name,
            i + 1,
            dtype,
            "N",
            "N",
            clustering_key,
        ]
        for i, (name, dtype) in enumerate(
            [("ENTRY_ID", "NUMBER(38,0)"), ("NOTE", "VARCHAR"), ("BOOKED_AT", "TIMESTAMP_NTZ(9)")]
        )
    ]
    header = META_HEADER
    if chunk_column:
        header = [*META_HEADER, "chunk_column"]
        rows = [[*r, chunk_column] for r in rows]
    return generate.read_metadata(write_csv(tmp_path / "meta.csv", header, rows))


def test_integer_clustering_key_is_the_chunk_column(tmp_path):
    (meta,) = ledger_metadata(tmp_path, "LINEAR(ENTRY_ID)")
    # A clustered integer key wins over the date column the name heuristic would pick.
    assert meta.chunk_column == "ENTRY_ID" and meta.chunk_kind == "key"
    summary = generate.generate([meta], {}, settings, tmp_path / "out")
    assert summary[0]["status"] == "NEEDS_PROFILE"
    assert "leads the clustering key" in summary[0]["notes"]
    profile = (tmp_path / "out" / summary[0]["file"]).read_text()
    assert 'MIN("ENTRY_ID")' in profile

    profiles = write_csv(
        tmp_path / "profiles.csv",
        ["table_database", "table_schema", "table_name", "bucket_start", "row_count"],
        [["SHOP", "SALES", "LEDGER", str(i * 1000), 100] for i in range(8)]
        + [["SHOP", "SALES", "LEDGER", "", 5]],
    )
    rows = generate.generate([meta], generate.read_profiles(profiles), settings, tmp_path / "out2")
    assert [r["chunk_id"] for r in rows][-1] == "null"
    assert rows[0]["chunk_id"].startswith("0001_k0_k")
    sql = (tmp_path / "out2" / rows[1]["file"]).read_text()
    assert 't."ENTRY_ID" >= ' in sql


def test_non_numeric_chunk_column_is_ignored(tmp_path):
    (meta,) = ledger_metadata(tmp_path, "", chunk_column="NOTE")
    assert meta.chunk_column is None
    assert "NOTE is not a date, timestamp or integer column" in "; ".join(meta.notes)


def test_old_month_profiles_still_read(tmp_path):
    # Files written before bucket_start existed use month_start; they still plan.
    old = write_csv(
        tmp_path / "old.csv",
        ["table_database", "table_schema", "table_name", "month_start", "row_count"],
        [
            ["TRAVEL", "BOOKINGS", "RESERVATION", "2022-01-01", 10],
            ["TRAVEL", "BOOKINGS", "RESERVATION", "", 1],
        ],
    )
    assert generate.read_profiles(old) == {
        ("TRAVEL", "BOOKINGS", "RESERVATION"): [(date(2022, 1, 1), 10), (None, 1)]
    }


def test_offline_plan_can_pin_a_snapshot(tmp_path):
    tables = generate.read_metadata(METADATA)
    summary = generate.generate(
        tables, generate.read_profiles(PROFILES), settings, tmp_path, snapshot="2026-01-31 12:00:00"
    )
    sql = (tmp_path / by_table(summary, "RESERVATION")[0]["file"]).read_text()
    assert sql.count("AT(TIMESTAMP => '2026-01-31 12:00:00 +00:00'::TIMESTAMP_TZ)") == 4
    profile = (tmp_path / by_table(summary, "CLICK_EVENTS")[0]["file"]).read_text()
    assert "AT(TIMESTAMP =>" in profile


def test_changes_since_writes_one_change_set_per_table(tmp_path):
    import pytest

    tables = generate.read_metadata(METADATA)
    summary = generate.generate(
        tables,
        {},
        settings,
        tmp_path,
        snapshot="2026-02-01 09:00:00",
        changes_from="2026-01-31 12:00:00",
    )
    assert len(summary) == len(tables)
    assert {r["chunk_id"] for r in summary} == {"changes_20260131T120000_20260201T090000"}
    sql = (tmp_path / summary[0]["file"]).read_text()
    assert "CHANGES(INFORMATION => DEFAULT)" in sql and "ROWS_FROM" in sql
    with pytest.raises(ValueError, match="needs --snapshot"):
        generate.generate(tables, {}, settings, tmp_path, changes_from="2026-01-31 12:00:00")
