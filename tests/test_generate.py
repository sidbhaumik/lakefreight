import csv
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
    assert """DATE_TRUNC('MONTH', "EVENT_TS")""" in profile


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
