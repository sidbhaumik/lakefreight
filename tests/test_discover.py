"""Discovery SQL is executed for real: transpiled from Snowflake SQL to DuckDB and
run against stand-in SNOWFLAKE.ACCOUNT_USAGE views."""

import csv

import pytest

from lakefreight import cli, discover, generate
from lakefreight.discover import DiscoverError, DiscoverSettings, TagRule, build_sql

duckdb = pytest.importorskip("duckdb")
sqlglot = pytest.importorskip("sqlglot")

SETTINGS = DiscoverSettings.from_raw(
    {
        "discover": {
            "pii_column_patterns": ["email|phone"],
            "restricted_tags": ["GOV.TAGS.TOKENIZED"],
        }
    }
)

# (database, schema, table, type, bytes, deleted)
TABLES = [
    ("TRAVEL", "CRM", "TRAVELER", "BASE TABLE", 4_000, None),
    ("TRAVEL", "CATALOG", "HOTEL_DIM", "BASE TABLE", 1_000, None),
    ("TRAVEL", "BOOKINGS", "RESERVATION", "BASE TABLE", 900_000_000_000, None),
    ("TRAVEL", "WEB", "CLICK_EVENTS", "BASE TABLE", 2_000, None),
    ("TRAVEL", "WEB", "CLICK_VIEW", "VIEW", 0, None),
    ("TRAVEL", "WEB", "OLD_EVENTS", "BASE TABLE", 10, "2026-01-01"),
    ("TRAVEL", "INFORMATION_SCHEMA", "TABLES", "BASE TABLE", 0, None),
    ("OTHERDB", "PUBLIC", "SECRET", "BASE TABLE", 10, None),
]
# (database, schema, table, column, ordinal, type, precision, scale, length, comment)
COLUMNS = [
    ("TRAVEL", "CRM", "TRAVELER", "TRAVELER_ID", 1, "NUMBER", 38, 0, None, "Key"),
    ("TRAVEL", "CRM", "TRAVELER", "FULL_NAME", 2, "TEXT", None, None, 200, "Name"),
    ("TRAVEL", "CATALOG", "HOTEL_DIM", "HOTEL_ID", 1, "NUMBER", 38, 0, None, None),
    ("TRAVEL", "CATALOG", "HOTEL_DIM", "HOTEL_NAME", 2, "TEXT", None, None, 200, None),
    ("TRAVEL", "BOOKINGS", "RESERVATION", "RESERVATION_ID", 1, "NUMBER", 38, 0, None, None),
    ("TRAVEL", "BOOKINGS", "RESERVATION", "PASSPORT_NUMBER", 2, "TEXT", None, None, 64, None),
    ("TRAVEL", "BOOKINGS", "RESERVATION", "LOAD_TS", 3, "TIMESTAMP_NTZ", None, None, None, None),
    ("TRAVEL", "WEB", "CLICK_EVENTS", "EVENT_ID", 1, "TEXT", None, None, 36, None),
    ("TRAVEL", "WEB", "CLICK_EVENTS", "Email", 2, "TEXT", None, None, 200, None),
    ("TRAVEL", "WEB", "CLICK_VIEW", "EVENT_ID", 1, "TEXT", None, None, 36, None),
    ("TRAVEL", "WEB", "OLD_EVENTS", "EVENT_ID", 1, "TEXT", None, None, 36, None),
    ("TRAVEL", "INFORMATION_SCHEMA", "TABLES", "X", 1, "TEXT", None, None, 1, None),
    ("OTHERDB", "PUBLIC", "SECRET", "X", 1, "TEXT", None, None, 1, None),
]
# (tag db, tag schema, tag name, value, object db, schema, name, column, domain, deleted)
TAGS = [
    (
        "SNOWFLAKE",
        "CORE",
        "PRIVACY_CATEGORY",
        "IDENTIFIER",
        "TRAVEL",
        "CRM",
        "TRAVELER",
        "FULL_NAME",
        "COLUMN",
        None,
    ),
    (
        "SNOWFLAKE",
        "CORE",
        "SEMANTIC_CATEGORY",
        "NAME",
        "TRAVEL",
        "CRM",
        "TRAVELER",
        "FULL_NAME",
        "COLUMN",
        None,
    ),
    (
        "GOV",
        "TAGS",
        "TOKENIZED",
        "yes",
        "TRAVEL",
        "BOOKINGS",
        "RESERVATION",
        "PASSPORT_NUMBER",
        "COLUMN",
        None,
    ),
    (
        "GOV",
        "TAGS",
        "TOKENIZED",
        "yes",
        "TRAVEL",
        "CATALOG",
        "HOTEL_DIM",
        "HOTEL_NAME",
        "COLUMN",
        "2026-01-01",
    ),
]
CLASSIFIED = [("TRAVEL", "CRM", "TRAVELER"), ("TRAVEL", "CATALOG", "HOTEL_DIM")]


def snowflake_stand_in():
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS snowflake")
    con.execute("CREATE SCHEMA snowflake.account_usage")
    con.execute(
        "CREATE TABLE snowflake.account_usage.tables (TABLE_CATALOG TEXT, TABLE_SCHEMA TEXT, "
        "TABLE_NAME TEXT, TABLE_TYPE TEXT, BYTES BIGINT, DELETED TEXT)"
    )
    con.executemany("INSERT INTO snowflake.account_usage.tables VALUES (?,?,?,?,?,?)", TABLES)
    con.execute(
        "CREATE TABLE snowflake.account_usage.columns (TABLE_CATALOG TEXT, TABLE_SCHEMA TEXT, "
        "TABLE_NAME TEXT, COLUMN_NAME TEXT, ORDINAL_POSITION INT, DATA_TYPE TEXT, "
        "NUMERIC_PRECISION INT, NUMERIC_SCALE INT, CHARACTER_MAXIMUM_LENGTH INT, COMMENT TEXT, "
        "DELETED TEXT)"
    )
    con.executemany(
        "INSERT INTO snowflake.account_usage.columns VALUES (?,?,?,?,?,?,?,?,?,?,NULL)", COLUMNS
    )
    con.execute(
        "CREATE TABLE snowflake.account_usage.tag_references (TAG_DATABASE TEXT, TAG_SCHEMA TEXT, "
        "TAG_NAME TEXT, TAG_VALUE TEXT, OBJECT_DATABASE TEXT, OBJECT_SCHEMA TEXT, "
        "OBJECT_NAME TEXT, COLUMN_NAME TEXT, DOMAIN TEXT, OBJECT_DELETED TEXT)"
    )
    con.executemany(
        "INSERT INTO snowflake.account_usage.tag_references VALUES (?,?,?,?,?,?,?,?,?,?)", TAGS
    )
    con.execute(
        "CREATE TABLE snowflake.account_usage.data_classification_latest (DATABASE_NAME TEXT, "
        "SCHEMA_NAME TEXT, TABLE_NAME TEXT, LAST_CLASSIFIED_ON TEXT)"
    )
    con.executemany(
        "INSERT INTO snowflake.account_usage.data_classification_latest VALUES (?,?,?,'2026-11-01')",
        CLASSIFIED,
    )
    return con


def run_discovery(settings=SETTINGS, databases=("TRAVEL",)):
    sql = build_sql(list(databases), settings)
    duck_sql = sqlglot.transpile(sql, read="snowflake", write="duckdb")[0]
    return snowflake_stand_in().execute(duck_sql).fetchall()


def by_column(rows):
    return {(r[2], r[6]): r for r in rows}


def test_discovery_flags_columns_and_tables():
    rows = run_discovery()
    cols = by_column(rows)
    tables = {r[2] for r in rows}
    # Views, dropped tables, INFORMATION_SCHEMA and other databases are left out.
    assert tables == {"TRAVELER", "HOTEL_DIM", "RESERVATION", "CLICK_EVENTS"}

    # Classified table: the tagged column is personal data, the rest is not.
    assert cols[("TRAVELER", "FULL_NAME")][10] == "Y"
    assert cols[("TRAVELER", "TRAVELER_ID")][10] == "N"
    assert cols[("TRAVELER", "TRAVELER_ID")][4] == "Y"  # table flag
    # Classified and untagged: not personal data. A tag on a dropped object is ignored.
    assert {cols[("HOTEL_DIM", c)][10] for c in ("HOTEL_ID", "HOTEL_NAME")} == {"N"}
    assert cols[("HOTEL_DIM", "HOTEL_NAME")][11] == "N"
    assert cols[("HOTEL_DIM", "HOTEL_ID")][4] == "N"
    # Never classified: unknown, so the table flag is unknown too (fails closed).
    assert cols[("RESERVATION", "LOAD_TS")][10] is None
    assert cols[("RESERVATION", "LOAD_TS")][4] is None
    # Restricted by tag, at column and table level.
    assert cols[("RESERVATION", "PASSPORT_NUMBER")][11] == "Y"
    assert cols[("RESERVATION", "LOAD_TS")][5] == "Y"
    # Column-name pattern, case-insensitive.
    assert cols[("CLICK_EVENTS", "Email")][10] == "Y"
    assert cols[("CLICK_EVENTS", "EVENT_ID")][4] == "Y"
    # Types and ordering.
    assert cols[("TRAVELER", "TRAVELER_ID")][8] == "NUMBER(38,0)"
    assert [r[6] for r in rows if r[2] == "RESERVATION"] == [
        "RESERVATION_ID",
        "PASSPORT_NUMBER",
        "LOAD_TS",
    ]


def test_discovered_csv_feeds_plan(tmp_path):
    out = tmp_path / "metadata.csv"
    discover.write_csv(run_discovery(), out)
    zones = {m.spec.table: m.spec.zone for m in generate.read_metadata(out)}
    assert zones == {
        "TRAVELER": "PII",
        "HOTEL_DIM": "INTERNAL",
        "RESERVATION": "RESTRICTED",
        "CLICK_EVENTS": "PII",
    }
    reservation = next(m for m in generate.read_metadata(out) if m.spec.table == "RESERVATION")
    assert [c.name for c in reservation.spec.columns if c.use_xref] == ["PASSPORT_NUMBER"]


def test_without_classification_everything_untagged_is_unknown():
    settings = DiscoverSettings.from_raw({"discover": {"use_snowflake_classification": False}})
    rows = run_discovery(settings)
    assert {r[10] for r in rows} == {None}
    assert "DATA_CLASSIFICATION_LATEST" not in build_sql(["TRAVEL"], settings)
    assert "TAG_REFERENCES" not in build_sql(["TRAVEL"], settings)


def test_opt_in_to_treat_unclassified_as_non_pii():
    settings = DiscoverSettings.from_raw({"discover": {"treat_unclassified_as_non_pii": True}})
    cols = by_column(run_discovery(settings))
    assert cols[("RESERVATION", "LOAD_TS")][10] == "N"
    assert cols[("TRAVELER", "FULL_NAME")][10] == "Y"  # tags still win


def test_tag_value_rule_and_escaping():
    settings = DiscoverSettings.from_raw(
        {"discover": {"pii_tags": ["gov.tags.tokenized=it's"], "pii_column_patterns": ["o'brien"]}}
    )
    sql = build_sql(["travel"], settings)
    assert "TAG_VALUE = 'it''s'" in sql and "'o''brien'" in sql
    assert "IN ('TRAVEL')" in sql  # unquoted names are upper-cased
    run_discovery(settings)  # still valid SQL


def test_information_schema_source_queries_each_database():
    settings = DiscoverSettings.from_raw({"discover": {"source": "information_schema"}})
    sql = build_sql(["TRAVEL", "Mixed Case"], settings)
    assert 'FROM "TRAVEL".INFORMATION_SCHEMA.COLUMNS c' in sql
    assert 'FROM "Mixed Case".INFORMATION_SCHEMA.COLUMNS c' in sql
    assert "UNION ALL" in sql
    sqlglot.transpile(sql, read="snowflake", write="duckdb")  # parses


@pytest.mark.parametrize(
    "raw, message",
    [
        ({"source": "elsewhere"}, "discover.source"),
        ({"pii_tags": ["JUST_A_TAG"]}, "DATABASE.SCHEMA.TAG"),
        ({"pii_tags": ["A.B.C; DROP TABLE X"]}, "DATABASE.SCHEMA.TAG"),
        ({"pii_column_patterns": ["("]}, "bad column pattern"),
    ],
)
def test_bad_settings_are_refused(raw, message):
    with pytest.raises(DiscoverError, match=message):
        DiscoverSettings.from_raw({"discover": raw})


def test_tag_rule_parse():
    assert TagRule.parse("gov.tags.pii=Yes") == TagRule("GOV", "TAGS", "PII", "Yes")


def test_cli_sql_only_and_live_run(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("LAKEFREIGHT_CONFIG", raising=False)
    assert cli.main(["discover", "--database", "TRAVEL", "--sql-only"]) == 0
    assert "SNOWFLAKE.ACCOUNT_USAGE.COLUMNS" in capsys.readouterr().out

    rows = run_discovery()
    monkeypatch.setattr(discover, "run_query", lambda sql, name: rows)
    assert cli.main(["discover", "--database", "TRAVEL", "--out", "m.csv"]) == 0
    out = capsys.readouterr().out
    assert "wrote m.csv: 9 columns in 4 tables" in out
    assert "INTERNAL 1, PII 2, RESTRICTED 1" in out
    assert "1 tables have unclassified columns" in out
    with open(tmp_path / "m.csv", newline="") as f:
        header = next(csv.reader(f))
    assert header == discover.OUTPUT_COLUMNS

    monkeypatch.setattr(discover, "run_query", lambda sql, name: [])
    assert cli.main(["discover", "--database", "NOPE", "--out", "n.csv"]) == 1
    assert not (tmp_path / "n.csv").exists()


def test_cli_table_writes_create_table_as(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("LAKEFREIGHT_CONFIG", raising=False)
    args = ["discover", "--database", "TRAVEL", "--table", "LAKEFREIGHT.CONTROL.SOURCE_METADATA"]
    assert cli.main([*args, "--sql-only"]) == 0
    sql = capsys.readouterr().out
    assert sql.startswith("CREATE OR REPLACE TABLE LAKEFREIGHT.CONTROL.SOURCE_METADATA AS\nWITH")

    ran = []
    monkeypatch.setattr(discover, "run_query", lambda sql, name: ran.append(sql) or [("ok",)])
    assert cli.main(args) == 0
    assert ran and ran[0].startswith("CREATE OR REPLACE TABLE")
    assert cli.main(["discover", "--database", "TRAVEL", "--table", "x; DROP TABLE y"]) == 2


def test_seed_script_reads_discover_columns():
    from pathlib import Path

    seed = (Path(__file__).resolve().parents[1] / "sql" / "ddl" / "02_seed_config.sql").read_text()
    used = {
        "TABLE_SIZE_BYTES",
        "COLUMN_ORDINAL",
        "COLUMN_DATATYPE",
        "COLUMN_LENGTH",
        "COLUMN_DESCRIPTION",
        "COLUMN_PII_FLAG",
        "COLUMN_RESTRICTED_FLAG",
        "TABLE_PII_FLAG",
        "TABLE_RESTRICTED_FLAG",
    }
    assert used <= {c.upper() for c in discover.OUTPUT_COLUMNS}
    assert all(c in seed for c in used)
    assert "<METADATA_TABLE>" not in seed
