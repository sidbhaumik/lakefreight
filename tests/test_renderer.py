import pytest

from migrator import renderer
from migrator.config import load_settings
from migrator.models import ColumnSpec, TableSpec, resolve_zone
from migrator.reconcile import evaluate

settings = load_settings()


def table(zone="INTERNAL", columns=None, source_database=None):
    columns = columns or (
        ColumnSpec("ID", "NUMBER(38,0)"),
        ColumnSpec("CREATED_AT", "TIMESTAMP_TZ(9)"),
        ColumnSpec("PAYLOAD", "VARIANT"),
    )
    return TableSpec("SALES", "PUBLIC", "ORDERS", zone, tuple(columns), source_database)


def test_internal_unload_casts_and_targets_internal_stage():
    sql = renderer.unload_sql(table(), None, "full", "run1", settings)
    assert sql.startswith(
        "COPY INTO @MIGRATION.CONTROL.EGRESS_INTERNAL_STAGE/SALES/PUBLIC/ORDERS/chunk=full/run_id=run1/"
    )
    assert """CONVERT_TIMEZONE('UTC', t."CREATED_AT")::TIMESTAMP_NTZ AS "CREATED_AT\"""" in sql
    assert 'TO_JSON(t."PAYLOAD") AS "PAYLOAD"' in sql
    assert "JOIN" not in sql and "WHERE" not in sql
    assert "TYPE = PARQUET" in sql and "HEADER = TRUE" in sql


def test_restricted_unload_left_joins_xref_once_per_restricted_column():
    cols = (
        ColumnSpec("ID", "NUMBER"),
        ColumnSpec("PASSPORT_NO", "VARCHAR", use_xref=True),
        ColumnSpec("ALT_PASSPORT_NO", "VARCHAR", use_xref=True),
    )
    sql = renderer.unload_sql(
        table("RESTRICTED", cols), """t."TXN_DT" < '2020-01-01'""", "0001_201901_201912", "r", settings
    )
    assert "@MIGRATION.CONTROL.EGRESS_RESTRICTED_STAGE/" in sql
    assert sql.count("LEFT JOIN SECURE.PUBLIC.TOKEN_XREF") == 2
    assert "INNER JOIN" not in sql
    assert 'x1."CLEAR_VALUE" AS "PASSPORT_NO"' in sql
    assert 'x2."CLEAR_VALUE" AS "ALT_PASSPORT_NO"' in sql
    assert 'ON t."ALT_PASSPORT_NO" = x2."TOKEN_VALUE"' in sql
    assert """WHERE t."TXN_DT" < '2020-01-01'""" in sql


def test_xref_column_outside_restricted_zone_is_refused():
    cols = (ColumnSpec("PASSPORT_NO", "VARCHAR", use_xref=True),)
    with pytest.raises(ValueError, match="may only go to RESTRICTED"):
        renderer.unload_sql(table("INTERNAL", cols), None, "full", "r", settings)


def test_clone_database_is_read_but_path_keeps_original_name():
    sql = renderer.unload_sql(table(source_database="SALES_CLONE_T0"), None, "full", "r", settings)
    assert 'FROM "SALES_CLONE_T0"."PUBLIC"."ORDERS" t' in sql
    assert "/SALES/PUBLIC/ORDERS/" in sql


def test_count_and_unmatched_sql():
    cols = (ColumnSpec("ID", "NUMBER"), ColumnSpec("PASSPORT_NO", "VARCHAR", use_xref=True))
    t = table("RESTRICTED", cols)
    pred = 't."D" IS NULL'
    assert renderer.source_count_sql(t, pred) == (
        'SELECT COUNT(*) FROM "SALES"."PUBLIC"."ORDERS" t WHERE t."D" IS NULL'
    )
    unmatched = renderer.unmatched_sql(t, pred, settings)
    assert '(t."D" IS NULL) AND ((t."PASSPORT_NO" IS NOT NULL AND x1."TOKEN_VALUE" IS NULL))' in unmatched
    assert renderer.unmatched_sql(table(), None, settings) is None


def test_unsafe_path_is_refused():
    with pytest.raises(ValueError, match="unsafe"):
        renderer.unload_path(table(), "x'; DROP", "r")


def test_zone_fails_closed():
    assert resolve_zone("Y", "N") == "RESTRICTED"
    assert resolve_zone("Y", "Y") == "RESTRICTED"
    assert resolve_zone("N", "Y") == "PII"
    assert resolve_zone("N", "N") == "INTERNAL"
    assert resolve_zone(None, "N") == "PII"
    assert resolve_zone("N", None) == "PII"


def test_reconcile_rules():
    assert evaluate(100, 100, None, 0) == (True, None)
    assert evaluate(99, 100, None, 0)[0] is False   # rows dropped
    assert evaluate(101, 100, 0, 0)[0] is False     # rows duplicated by the join
    assert evaluate(100, 100, 5, 0)[0] is False     # unmatched tokens
    assert evaluate(100, 100, 5, 10)[0] is True
