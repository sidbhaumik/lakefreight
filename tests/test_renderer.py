import pytest

from lakefreight import renderer
from lakefreight.config import load_settings
from lakefreight.models import ColumnSpec, TableSpec, resolve_zone
from lakefreight.reconcile import evaluate

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
    assert """CONVERT_TIMEZONE('UTC', t."CREATED_AT")::TIMESTAMP_NTZ(6) AS "CREATED_AT\"""" in sql
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
        table("RESTRICTED", cols),
        """t."TXN_DT" < '2020-01-01'""",
        "0001_201901_201912",
        "r",
        settings,
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
    assert (
        '(t."D" IS NULL) AND ((t."PASSPORT_NO" IS NOT NULL AND x1."TOKEN_VALUE" IS NULL))'
        in unmatched
    )
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
    assert evaluate(99, 100, None, 0)[0] is False  # rows dropped
    assert evaluate(101, 100, 0, 0)[0] is False  # rows duplicated by the join
    assert evaluate(100, 100, 5, 0)[0] is False  # unmatched tokens
    assert evaluate(100, 100, 5, 10)[0] is True


def test_timestamps_leave_as_microseconds_and_time_as_text():
    cols = (
        ColumnSpec("LOADED", "TIMESTAMP_NTZ(9)"),
        ColumnSpec("SEEN", "TIMESTAMP_LTZ(9)"),
        ColumnSpec("OPENS", "TIME(9)"),
    )
    sql = renderer.unload_sql(table(columns=cols), None, "full", "r", settings)
    assert 't."LOADED"::TIMESTAMP_NTZ(6) AS "LOADED"' in sql
    assert """CONVERT_TIMEZONE('UTC', t."SEEN")::TIMESTAMP_NTZ(6) AS "SEEN\"""" in sql
    assert """TO_VARCHAR(t."OPENS", 'HH24:MI:SS.FF6') AS "OPENS\"""" in sql


# --- Value reconciliation ------------------------------------------------------


def test_source_fingerprint_matches_the_unload_and_counts_sub_millisecond_values():
    cols = (
        ColumnSpec("ID", "NUMBER(38,0)"),
        ColumnSpec("PASSPORT_NO", "VARCHAR", use_xref=True),
        ColumnSpec("SCORE", "DOUBLE"),
        ColumnSpec("CREATED_AT", "TIMESTAMP_TZ(9)"),
        ColumnSpec("OPENS", "TIME(9)"),
        ColumnSpec("LOADED_AT", "TIMESTAMP_NTZ(9)"),
    )
    sql = renderer.source_values_sql(table("RESTRICTED", cols), 't."ID" < 10', settings)
    assert '(t."ID")::NUMBER(38,0)' in sql
    assert '(x1."CLEAR_VALUE")::VARCHAR' in sql  # detokenized values are compared too
    assert '(t."SCORE")::FLOAT' in sql
    # Timestamps are cut to the millisecond toward zero on the epoch, like the writer.
    assert (
        "TO_TIMESTAMP_NTZ(TRUNC(DATE_PART(EPOCH_NANOSECOND, (CONVERT_TIMEZONE('UTC', "
        't."CREATED_AT")::TIMESTAMP_NTZ(6))::TIMESTAMP_NTZ(9)) / 1000000), 3)'
    ) in sql
    assert """(TO_VARCHAR(t."OPENS", 'HH24:MI:SS.FF6'))::VARCHAR""" in sql
    assert "LEFT JOIN SECURE.PUBLIC.TOKEN_XREF x1" in sql and sql.endswith('WHERE t."ID" < 10')
    assert "AS SUB_MS_1" in sql and "AS SUB_MS_2" in sql and "SUB_MS_3" not in sql
    assert renderer.timestamp_columns(table("RESTRICTED", cols)) == ["CREATED_AT", "LOADED_AT"]


def test_staged_fingerprint_reads_one_attempt_with_the_same_expressions():
    cols = (ColumnSpec("ID", "NUMBER(38,0)"), ColumnSpec("Owner's Name", "VARCHAR"))
    sql = renderer.staged_values_sql(table(columns=cols), "0001_k0_k10", "abc", settings)
    assert "SELECT COUNT(*) AS ROW_COUNT, HASH_AGG(($1['ID'])::NUMBER(38,0)" in sql
    assert "($1['Owner''s Name'])::VARCHAR" in sql
    assert (
        "@MIGRATION.CONTROL.EGRESS_INTERNAL_STAGE/SALES/PUBLIC/ORDERS/chunk=0001_k0_k10/run_id=abc/"
        in sql
    )
    assert "FILE_FORMAT => 'MIGRATION.CONTROL.LAKEFREIGHT_PARQUET'" in sql


def test_unload_select_is_unchanged_by_the_split():
    # select_expr keeps plain columns unaliased and casts aliased, as before.
    col = ColumnSpec("ID", "NUMBER(38,0)")
    assert renderer.select_expr(col, None, "CLEAR_VALUE") == 't."ID"'
    assert renderer.select_expr(col, "x1", "CLEAR_VALUE") == 'x1."CLEAR_VALUE" AS "ID"'


def test_reconcile_rules_for_values():
    ok = dict(staged_rows=5, source_hash=9, staged_hash=9)
    assert evaluate(5, 5, None, 0, **ok) == (True, None)
    assert (
        "values differ" in evaluate(5, 5, None, 0, staged_rows=5, source_hash=9, staged_hash=8)[1]
    )
    lossy = evaluate(5, 5, None, 0, sub_millisecond={"A": 0, "B": 3}, **ok)
    assert not lossy[0] and "B (3 values)" in lossy[1] and "A (" not in lossy[1]
    assert evaluate(5, 5, None, 0, sub_millisecond={"B": 3}, allow_precision_loss=True, **ok)[0]
    # The row count check still comes first.
    assert "row count mismatch" in evaluate(5, 4, None, 0, **ok)[1]


def test_parquet_file_format_setting(tmp_path):
    from lakefreight.config import default_config_text

    text = default_config_text().replace(
        "allow_timestamp_precision_loss = false",
        'allow_timestamp_precision_loss = true\nparquet_file_format = "OPS.FMT.PQ"',
        1,
    )
    path = tmp_path / "lakefreight.toml"
    path.write_text(text)
    custom = load_settings(path)
    assert custom.parquet_file_format == "OPS.FMT.PQ" and custom.allow_timestamp_precision_loss
    assert settings.parquet_file_format == "MIGRATION.CONTROL.LAKEFREIGHT_PARQUET"
    assert not settings.allow_timestamp_precision_loss
