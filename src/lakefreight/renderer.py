"""Render the SQL for one work item from current metadata.

Statements per work item:
  unload_sql          COPY INTO the zone's stage as Parquet
  source_count        row count of the un-joined source for the same predicate
  unmatched_sql       RESTRICTED only: non-null tokens with no xref match
  source_values_sql   fingerprint of the values as unloaded, and timestamp
                      values finer than a millisecond, from the source
  staged_values_sql   the same fingerprint, read back from the unloaded files
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from jinja2 import Environment, PackageLoader, StrictUndefined

from .config import Settings
from .models import ColumnSpec, TableSpec, quote_ident

_env = Environment(
    loader=PackageLoader("lakefreight", "templates"),
    undefined=StrictUndefined,
    keep_trailing_newline=False,
)

_SAFE_PATH = re.compile(r"^[A-Za-z0-9_.=\-/$]+$")


@dataclass(frozen=True)
class XrefJoin:
    alias: str
    column: str  # quoted source column


def _base_type(data_type: str) -> str:
    return data_type.upper().split("(")[0].strip()


_TIMESTAMPS = ("TIMESTAMP_TZ", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ", "DATETIME", "TIMESTAMP")
_AS_TEXT = ("TIME", "VARIANT", "OBJECT", "ARRAY", "GEOGRAPHY", "GEOMETRY")
_FLOATS = ("FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL")


def value_expr(col: ColumnSpec, xref_alias: str | None, clear_column: str) -> str:
    """The value unloaded for a column: the source column with the cast Parquet needs."""
    name = quote_ident(col.name)
    if xref_alias:
        return f"{xref_alias}.{quote_ident(clear_column)}"
    base = _base_type(col.data_type)
    # Snowflake refuses to unload TIMESTAMP_TZ/LTZ to Parquet, so they are converted
    # to UTC first. Snowflake writes every Parquet timestamp in milliseconds,
    # whatever the column's precision (confirmed on a live account), so digits
    # finer than a millisecond are lost; see docs/design-decisions.md, 14.
    if base in ("TIMESTAMP_TZ", "TIMESTAMP_LTZ"):
        return f"CONVERT_TIMEZONE('UTC', t.{name})::TIMESTAMP_NTZ(6)"
    if base in ("TIMESTAMP_NTZ", "DATETIME"):
        return f"t.{name}::TIMESTAMP_NTZ(6)"
    if base == "TIME":
        # Delta Lake has no TIME type.
        return f"TO_VARCHAR(t.{name}, 'HH24:MI:SS.FF6')"
    if base in ("VARIANT", "OBJECT", "ARRAY"):
        return f"TO_JSON(t.{name})"
    if base in ("GEOGRAPHY", "GEOMETRY"):
        return f"ST_ASWKT(t.{name})"
    return f"t.{name}"


def select_expr(col: ColumnSpec, xref_alias: str | None, clear_column: str) -> str:
    """Column expression with the cast Parquet needs, aliased to the original name."""
    value = value_expr(col, xref_alias, clear_column)
    name = quote_ident(col.name)
    return value if value == f"t.{name}" else f"{value} AS {name}"


def _is_timestamp(col: ColumnSpec) -> bool:
    return _base_type(col.data_type) in _TIMESTAMPS


def _fingerprint(col: ColumnSpec, value: str, detokenized: bool) -> str:
    """The value in a form both sides can hash alike: the type it was unloaded as.

    Timestamps are compared to the millisecond, the precision Snowflake writes,
    cut the way its Parquet writer cuts them: toward zero on the epoch, which
    moves a value before 1970 forward. The precision check reports anything
    finer separately.
    """
    base = _base_type(col.data_type)
    if base in _TIMESTAMPS:
        nanos = f"DATE_PART(EPOCH_NANOSECOND, ({value})::TIMESTAMP_NTZ(9))"
        return f"TO_TIMESTAMP_NTZ(TRUNC({nanos} / 1000000), 3)"
    if detokenized or base in _AS_TEXT:
        return f"({value})::VARCHAR"
    if base in _FLOATS:
        return f"({value})::FLOAT"
    return f"({value})::{col.data_type}"


def _literal(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "''") + "'"


def unload_path(table: TableSpec, chunk_id: str, run_id: str) -> str:
    path = f"{table.path}/chunk={chunk_id}/run_id={run_id}"
    if not _SAFE_PATH.match(path):
        raise ValueError(f"unsafe characters in unload path: {path!r}")
    return path


def _xref_joins(table: TableSpec) -> dict[str, XrefJoin]:
    joins: dict[str, XrefJoin] = {}
    for col in table.columns:
        if col.use_xref:
            joins[col.name] = XrefJoin(alias=f"x{len(joins) + 1}", column=quote_ident(col.name))
    return joins


def _check_zone(table: TableSpec, joins: dict[str, XrefJoin]) -> None:
    if joins and table.zone != "RESTRICTED":
        raise ValueError(
            f"{table.path} has xref columns but zone {table.zone}; clear values may only go to RESTRICTED"
        )
    if not table.columns:
        raise ValueError(f"{table.path} has no columns")


def unload_sql(
    table: TableSpec, predicate: str | None, chunk_id: str, run_id: str, settings: Settings
) -> str:
    joins = _xref_joins(table)
    _check_zone(table, joins)
    exprs = [
        select_expr(c, joins[c.name].alias if c.name in joins else None, settings.xref_clear_column)
        for c in table.columns
    ]
    return _env.get_template("copy_unload.sql.j2").render(
        stage=settings.zones[table.zone].stage,
        unload_path=unload_path(table, chunk_id, run_id),
        select_exprs=exprs,
        source_fqn=table.source_fqn,
        xref_joins=list(joins.values()),
        xref_table=settings.xref_table,
        token_column=quote_ident(settings.xref_token_column),
        predicate=predicate,
        max_file_size=settings.max_file_size_bytes,
    )


def source_count_sql(table: TableSpec, predicate: str | None) -> str:
    sql = f"SELECT COUNT(*) FROM {table.source_fqn} t"
    return f"{sql} WHERE {predicate}" if predicate else sql


def source_values_sql(table: TableSpec, predicate: str | None, settings: Settings) -> str:
    """One row: VALUE_HASH over every column as unloaded, then for each timestamp
    column a count of values with digits finer than a millisecond (SUB_MS_1...).
    Uses the same joins as the unload, so detokenized values are compared too."""
    joins = _xref_joins(table)
    _check_zone(table, joins)
    hashed = [
        _fingerprint(
            c,
            value_expr(
                c, joins[c.name].alias if c.name in joins else None, settings.xref_clear_column
            ),
            c.name in joins,
        )
        for c in table.columns
    ]
    checks = [
        f"COUNT_IF(t.{quote_ident(c.name)} <> DATE_TRUNC('MILLISECOND', t.{quote_ident(c.name)}))"
        f" AS SUB_MS_{i + 1}"
        for i, c in enumerate(c for c in table.columns if _is_timestamp(c))
    ]
    token = quote_ident(settings.xref_token_column)
    join_sql = "".join(
        f"\nLEFT JOIN {settings.xref_table} {j.alias} ON t.{j.column} = {j.alias}.{token}"
        for j in joins.values()
    )
    select = ",\n  ".join([f"HASH_AGG({', '.join(hashed)}) AS VALUE_HASH", *checks])
    sql = f"SELECT\n  {select}\nFROM {table.source_fqn} t{join_sql}"
    return f"{sql}\nWHERE {predicate}" if predicate else sql


def timestamp_columns(table: TableSpec) -> list[str]:
    """Names of the columns source_values_sql checks, in SUB_MS_n order."""
    return [c.name for c in table.columns if _is_timestamp(c)]


def staged_values_sql(table: TableSpec, chunk_id: str, run_id: str, settings: Settings) -> str:
    """ROW_COUNT and VALUE_HASH read back from the files one unload attempt wrote."""
    joins = _xref_joins(table)
    hashed = [_fingerprint(c, f"$1[{_literal(c.name)}]", c.name in joins) for c in table.columns]
    stage = settings.zones[table.zone].stage
    return (
        f"SELECT COUNT(*) AS ROW_COUNT, HASH_AGG({', '.join(hashed)}) AS VALUE_HASH\n"
        f"FROM @{stage}/{unload_path(table, chunk_id, run_id)}/\n"
        f"  (FILE_FORMAT => {_literal(settings.parquet_file_format)})"
    )


def unmatched_sql(table: TableSpec, predicate: str | None, settings: Settings) -> str | None:
    """Count rows where any RESTRICTED token is non-null but missing from the xref."""
    joins = _xref_joins(table)
    if not joins:
        return None
    token = quote_ident(settings.xref_token_column)
    join_sql = "\n".join(
        f"LEFT JOIN {settings.xref_table} {j.alias} ON t.{j.column} = {j.alias}.{token}"
        for j in joins.values()
    )
    misses = " OR ".join(
        f"(t.{j.column} IS NOT NULL AND {j.alias}.{token} IS NULL)" for j in joins.values()
    )
    where = f"({predicate}) AND ({misses})" if predicate else misses
    return f"SELECT COUNT(*) FROM {table.source_fqn} t\n{join_sql}\nWHERE {where}"
