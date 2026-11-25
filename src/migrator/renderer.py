"""Render the SQL for one work item from current metadata.

Three statements per work item:
  unload_sql      COPY INTO the zone's stage as Parquet
  source_count    row count of the un-joined source for the same predicate
  unmatched_sql   RESTRICTED only: non-null tokens with no xref match
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from jinja2 import Environment, PackageLoader, StrictUndefined

from .config import Settings
from .models import ColumnSpec, TableSpec, quote_ident

_env = Environment(
    loader=PackageLoader("migrator", "templates"),
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


def select_expr(col: ColumnSpec, xref_alias: str | None, clear_column: str) -> str:
    """Column expression with the cast Parquet needs, aliased to the original name."""
    name = quote_ident(col.name)
    if xref_alias:
        return f"{xref_alias}.{quote_ident(clear_column)} AS {name}"
    base = _base_type(col.data_type)
    if base in ("TIMESTAMP_TZ", "TIMESTAMP_LTZ"):
        return f"CONVERT_TIMEZONE('UTC', t.{name})::TIMESTAMP_NTZ AS {name}"
    if base in ("VARIANT", "OBJECT", "ARRAY"):
        return f"TO_JSON(t.{name}) AS {name}"
    if base in ("GEOGRAPHY", "GEOMETRY"):
        return f"ST_ASWKT(t.{name}) AS {name}"
    return f"t.{name}"


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
