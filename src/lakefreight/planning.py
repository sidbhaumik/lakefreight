"""Create work items for every enabled table that has none yet.

Run once after seeding (and again after fixing chunk columns):

    PLANNER_ROLE=... PLANNER_WAREHOUSE=... python -m lakefreight.planning
"""

from __future__ import annotations

import os

from . import chunk_planner
from .config import Settings, load_settings
from .models import quote_ident
from .renderer import snapshot_literal
from .store import connect


def _pinned(settings: Settings, still_written) -> bool:
    return settings.snapshot == "all" or (
        settings.snapshot == "still_written" and bool(still_written)
    )


def _retention_days(cur, db: str, schema: str, table: str) -> int | None:
    cur.execute(
        f"SELECT RETENTION_TIME FROM {quote_ident(db)}.INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
        (schema, table),
    )
    row = cur.fetchone()
    return None if row is None or row[0] is None else int(row[0])


def build_work_items(conn, settings: Settings) -> int:
    """Create work items; pinned tables all share one snapshot taken now."""
    ctl = settings.control_schema
    cur = conn.cursor()
    cur.execute(
        "SELECT c.DATABASE_NAME, c.SCHEMA_NAME, c.TABLE_NAME, c.TABLE_BYTES, c.ZONE, c.LANE, "
        "c.STRATEGY, c.CHUNK_COLUMN, c.SOURCE_DATABASE, k.DATA_TYPE, c.STILL_WRITTEN "
        f"FROM {ctl}.EXTRACT_CONFIG c "
        f"LEFT JOIN {ctl}.EXTRACT_COLUMN_CONFIG k ON k.DATABASE_NAME = c.DATABASE_NAME "
        "AND k.SCHEMA_NAME = c.SCHEMA_NAME AND k.TABLE_NAME = c.TABLE_NAME "
        "AND k.COLUMN_NAME = c.CHUNK_COLUMN "
        "WHERE c.ENABLED AND NOT EXISTS ("
        f"  SELECT 1 FROM {ctl}.EXTRACT_WORK_ITEM w "
        "  WHERE w.DATABASE_NAME = c.DATABASE_NAME AND w.SCHEMA_NAME = c.SCHEMA_NAME "
        "    AND w.TABLE_NAME = c.TABLE_NAME)"
    )
    tables = cur.fetchall()

    # Check every pinned table before planning any, so a refusal leaves nothing half done.
    pinned = {t[:3] for t in tables if _pinned(settings, t[10])}
    for db, schema, table, *_rest in (t for t in tables if t[:3] in pinned):
        source_db = _rest[5] or db
        days = _retention_days(cur, source_db, schema, table)
        if days is None or days * 24 < settings.snapshot_window_hours:
            have = "no time travel" if not days else f"{days} day(s) of time travel"
            raise ValueError(
                f"{db}.{schema}.{table} is read as of a snapshot but has {have}, less than "
                f"snapshot_window_hours ({settings.snapshot_window_hours:g}). Raise its "
                "DATA_RETENTION_TIME_IN_DAYS, extract from a zero-copy clone, or lower the window."
            )
    snapshot = None
    if pinned:
        cur.execute("SELECT SYSDATE()")
        snapshot = cur.fetchone()[0]

    created = 0
    for (
        db,
        schema,
        table,
        table_bytes,
        zone,
        lane,
        strategy,
        chunk_col,
        source_db,
        chunk_type,
        _still_written,
    ) in tables:
        table_bytes = int(table_bytes or 0)
        table_snapshot = snapshot if (db, schema, table) in pinned else None
        if strategy == "CHUNKED" and chunk_col:
            kind = chunk_planner.chunk_kind(chunk_type)
            if kind is None:
                raise ValueError(
                    f"{db}.{schema}.{table}: chunk column {chunk_col} is "
                    f"{chunk_type or 'not a column of the table'}; it must be a date, "
                    "timestamp or integer column. Fix CHUNK_COLUMN and run the planner again."
                )
            fqn = ".".join(quote_ident(p) for p in (source_db or db, schema, table))
            if table_snapshot is not None:
                fqn += f" AT(TIMESTAMP => {snapshot_literal(table_snapshot)})"
            cur.execute(chunk_planner.profile_sql(fqn, chunk_col, kind))
            chunks = chunk_planner.plan_chunks(
                chunk_col, cur.fetchall(), table_bytes, settings.target_chunk_bytes
            )
        else:
            # FULL, or CHUNKED with no usable chunk column: one pass over the table.
            chunks = [chunk_planner.full_chunk(table_bytes)]
        cur.executemany(
            f"INSERT INTO {ctl}.EXTRACT_WORK_ITEM "
            "(WORK_ITEM_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, CHUNK_ID, PREDICATE, "
            "ZONE, LANE, EST_BYTES, SNAPSHOT_TS) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            [
                (
                    f"{db}.{schema}.{table}#{c.chunk_id}",
                    db,
                    schema,
                    table,
                    c.chunk_id,
                    c.predicate,
                    zone,
                    lane,
                    c.est_bytes,
                    table_snapshot,
                )
                for c in chunks
            ],
        )
        created += len(chunks)
    return created


if __name__ == "__main__":
    settings = load_settings()
    conn = connect(os.environ["PLANNER_ROLE"], os.environ["PLANNER_WAREHOUSE"])
    try:
        print(f"created {build_work_items(conn, settings)} work items")
    finally:
        conn.close()
