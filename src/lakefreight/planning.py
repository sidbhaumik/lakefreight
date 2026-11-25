"""Create work items for every enabled table that has none yet.

Run once after seeding (and again after fixing chunk columns):

    PLANNER_ROLE=... PLANNER_WAREHOUSE=... python -m lakefreight.planning
"""

from __future__ import annotations

import os

from . import chunk_planner
from .config import Settings, load_settings
from .models import quote_ident
from .store import connect


def build_work_items(conn, settings: Settings) -> int:
    ctl = settings.control_schema
    cur = conn.cursor()
    cur.execute(
        "SELECT c.DATABASE_NAME, c.SCHEMA_NAME, c.TABLE_NAME, c.TABLE_BYTES, c.ZONE, c.LANE, "
        "c.STRATEGY, c.CHUNK_COLUMN, c.SOURCE_DATABASE "
        f"FROM {ctl}.EXTRACT_CONFIG c "
        "WHERE c.ENABLED AND NOT EXISTS ("
        f"  SELECT 1 FROM {ctl}.EXTRACT_WORK_ITEM w "
        "  WHERE w.DATABASE_NAME = c.DATABASE_NAME AND w.SCHEMA_NAME = c.SCHEMA_NAME "
        "    AND w.TABLE_NAME = c.TABLE_NAME)"
    )
    created = 0
    for db, schema, table, table_bytes, zone, lane, strategy, chunk_col, source_db in cur.fetchall():
        table_bytes = int(table_bytes or 0)
        if strategy == "CHUNKED" and chunk_col:
            fqn = ".".join(quote_ident(p) for p in (source_db or db, schema, table))
            cur.execute(chunk_planner.profile_sql(fqn, chunk_col))
            chunks = chunk_planner.plan_chunks(
                chunk_col, cur.fetchall(), table_bytes, settings.target_chunk_bytes
            )
        else:
            # FULL, or CHUNKED with no usable date column: one pass over the table.
            chunks = [chunk_planner.full_chunk(table_bytes)]
        cur.executemany(
            f"INSERT INTO {ctl}.EXTRACT_WORK_ITEM "
            "(WORK_ITEM_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, CHUNK_ID, PREDICATE, "
            "ZONE, LANE, EST_BYTES) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            [
                (f"{db}.{schema}.{table}#{c.chunk_id}", db, schema, table, c.chunk_id,
                 c.predicate, zone, lane, c.est_bytes)
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
