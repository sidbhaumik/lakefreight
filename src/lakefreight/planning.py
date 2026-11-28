"""Create work items for every enabled table that has none yet.

Run once after seeding (and again after fixing chunk columns):

    PLANNER_ROLE=... PLANNER_WAREHOUSE=... python -m lakefreight.planning

The delta pass: once a table's bulk extract has reconciled, create a change set
holding the rows that changed since its snapshot (or since its last change set):

    PLANNER_ROLE=... PLANNER_WAREHOUSE=... python -m lakefreight.planning --changes
"""

from __future__ import annotations

import argparse
import os

from . import chunk_planner
from .config import Settings, load_settings
from .models import TableSpec, quote_ident
from .renderer import change_chunk_id, snapshot_literal, source_ref
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


_DONE = "('RECONCILED', 'REPLICATED', 'ACKED')"


def build_change_items(conn, settings: Settings) -> tuple[int, list[str]]:
    """Create one change set per table whose bulk extract has reconciled.

    Each holds the rows that changed from the table's last snapshot (its bulk
    snapshot, or its newest change set's) to a new snapshot shared by every
    table in this run. Returns the number created and a note for each table
    skipped. Every table is checked before any item is created: its time travel
    must reach back to the last snapshot, and change tracking must have been on
    since then.
    """
    ctl = settings.control_schema
    cur = conn.cursor()
    cur.execute(
        "SELECT w.DATABASE_NAME, w.SCHEMA_NAME, w.TABLE_NAME, c.ZONE, c.LANE, c.SOURCE_DATABASE, "
        "COUNT_IF(COALESCE(w.KIND, 'BULK') = 'BULK') AS BULK_ITEMS, "
        f"COUNT_IF(COALESCE(w.KIND, 'BULK') = 'BULK' AND w.STATUS IN {_DONE}) AS BULK_DONE, "
        "MAX(IFF(COALESCE(w.KIND, 'BULK') = 'BULK', w.SNAPSHOT_TS, NULL)) AS BULK_SNAPSHOT, "
        f"COUNT_IF(w.KIND = 'CHANGES' AND w.STATUS NOT IN {_DONE}) AS CHANGES_OPEN, "
        "MAX(IFF(w.KIND = 'CHANGES', w.SNAPSHOT_TS, NULL)) AS LAST_CHANGES "
        f"FROM {ctl}.EXTRACT_WORK_ITEM w "
        f"JOIN {ctl}.EXTRACT_CONFIG c ON c.DATABASE_NAME = w.DATABASE_NAME "
        "AND c.SCHEMA_NAME = w.SCHEMA_NAME AND c.TABLE_NAME = w.TABLE_NAME "
        "WHERE c.ENABLED "
        "GROUP BY w.DATABASE_NAME, w.SCHEMA_NAME, w.TABLE_NAME, c.ZONE, c.LANE, c.SOURCE_DATABASE "
        "ORDER BY 1, 2, 3"
    )
    eligible, skipped = [], []
    for (
        db,
        schema,
        table,
        zone,
        lane,
        source_db,
        items,
        done,
        bulk_snap,
        open_,
        last,
    ) in cur.fetchall():
        name = f"{db}.{schema}.{table}"
        if int(done or 0) < int(items or 0):
            skipped.append(f"{name}: bulk extract not finished ({done} of {items} reconciled)")
        elif bulk_snap is None:
            skipped.append(f"{name}: read live, not as of a snapshot, so it has no delta pass")
        elif source_db:
            skipped.append(
                f"{name}: extracted from the clone {source_db}; take its changes from the "
                "clone's creation time instead"
            )
        elif int(open_ or 0):
            skipped.append(f"{name}: a change set is still being extracted")
        else:
            spec = TableSpec(db, schema, table, zone, ())
            eligible.append((spec, lane, last or bulk_snap))
    if not eligible:
        return 0, skipped

    cur.execute("SELECT SYSDATE()")
    snapshot = cur.fetchone()[0]
    problems = []
    for spec, _lane, since in eligible:
        name = f"{spec.database}.{spec.schema}.{spec.table}"
        hours = (snapshot - since).total_seconds() / 3600 + settings.snapshot_window_hours
        days = _retention_days(cur, spec.database, spec.schema, spec.table)
        if days is None or days * 24 < hours:
            have = "no time travel" if not days else f"{days} day(s) of time travel"
            problems.append(
                f"{name} has {have}, but its change set reaches back to {since} and may run "
                f"for snapshot_window_hours ({settings.snapshot_window_hours:g}) more"
            )
            continue
        try:  # compiles only if change tracking has covered the whole range
            cur.execute(f"SELECT 1 FROM {source_ref(spec, snapshot, since)} t LIMIT 0")
        except Exception as exc:  # the connector raises its own error types
            problems.append(f"{name}: {str(exc).splitlines()[0]}")
    if problems:
        raise ValueError(
            "no change sets created:\n  " + "\n  ".join(problems) + "\nEnable change "
            "tracking (ALTER TABLE ... SET CHANGE_TRACKING = TRUE) before the bulk snapshot, "
            "and keep time travel long enough to reach it."
        )

    rows = []
    for spec, lane, since in eligible:
        chunk_id = change_chunk_id(since, snapshot)
        rows.append(
            (
                f"{spec.database}.{spec.schema}.{spec.table}#{chunk_id}",
                spec.database,
                spec.schema,
                spec.table,
                chunk_id,
                spec.zone,
                lane,
                "CHANGES",
                since,
                snapshot,
            )
        )
    cur.executemany(
        f"INSERT INTO {ctl}.EXTRACT_WORK_ITEM "
        "(WORK_ITEM_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, CHUNK_ID, ZONE, LANE, KIND, "
        "CHANGES_FROM_TS, SNAPSHOT_TS) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        rows,
    )
    return len(rows), skipped


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m lakefreight.planning", description=__doc__.splitlines()[0]
    )
    parser.add_argument(
        "--changes",
        action="store_true",
        help="create change sets (the delta pass) instead of bulk work items",
    )
    args = parser.parse_args(argv)
    settings = load_settings()
    conn = connect(os.environ["PLANNER_ROLE"], os.environ["PLANNER_WAREHOUSE"])
    try:
        if args.changes:
            created, skipped = build_change_items(conn, settings)
            print(f"created {created} change sets")
            for note in skipped:
                print(f"  skipped {note}")
        else:
            print(f"created {build_work_items(conn, settings)} work items")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
