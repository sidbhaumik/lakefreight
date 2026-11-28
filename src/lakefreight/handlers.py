"""Lambda handlers called by the Step Functions state machine.

    list_items  write the lane's pending work items to S3 for the Distributed Map
    submit      render and start the COPY asynchronously
    poll        check the COPY's status
    reconcile   compare counts, write the manifest, mark the item

A work item is a chunk of the bulk extract, or a change set from the delta pass
(KIND = 'CHANGES'), which reads the rows that changed between CHANGES_FROM_TS
and SNAPSHOT_TS.
    summarize   count how the run's items ended, so a lane with failures fails

Environment: LAKEFREIGHT_CONFIG (optional), ITEMS_BUCKET, plus the Snowflake
variables in store.connect.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime

from . import reconcile as rules
from . import renderer
from .config import Settings, load_settings
from .store import connect, get_work_item, load_table_spec, update_work_item

MAX_ATTEMPTS = 3


def _s3():
    import boto3

    return boto3.client("s3")


def _connect_for(settings: Settings, zone: str, lane: str):
    return connect(settings.zones[zone].role, settings.lanes[lane].warehouse)


def list_items(event, _context=None):
    """Input: {"lane": "LARGE"}. Output feeds the Distributed Map's ItemReader."""
    settings = load_settings()
    lane = event["lane"]
    # Any zone role can read the control tables; use the lane's own zone for RESTRICTED.
    zone = "RESTRICTED" if lane == "RESTRICTED" else "INTERNAL"
    conn = _connect_for(settings, zone, lane)
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT WORK_ITEM_ID FROM {settings.control_schema}.EXTRACT_WORK_ITEM "
            "WHERE LANE = %s AND (STATUS = 'PENDING' OR (STATUS = 'FAILED' AND ATTEMPTS < %s)) "
            "ORDER BY TABLE_NAME, CHUNK_ID",  # chunk ids sort oldest first
            (lane, MAX_ATTEMPTS),
        )
        items = [{"work_item_id": r[0]} for r in cur.fetchall()]
    finally:
        conn.close()
    bucket = os.environ["ITEMS_BUCKET"]
    key = f"runs/{lane}/{datetime.now(UTC):%Y%m%dT%H%M%SZ}.json"
    _s3().put_object(Bucket=bucket, Key=key, Body=json.dumps(items).encode())
    return {
        "lane": lane,
        "count": len(items),
        "max_concurrency": settings.lanes[lane].max_concurrency,
        "items": {"bucket": bucket, "key": key},
    }


def submit(event, _context=None):
    """Input: {"work_item_id": ...}. Starts the COPY and returns its query id."""
    settings = load_settings()
    work_item_id = event["work_item_id"]
    # Read the item with a neutral role first to learn its zone and lane.
    boot = _connect_for(settings, "INTERNAL", "SMALL")
    try:
        item = get_work_item(boot.cursor(), settings, work_item_id)
    finally:
        boot.close()

    conn = _connect_for(settings, item["ZONE"], item["LANE"])
    cur = conn.cursor()
    table = load_table_spec(
        cur, settings, item["DATABASE_NAME"], item["SCHEMA_NAME"], item["TABLE_NAME"]
    )
    run_id = uuid.uuid4().hex  # new prefix per attempt, so retries never mix files
    # A pinned table is read as of its snapshot, so every chunk and every retry
    # sees the same data however much the live table changes.
    snapshot = item["SNAPSHOT_TS"]
    sql = renderer.unload_sql(
        table,
        item["PREDICATE"],
        item["CHUNK_ID"],
        run_id,
        settings,
        snapshot,
        changes_from=item.get("CHANGES_FROM_TS"),
    )
    cur.execute_async(sql)
    query_id = cur.sfqid
    update_work_item(
        cur,
        settings,
        work_item_id,
        STATUS="RUNNING",
        RUN_ID=run_id,
        QUERY_ID=query_id,
        ATTEMPTS=int(item["ATTEMPTS"] or 0) + 1,
        LAST_ERROR=None,
    )
    # The connection is deliberately not closed: see "Known gaps" in docs/design-decisions.md.
    return {
        "work_item_id": work_item_id,
        "query_id": query_id,
        "run_id": run_id,
        "zone": item["ZONE"],
        "lane": item["LANE"],
    }


def poll(event, _context=None):
    """Adds "state": RUNNING | SUCCEEDED | FAILED to the event."""
    settings = load_settings()
    conn = _connect_for(settings, event["zone"], event["lane"])
    try:
        status = conn.get_query_status(event["query_id"])
        if conn.is_still_running(status):
            state = "RUNNING"
        elif status.name == "SUCCESS":
            state = "SUCCEEDED"
        else:
            state = "FAILED"
            update_work_item(
                conn.cursor(),
                settings,
                event["work_item_id"],
                STATUS="FAILED",
                LAST_ERROR=f"COPY ended with status {status.name}",
            )
    finally:
        conn.close()
    return {**event, "state": state}


def reconcile(event, _context=None):
    """Adds "state": RECONCILED | FAILED. Writes the manifest only on success."""
    settings = load_settings()
    work_item_id = event["work_item_id"]
    conn = _connect_for(settings, event["zone"], event["lane"])
    try:
        cur = conn.cursor()
        item = get_work_item(cur, settings, work_item_id)
        table = load_table_spec(
            cur, settings, item["DATABASE_NAME"], item["SCHEMA_NAME"], item["TABLE_NAME"]
        )

        # DETAILED_OUTPUT = TRUE returns one row per file.
        cur.get_results_from_sfqid(event["query_id"])
        # Fetch first: the connector fills in cur.description only once rows are read.
        # An empty chunk returns no rows and writes no file.
        result = cur.fetchall()
        cols = [d[0].upper() for d in cur.description or []]
        files = [
            {"name": r["FILE_NAME"], "size": int(r["FILE_SIZE"]), "rows": int(r["ROW_COUNT"])}
            for r in (dict(zip(cols, row, strict=True)) for row in result)
        ]
        rows_unloaded = sum(f["rows"] for f in files)

        snapshot = item["SNAPSHOT_TS"]
        changes_from = item.get("CHANGES_FROM_TS")
        changes = changes_from is not None
        cur.execute(renderer.source_count_sql(table, item["PREDICATE"], snapshot, changes_from))
        source_rows = int(cur.fetchone()[0])

        unmatched = None
        unmatched_sql = renderer.unmatched_sql(
            table, item["PREDICATE"], settings, snapshot, changes_from
        )
        if unmatched_sql:
            cur.execute(unmatched_sql)
            unmatched = int(cur.fetchone()[0])

        # Values, not just counts: fingerprint the source as unloaded and the files
        # as written, and count timestamps that lose digits below a millisecond.
        cur.execute(
            renderer.source_values_sql(table, item["PREDICATE"], settings, snapshot, changes_from)
        )
        source_values = cur.fetchone()
        source_hash = int(source_values[0] or 0)
        sub_ms = {
            col: int(n or 0)
            for col, n in zip(renderer.timestamp_columns(table), source_values[1:], strict=True)
        }
        cur.execute(
            renderer.staged_values_sql(table, item["CHUNK_ID"], event["run_id"], settings, changes)
        )
        staged_rows, staged_hash = cur.fetchone()

        balance = None
        if changes:
            # A complete change set balances the table's counts at both snapshots.
            cur.execute(renderer.change_balance_sql(table, changes_from, snapshot))
            balance = tuple(int(n or 0) for n in cur.fetchone())

        ok, reason = rules.evaluate(
            rows_unloaded,
            source_rows,
            unmatched,
            settings.unmatched_token_threshold,
            sub_millisecond=sub_ms,
            allow_precision_loss=settings.allow_timestamp_precision_loss,
            staged_rows=int(staged_rows or 0),
            source_hash=source_hash,
            staged_hash=int(staged_hash or 0),
            balance=balance,
        )
        counts = dict(
            ROWS_UNLOADED=rows_unloaded,
            SOURCE_ROWS=source_rows,
            FILE_COUNT=len(files),
            BYTES_UNLOADED=sum(f["size"] for f in files),
            UNMATCHED_TOKENS=unmatched,
        )
        if not ok:
            update_work_item(
                cur, settings, work_item_id, STATUS="FAILED", LAST_ERROR=reason, **counts
            )
            return {**event, "state": "FAILED", "reason": reason}

        zone = settings.zones[table.zone]
        if changes:
            # Each change set stands alone; its folder is not part of the table.
            base = f"{zone.prefix}/{renderer.CHANGES_FOLDER}/{table.path}"
            chunks_in_table = None
        else:
            base = f"{zone.prefix}/{table.path}"
            cur.execute(
                f"SELECT COUNT(*) FROM {settings.control_schema}.EXTRACT_WORK_ITEM "
                "WHERE DATABASE_NAME = %s AND SCHEMA_NAME = %s AND TABLE_NAME = %s "
                "AND COALESCE(KIND, 'BULK') = 'BULK'",
                (item["DATABASE_NAME"], item["SCHEMA_NAME"], item["TABLE_NAME"]),
            )
            chunks_in_table = int(cur.fetchone()[0])
        manifest = rules.build_manifest(
            table,
            item["CHUNK_ID"],
            event["run_id"],
            item["PREDICATE"],
            str(snapshot) if snapshot else None,
            files,
            rows_unloaded,
            chunks_in_table=chunks_in_table,
            value_hash=source_hash,
            changes_from_ts=str(changes_from) if changes else None,
        )
        s3 = _s3()
        # Encrypt lakefreight's own files with the zone's key, like the data files.
        sse = (
            {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": zone.kms_key_id}
            if zone.kms_key_id
            else {}
        )
        s3.put_object(
            Bucket=zone.bucket,
            Key=f"{base}/_schema.json",
            Body=json.dumps(rules.build_schema(table), indent=2).encode(),
            **sse,
        )
        # The manifest is the commit marker: written last.
        s3.put_object(
            Bucket=zone.bucket,
            Key=f"{base}/chunk={item['CHUNK_ID']}/run_id={event['run_id']}/manifest.json",
            Body=json.dumps(manifest, indent=2).encode(),
            **sse,
        )
        update_work_item(cur, settings, work_item_id, STATUS="RECONCILED", **counts)
        return {**event, "state": "RECONCILED"}
    finally:
        conn.close()


def summarize(event, _context=None):
    """Input: list_items' output, passed through the Map. Counts this run's items by status.

    The Map tolerates failed items so every item gets its attempt; this step makes the
    execution fail afterwards if any item did not reconcile.
    """
    settings = load_settings()
    lane = event["lane"]
    body = _s3().get_object(Bucket=event["items"]["bucket"], Key=event["items"]["key"])["Body"]
    ids = [i["work_item_id"] for i in json.loads(body.read())]
    zone = "RESTRICTED" if lane == "RESTRICTED" else "INTERNAL"
    conn = _connect_for(settings, zone, lane)
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT STATUS, COUNT(*) FROM {settings.control_schema}.EXTRACT_WORK_ITEM "
            "WHERE WORK_ITEM_ID IN (SELECT VALUE::STRING FROM TABLE(FLATTEN(PARSE_JSON(%s)))) "
            "GROUP BY STATUS",
            (json.dumps(ids),),
        )
        statuses = {status: int(n) for status, n in cur.fetchall()}
    finally:
        conn.close()
    return {
        "lane": lane,
        "count": len(ids),
        "statuses": statuses,
        "not_reconciled": len(ids) - statuses.get("RECONCILED", 0),
    }
