"""Lambda handlers called by the Step Functions state machine.

    list_items  write the lane's pending work items to S3 for the Distributed Map
    submit      render and start the COPY asynchronously
    poll        check the COPY's status
    reconcile   compare counts, write the manifest, mark the item

Environment: LAKEFREIGHT_CONFIG (optional), ITEMS_BUCKET, plus the Snowflake
variables in store.connect.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone

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
    key = f"runs/{lane}/{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json"
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
    sql = renderer.unload_sql(table, item["PREDICATE"], item["CHUNK_ID"], run_id, settings)
    cur.execute_async(sql)
    query_id = cur.sfqid
    update_work_item(
        cur, settings, work_item_id,
        STATUS="RUNNING", RUN_ID=run_id, QUERY_ID=query_id,
        SNAPSHOT_TS=datetime.now(timezone.utc).replace(tzinfo=None),
        ATTEMPTS=int(item["ATTEMPTS"] or 0) + 1, LAST_ERROR=None,
    )
    # The connection is deliberately not closed: see README, "Verify in the pilot".
    return {
        "work_item_id": work_item_id, "query_id": query_id, "run_id": run_id,
        "zone": item["ZONE"], "lane": item["LANE"],
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
                conn.cursor(), settings, event["work_item_id"],
                STATUS="FAILED", LAST_ERROR=f"COPY ended with status {status.name}",
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
        cols = [d[0].upper() for d in cur.description]
        files = [
            {"name": r["FILE_NAME"], "size": int(r["FILE_SIZE"]), "rows": int(r["ROW_COUNT"])}
            for r in (dict(zip(cols, row)) for row in cur.fetchall())
        ]
        rows_unloaded = sum(f["rows"] for f in files)

        cur.execute(renderer.source_count_sql(table, item["PREDICATE"]))
        source_rows = int(cur.fetchone()[0])

        unmatched = None
        unmatched_sql = renderer.unmatched_sql(table, item["PREDICATE"], settings)
        if unmatched_sql:
            cur.execute(unmatched_sql)
            unmatched = int(cur.fetchone()[0])

        ok, reason = rules.evaluate(
            rows_unloaded, source_rows, unmatched, settings.unmatched_token_threshold
        )
        counts = dict(
            ROWS_UNLOADED=rows_unloaded, SOURCE_ROWS=source_rows, FILE_COUNT=len(files),
            BYTES_UNLOADED=sum(f["size"] for f in files), UNMATCHED_TOKENS=unmatched,
        )
        if not ok:
            update_work_item(cur, settings, work_item_id, STATUS="FAILED", LAST_ERROR=reason, **counts)
            return {**event, "state": "FAILED", "reason": reason}

        zone = settings.zones[table.zone]
        base = f"{zone.prefix}/{table.path}"
        manifest = rules.build_manifest(
            table, item["CHUNK_ID"], event["run_id"], item["PREDICATE"],
            str(item["SNAPSHOT_TS"]) if item["SNAPSHOT_TS"] else None, files, rows_unloaded,
        )
        s3 = _s3()
        s3.put_object(
            Bucket=zone.bucket, Key=f"{base}/_schema.json",
            Body=json.dumps(rules.build_schema(table), indent=2).encode(),
        )
        # The manifest is the commit marker: written last.
        s3.put_object(
            Bucket=zone.bucket,
            Key=f"{base}/chunk={item['CHUNK_ID']}/run_id={event['run_id']}/manifest.json",
            Body=json.dumps(manifest, indent=2).encode(),
        )
        update_work_item(cur, settings, work_item_id, STATUS="RECONCILED", **counts)
        return {**event, "state": "RECONCILED"}
    finally:
        conn.close()
