"""Reconciliation rules and manifest contents. No I/O here."""

from __future__ import annotations

from datetime import UTC, datetime

from .models import TableSpec

# Version of the manifest and schema file layout. Readers refuse a newer one.
# Files without the field, written before 1.0, are version 1.
FORMAT_VERSION = 1


def evaluate(
    rows_unloaded: int,
    source_rows: int,
    unmatched_tokens: int | None,
    unmatched_threshold: int,
    *,
    sub_millisecond: dict[str, int] | None = None,
    allow_precision_loss: bool = False,
    staged_rows: int | None = None,
    source_hash: int | None = None,
    staged_hash: int | None = None,
    balance: tuple[int, int, int, int] | None = None,
) -> tuple[bool, str | None]:
    """A work item passes only if no rows were dropped or duplicated; for
    RESTRICTED, unmatched tokens are within the threshold; no timestamp loses
    digits unless that was accepted; and the values read back from the files
    match the source, by fingerprint. A change set must also balance: the
    rows at its first snapshot, plus inserts, minus deletes, equal the rows at
    its second (`balance` is ROWS_FROM, ROWS_TO, INSERTS, DELETES)."""
    if rows_unloaded != source_rows:
        return False, f"row count mismatch: unloaded {rows_unloaded}, source {source_rows}"
    if unmatched_tokens is not None and unmatched_tokens > unmatched_threshold:
        return False, (
            f"{unmatched_tokens} rows have a token with no xref match "
            f"(threshold {unmatched_threshold})"
        )
    lossy = {col: n for col, n in (sub_millisecond or {}).items() if n}
    if lossy and not allow_precision_loss:
        detail = ", ".join(f"{col} ({n} values)" for col, n in sorted(lossy.items()))
        return False, (
            f"timestamps finer than a millisecond would be truncated: {detail}. Parquet "
            "from Snowflake keeps milliseconds only; set allow_timestamp_precision_loss "
            "to accept this"
        )
    if staged_rows is not None and staged_rows != rows_unloaded:
        return False, (
            f"the unloaded files hold {staged_rows} rows, the COPY reported {rows_unloaded}"
        )
    if source_hash is not None and staged_hash is not None and source_hash != staged_hash:
        return False, (
            "values differ between the source and the unloaded files "
            f"(fingerprint {source_hash} vs {staged_hash})"
        )
    if balance is not None:
        rows_from, rows_to, inserts, deletes = balance
        if rows_from + inserts - deletes != rows_to:
            return False, (
                f"change set does not balance: {rows_from} rows at the first snapshot "
                f"+ {inserts} inserts - {deletes} deletes != {rows_to} rows at the second"
            )
    return True, None


def build_manifest(
    table: TableSpec,
    chunk_id: str,
    run_id: str,
    predicate: str | None,
    snapshot_ts: str | None,
    files: list[dict],
    rows: int,
    chunks_in_table: int | None = None,
    written_at: str | None = None,
    value_hash: int | None = None,
    changes_from_ts: str | None = None,
) -> dict:
    """`files` entries are {"name", "size", "rows"}; names are relative to the
    manifest's own folder. `chunks_in_table` lets a reader check completeness."""
    return {
        "format_version": FORMAT_VERSION,
        "database": table.database,
        "schema": table.schema,
        "table": table.table,
        "zone": table.zone,
        "chunk_id": chunk_id,
        "run_id": run_id,
        "predicate": predicate,
        "snapshot_ts": snapshot_ts,
        # Change sets only: the first snapshot. The set holds the rows that changed
        # from changes_from_ts to snapshot_ts, with LF_ACTION and LF_IS_UPDATE.
        "changes_from_ts": changes_from_ts,
        "row_count": rows,
        "file_count": len(files),
        "bytes": sum(f["size"] for f in files),
        "files": files,
        "chunks_in_table": chunks_in_table,
        # Snowflake HASH_AGG of the chunk's values, as unloaded; equal on both sides.
        "value_hash": value_hash,
        "written_at": written_at or datetime.now(UTC).isoformat(timespec="seconds"),
    }


def build_schema(table: TableSpec) -> dict:
    """Sidecar the target uses to create tables and reapply tags."""
    return {
        "format_version": FORMAT_VERSION,
        "database": table.database,
        "schema": table.schema,
        "table": table.table,
        "zone": table.zone,
        "columns": [
            {
                "name": c.name,
                "source_type": c.data_type,
                "length": c.length,
                "description": c.description,
                "pii": c.pii_flag,
                "restricted": c.restricted_flag,
                "detokenized": c.use_xref,
            }
            for c in table.columns
        ],
    }
