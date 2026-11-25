"""Reconciliation rules and manifest contents. No I/O here."""

from __future__ import annotations

from datetime import UTC, datetime

from .models import TableSpec


def evaluate(
    rows_unloaded: int,
    source_rows: int,
    unmatched_tokens: int | None,
    unmatched_threshold: int,
) -> tuple[bool, str | None]:
    """A work item passes only if no rows were dropped or duplicated and,
    for RESTRICTED, unmatched tokens are within the threshold."""
    if rows_unloaded != source_rows:
        return False, f"row count mismatch: unloaded {rows_unloaded}, source {source_rows}"
    if unmatched_tokens is not None and unmatched_tokens > unmatched_threshold:
        return False, (
            f"{unmatched_tokens} rows have a token with no xref match "
            f"(threshold {unmatched_threshold})"
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
) -> dict:
    """`files` entries are {"name", "size", "rows"}; names are relative to the
    manifest's own folder. `chunks_in_table` lets a reader check completeness."""
    return {
        "database": table.database,
        "schema": table.schema,
        "table": table.table,
        "zone": table.zone,
        "chunk_id": chunk_id,
        "run_id": run_id,
        "predicate": predicate,
        "snapshot_ts": snapshot_ts,
        "row_count": rows,
        "file_count": len(files),
        "bytes": sum(f["size"] for f in files),
        "files": files,
        "chunks_in_table": chunks_in_table,
        "written_at": written_at or datetime.now(UTC).isoformat(timespec="seconds"),
    }


def build_schema(table: TableSpec) -> dict:
    """Sidecar the target uses to create tables and reapply tags."""
    return {
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
