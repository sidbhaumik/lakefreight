"""Split a large table into date-range chunks of roughly equal size.

Input is one profile query per table (row count by month). Months are packed
in order into chunks close to the target size. The resulting predicates cover
every possible value of the chunk column:

    first chunk   col <  end            (no lower bound)
    middle        col >= start AND col < end
    last chunk    col >= start          (no upper bound)
    null chunk    col IS NULL           (always emitted)

so rows outside the profiled range cannot be missed.
"""

from __future__ import annotations

from datetime import date

from .models import Chunk, quote_ident

FULL_CHUNK_ID = "full"
NULL_CHUNK_ID = "null"


def profile_sql(source_fqn: str, column: str) -> str:
    col = quote_ident(column)
    return (
        f"SELECT DATE_TRUNC('MONTH', {col})::DATE AS MONTH_START, COUNT(*) AS ROW_COUNT "
        f"FROM {source_fqn} GROUP BY 1"
    )


def full_chunk(table_bytes: int = 0) -> Chunk:
    return Chunk(chunk_id=FULL_CHUNK_ID, predicate=None, est_bytes=table_bytes)


def plan_chunks(
    column: str,
    month_counts: list[tuple[date | None, int]],
    table_bytes: int,
    target_bytes: int,
) -> list[Chunk]:
    """Return chunks for one table. `month_counts` is the profile query result."""
    if target_bytes <= 0:
        raise ValueError("target_bytes must be positive")

    col = f"t.{quote_ident(column)}"  # every rendered statement aliases the source as t
    null_rows = sum(n for m, n in month_counts if m is None)
    months = sorted((m, n) for m, n in month_counts if m is not None)
    total_rows = null_rows + sum(n for _, n in months)
    bytes_per_row = table_bytes / total_rows if total_rows else 0.0

    # Greedy packing of consecutive months.
    groups: list[list[tuple[date, int]]] = []
    current: list[tuple[date, int]] = []
    current_bytes = 0.0
    for month, rows in months:
        month_bytes = rows * bytes_per_row
        if current and current_bytes + month_bytes > target_bytes:
            groups.append(current)
            current, current_bytes = [], 0.0
        current.append((month, rows))
        current_bytes += month_bytes
    if current:
        groups.append(current)

    chunks: list[Chunk] = []
    for i, group in enumerate(groups):
        start = group[0][0]
        is_first, is_last = i == 0, i == len(groups) - 1
        end = None if is_last else groups[i + 1][0][0]
        parts = []
        if not is_first:
            parts.append(f"{col} >= '{start.isoformat()}'")
        if not is_last:
            parts.append(f"{col} < '{end.isoformat()}'")
        # A single group has no bounds at all, so match every non-null value.
        predicate = " AND ".join(parts) if parts else f"{col} IS NOT NULL"
        rows = sum(n for _, n in group)
        chunks.append(
            Chunk(
                chunk_id=f"{i + 1:04d}_{start:%Y%m}_{group[-1][0]:%Y%m}",
                predicate=predicate,
                est_rows=rows,
                est_bytes=int(rows * bytes_per_row),
                months=tuple(m for m, _ in group),
            )
        )

    if not chunks:
        chunks.append(Chunk(chunk_id="0001_all", predicate=f"{col} IS NOT NULL"))

    chunks.append(
        Chunk(
            chunk_id=NULL_CHUNK_ID,
            predicate=f"{col} IS NULL",
            est_rows=null_rows,
            est_bytes=int(null_rows * bytes_per_row),
        )
    )
    return chunks


def oversized(chunks: list[Chunk], target_bytes: int, factor: float = 2.0) -> list[Chunk]:
    """Chunks far above target: a single month too big to pack. Review these;
    they may need a finer (weekly or daily) split."""
    return [c for c in chunks if c.est_bytes > target_bytes * factor]
