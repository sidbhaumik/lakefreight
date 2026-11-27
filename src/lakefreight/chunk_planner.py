"""Split a large table into range chunks of roughly equal size.

Input is one profile query per table: row counts by day for a date or timestamp
column, or by equal-width bucket for an integer column. Buckets are packed in
order into chunks close to the target size. Date columns pack whole months, and
fall back to days only for a month too big for one chunk. The resulting
predicates cover every possible value of the chunk column:

    first chunk   col <  end            (no lower bound)
    middle        col >= start AND col < end
    last chunk    col >= start          (no upper bound)
    null chunk    col IS NULL           (always emitted)

so rows outside the profiled range cannot be missed.
"""

from __future__ import annotations

import re
from datetime import date
from decimal import Decimal

from .models import Chunk, quote_ident

FULL_CHUNK_ID = "full"
NULL_CHUNK_ID = "null"
KEY_BUCKETS = 10_000

_INTEGER_TYPES = re.compile(
    r"^(NUMBER|DECIMAL|NUMERIC)\s*\(\s*\d+\s*,\s*0\s*\)$|^(INT|INTEGER|BIGINT|SMALLINT|TINYINT|BYTEINT)$"
)


def chunk_kind(data_type: str | None) -> str | None:
    """'date' for DATE/TIMESTAMP columns, 'key' for integer columns, else None."""
    t = (data_type or "").strip().upper()
    if t.startswith(("DATE", "TIMESTAMP")):
        return "date"
    if _INTEGER_TYPES.match(t):
        return "key"
    return None


def leading_cluster_column(clustering_key: str | None, columns: list[str]) -> str | None:
    """The column a clustering key leads with, such as LOAD_TS in
    LINEAR(TO_DATE(LOAD_TS), REGION). None if no column appears in it.

    The same rule runs in sql/ddl/02_seed_config.sql; keep the two in step.
    """
    if not clustering_key:
        return None
    text = re.sub(r"'[^']*'", "", clustering_key)
    text = re.sub(r"^ *LINEAR *[(]", "", text, flags=re.IGNORECASE)
    best: tuple[int, str] | None = None
    for name in columns:
        pattern = r"(^|[^A-Za-z0-9_$])" + re.escape(name) + r"([^A-Za-z0-9_$]|$)"
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match and (best is None or match.start() < best[0]):
            best = (match.start(), name)
    return best[1] if best else None


def profile_sql(source_fqn: str, column: str, kind: str = "date") -> str:
    """Row counts per bucket of the chunk column; NULL values form their own bucket."""
    col = quote_ident(column)
    if kind == "date":
        return (
            f"SELECT DATE_TRUNC('DAY', {col})::DATE AS BUCKET_START, COUNT(*) AS ROW_COUNT "
            f"FROM {source_fqn} GROUP BY 1"
        )
    if kind == "key":
        # Equal-width buckets between the column's min and max, aligned to the min.
        width = f"GREATEST(CEIL((r.HI - r.LO + 1) / {KEY_BUCKETS}), 1)"
        return (
            f"WITH r AS (SELECT MIN({col}) AS LO, MAX({col}) AS HI FROM {source_fqn}) "
            f"SELECT (r.LO + FLOOR((s.{col} - r.LO) / {width}) * {width})::NUMBER(38,0) "
            f"AS BUCKET_START, COUNT(*) AS ROW_COUNT FROM {source_fqn} s, r GROUP BY 1"
        )
    raise ValueError(f"chunk columns must be dates, timestamps or integers, not {kind!r}")


def full_chunk(table_bytes: int = 0) -> Chunk:
    return Chunk(chunk_id=FULL_CHUNK_ID, predicate=None, est_bytes=table_bytes)


def _normalize(value):
    """Profile values arrive as date, int or Decimal; keys become int."""
    if value is None or isinstance(value, date):
        return value
    if isinstance(value, (int, Decimal)) and not isinstance(value, bool):
        if Decimal(value) != Decimal(value).to_integral_value():
            raise ValueError(f"key bucket {value} is not an integer")
        return int(value)
    raise ValueError(f"unexpected profile value {value!r}")


def _pack(units: list[tuple[object, int]], bytes_per_row: float, target: float):
    """Greedy packing of consecutive (start, rows) units into groups."""
    groups: list[list[tuple[object, int]]] = []
    current: list[tuple[object, int]] = []
    current_bytes = 0.0
    for start, rows in units:
        unit_bytes = rows * bytes_per_row
        if current and current_bytes + unit_bytes > target:
            groups.append(current)
            current, current_bytes = [], 0.0
        current.append((start, rows))
        current_bytes += unit_bytes
    if current:
        groups.append(current)
    return groups


def _month(d: date) -> date:
    return d.replace(day=1)


def _date_groups(counts: list[tuple[date, int]], bytes_per_row: float, target: float):
    """Pack whole months; a month above target on its own is packed by day.

    Returns (groups, granularity) with granularity 'month' or 'day' per group.
    """
    months: dict[date, list[tuple[date, int]]] = {}
    for day, rows in counts:
        months.setdefault(_month(day), []).append((day, rows))
    groups: list[tuple[list[tuple[date, int]], str]] = []
    pending: list[tuple[date, int]] = []  # whole months waiting to be packed
    for month in sorted(months):
        days = sorted(months[month])
        rows = sum(n for _, n in days)
        if rows * bytes_per_row > target and len(days) > 1:
            groups += [(g, "month") for g in _pack(pending, bytes_per_row, target)]
            pending = []
            groups += [(g, "day") for g in _pack(days, bytes_per_row, target)]
        else:
            pending.append((month, rows))
    groups += [(g, "month") for g in _pack(pending, bytes_per_row, target)]
    return groups


def _literal(value) -> str:
    return f"'{value.isoformat()}'" if isinstance(value, date) else str(value)


def plan_chunks(
    column: str,
    bucket_counts: list[tuple[date | int | None, int]],
    table_bytes: int,
    target_bytes: int,
) -> list[Chunk]:
    """Return chunks for one table. `bucket_counts` is the profile query result.

    A profile by month (from an older profile query) also works; it is never
    split below a month.
    """
    if target_bytes <= 0:
        raise ValueError("target_bytes must be positive")

    col = f"t.{quote_ident(column)}"  # every rendered statement aliases the source as t
    counts = [(_normalize(b), int(n)) for b, n in bucket_counts]
    null_rows = sum(n for b, n in counts if b is None)
    kinds = {isinstance(b, date) for b, _ in counts if b is not None}
    if len(kinds) > 1:
        raise ValueError(f"profile for {column} mixes dates and numbers")
    is_date = kinds == {True}
    buckets = sorted((b, n) for b, n in counts if b is not None)
    total_rows = null_rows + sum(n for _, n in buckets)
    bytes_per_row = table_bytes / total_rows if total_rows else 0.0

    if is_date:
        groups = _date_groups(buckets, bytes_per_row, target_bytes)
    else:
        groups = [(g, "key") for g in _pack(buckets, bytes_per_row, target_bytes)]

    chunks: list[Chunk] = []
    for i, (group, grain) in enumerate(groups):
        start = group[0][0]
        is_first, is_last = i == 0, i == len(groups) - 1
        end = None if is_last else groups[i + 1][0][0][0]
        parts = []
        if not is_first:
            parts.append(f"{col} >= {_literal(start)}")
        if not is_last:
            parts.append(f"{col} < {_literal(end)}")
        # A single group has no bounds at all, so match every non-null value.
        predicate = " AND ".join(parts) if parts else f"{col} IS NOT NULL"
        rows = sum(n for _, n in group)
        last = group[-1][0]
        if grain == "month":
            chunk_id = f"{i + 1:04d}_{start:%Y%m}_{last:%Y%m}"
            months = tuple(m for m, _ in group)
        elif grain == "day":
            chunk_id = f"{i + 1:04d}_{start:%Y%m%d}_{last:%Y%m%d}"
            months = (_month(start),)
        else:
            chunk_id = f"{i + 1:04d}_k{start}_k{'max' if end is None else end}"
            months = ()
        chunks.append(
            Chunk(
                chunk_id=chunk_id,
                predicate=predicate,
                est_rows=rows,
                est_bytes=int(rows * bytes_per_row),
                months=months,
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
    """Chunks far above target: a single day, key bucket or (from a month-level
    profile) month too big to split further. Review these."""
    return [c for c in chunks if c.est_bytes > target_bytes * factor]
