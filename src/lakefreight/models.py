"""Plain data structures shared by the planner, renderer and handlers."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date


def quote_ident(name: str) -> str:
    """Quote a Snowflake identifier exactly as stored in the metadata."""
    return '"' + name.replace('"', '""') + '"'


def resolve_zone(restricted_flag: str | None, pii_flag: str | None) -> str:
    """Fail closed: only an explicit N/N is INTERNAL."""
    if restricted_flag == "Y":
        return "RESTRICTED"
    if restricted_flag == "N" and pii_flag == "N":
        return "INTERNAL"
    return "PII"


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    data_type: str
    use_xref: bool = False
    length: int | None = None
    description: str | None = None
    pii_flag: str | None = None
    restricted_flag: str | None = None


@dataclass(frozen=True)
class TableSpec:
    database: str
    schema: str
    table: str
    zone: str
    columns: tuple[ColumnSpec, ...]
    source_database: str | None = None  # a zero-copy clone to extract from

    @property
    def source_fqn(self) -> str:
        db = self.source_database or self.database
        return ".".join(quote_ident(p) for p in (db, self.schema, self.table))

    @property
    def path(self) -> str:
        """Path under the zone prefix. Always uses the original database name."""
        return f"{self.database}/{self.schema}/{self.table}"


@dataclass(frozen=True)
class Chunk:
    chunk_id: str
    predicate: str | None  # None means the whole table
    est_rows: int = 0
    est_bytes: int = 0
    months: tuple[date, ...] = field(default=(), compare=False)
