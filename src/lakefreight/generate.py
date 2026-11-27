"""Generate extract SQL offline from a metadata CSV. No Snowflake access needed.

    lakefreight plan --metadata examples/metadata_sample.csv \
        --profiles examples/profiles_sample.csv --out generated

Writes one .sql file per work item under <out>/<ZONE>/<db>.<schema>.<table>/
and a work_items.csv summary. Large tables with no profile get a profile.sql
to run first; rerun with --profiles once you have the results.
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path

from . import chunk_planner, renderer
from .config import Settings, load_settings
from .models import ColumnSpec, TableSpec, resolve_zone

REQUIRED = {
    "table_database",
    "table_schema",
    "table_name",
    "table_size_bytes",
    "table_pii_flag",
    "table_restricted_flag",
    "column_name",
    "column_datatype",
    "column_pii_flag",
    "column_restricted_flag",
}
OPTIONAL = {
    "column_ordinal",
    "column_length",
    "column_description",
    "chunk_column",
    "table_clustering_key",
}
SUMMARY_FIELDS = [
    "database",
    "schema",
    "table",
    "zone",
    "lane",
    "strategy",
    "chunk_id",
    "est_bytes",
    "status",
    "file",
    "notes",
]


@dataclass
class TableMeta:
    spec: TableSpec
    table_bytes: int
    chunk_column: str | None
    notes: list[str] = field(default_factory=list)
    clustering_key: str | None = None

    @property
    def chunk_kind(self) -> str | None:
        if not self.chunk_column:
            return None
        types = {c.name: c.data_type for c in self.spec.columns}
        return chunk_planner.chunk_kind(types.get(self.chunk_column))

    @property
    def chunk_column_clustered(self) -> bool:
        names = [c.name for c in self.spec.columns]
        return chunk_planner.leading_cluster_column(self.clustering_key, names) == self.chunk_column


def _flag(value: str | None) -> str | None:
    value = (value or "").strip().upper()
    return value if value in ("Y", "N") else None


def _read_rows(path: Path, required: set[str]) -> list[dict[str, str]]:
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = {(h or "").strip().lower() for h in reader.fieldnames or []}
        missing = required - headers
        if missing:
            raise ValueError(f"{path}: missing columns {sorted(missing)}")
        rows = []
        for line, row in enumerate(reader, start=2):
            if None in row:  # more values than headers, usually an unquoted comma
                raise ValueError(
                    f"{path} line {line}: too many values; quote fields that contain "
                    'commas, such as "NUMBER(38,0)"'
                )
            rows.append({(k or "").strip().lower(): (v or "").strip() for k, v in row.items()})
        return rows


def pick_chunk_column(
    columns: tuple[ColumnSpec, ...], clustering_key: str | None = None
) -> str | None:
    """First guess at a column to chunk on.

    The column the clustering key leads with, if it is a date, timestamp or
    integer: chunks on it prune. Otherwise a date column by name: load dates
    first, because tables are usually clustered in load order; update dates last.
    """
    lead = chunk_planner.leading_cluster_column(clustering_key, [c.name for c in columns])
    by_name = {c.name: c for c in columns}
    if lead and chunk_planner.chunk_kind(by_name[lead].data_type):
        return lead

    def rank(name: str) -> int:
        name = name.upper()
        if any(k in name for k in ("LOAD", "INSERT", "CREAT")):
            return 1
        if any(k in name for k in ("TXN", "TRANS", "EVENT", "POST")):
            return 2
        if any(k in name for k in ("UPDAT", "MODIF")):
            return 4
        return 3

    dated = [c for c in columns if c.data_type.upper().startswith(("DATE", "TIMESTAMP"))]
    return min(dated, key=lambda c: (rank(c.name), c.name)).name if dated else None


def read_metadata(path: str | Path) -> list[TableMeta]:
    """One row per column in the CSV; returns one TableMeta per table."""
    grouped: dict[tuple[str, str, str], list[dict[str, str]]] = {}
    for row in _read_rows(Path(path), REQUIRED):
        key = (row["table_database"], row["table_schema"], row["table_name"])
        grouped.setdefault(key, []).append(row)

    tables = []
    for (db, schema, table), rows in grouped.items():
        notes: list[str] = []
        if all(r.get("column_ordinal") for r in rows):
            rows = sorted(rows, key=lambda r: int(float(r["column_ordinal"])))
        columns = tuple(
            ColumnSpec(
                name=r["column_name"],
                data_type=r["column_datatype"].upper(),
                use_xref=_flag(r["column_restricted_flag"]) == "Y",
                length=int(float(r["column_length"])) if r.get("column_length") else None,
                description=r.get("column_description") or None,
                pii_flag=_flag(r["column_pii_flag"]),
                restricted_flag=_flag(r["column_restricted_flag"]),
            )
            for r in rows
        )
        restricted, pii = _flag(rows[0]["table_restricted_flag"]), _flag(rows[0]["table_pii_flag"])
        # A flagged column outranks the table flag: route to the stricter zone.
        if any(c.restricted_flag == "Y" for c in columns) and restricted != "Y":
            notes.append("column flagged restricted but table flag is not Y; routed to RESTRICTED")
            restricted = "Y"
        if any(c.pii_flag == "Y" for c in columns) and pii != "Y":
            notes.append("column flagged PII but table flag is not Y; treated as PII")
            pii = "Y"
        if restricted is None or pii is None:
            notes.append("table PII/RESTRICTED flag missing; treated as sensitive")
        zone = resolve_zone(restricted, pii)
        if zone == "RESTRICTED" and not any(c.use_xref for c in columns):
            notes.append("RESTRICTED table with no restricted column; nothing will be detokenized")
        try:
            table_bytes = int(float(rows[0]["table_size_bytes"] or 0))
        except ValueError:
            table_bytes = 0
            notes.append("table size is not a number; treated as 0")
        clustering_key = rows[0].get("table_clustering_key") or None
        chunk_column = rows[0].get("chunk_column") or pick_chunk_column(columns, clustering_key)
        types = {c.name: c.data_type for c in columns}
        if chunk_column and not chunk_planner.chunk_kind(types.get(chunk_column)):
            notes.append(
                f"chunk column {chunk_column} is not a date, timestamp or integer column; ignored"
            )
            chunk_column = None
        tables.append(
            TableMeta(
                spec=TableSpec(db, schema, table, zone, columns),
                table_bytes=table_bytes,
                chunk_column=chunk_column,
                notes=notes,
                clustering_key=clustering_key,
            )
        )
    return tables


def _bucket(text: str) -> date | int | None:
    if not text:
        return None
    if len(text) >= 10 and text[4] == "-":
        return date.fromisoformat(text[:10])
    return int(Decimal(text))


def read_profiles(
    path: str | Path,
) -> dict[tuple[str, str, str], list[tuple[date | int | None, int]]]:
    """Profile query results: table_database, table_schema, table_name,
    bucket_start (a date, or an integer key; blank for NULL), row_count.
    Files from older versions, with month_start instead, still work."""
    rows = _read_rows(Path(path), {"table_database", "table_schema", "table_name", "row_count"})
    profiles: dict[tuple[str, str, str], list[tuple[date | int | None, int]]] = {}
    for row in rows:
        if "bucket_start" not in row and "month_start" not in row:
            raise ValueError(f"{path}: missing column bucket_start")
        key = (row["table_database"], row["table_schema"], row["table_name"])
        start = _bucket(row.get("bucket_start", row.get("month_start", "")))
        profiles.setdefault(key, []).append((start, int(float(row["row_count"]))))
    return profiles


def _statements(meta: TableMeta, chunk, run_id: str, settings: Settings) -> str:
    spec = meta.spec
    parts = [
        f"-- {spec.path} chunk {chunk.chunk_id} ({spec.zone})",
        "-- 1. Unload",
        renderer.unload_sql(spec, chunk.predicate, chunk.chunk_id, run_id, settings) + ";",
        "-- 2. Source row count: must equal the rows unloaded",
        renderer.source_count_sql(spec, chunk.predicate) + ";",
    ]
    unmatched = renderer.unmatched_sql(spec, chunk.predicate, settings)
    if unmatched:
        parts += ["-- 3. Tokens with no xref match: must be within the threshold", unmatched + ";"]
    return "\n\n".join(parts) + "\n"


def generate(
    tables: list[TableMeta],
    profiles: dict,
    settings: Settings,
    out_dir: str | Path,
    run_id: str = "manual",
) -> list[dict]:
    """Write the SQL files and return the summary rows."""
    out_dir = Path(out_dir)
    summary: list[dict] = []
    for meta in tables:
        spec = meta.spec
        key = (spec.database, spec.schema, spec.table)
        notes = list(meta.notes)
        chunked = meta.table_bytes > settings.full_threshold_bytes
        lane = "RESTRICTED" if spec.zone == "RESTRICTED" else ("LARGE" if chunked else "SMALL")
        base = dict(
            database=spec.database,
            schema=spec.schema,
            table=spec.table,
            zone=spec.zone,
            lane=lane,
            strategy="CHUNKED" if chunked else "FULL",
        )
        rel_dir = Path(spec.zone) / ".".join(key)

        def row(chunk_id, est_bytes, status, file, extra=(), base=base, notes=notes):
            summary.append(
                {
                    **base,
                    "chunk_id": chunk_id,
                    "est_bytes": est_bytes,
                    "status": status,
                    "file": str(file),
                    "notes": "; ".join([*notes, *extra]),
                }
            )

        try:
            if chunked and meta.chunk_column and key not in profiles:
                target = out_dir / rel_dir / "profile.sql"
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(
                    f"-- Run this, save the result, and pass it with --profiles.\n"
                    f"{chunk_planner.profile_sql(spec.source_fqn, meta.chunk_column, meta.chunk_kind)};\n"
                )
                check = (
                    f"chunk column {meta.chunk_column} leads the clustering key"
                    if meta.chunk_column_clustered
                    else f"chunk column {meta.chunk_column}: confirm it prunes before using it"
                )
                row("", meta.table_bytes, "NEEDS_PROFILE", rel_dir / "profile.sql", [check])
                continue

            if chunked and meta.chunk_column:
                chunks = chunk_planner.plan_chunks(
                    meta.chunk_column, profiles[key], meta.table_bytes, settings.target_chunk_bytes
                )
                big = {
                    c.chunk_id for c in chunk_planner.oversized(chunks, settings.target_chunk_bytes)
                }
            else:
                chunks, big = [chunk_planner.full_chunk(meta.table_bytes)], set()
                if chunked:
                    notes.append(
                        "over the size threshold with no date, timestamp or integer chunk "
                        "column; single pass"
                    )

            for chunk in chunks:
                target = out_dir / rel_dir / f"{chunk.chunk_id}.sql"
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(_statements(meta, chunk, run_id, settings))
                extra = (
                    ["far above target size; consider a finer split"]
                    if chunk.chunk_id in big
                    else []
                )
                row(
                    chunk.chunk_id,
                    chunk.est_bytes,
                    "READY",
                    rel_dir / f"{chunk.chunk_id}.sql",
                    extra,
                )
        except ValueError as exc:
            row("", meta.table_bytes, "ERROR", "", [str(exc)])

    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_dir / "work_items.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SUMMARY_FIELDS)
        writer.writeheader()
        writer.writerows(summary)
    return summary


def main(argv: list[str] | None = None, prog: str | None = None) -> int:
    parser = argparse.ArgumentParser(prog=prog, description=__doc__.splitlines()[0])
    parser.add_argument("--metadata", required=True, help="metadata CSV, one row per column")
    parser.add_argument("--profiles", help="profile CSV for large tables (from profile.sql)")
    parser.add_argument("--out", default="generated", help="output directory")
    parser.add_argument(
        "--config",
        help="settings TOML (default: $LAKEFREIGHT_CONFIG, ./lakefreight.toml, or built-in defaults)",
    )
    parser.add_argument("--run-id", default="manual", help="run id used in the unload path")
    args = parser.parse_args(argv)

    settings = load_settings(args.config)
    tables = read_metadata(args.metadata)
    profiles = read_profiles(args.profiles) if args.profiles else {}
    summary = generate(tables, profiles, settings, args.out, args.run_id)

    by_status = Counter(r["status"] for r in summary)
    by_zone = Counter(r["zone"] for r in summary if r["status"] == "READY")
    print(f"{len(tables)} tables -> {len(summary)} work items in {args.out}/")
    print("  status: " + ", ".join(f"{k} {v}" for k, v in sorted(by_status.items())))
    print(
        "  ready by zone: " + (", ".join(f"{k} {v}" for k, v in sorted(by_zone.items())) or "none")
    )
    flagged = [r for r in summary if r["notes"]]
    seen = set()
    for r in flagged:
        label = f"{r['database']}.{r['schema']}.{r['table']}"
        if (label, r["notes"]) not in seen:
            seen.add((label, r["notes"]))
            print(f"  note: {label}: {r['notes']}")
    return 1 if by_status.get("ERROR") else 0


if __name__ == "__main__":
    sys.exit(main())
