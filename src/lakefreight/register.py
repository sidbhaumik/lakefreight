"""Register unloaded Parquet files as a Delta Lake or Apache Iceberg table.

The files are not rewritten. Only files listed in a committed manifest are
included, so leftovers from failed attempts never become part of the table.

    lakefreight register --format delta --table-root s3://bucket/internal/DB/SCHEMA/TABLE
    lakefreight register --format iceberg --table-root s3://... \\
        --catalog glue --identifier analytics.orders

Needs the optional extras: `pip install "lakefreight[delta]"` or `"lakefreight[iceberg]"`.
"""

from __future__ import annotations

import argparse
import json
import posixpath
import sys
import time
import uuid
from dataclasses import dataclass, field
from urllib.parse import quote

MANIFEST = "manifest.json"
DELTA_LOG = "_delta_log"


class RegistrationError(Exception):
    """The table cannot be registered safely; the message says why."""


@dataclass(frozen=True)
class DataFile:
    path: str  # relative to the table root, forward slashes
    size: int
    rows: int


@dataclass
class Collected:
    files: list[DataFile]
    chunks: list[str]
    superseded: list[str] = field(default_factory=list)  # manifests ignored for a newer run

    @property
    def rows(self) -> int:
        return sum(f.rows for f in self.files)


def _pa():
    try:
        import pyarrow.fs as pafs
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - depends on extras
        raise RegistrationError(
            'pyarrow is required: pip install "lakefreight[delta]" or "lakefreight[iceberg]"'
        ) from exc
    return pafs, pq


def open_root(table_root: str):
    """Return (filesystem, root path) for a local path or a URI such as s3://."""
    pafs, _ = _pa()
    if "://" in table_root:
        fs, path = pafs.FileSystem.from_uri(table_root)
    else:
        import os

        fs, path = pafs.LocalFileSystem(), os.path.abspath(table_root)
    return fs, path.rstrip("/")


def collect(fs, root: str, allow_partial: bool = False) -> Collected:
    """Read every manifest under the table root and pick one run per chunk."""
    pafs, _ = _pa()
    infos = fs.get_file_info(pafs.FileSelector(root, recursive=True, allow_not_found=True))
    sizes = {i.path: i.size for i in infos if i.type == pafs.FileType.File}
    manifest_paths = sorted(p for p in sizes if posixpath.basename(p) == MANIFEST)
    if not manifest_paths:
        raise RegistrationError(f"no {MANIFEST} found under {root}")

    by_chunk: dict[str, list[tuple[str, dict]]] = {}
    for path in manifest_paths:
        with fs.open_input_stream(path) as f:
            manifest = json.loads(f.read())
        by_chunk.setdefault(str(manifest["chunk_id"]), []).append((path, manifest))

    chosen: dict[str, tuple[str, dict]] = {}
    superseded: list[str] = []
    for chunk_id, entries in by_chunk.items():
        if len(entries) > 1 and not all(m.get("written_at") for _, m in entries):
            raise RegistrationError(
                f"chunk {chunk_id} has {len(entries)} manifests and no written_at to choose "
                "between them: " + ", ".join(p for p, _ in entries)
            )
        entries.sort(key=lambda e: e[1].get("written_at") or "")
        chosen[chunk_id] = entries[-1]
        superseded += [p for p, _ in entries[:-1]]

    expected = {m.get("chunks_in_table") for _, m in chosen.values()} - {None}
    if len(expected) > 1:
        raise RegistrationError(f"manifests disagree on the number of chunks: {sorted(expected)}")
    if expected and len(chosen) < (total := expected.pop()) and not allow_partial:
        raise RegistrationError(
            f"only {len(chosen)} of {total} chunks have a manifest; wait for the rest "
            "or pass --allow-partial"
        )

    files: list[DataFile] = []
    for chunk_id in sorted(chosen):
        manifest_path, manifest = chosen[chunk_id]
        folder = posixpath.dirname(manifest_path)
        for entry in manifest["files"]:
            full = posixpath.join(folder, posixpath.basename(entry["name"]))
            if full not in sizes:
                raise RegistrationError(f"chunk {chunk_id}: listed file is missing: {full}")
            if sizes[full] != int(entry["size"]):
                raise RegistrationError(
                    f"chunk {chunk_id}: {full} is {sizes[full]} bytes, manifest says "
                    f"{entry['size']}; it may still be replicating"
                )
            files.append(DataFile(posixpath.relpath(full, root), sizes[full], int(entry["rows"])))
    if not files:
        raise RegistrationError("the manifests list no data files")
    return Collected(files=files, chunks=sorted(chosen), superseded=superseded)


def common_schema(fs, root: str, files: list[DataFile]):
    """Arrow schema shared by every file. Refuses drift and nanosecond timestamps."""
    import pyarrow as pa

    _, pq = _pa()
    schema = None
    for f in files:
        with fs.open_input_file(posixpath.join(root, f.path)) as handle:
            current = pq.read_schema(handle).remove_metadata()
        if schema is None:
            schema = current
        elif not current.equals(schema):
            raise RegistrationError(
                f"{f.path} has a different schema from {files[0].path}; "
                "the table changed between chunks"
            )
    nanos = [
        fld.name for fld in schema if pa.types.is_timestamp(fld.type) and fld.type.unit == "ns"
    ]
    if nanos:
        raise RegistrationError(
            f"nanosecond timestamps in {nanos}: Spark, Databricks and Iceberg v2 cannot read "
            "them. Unload with TIMESTAMP_NTZ(6), as lakefreight's renderer does."
        )
    return schema


# --- Delta Lake ---------------------------------------------------------------


def delta_commit(schema, files: list[DataFile], now_ms: int | None = None) -> str:
    """Version 0 of a Delta log adding exactly these files."""
    try:
        from deltalake import Schema
    except ImportError as exc:  # pragma: no cover - depends on extras
        raise RegistrationError('deltalake is required: pip install "lakefreight[delta]"') from exc

    now_ms = now_ms or int(time.time() * 1000)
    schema_string = Schema.from_arrow(schema).to_json()
    if '"timestamp_ntz"' in schema_string:
        protocol = {
            "minReaderVersion": 3,
            "minWriterVersion": 7,
            "readerFeatures": ["timestampNtz"],
            "writerFeatures": ["timestampNtz"],
        }
    else:
        protocol = {"minReaderVersion": 1, "minWriterVersion": 2}
    actions = [
        {
            "commitInfo": {
                "timestamp": now_ms,
                "operation": "CONVERT",
                "operationParameters": {},
                "engineInfo": "lakefreight",
            }
        },
        {"protocol": protocol},
        {
            "metaData": {
                "id": str(uuid.uuid4()),
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema_string,
                "partitionColumns": [],
                "configuration": {},
                "createdTime": now_ms,
            }
        },
    ]
    for f in files:
        actions.append(
            {
                "add": {
                    "path": quote(f.path, safe="/="),
                    "partitionValues": {},
                    "size": f.size,
                    "modificationTime": now_ms,
                    "dataChange": True,
                    "stats": json.dumps({"numRecords": f.rows}),
                }
            }
        )
    return "\n".join(json.dumps(a) for a in actions) + "\n"


def register_delta(table_root: str, allow_partial: bool = False) -> Collected:
    fs, root = open_root(table_root)
    pafs, _ = _pa()
    log_dir = posixpath.join(root, DELTA_LOG)
    if fs.get_file_info(log_dir).type != pafs.FileType.NotFound:
        raise RegistrationError(f"{log_dir} already exists; this table is already registered")
    collected = collect(fs, root, allow_partial)
    commit = delta_commit(common_schema(fs, root, collected.files), collected.files)
    fs.create_dir(log_dir, recursive=True)
    with fs.open_output_stream(posixpath.join(log_dir, f"{0:020d}.json")) as out:
        out.write(commit.encode())
    return collected


# --- Apache Iceberg -----------------------------------------------------------


def register_iceberg(
    table_root: str,
    catalog_name: str,
    identifier: str,
    catalog_properties: dict[str, str] | None = None,
    allow_partial: bool = False,
) -> Collected:
    """Create an Iceberg table at the table root and add the files to it.

    Iceberg stores absolute file paths, so run this where the files will be read
    (for a cross-account copy, against the destination bucket).
    """
    try:
        from pyiceberg.catalog import load_catalog
    except ImportError as exc:  # pragma: no cover - depends on extras
        raise RegistrationError(
            'pyiceberg is required: pip install "lakefreight[iceberg]"'
        ) from exc

    fs, root = open_root(table_root)
    collected = collect(fs, root, allow_partial)
    schema = common_schema(fs, root, collected.files)
    base = table_root.rstrip("/") if "://" in table_root else root
    catalog = load_catalog(catalog_name, **(catalog_properties or {}))
    if catalog.table_exists(identifier):
        raise RegistrationError(f"Iceberg table {identifier} already exists")
    namespace = identifier.rsplit(".", 1)[0] if "." in identifier else None
    if namespace:
        catalog.create_namespace_if_not_exists(namespace)
    table = catalog.create_table(identifier, schema=schema, location=base)
    table.add_files([posixpath.join(base, f.path) for f in collected.files])
    return collected


# --- Command line -------------------------------------------------------------


def main(argv: list[str] | None = None, prog: str | None = None) -> int:
    parser = argparse.ArgumentParser(prog=prog, description=__doc__.splitlines()[0])
    parser.add_argument("--format", required=True, choices=["delta", "iceberg"])
    parser.add_argument("--table-root", required=True, help="folder holding the table's chunks")
    parser.add_argument(
        "--allow-partial", action="store_true", help="register even if some chunks are missing"
    )
    parser.add_argument("--catalog", help="Iceberg: catalog name from .pyiceberg.yaml or env")
    parser.add_argument("--identifier", help="Iceberg: namespace.table to create")
    parser.add_argument(
        "--catalog-prop",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Iceberg: extra catalog property; repeatable",
    )
    args = parser.parse_args(argv)

    try:
        if args.format == "delta":
            collected = register_delta(args.table_root, args.allow_partial)
        else:
            if not (args.catalog and args.identifier):
                parser.error("--format iceberg needs --catalog and --identifier")
            props = dict(p.split("=", 1) for p in args.catalog_prop)
            collected = register_iceberg(
                args.table_root, args.catalog, args.identifier, props, args.allow_partial
            )
    except RegistrationError as exc:
        print(f"lakefreight register: {exc}", file=sys.stderr)
        return 1

    print(
        f"registered {args.format} table at {args.table_root}: "
        f"{len(collected.chunks)} chunks, {len(collected.files)} files, {collected.rows} rows"
    )
    for path in collected.superseded:
        print(f"  ignored older run: {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
