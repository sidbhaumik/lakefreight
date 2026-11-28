"""Register unloaded Parquet files as a Delta Lake or Apache Iceberg table.

The files are not rewritten. Only files listed in a committed manifest are
included, so leftovers from failed attempts never become part of the table.

    lakefreight register --format delta --table-root s3://bucket/internal/DB/SCHEMA/TABLE
    lakefreight register --format iceberg --table-root s3://... \\
        --catalog glue --identifier analytics.orders

With --update, an existing table is brought up to date instead: chunks that were
re-extracted, or that arrived after an --allow-partial registration, replace or
join the table in one new commit. Unchanged chunks are left alone.

Updates of the same table can run at the same time. Each Delta commit is created
only if its version does not exist yet (a conditional put on S3, a hard link on
local disk); a writer that loses re-reads the table and its manifests, plans
again and retries. Iceberg commits go through the catalog, and are retried the
same way when the catalog reports a conflict.

Needs the optional extras: `pip install "lakefreight[delta]"` or `"lakefreight[iceberg]"`.
"""

from __future__ import annotations

import argparse
import json
import posixpath
import random
import sys
import time
import uuid
from dataclasses import dataclass, field, replace
from urllib.parse import quote, unquote

from .reconcile import FORMAT_VERSION

MANIFEST = "manifest.json"
DELTA_LOG = "_delta_log"


class RegistrationError(Exception):
    """The table cannot be registered safely; the message says why."""


class ConcurrentCommit(RegistrationError):
    """Another writer committed to the table first."""


# How many times an update re-plans after losing a commit to another writer.
MAX_COMMIT_ATTEMPTS = 10


def _backoff(attempt: int) -> None:
    if attempt:
        time.sleep(random.uniform(0, min(0.1 * 2**attempt, 3.0)))


@dataclass(frozen=True)
class DataFile:
    path: str  # relative to the table root, forward slashes
    size: int
    rows: int
    stats: str | None = None  # Delta "stats" JSON; None means row count only


@dataclass
class Collected:
    files: list[DataFile]
    chunks: list[str]
    superseded: list[str] = field(default_factory=list)  # manifests ignored for a newer run
    # Every file any manifest lists, newest run or not: relative path -> chunk id.
    listed: dict[str, str] = field(default_factory=dict)

    @property
    def rows(self) -> int:
        return sum(f.rows for f in self.files)


@dataclass
class Update:
    """What an update changed. Nothing was written if `changed` is false."""

    collected: Collected
    added: list[str]  # chunks new to the table
    replaced: list[str]  # chunks whose files were swapped for a newer run
    removed_files: int
    added_files: int
    version: int | None = None  # Delta log version written
    attempts: int = 1  # commits tried; more than one means other writers won first

    @property
    def changed(self) -> bool:
        return bool(self.added or self.replaced)


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
    listed: dict[str, str] = {}
    for path in manifest_paths:
        with fs.open_input_stream(path) as f:
            manifest = json.loads(f.read())
        version = manifest.get("format_version", 1)
        if not isinstance(version, int) or version > FORMAT_VERSION:
            raise RegistrationError(
                f"{path} has format_version {version!r}; this lakefreight reads up to "
                f"{FORMAT_VERSION}. Upgrade lakefreight to register it."
            )
        chunk = str(manifest["chunk_id"])
        by_chunk.setdefault(chunk, []).append((path, manifest))
        folder = posixpath.relpath(posixpath.dirname(path), root)
        for entry in manifest["files"]:
            listed[posixpath.join(folder, posixpath.basename(entry["name"]))] = chunk

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
    return Collected(files=files, chunks=sorted(chosen), superseded=superseded, listed=listed)


def plan_update(collected: Collected, active: set[str]):
    """Compare the table's current files with the newest run of each chunk.

    Returns (files to add, paths to remove, chunks added, chunks replaced).
    Refuses if the table holds a file no manifest lists: another writer has
    changed it (compaction, deletes), and swapping chunks could lose that work.
    """
    unknown = sorted(p for p in active if p not in collected.listed)
    if unknown:
        raise RegistrationError(
            f"the table holds {len(unknown)} file(s) that no manifest lists, such as "
            f"{unknown[0]}; another writer has changed it, so lakefreight will not update it"
        )
    have: dict[str, set[str]] = {}
    for path in active:
        have.setdefault(collected.listed[path], set()).add(path)
    want: dict[str, list[DataFile]] = {}
    for f in collected.files:
        want.setdefault(collected.listed[f.path], []).append(f)

    to_add: list[DataFile] = []
    to_remove: set[str] = set()
    added: list[str] = []
    replaced: list[str] = []
    for chunk in collected.chunks:
        current = have.get(chunk, set())
        if {f.path for f in want[chunk]} == current:
            continue
        (replaced if current else added).append(chunk)
        to_add += want[chunk]
        to_remove |= current
    return to_add, to_remove, added, replaced


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

# Column statistics let engines skip files on filters, so a wrong value returns
# wrong results. Only types whose Parquet min/max are exact and whose Delta text
# form is unambiguous get min/max: integers, decimals, dates and timestamps.
# Strings (writers may truncate them), floats (NaN) and booleans get null counts only.


def _stat_value(value, arrow_type, upper: bool):
    """Delta stats form of a Parquet min (upper=False) or max (upper=True), or None."""
    import datetime as dt
    from decimal import Decimal

    import pyarrow as pa

    if pa.types.is_integer(arrow_type) and isinstance(value, int):
        return value
    if pa.types.is_decimal(arrow_type) and isinstance(value, Decimal):
        return _Exact(value)
    if pa.types.is_date32(arrow_type) and isinstance(value, dt.date):
        return value.isoformat()
    if pa.types.is_timestamp(arrow_type) and isinstance(value, dt.datetime):
        # Delta readers take timestamp stats to the millisecond. Round a max with
        # finer digits up, so it never sits below a real value.
        if upper and value.microsecond % 1000:
            value += dt.timedelta(microseconds=1000 - value.microsecond % 1000)
        else:
            value -= dt.timedelta(microseconds=value.microsecond % 1000)
        if arrow_type.tz is not None:
            text = value.astimezone(dt.UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        else:
            text = value.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        return text
    return None


class _Exact:
    """A decimal to write as an exact JSON number, not a float."""

    def __init__(self, value):
        self.value = value


def _stats_json(stats: dict) -> str:
    exact: dict[str, str] = {}

    def default(obj):
        if isinstance(obj, _Exact):
            token = f"@@exact{len(exact)}@@"
            exact[token] = format(obj.value, "f")
            return token
        raise TypeError(type(obj))

    text = json.dumps(stats, default=default)
    for token, number in exact.items():
        text = text.replace(f'"{token}"', number)
    return text


def file_stats(fs, root: str, f: DataFile, schema) -> str:
    """Delta stats for one file, from its Parquet footer: row count, null counts, min/max."""
    _, pq = _pa()
    with fs.open_input_file(posixpath.join(root, f.path)) as handle:
        meta = pq.ParquetFile(handle).metadata
    if meta.num_rows != f.rows:
        raise RegistrationError(f"{f.path} holds {meta.num_rows} rows, its manifest says {f.rows}")
    stats: dict = {"numRecords": f.rows}
    mins, maxs, nulls = {}, {}, {}
    # Map top-level columns to their leaf index; nested columns are skipped.
    leaf = {meta.schema.column(i).path: i for i in range(meta.num_columns)}
    for field_ in schema:
        i = leaf.get(field_.name)
        if i is None:
            continue
        groups = [meta.row_group(g).column(i).statistics for g in range(meta.num_row_groups)]
        if not groups or any(st is None for st in groups):
            continue
        if all(st.has_null_count for st in groups):
            nulls[field_.name] = sum(st.null_count for st in groups)
        with_values = [st for st in groups if st.has_min_max]
        # A row group without min/max must be all nulls, or the column's range is unknown.
        if not with_values or any(
            not st.has_min_max and not (st.has_null_count and st.null_count == st.num_values)
            for st in groups
        ):
            continue
        lo = _stat_value(min(st.min for st in with_values), field_.type, upper=False)
        hi = _stat_value(max(st.max for st in with_values), field_.type, upper=True)
        if lo is not None and hi is not None:
            mins[field_.name], maxs[field_.name] = lo, hi
    if mins:
        stats["minValues"], stats["maxValues"] = mins, maxs
    if nulls:
        stats["nullCount"] = nulls
    return _stats_json(stats)


def _add_action(f: DataFile, now_ms: int) -> dict:
    return {
        "add": {
            "path": quote(f.path, safe="/="),
            "partitionValues": {},
            "size": f.size,
            "modificationTime": now_ms,
            "dataChange": True,
            "stats": f.stats or json.dumps({"numRecords": f.rows}),
        }
    }


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
    actions += [_add_action(f, now_ms) for f in files]
    return "\n".join(json.dumps(a) for a in actions) + "\n"


def delta_update_commit(
    add: list[DataFile],
    remove: dict[str, int],
    added: list[str],
    replaced: list[str],
    now_ms: int | None = None,
) -> str:
    """A Delta commit that swaps chunks: remove their old files, add the new ones."""
    now_ms = now_ms or int(time.time() * 1000)
    actions: list[dict] = [
        {
            "commitInfo": {
                "timestamp": now_ms,
                "operation": "REPLACE CHUNKS",
                "operationParameters": {
                    "addedChunks": json.dumps(added),
                    "replacedChunks": json.dumps(replaced),
                },
                "engineInfo": "lakefreight",
            }
        }
    ]
    for path, size in sorted(remove.items()):
        actions.append(
            {
                "remove": {
                    "path": quote(path, safe="/="),
                    "deletionTimestamp": now_ms,
                    "dataChange": True,
                    "extendedFileMetadata": True,
                    "partitionValues": {},
                    "size": size,
                }
            }
        )
    actions += [_add_action(f, now_ms) for f in add]
    return "\n".join(json.dumps(a) for a in actions) + "\n"


def _log_path(root: str, version: int) -> str:
    return posixpath.join(root, DELTA_LOG, f"{version:020d}.json")


def register_delta(table_root: str, allow_partial: bool = False) -> Collected:
    fs, root = open_root(table_root)
    pafs, _ = _pa()
    log_dir = posixpath.join(root, DELTA_LOG)
    if fs.get_file_info(log_dir).type != pafs.FileType.NotFound:
        raise RegistrationError(
            f"{log_dir} already exists; this table is already registered "
            "(pass --update to bring it up to date)"
        )
    return _register_delta(fs, root, allow_partial)


def _register_delta(fs, root: str, allow_partial: bool) -> Collected:
    """Write version 0. Raises ConcurrentCommit if another writer wrote it first."""
    collected = collect(fs, root, allow_partial)
    schema = common_schema(fs, root, collected.files)
    files = [replace(f, stats=file_stats(fs, root, f, schema)) for f in collected.files]
    commit = delta_commit(schema, files)
    fs.create_dir(posixpath.join(root, DELTA_LOG), recursive=True)
    if not put_if_absent(fs, _log_path(root, 0), commit.encode()):
        raise ConcurrentCommit(
            f"another writer registered {root} first (pass --update to bring it up to date)"
        )
    return collected


def put_if_absent(fs, path: str, data: bytes) -> bool:
    """Create `path` holding `data` only if it does not exist yet, atomically.

    Returns False, writing nothing, if it exists. This is what makes concurrent
    Delta commits safe: two writers can never both create the same log version.
    """
    pafs, _ = _pa()
    if isinstance(fs, pafs.LocalFileSystem):
        import os
        import tempfile

        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), prefix=".lakefreight-")
        try:
            with os.fdopen(fd, "wb") as out:
                out.write(data)
                out.flush()
                os.fsync(out.fileno())
            try:
                os.link(tmp, path)  # fails if the path exists, and is never half written
            except FileExistsError:
                return False
            return True
        finally:
            os.unlink(tmp)
    if isinstance(fs, pafs.S3FileSystem):
        bucket, _, key = path.partition("/")
        return _s3_put_if_absent(fs.region, bucket, key, data)
    # No atomic create on other filesystems: this narrows the race but cannot close it.
    if fs.get_file_info(path).type != pafs.FileType.NotFound:
        return False
    with fs.open_output_stream(path) as out:
        out.write(data)
    return True


def _s3_client(region: str | None):
    try:
        import boto3
    except ImportError as exc:  # pragma: no cover - depends on extras
        raise RegistrationError(
            'boto3 is required to commit to a table on S3: pip install "lakefreight[delta]"'
        ) from exc
    return boto3.client("s3", region_name=region or None)


def _s3_put_if_absent(region: str | None, bucket: str, key: str, data: bytes) -> bool:
    """A conditional PutObject (If-None-Match: *), which S3 refuses if the key exists."""
    from botocore.exceptions import ClientError

    try:
        _s3_client(region).put_object(Bucket=bucket, Key=key, Body=data, IfNoneMatch="*")
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in ("PreconditionFailed", "ConditionalRequestConflict"):
            return False
        raise
    return True


def update_delta(table_root: str, allow_partial: bool = False) -> Update:
    """Bring a Delta table up to date with the newest run of every chunk.

    Registers the table if it has no log yet. Otherwise writes one commit that
    replaces re-extracted chunks and adds new ones, or nothing if all match.
    """
    try:
        import pyarrow as pa
        from deltalake import DeltaTable, Schema
    except ImportError as exc:  # pragma: no cover - depends on extras
        raise RegistrationError('deltalake is required: pip install "lakefreight[delta]"') from exc

    fs, root = open_root(table_root)
    pafs, _ = _pa()
    for attempt in range(MAX_COMMIT_ATTEMPTS):
        _backoff(attempt)
        if fs.get_file_info(_log_path(root, 0)).type == pafs.FileType.NotFound:
            try:
                collected = _register_delta(fs, root, allow_partial)
            except ConcurrentCommit:
                continue
            return Update(collected, collected.chunks, [], 0, len(collected.files), 0, attempt + 1)

        # Read the table before the manifests: every manifest a commit in this
        # snapshot was planned from is then in the listing, so a slower writer
        # can never swap a chunk back to an older run.
        table = DeltaTable(table_root if "://" in table_root else root)
        collected = collect(fs, root, allow_partial)
        schema = common_schema(fs, root, collected.files)
        features = set(table.protocol().reader_features or [])
        if features - {"timestampNtz"}:
            raise RegistrationError(
                f"the table uses Delta features lakefreight does not write: {sorted(features)}; "
                "another writer has changed it"
            )
        if json.loads(table.schema().to_json()) != json.loads(Schema.from_arrow(schema).to_json()):
            raise RegistrationError(
                "the newest runs have a different schema from the registered table; "
                "register them as a new table"
            )
        actions = pa.table(table.get_add_actions(flatten=False)).to_pydict()
        sizes = {
            unquote(p): int(n) for p, n in zip(actions["path"], actions["size_bytes"], strict=True)
        }
        to_add, to_remove, added, replaced = plan_update(collected, set(sizes))
        result = Update(collected, added, replaced, len(to_remove), len(to_add))
        result.attempts = attempt + 1
        if not result.changed:
            return result
        files = [replace(f, stats=file_stats(fs, root, f, schema)) for f in to_add]
        version = table.version() + 1
        commit = delta_update_commit(files, {p: sizes[p] for p in to_remove}, added, replaced)
        if put_if_absent(fs, _log_path(root, version), commit.encode()):
            result.version = version
            return result
    raise ConcurrentCommit(
        f"gave up after {MAX_COMMIT_ATTEMPTS} attempts: other writers kept committing to {root}"
    )


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
    from pyiceberg.exceptions import TableAlreadyExistsError

    # The table and its files arrive in one commit, so no reader or concurrent
    # update ever sees the table empty.
    try:
        with catalog.create_table_transaction(identifier, schema=schema, location=base) as tx:
            tx.add_files([posixpath.join(base, f.path) for f in collected.files])
    except TableAlreadyExistsError as exc:
        raise ConcurrentCommit(f"another writer created Iceberg table {identifier} first") from exc
    return collected


def update_iceberg(
    table_root: str,
    catalog_name: str,
    identifier: str,
    catalog_properties: dict[str, str] | None = None,
    allow_partial: bool = False,
) -> Update:
    """Bring an Iceberg table up to date with the newest run of every chunk.

    Creates the table if the catalog does not have it. Otherwise commits one
    overwrite snapshot that replaces re-extracted chunks and adds new ones.
    """
    try:
        from pyiceberg.catalog import load_catalog
        from pyiceberg.io.pyarrow import parquet_files_to_data_files
    except ImportError as exc:  # pragma: no cover - depends on extras
        raise RegistrationError(
            'pyiceberg is required: pip install "lakefreight[iceberg]"'
        ) from exc

    from pyiceberg.exceptions import CommitFailedException, ValidationException

    catalog = load_catalog(catalog_name, **(catalog_properties or {}))
    fs, root = open_root(table_root)
    base = table_root.rstrip("/") if "://" in table_root else root
    for attempt in range(MAX_COMMIT_ATTEMPTS):
        _backoff(attempt)
        if not catalog.table_exists(identifier):
            try:
                collected = register_iceberg(
                    table_root, catalog_name, identifier, catalog_properties, allow_partial
                )
            except ConcurrentCommit:
                continue
            return Update(
                collected, collected.chunks, [], 0, len(collected.files), None, attempt + 1
            )

        # As for Delta: the table first, then the manifests.
        table = catalog.load_table(identifier)
        collected = collect(fs, root, allow_partial)
        common_schema(fs, root, collected.files)
        current = {}
        for task in table.scan().plan_files():
            if task.delete_files:
                raise RegistrationError(
                    f"{task.file.file_path} has row-level deletes; another writer has changed "
                    "the table, so lakefreight will not update it"
                )
            path = task.file.file_path
            rel = path[len(base) + 1 :] if path.startswith(base + "/") else path
            current[rel] = task.file
        to_add, to_remove, added, replaced = plan_update(collected, set(current))
        result = Update(collected, added, replaced, len(to_remove), len(to_add))
        result.attempts = attempt + 1
        if not result.changed:
            return result
        props = {
            "lakefreight.added-chunks": ",".join(added),
            "lakefreight.replaced-chunks": ",".join(replaced),
        }
        try:
            with table.transaction() as tx:
                with tx.update_snapshot(snapshot_properties=props).overwrite() as overwrite:
                    for rel in sorted(to_remove):
                        overwrite.delete_data_file(current[rel])
                    new_files = parquet_files_to_data_files(
                        table.io,
                        tx.table_metadata,
                        iter(posixpath.join(base, f.path) for f in to_add),
                    )
                    for data_file in new_files:
                        overwrite.append_data_file(data_file)
        except (CommitFailedException, ValidationException):
            continue  # another writer committed first: plan again from its snapshot
        except ValueError as exc:
            raise RegistrationError(f"the newest runs do not fit the table: {exc}") from exc
        return result
    raise ConcurrentCommit(
        f"gave up after {MAX_COMMIT_ATTEMPTS} attempts: other writers kept committing to "
        f"{identifier}"
    )


# --- Command line -------------------------------------------------------------


def main(argv: list[str] | None = None, prog: str | None = None) -> int:
    parser = argparse.ArgumentParser(prog=prog, description=__doc__.splitlines()[0])
    parser.add_argument("--format", required=True, choices=["delta", "iceberg"])
    parser.add_argument("--table-root", required=True, help="folder holding the table's chunks")
    parser.add_argument(
        "--allow-partial", action="store_true", help="register even if some chunks are missing"
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="bring an existing table up to date: replace re-extracted chunks, add new ones",
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

    if args.format == "iceberg" and not (args.catalog and args.identifier):
        parser.error("--format iceberg needs --catalog and --identifier")
    props = dict(p.split("=", 1) for p in args.catalog_prop)
    try:
        if args.update and args.format == "delta":
            result = update_delta(args.table_root, args.allow_partial)
        elif args.update:
            result = update_iceberg(
                args.table_root, args.catalog, args.identifier, props, args.allow_partial
            )
        elif args.format == "delta":
            collected = register_delta(args.table_root, args.allow_partial)
        else:
            collected = register_iceberg(
                args.table_root, args.catalog, args.identifier, props, args.allow_partial
            )
    except RegistrationError as exc:
        print(f"lakefreight register: {exc}", file=sys.stderr)
        return 1

    if args.update:
        collected = result.collected
        if not result.changed:
            print(f"{args.format} table at {args.table_root} is up to date: nothing to commit")
        else:
            version = "" if result.version is None else f" (version {result.version})"
            print(
                f"updated {args.format} table at {args.table_root}{version}: "
                f"{len(result.added)} chunk(s) added, {len(result.replaced)} replaced; "
                f"{result.added_files} file(s) added, {result.removed_files} removed; "
                f"table now {len(collected.chunks)} chunks, {collected.rows} rows"
            )
            for chunk in result.replaced:
                print(f"  replaced chunk: {chunk}")
        if result.attempts > 1:
            print(f"  other writers committed first; planned again {result.attempts - 1} time(s)")
    else:
        print(
            f"registered {args.format} table at {args.table_root}: "
            f"{len(collected.chunks)} chunks, {len(collected.files)} files, {collected.rows} rows"
        )
    for path in collected.superseded:
        print(f"  ignored older run: {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
