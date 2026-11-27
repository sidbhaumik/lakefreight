import json
from datetime import datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lakefreight import cli
from lakefreight.models import ColumnSpec, TableSpec
from lakefreight.reconcile import build_manifest
from lakefreight.register import (
    RegistrationError,
    collect,
    open_root,
    register_delta,
    register_iceberg,
    update_delta,
    update_iceberg,
)

deltalake = pytest.importorskip("deltalake")

SPEC = TableSpec("TRAVEL", "BOOKINGS", "RESERVATION", "INTERNAL", (ColumnSpec("ID", "NUMBER"),))
SCHEMA = pa.schema(
    [
        ("ID", pa.decimal128(38, 0)),
        ("CITY", pa.string()),
        ("CHECKIN_TS", pa.timestamp("us")),
    ]
)


def write_parquet(path, n, start=0, schema=SCHEMA):
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = {
        "ID": [Decimal(start + i) for i in range(n)],
        "CITY": [f"city-{start + i}" for i in range(n)],
        "CHECKIN_TS": [datetime(2024, 1, 1, 12, 0, i % 60) for i in range(n)],
    }
    table = pa.table({k: columns[k] for k in schema.names}, schema=schema)
    pq.write_table(table, path)
    return path.stat().st_size


def write_run(root, chunk, run, rows, chunks_in_table=2, written_at=None, start=0, **kw):
    folder = root / f"chunk={chunk}" / f"run_id={run}"
    size = write_parquet(folder / "data_0_0_0.snappy.parquet", rows, start, **kw)
    files = [{"name": "data_0_0_0.snappy.parquet", "size": size, "rows": rows}]
    manifest = build_manifest(
        SPEC, chunk, run, None, None, files, rows, chunks_in_table, written_at
    )
    (folder / "manifest.json").write_text(json.dumps(manifest))
    return folder


@pytest.fixture
def table_root(tmp_path):
    root = tmp_path / "internal" / "TRAVEL" / "BOOKINGS" / "RESERVATION"
    write_run(root, "0001_202401_202406", "old", 5, written_at="2026-01-01T00:00:00+00:00")
    write_run(root, "0001_202401_202406", "new", 7, written_at="2026-01-02T00:00:00+00:00")
    write_run(root, "null", "r1", 3, start=100)
    # A failed attempt: data but no manifest. It must never join the table.
    write_parquet(root / "chunk=null" / "run_id=failed" / "data_0_0_0.snappy.parquet", 50)
    (root / "_schema.json").write_text("{}")
    return root


def _rewrite_manifests(root, change):
    for path in root.rglob("manifest.json"):
        manifest = json.loads(path.read_text())
        change(manifest)
        path.write_text(json.dumps(manifest))


def test_manifests_carry_a_format_version(table_root):
    manifest = json.loads(next(table_root.rglob("manifest.json")).read_text())
    assert manifest["format_version"] == 1


def test_manifests_from_before_1_0_still_register(table_root):
    _rewrite_manifests(table_root, lambda m: m.pop("format_version"))
    fs, root = open_root(str(table_root))
    assert collect(fs, root).rows == 10


def test_a_newer_manifest_format_is_refused(table_root):
    _rewrite_manifests(table_root, lambda m: m.update(format_version=2))
    fs, root = open_root(str(table_root))
    with pytest.raises(RegistrationError, match="format_version 2; this lakefreight reads up to 1"):
        collect(fs, root)


def test_collect_picks_latest_run_and_ignores_unmanifested_files(table_root):
    fs, root = open_root(str(table_root))
    collected = collect(fs, root)
    assert collected.rows == 10
    assert sorted(collected.chunks) == ["0001_202401_202406", "null"]
    assert all(
        "run_id=failed" not in f.path and "run_id=old" not in f.path for f in collected.files
    )
    assert len(collected.superseded) == 1


def test_delta_table_is_readable_by_delta_rs(table_root):
    register_delta(str(table_root))
    table = deltalake.DeltaTable(str(table_root))
    assert table.version() == 0
    data = table.to_pyarrow_dataset().to_table()
    assert data.num_rows == 10
    assert data.schema.field("CHECKIN_TS").type == pa.timestamp("us")
    assert sorted(data.column("ID").to_pylist())[-1] == Decimal(102)
    assert "timestampNtz" in table.protocol().reader_features


def test_delta_refuses_to_register_twice(table_root):
    register_delta(str(table_root))
    with pytest.raises(RegistrationError, match="already registered"):
        register_delta(str(table_root))


def test_missing_chunk_blocks_registration_unless_allowed(tmp_path):
    root = tmp_path / "t"
    write_run(root, "0001", "r", 4, chunks_in_table=3)
    write_run(root, "null", "r", 1, chunks_in_table=3)
    with pytest.raises(RegistrationError, match="only 2 of 3 chunks"):
        register_delta(str(root))
    assert register_delta(str(root), allow_partial=True).rows == 5


def test_file_still_replicating_is_refused(tmp_path):
    root = tmp_path / "t"
    folder = write_run(root, "0001", "r", 4, chunks_in_table=1)
    (folder / "data_0_0_0.snappy.parquet").write_bytes(b"partial")
    with pytest.raises(RegistrationError, match="still be replicating"):
        register_delta(str(root))


def test_nanosecond_timestamps_are_refused(tmp_path):
    root = tmp_path / "t"
    nanos = pa.schema([("ID", pa.decimal128(38, 0)), ("CHECKIN_TS", pa.timestamp("ns"))])
    write_run(root, "0001", "r", 2, chunks_in_table=1, schema=nanos)
    with pytest.raises(RegistrationError, match="nanosecond"):
        register_delta(str(root))


def test_schema_drift_between_chunks_is_refused(tmp_path):
    root = tmp_path / "t"
    write_run(root, "0001", "r", 2)
    narrower = pa.schema([("ID", pa.decimal128(38, 0)), ("CITY", pa.string())])
    write_run(root, "null", "r", 2, schema=narrower)
    with pytest.raises(RegistrationError, match="different schema"):
        register_delta(str(root))


def test_iceberg_table_is_readable_by_pyiceberg(table_root, tmp_path):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("sqlalchemy")
    from pyiceberg.catalog import load_catalog

    props = {"type": "sql", "uri": f"sqlite:///{tmp_path}/catalog.db", "warehouse": str(tmp_path)}
    register_iceberg(str(table_root), "local", "travel.reservation", props)
    table = load_catalog("local", **props).load_table("travel.reservation")
    data = table.scan().to_arrow()
    assert data.num_rows == 10
    assert len(table.inspect.files()) == 2
    with pytest.raises(RegistrationError, match="already exists"):
        register_iceberg(str(table_root), "local", "travel.reservation", props)


def test_cli_register(table_root, capsys):
    assert cli.main(["register", "--format", "delta", "--table-root", str(table_root)]) == 0
    out = capsys.readouterr().out
    assert "2 chunks, 2 files, 10 rows" in out and "ignored older run" in out
    assert cli.main(["register", "--format", "delta", "--table-root", str(table_root)]) == 1


# --- Column statistics in the Delta log ---------------------------------------


def add_stats(root):
    log = (root / "_delta_log" / "00000000000000000000.json").read_text().splitlines()
    adds = [json.loads(line)["add"] for line in log if '"add"' in line]
    return {a["path"].split("/")[0]: json.loads(a["stats"]) for a in adds}


def test_delta_log_has_min_max_and_null_counts(table_root):
    register_delta(str(table_root))
    stats = add_stats(table_root)
    first = stats["chunk=0001_202401_202406"]  # the "new" run: 7 rows from 0
    assert first["numRecords"] == 7
    assert first["minValues"]["ID"] == 0 and first["maxValues"]["ID"] == 6
    assert first["minValues"]["CHECKIN_TS"] == "2024-01-01T12:00:00.000"
    assert first["maxValues"]["CHECKIN_TS"] == "2024-01-01T12:00:06.000"
    assert first["nullCount"] == {"ID": 0, "CITY": 0, "CHECKIN_TS": 0}
    # Strings may be truncated by writers, so they get no min/max.
    assert "CITY" not in first["minValues"]
    assert stats["chunk=null"]["minValues"]["ID"] == 100


def test_utc_timestamps_and_rounding(tmp_path):
    # Snowflake writes UTC-adjusted millisecond timestamps; finer digits must
    # round the max up and the min down, so no real value falls outside.
    schema = pa.schema([("ID", pa.decimal128(38, 0)), ("TS", pa.timestamp("us", tz="UTC"))])
    root = tmp_path / "t"
    folder = root / "chunk=full" / "run_id=r"
    folder.mkdir(parents=True)
    ts = [datetime(2025, 1, 1, 5, 0, 0, 555001), datetime(2025, 1, 1, 6, 0, 0, 999999)]
    pq.write_table(
        pa.table({"ID": [Decimal(1), Decimal(2)], "TS": ts}, schema=schema),
        folder / "data_0_0_0.snappy.parquet",
    )
    size = (folder / "data_0_0_0.snappy.parquet").stat().st_size
    files = [{"name": "data_0_0_0.snappy.parquet", "size": size, "rows": 2}]
    (folder / "manifest.json").write_text(
        json.dumps(build_manifest(SPEC, "full", "r", None, None, files, 2, 1))
    )
    register_delta(str(root))
    s = add_stats(root)["chunk=full"]
    assert s["minValues"]["TS"] == "2025-01-01T05:00:00.555Z"
    assert s["maxValues"]["TS"] == "2025-01-01T06:00:01.000Z"


def test_decimal_stats_stay_exact():
    from lakefreight.register import _Exact, _stats_json

    big = Decimal("12345678901234567890123456789012345678")
    text = _stats_json({"minValues": {"ID": _Exact(big), "AMT": _Exact(Decimal("0.10"))}})
    assert '"ID": 12345678901234567890123456789012345678' in text
    assert json.loads(text, parse_float=Decimal)["minValues"]["AMT"] == Decimal("0.10")


def test_all_null_column_gets_null_count_only(tmp_path):
    root = tmp_path / "internal" / "TRAVEL" / "BOOKINGS" / "RESERVATION"
    folder = root / "chunk=full" / "run_id=r"
    folder.mkdir(parents=True)
    table = pa.table(
        {"ID": [Decimal(1), Decimal(2)], "CITY": ["a", "b"], "CHECKIN_TS": [None, None]},
        schema=SCHEMA,
    )
    pq.write_table(table, folder / "data_0_0_0.snappy.parquet")
    size = (folder / "data_0_0_0.snappy.parquet").stat().st_size
    files = [{"name": "data_0_0_0.snappy.parquet", "size": size, "rows": 2}]
    (folder / "manifest.json").write_text(
        json.dumps(build_manifest(SPEC, "full", "r", None, None, files, 2, 1))
    )
    register_delta(str(root))
    s = add_stats(root)["chunk=full"]
    assert s["nullCount"]["CHECKIN_TS"] == 2 and "CHECKIN_TS" not in s["minValues"]


def test_delta_rs_parses_the_stats(table_root):
    import pyarrow.dataset as ds

    register_delta(str(table_root))
    table = deltalake.DeltaTable(str(table_root))
    actions = pa.table(table.get_add_actions(flatten=True)).to_pydict()
    assert sorted(actions["min.ID"]) == [Decimal(0), Decimal(100)]
    assert sorted(actions["null_count.CITY"]) == [0, 0]
    # Filters must still return every matching row with the stats in play.
    data = table.to_pyarrow_dataset().to_table(filter=ds.field("ID") >= Decimal(100))
    assert sorted(data.column("ID").to_pylist()) == [Decimal(100), Decimal(101), Decimal(102)]


def test_footer_row_count_must_match_manifest(tmp_path):
    root = tmp_path / "t"
    folder = write_run(root, "full", "r", 4, chunks_in_table=1)
    manifest = json.loads((folder / "manifest.json").read_text())
    manifest["files"][0]["rows"] = 5
    (folder / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(RegistrationError, match="holds 4 rows"):
        register_delta(str(root))


# --- Updating a registered table ---------------------------------------------


def delta_rows(root, version=None):
    table = deltalake.DeltaTable(str(root), version=version)
    return sorted(int(v) for v in table.to_pyarrow_dataset().to_table().column("ID").to_pylist())


def log_versions(root):
    return sorted(p.name for p in (root / "_delta_log").glob("*.json"))


def test_delta_update_replaces_a_re_extracted_chunk(table_root):
    register_delta(str(table_root))
    # The chunk is extracted again, with corrected data: 4 rows from 500.
    write_run(
        table_root,
        "0001_202401_202406",
        "fix",
        4,
        start=500,
        written_at="2026-01-03T00:00:00+00:00",
    )
    result = update_delta(str(table_root))
    assert (result.version, result.replaced, result.added) == (1, ["0001_202401_202406"], [])
    assert (result.removed_files, result.added_files) == (1, 1)
    assert delta_rows(table_root) == [100, 101, 102, 500, 501, 502, 503]
    # The old version is still there for time travel.
    assert delta_rows(table_root, version=0) == [0, 1, 2, 3, 4, 5, 6, 100, 101, 102]
    new = add_stats_v(table_root, 1)["chunk=0001_202401_202406"]
    assert new["minValues"]["ID"] == 500 and new["numRecords"] == 4


def add_stats_v(root, version):
    name = f"{version:020d}.json"
    lines = (root / "_delta_log" / name).read_text().splitlines()
    adds = [json.loads(line)["add"] for line in lines if '"add"' in line]
    return {a["path"].split("/")[0]: json.loads(a["stats"]) for a in adds}


def test_delta_update_with_nothing_new_writes_nothing(table_root):
    register_delta(str(table_root))
    result = update_delta(str(table_root))
    assert not result.changed and result.version is None
    assert log_versions(table_root) == ["00000000000000000000.json"]


def test_delta_update_completes_a_partial_registration(tmp_path):
    root = tmp_path / "t"
    write_run(root, "0001", "r", 4, chunks_in_table=3)
    write_run(root, "null", "r", 1, chunks_in_table=3, start=900)
    register_delta(str(root), allow_partial=True)
    with pytest.raises(RegistrationError, match="only 2 of 3"):
        update_delta(str(root))
    write_run(root, "0002", "r", 2, chunks_in_table=3, start=50)
    result = update_delta(str(root))
    assert (result.added, result.replaced, result.removed_files) == (["0002"], [], 0)
    assert delta_rows(root) == [0, 1, 2, 3, 50, 51, 900]


def test_delta_update_registers_a_new_table(table_root):
    result = update_delta(str(table_root))
    assert result.version == 0 and sorted(result.added) == ["0001_202401_202406", "null"]
    assert delta_rows(table_root) == [0, 1, 2, 3, 4, 5, 6, 100, 101, 102]


def test_delta_update_refuses_a_table_another_writer_changed(table_root):
    register_delta(str(table_root))
    extra = pa.table(
        {"ID": [Decimal(7)], "CITY": ["x"], "CHECKIN_TS": [datetime(2024, 1, 1)]}, schema=SCHEMA
    )
    deltalake.write_deltalake(str(table_root), extra, mode="append")
    with pytest.raises(RegistrationError, match="another writer"):
        update_delta(str(table_root))


def test_delta_update_refuses_a_schema_change(tmp_path):
    root = tmp_path / "t"
    write_run(root, "full", "r1", 3, chunks_in_table=1, written_at="2026-01-01T00:00:00+00:00")
    register_delta(str(root))
    narrower = pa.schema([("ID", pa.decimal128(38, 0)), ("CITY", pa.string())])
    write_run(
        root,
        "full",
        "r2",
        3,
        chunks_in_table=1,
        schema=narrower,
        written_at="2026-01-02T00:00:00+00:00",
    )
    with pytest.raises(RegistrationError, match="different schema from the registered table"):
        update_delta(str(root))
    assert log_versions(root) == ["00000000000000000000.json"]


def test_iceberg_update_replaces_a_re_extracted_chunk(table_root, tmp_path):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("sqlalchemy")
    from pyiceberg.catalog import load_catalog

    props = {"type": "sql", "uri": f"sqlite:///{tmp_path}/catalog.db", "warehouse": str(tmp_path)}
    first = update_iceberg(str(table_root), "local", "travel.reservation", props)
    assert sorted(first.added) == ["0001_202401_202406", "null"]
    write_run(
        table_root,
        "0001_202401_202406",
        "fix",
        4,
        start=500,
        written_at="2026-01-03T00:00:00+00:00",
    )
    result = update_iceberg(str(table_root), "local", "travel.reservation", props)
    assert result.replaced == ["0001_202401_202406"]
    assert (result.removed_files, result.added_files) == (1, 1)
    table = load_catalog("local", **props).load_table("travel.reservation")
    ids = sorted(int(v) for v in table.scan().to_arrow().column("ID").to_pylist())
    assert ids == [100, 101, 102, 500, 501, 502, 503]
    assert len(table.inspect.files()) == 2
    summary = table.current_snapshot().summary
    assert summary.operation.value == "overwrite"
    assert summary["lakefreight.replaced-chunks"] == "0001_202401_202406"
    assert not update_iceberg(str(table_root), "local", "travel.reservation", props).changed


def test_cli_register_update(table_root, capsys):
    assert cli.main(["register", "--format", "delta", "--table-root", str(table_root)]) == 0
    capsys.readouterr()
    assert cli.main(["register", "--format", "delta", "--table-root", str(table_root)]) == 1
    assert "pass --update" in capsys.readouterr().err
    args = ["register", "--format", "delta", "--table-root", str(table_root), "--update"]
    assert cli.main(args) == 0
    assert "is up to date" in capsys.readouterr().out
    write_run(table_root, "null", "r2", 2, start=700, written_at="2999-01-01T00:00:00+00:00")
    assert cli.main(args) == 0
    out = capsys.readouterr().out
    assert "(version 1): 0 chunk(s) added, 1 replaced" in out
    assert "replaced chunk: null" in out
