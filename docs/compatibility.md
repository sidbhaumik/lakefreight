# Compatibility

lakefreight follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html) from 1.0.0. This page says what that covers.

A migration runs for weeks or months, so the promise is about what a migration depends on: files already written, control tables already filled and settings already reviewed must keep working across every 1.x release.

## What is stable

Within 1.x, these change only in ways that keep existing migrations working. A change that breaks one of them waits for 2.0.

| Interface | Stable | 1.x may |
|---|---|---|
| **Command line** | The `discover`, `plan`, `register` and `config` commands, their options, and exit codes: 0 for success, non-zero for failure | Add commands and options |
| **Work-item planner** | `python -m lakefreight.planning`, its `--changes` option, and `PLANNER_ROLE` and `PLANNER_WAREHOUSE` | Add a command-line equivalent |
| **Settings** | Every key in the built-in defaults, its meaning and its default, and the lookup order: `--config`, `$LAKEFREIGHT_CONFIG`, `./lakefreight.toml`, then the defaults | Add keys whose defaults keep today's behaviour |
| **Input files** | Metadata CSV columns, and profile CSV columns (`bucket_start` or the older `month_start`, plus `row_count`) | Accept more optional columns |
| **Control tables** | `EXTRACT_CONFIG`, `EXTRACT_COLUMN_CONFIG`, `EXTRACT_WORK_ITEM` and the `EXTRACT_PROGRESS` view: their columns and the values they hold, such as zones, lanes, strategies, statuses and work-item kinds | Add nullable columns, or columns with defaults. Each release that does gives the `ALTER TABLE` statements under **Upgrading** in the changelog |
| **Output layout** | `<prefix>/<database>/<schema>/<table>/chunk=<chunk id>/run_id=<run id>/`, with `manifest.json` in each run folder and `_schema.json` at the table root. Change sets use the same layout under `<prefix>/_changes/`, with the columns `LF_ACTION` and `LF_IS_UPDATE` added | |
| **Chunk IDs** | Their formats, and that planning the same data with the same settings gives the same IDs. `register --update` relies on this to match chunks | |
| **Manifest and schema files** | `format_version` 1 and every field in it | Add fields. Readers ignore fields they don't know |
| **Reconciliation** | A chunk that passes today keeps passing on the same data and settings | Add checks only behind a setting that is off by default |
| **Terraform module** | Variables, their defaults and outputs | Add variables whose defaults keep today's behaviour. Buckets, keys and other resources that hold data are never renamed in a way that would replace them |

## What is not

- **The Python API.** Modules under `lakefreight` are implementation, and their functions can change in any release. Use the command line, or pin an exact version.
- **The Lambda handlers' events and the state machine.** The handlers and `stepfunctions/extract_lane.asl.json` are deployed together by the Terraform module, and change together.
- **The generated SQL.** Statements may change in any release, for example to prune better, as long as they unload the same rows and values.
- **Text output.** Messages, notes and the summary printed by `plan` are for people. Don't parse them.
- **Delta and Iceberg commit details**, such as the operation names in `DESCRIBE HISTORY`. The tables themselves follow the Delta Lake and Iceberg specifications.

## Deprecation

A stable interface is removed only in a major release. Before that it is deprecated: the changelog says so, and using it prints a warning, for at least one minor release.

## Python and dependencies

lakefreight supports every Python version that has not reached end of life and that CI tests: 3.11 to 3.14 at 1.0.0. Support for a version can end in a minor release once that version reaches end of life. Minimum versions of the optional dependencies (`snowflake`, `delta`, `iceberg`) can rise in a minor release, and the changelog notes each rise.

## Upgrading during a migration

- **Patch releases** (1.0.x) are safe to take mid-migration.
- **Minor releases** (1.x) are too. Read the changelog's **Upgrading** notes first: a release may ask you to add a control-table column or run `terraform apply`.
- **Work items stay valid.** Work items, manifests and registered tables from any 1.x release work with any later 1.x release.
