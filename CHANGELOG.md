# Changelog

All notable changes to lakefreight are recorded here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `lakefreight register`: registers a table's unloaded Parquet files as a Delta Lake or Apache Iceberg table without rewriting them. Only files named in a committed manifest are included, and the newest manifest per chunk wins. Refuses incomplete tables, files with the wrong size, schema drift between chunks, and nanosecond timestamps.
- `delta` and `iceberg` install extras.
- Manifests record `chunks_in_table` and `written_at`, so readers can check completeness and choose between reruns.

- `lakefreight config` shows the settings in use and writes an editable `lakefreight.toml`.
- Packaging for PyPI: default settings, templates and `py.typed` ship in the wheel, and a release workflow publishes with trusted publishing.

### Changed

- Default settings ship inside the package. Settings are read from `--config`, then `$LAKEFREIGHT_CONFIG`, then `./lakefreight.toml`, then the built-in defaults. The repository's `config/defaults.toml` is gone.

- Timestamp columns are unloaded at microsecond precision (`TIMESTAMP_NTZ(6)`), and `TIME` columns as text, so Spark, Databricks and Iceberg v2 can read the files.

## [0.1.0] - 2026-11-25

First public release.

### Added

- `lakefreight plan`: generates Snowflake `COPY INTO` unload SQL and a work-item list from a metadata CSV, with no database connection.
- Chunk planner that packs months into even-sized chunks, with predicates that cover every value including NULL dates.
- Sensitivity routing into `RESTRICTED`, `PII` and `INTERNAL` zones that fails closed on missing classification.
- Detokenization of restricted columns through a cross-reference table, with row-count and unmatched-token reconciliation.
- Control-table DDL and a seed script for Snowflake.
- AWS Lambda handlers and a Step Functions state machine for running work items at scale. Not yet run against live services.
- Claude Code skill for generating extract SQL.
- Migration guide and design decision records.

[Unreleased]: https://github.com/sidbhaumik/lakefreight/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/sidbhaumik/lakefreight/releases/tag/v0.1.0
