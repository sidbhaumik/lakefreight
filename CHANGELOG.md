# Changelog

All notable changes to lakefreight are recorded here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.7.0] - 2026-11-27

Chunking for large tables with no date column, and for months too big for one chunk.

### Added

- Large tables with no date column are chunked by ranges of an integer key, profiled into up to 10,000 equal-width ranges and packed to the target size. Chunk IDs look like `0002_k1000000_k2000000`.
- A month too big for one chunk is split by day, with chunk IDs like `0003_20240301_20240313`. Months that fit are packed exactly as before, so existing chunk IDs do not change.
- `discover` records each table's clustering key (`table_clustering_key`). Seeding, and `lakefreight plan`, pick the column the key leads with as the chunk column when it is a date, timestamp or integer, so chunks prune without a manual review. Otherwise the name-based date guess still applies.

### Changed

- Profile queries count rows by day (or by key range) and return `BUCKET_START`. Profile files with the old `month_start` column still plan, but are never split below a month.
- The planner refuses a chunk column that is not a date, timestamp or integer, instead of profiling it.
- Re-run `lakefreight discover` before seeding: the seed script reads the new clustering-key column.

## [0.6.0] - 2026-11-27

Registered tables can now be updated in place.

### Added

- `lakefreight register --update` brings a registered Delta or Iceberg table up to date in one commit: chunks with a newer run replace their old files, chunks that arrived later are added, and unchanged chunks are left alone. Nothing is written if the table already matches. It refuses a table another writer has changed, or whose newest runs change the schema. For Delta, earlier versions stay readable for time travel.

### Changed

- Documentation records an updated Delta table read in Databricks SQL: the current version, time travel to the original, `DESCRIBE HISTORY`, `DEEP CLONE` and file skipping. The migration guide explains `--update`, when it refuses, and why not to `VACUUM` a table you still update.

## [0.5.1] - 2026-11-26

A security fix found in the first full live run on AWS.

### Security

- Zone roles now isolate each session. Snowflake activates all of a user's granted roles as secondary roles by default, so a session opened as the INTERNAL role could also read the PII and RESTRICTED stages. The Terraform service user now has `default_secondary_roles_option = "NONE"`, and every connection runs `USE SECONDARY ROLES NONE`. Found in a live AWS run; routing was not affected, since the generated SQL picks each table's stage from its zone.

### Changed

- Documentation records a full live run on a paid AWS account: Snowflake unloading into S3 through the zone roles, per-zone KMS encryption, the `LaneHadFailures` gate and retry, and registration and read back on S3.

## [0.5.0] - 2026-11-26

Column statistics for registered Delta tables, and Delta output verified in Databricks.

### Added

- `SECURITY.md`, private vulnerability reporting on the repository, and issue templates.
- `lakefreight register --format delta` writes per-file column statistics into the Delta log: null counts for every column, and minimum and maximum for integers, decimals, dates and timestamps, from the Parquet footers. Engines such as Databricks use them to skip files on filters. Registration also refuses a file whose footer row count differs from its manifest.

### Changed

- Documentation records Delta tables from a live Snowflake unload read back in Databricks SQL, and the migration guide explains reading them in Databricks: by path, or into Unity Catalog with `DEEP CLONE`.

## [0.4.2] - 2026-11-26

Fixes from the first live deployment to AWS.

### Fixed

- A lane's Step Functions execution now fails with `LaneHadFailures` when any of its work items did not reconcile. Before, it reported success even when every item failed. A new `summarize` Lambda counts the run's items by status after the Map.
- `sql/ddl/02_seed_config.sql` takes the size threshold from a `FULL_THRESHOLD_BYTES` variable at its top, instead of a hard-coded default, and ends on a statement so it runs through the Python connector.

### Changed

- `infra/terraform` no longer lists the storage-integration preview feature, which provider 2.24 made stable.
- Documentation records the live AWS deployment and a limit of AWS free-plan accounts: their managed policies stop Snowflake from assuming the zone roles. See `docs/live-validation.md`.

## [0.4.1] - 2026-11-26

Fixes from the first live run on Snowflake.

### Fixed

- `lakefreight discover`: column-name patterns now match anywhere in the name on Snowflake, whose `REGEXP_LIKE` matches the whole string. Before, a pattern such as `PASSPORT` missed `PASSPORT_NUMBER`.
- Reconcile Lambda: reads the `COPY` result before its column descriptions, which Snowflake connector 5.x fills in only after a fetch.

### Changed

- Documentation reflects a live run on Snowflake: Parquet timestamps are written in milliseconds, and `TIMESTAMP_LTZ`/`TZ` cannot be unloaded to Parquet. See `docs/live-validation.md`.

## [0.4.0] - 2026-11-25

### Added

- `infra/terraform`: Terraform/OpenTofu that deploys the pipeline. On AWS: encrypted, versioned egress buckets with a key per zone, the roles Snowflake uses to write each zone, Secrets Manager for the service user's key, the four Lambda functions, the Step Functions state machine, and optional cross-account replication. On Snowflake: lane warehouses, zone roles, storage integrations, stages, grants and a key-pair service user. Its tests plan the module with mocked Snowflake and check the access policies.
- `infra/build_lambda.sh` builds the Lambda package from Linux wheels.
- Lambda functions read their settings from `LAKEFREIGHT_CONFIG_TOML` and Snowflake credentials from Secrets Manager (`SNOWFLAKE_SECRET_ARN`).
- Zones accept a `kms_key_id`; manifests and schema files are encrypted with the zone's key.
- `lakefreight discover --table` writes the discovered metadata to a Snowflake table, which `sql/ddl/02_seed_config.sql` now reads directly.

### Changed

- The control-table SQL no longer creates or switches to `MIGRATION.CONTROL`; run it in your control schema.

## [0.3.0] - 2026-11-25

### Added

- `lakefreight discover`: builds the metadata CSV from Snowflake's `ACCOUNT_USAGE` or `INFORMATION_SCHEMA` views. Personal-data flags come from Snowflake's sensitive data classification, your own tags and column-name patterns; restricted flags from your tags and patterns. Columns in tables Snowflake has never classified are left unknown, so their tables go to the PII zone. `--sql-only` prints the query to run yourself.
- `snowflake` install extra for the Snowflake connector.
- `[discover]` section in the settings.

## [0.2.0] - 2026-11-25

First release published to PyPI.

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

[Unreleased]: https://github.com/sidbhaumik/lakefreight/compare/v0.7.0...HEAD
[0.7.0]: https://github.com/sidbhaumik/lakefreight/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/sidbhaumik/lakefreight/compare/v0.5.1...v0.6.0
[0.5.1]: https://github.com/sidbhaumik/lakefreight/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/sidbhaumik/lakefreight/compare/v0.4.2...v0.5.0
[0.4.2]: https://github.com/sidbhaumik/lakefreight/compare/v0.4.1...v0.4.2
[0.4.1]: https://github.com/sidbhaumik/lakefreight/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/sidbhaumik/lakefreight/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/sidbhaumik/lakefreight/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/sidbhaumik/lakefreight/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/sidbhaumik/lakefreight/releases/tag/v0.1.0
