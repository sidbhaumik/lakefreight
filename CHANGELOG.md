# Changelog

All notable changes to lakefreight are recorded here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/sidbhaumik/lakefreight/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/sidbhaumik/lakefreight/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/sidbhaumik/lakefreight/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/sidbhaumik/lakefreight/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/sidbhaumik/lakefreight/releases/tag/v0.1.0
