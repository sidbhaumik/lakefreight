# Changelog

All notable changes to lakefreight are recorded here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
