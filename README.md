# lakefreight

[![CI](https://github.com/sidbhaumik/lakefreight/actions/workflows/ci.yml/badge.svg)](https://github.com/sidbhaumik/lakefreight/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/lakefreight)](https://pypi.org/project/lakefreight/)
[![Python 3.11 | 3.12 | 3.13 | 3.14](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13%20%7C%203.14-blue)](https://github.com/sidbhaumik/lakefreight/blob/main/pyproject.toml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](https://github.com/sidbhaumik/lakefreight/blob/main/LICENSE)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-261230)](https://github.com/astral-sh/ruff)

**Bulk-move warehouse tables into open lakehouse files on cloud storage, so any engine can read them.**

lakefreight unloads thousands of tables from a source warehouse into Parquet files on object storage, in a layout that Databricks, Snowflake, Redshift and BigQuery can all load or query. It plans the work from your metadata, keeps sensitive data in separate zones, and reconciles every extract before it is released.

It is built for the hard version of the job: thousands of tables, petabytes of data, many years of history, a fixed deadline, and a hand-off to storage owned by another cloud account in another region. The same method works for much smaller moves.

## Support matrix

| | Supported today | On the roadmap |
|---|---|---|
| **Source** | Snowflake | Other warehouses |
| **Target storage** | Amazon S3 | Google Cloud Storage, Azure Data Lake Storage |
| **File format** | Parquet, with a manifest and schema sidecar per table | |
| **Table format** | Delta Lake or Apache Iceberg, registered over the same Parquet files without rewriting them | Incremental re-registration after a re-extract |
| **Readers** | Any engine that reads Parquet, Delta or Iceberg: Databricks, Snowflake, Redshift, BigQuery | One-step registration in each engine's catalog |
| **Orchestration** | AWS Step Functions, deployed with Terraform or OpenTofu | |

**Status:** alpha. The Snowflake path (discover, plan, unload, reconcile, register as Delta and Iceberg) has been [run end to end on a live Snowflake account](https://github.com/sidbhaumik/lakefreight/blob/main/docs/live-validation.md), with every table read back at exactly Snowflake's row counts. The Delta tables were [read back in Databricks](https://github.com/sidbhaumik/lakefreight/blob/main/docs/live-validation.md#read-back-in-databricks) with identical counts and values. The [AWS deployment](https://github.com/sidbhaumik/lakefreight/blob/main/docs/live-validation.md#live-deployment-on-aws) applied and ran end to end, but Snowflake writing into S3 is untested because the test account's free plan blocked it.

**Known limitation:** Snowflake writes Parquet timestamps in milliseconds, so finer digits are lost on unload.

## What it does

![lakefreight architecture: metadata drives a chunk planner and work items; Snowflake unloads Parquet into sensitivity zones; reconciled chunks get a manifest, are replicated to the target account, and are registered as Delta Lake or Iceberg tables](https://raw.githubusercontent.com/sidbhaumik/lakefreight/main/docs/architecture.svg)

- **Discovers your tables.** `lakefreight discover` reads sizes, columns and types from Snowflake, and flags personal data using Snowflake's own sensitive data classification plus any tags or column-name rules you add. Columns Snowflake has never classified are treated as sensitive.
- **Generates extract SQL from metadata.** One `COPY INTO` per table, or per date chunk for large tables. Nothing is hand-written.
- **Routes by sensitivity.** Three zones, each with its own stage, bucket location, key and role:
  - `RESTRICTED`: tables with tokenized identifiers, such as government ID numbers, that must leave in clear form
  - `PII`: tables with other personal data
  - `INTERNAL`: tables explicitly classified as non-sensitive

  Unknown classification is treated as sensitive.
- **Splits large tables into even chunks.** Months are packed to a target size, and the predicates cover every possible value, including NULL dates.
- **Detokenizes restricted columns** through a cross-reference table, with checks that the join neither drops nor duplicates rows.
- **Reconciles before release.** A manifest is written only when the unloaded row count equals the source count; the receiving side loads only what has a manifest.
- **Restarts at chunk level.** Every table or chunk is a tracked work item.
- **Registers open table formats.** `lakefreight register` turns a table's unloaded files into a Delta Lake or Apache Iceberg table without rewriting them. Only files named in a committed manifest are included; it refuses incomplete tables, files still replicating, schema drift between chunks, and nanosecond timestamps that Spark and Iceberg v2 can't read.

## Install

```bash
pip install lakefreight
```

Add the extra for the table format you want to register:

```bash
pip install "lakefreight[delta]"
```

```bash
pip install "lakefreight[iceberg]"
```

To read metadata straight from Snowflake, add the connector:

```bash
pip install "lakefreight[snowflake]"
```

Then create a settings file to edit, and run `lakefreight --help` for the commands:

```bash
lakefreight config --write
```

## Typical workflow

1. **Discover** the tables and their sensitivity. This uses a connection from `~/.snowflake/connections.toml`; add `--sql-only` to get the query and run it yourself instead.

   ```bash
   lakefreight discover --database SALES --database HR --out metadata.csv
   ```

2. **Plan** the work: one `COPY INTO` per table or date chunk, routed by zone.

   ```bash
   lakefreight plan --metadata metadata.csv --out generated
   ```

3. **Unload** with the generated SQL, or at scale with the Step Functions pipeline. [`infra/`](https://github.com/sidbhaumik/lakefreight/blob/main/infra/README.md) deploys it: buckets, keys, Snowflake roles and stages, the Lambda functions and the state machine.
4. **Register** each table as Delta Lake or Iceberg once its chunks have landed.

   ```bash
   lakefreight register --format delta --table-root s3://my-egress/internal/SALES/PUBLIC/ORDERS
   ```

## Try it in two minutes

No Snowflake or AWS account needed. Clone the repo for the sample data; this generates extract SQL for five sample tables from a fictional travel company:

```bash
git clone https://github.com/sidbhaumik/lakefreight.git
```

```bash
cd lakefreight && python3 -m venv .venv && .venv/bin/pip install -e ".[dev]"
```

```bash
.venv/bin/lakefreight plan --metadata examples/metadata_sample.csv --profiles examples/profiles_sample.csv --out generated
```

Output:

```
5 tables -> 17 work items in generated/
  status: NEEDS_PROFILE 1, READY 16
  ready by zone: INTERNAL 1, PII 2, RESTRICTED 13
  note: TRAVEL.WEB.CLICK_EVENTS: chunk column EVENT_TS: confirm it prunes before using it
  note: TRAVEL.LEGACY.MISC_NOTES: table PII/RESTRICTED flag missing; treated as sensitive
```

Open `generated/RESTRICTED/TRAVEL.BOOKINGS.RESERVATION/0002_202203_202204.sql` to see an unload with the xref join and its two reconciliation queries.

To register a table's unloaded files as Delta Lake (Iceberg works the same way with `--format iceberg`, a catalog and a table name):

```bash
.venv/bin/lakefreight register --format delta --table-root s3://my-egress/internal/TRAVEL/BOOKINGS/RESERVATION
```

Run the tests:

```bash
.venv/bin/python -m pytest -q
```

## Documentation

| Document | Read it when |
|---|---|
| [Migration guide](https://github.com/sidbhaumik/lakefreight/blob/main/docs/migration-guide.md) | You have a migration like this to run and want the method, step by step |
| [Design decisions](https://github.com/sidbhaumik/lakefreight/blob/main/docs/design-decisions.md) | You want to know why it is built this way, and what was rejected |
| [Live validation](https://github.com/sidbhaumik/lakefreight/blob/main/docs/live-validation.md) | You want to see what was proven on a real Snowflake account, and what that run found |

## Claude skill

The repo includes a [Claude Code skill](https://github.com/sidbhaumik/lakefreight/blob/main/.claude/skills/generate-extract-sql/SKILL.md) that drives the generator. Open the repo in Claude Code and ask, for example, "generate extract SQL for the tables in my_metadata.csv". Claude normalizes the file, runs the generator, and reports which tables need a decision.

## Repository layout

| Path | Purpose |
|---|---|
| `src/lakefreight/cli.py` | The `lakefreight` command |
| `src/lakefreight/discover.py` | `lakefreight discover`: the metadata CSV from Snowflake's metadata and classification |
| `src/lakefreight/generate.py` | `lakefreight plan`: metadata CSV in, SQL files out |
| `src/lakefreight/chunk_planner.py` | Packs months into chunks with gap-free predicates |
| `src/lakefreight/renderer.py` | Renders the unload, source-count and unmatched-token SQL |
| `src/lakefreight/templates/copy_unload.sql.j2` | The `COPY INTO` template |
| `src/lakefreight/reconcile.py` | Pass/fail rules, manifest and schema sidecar |
| `src/lakefreight/planning.py` | Creates work items in Snowflake from the control tables |
| `src/lakefreight/handlers.py` | Lambda handlers: list, submit, poll, reconcile, summarize |
| `src/lakefreight/register.py` | `lakefreight register`: Delta Lake or Iceberg metadata over the unloaded files |
| `infra/terraform/` | Terraform/OpenTofu for the AWS and Snowflake side; see `infra/README.md` |
| `infra/build_lambda.sh` | Builds the Lambda package |
| `stepfunctions/extract_lane.asl.json` | State machine, one execution per lane |
| `sql/ddl/` | Control tables and the script that seeds them from metadata |
| `src/lakefreight/defaults.toml` | Default settings: thresholds, zones, lanes, xref names. `lakefreight config --write` makes an editable copy |
| `examples/` | Sample metadata and month profiles |
| `.claude/skills/` | The Claude skill |

## Not included

- A single-pass `PARTITION BY` unload for large tables with no date column.
- Replication tracking, egress purge, and the delta pass for tables that change during the migration.
- Adding a re-extracted chunk to a table that is already registered. Today, delete the table metadata and register again.

## Contributing

See [CONTRIBUTING.md](https://github.com/sidbhaumik/lakefreight/blob/main/CONTRIBUTING.md). Changes are listed in [CHANGELOG.md](https://github.com/sidbhaumik/lakefreight/blob/main/CHANGELOG.md).

## License

[MIT](https://github.com/sidbhaumik/lakefreight/blob/main/LICENSE)
