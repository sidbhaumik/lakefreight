# Migration guide

How to move a large Snowflake estate to S3 as Parquet. Follow the phases in order; each one ends with something you can check before going on.

The examples use an illustrative migration: 3,000 tables, 2 PB, 10 weeks, with source and target in different AWS accounts and regions. Substitute your own numbers.

## Contents

1. [Before you start](#1-before-you-start)
2. [Work out your throughput target](#2-work-out-your-throughput-target)
3. [Profile the metadata](#3-profile-the-metadata)
4. [Set up sensitivity zones](#4-set-up-sensitivity-zones)
5. [Plan the chunks](#5-plan-the-chunks)
6. [Handle tokenized columns](#6-handle-tokenized-columns)
7. [Run a pilot](#7-run-a-pilot)
8. [Run the bulk extraction](#8-run-the-bulk-extraction)
9. [Transfer to the target account](#9-transfer-to-the-target-account)
10. [Register Delta or Iceberg tables](#10-register-delta-or-iceberg-tables)
11. [Reconcile and close out](#11-reconcile-and-close-out)
12. [Common mistakes](#12-common-mistakes)

## 1. Before you start

You need:

- **Metadata** with one row per column: database, schema, table, size, column name, datatype, and sensitivity flags (personal data, restricted data) at table and column level. `lakefreight discover` builds this from Snowflake; see step 3.
- **Answers to five questions.** They change the design more than anything else.

| Question | Why it matters |
|---|---|
| One-time migration or ongoing feed? | A one-time move needs no change tracking. An ongoing feed needs watermarks and a way to carry deletes. |
| Do source tables change during the migration? | If yes, extract from a snapshot and plan a delta pass. |
| Who owns the target bucket, and in which region? | Another account means cross-account permissions and key policies. Another region means transfer cost and a rate limit to plan for. |
| What will read the files? | The reader decides which Parquet types are safe. |
| Can sensitive data leave in clear form? | If tokenized columns must be detokenized, that path needs its own controls. |

## 2. Work out your throughput target

Do this first. It tells you whether the timeline is realistic before you build anything.

```
bulk days       = total days - build and pilot days - closeout days
minimum rate    = total volume / bulk days
design rate     = minimum rate x 1.25      (leave slack for reruns)
```

Illustrative example: 10 weeks, 3 weeks to build and pilot, 1 week to close out, leaving 42 bulk days.

| Measure | Value |
|---|---|
| Minimum rate | 2,000 TB / 42 days = about 48 TB/day (0.55 GB/s) |
| Design rate | about 60 TB/day, finishing in about 34 days |

Three things limit the rate. Find each limit early:

1. **Snowflake unload capacity.** Measure TB/hour and credits/TB by warehouse size in the pilot.
2. **Cross-region or cross-account transfer.** Ask your AWS account team about S3 replication rate quotas for your volume. Do it in week 1; increases take time.
3. **Detokenization**, if a per-row call to an external service is involved.

Snowflake reports compressed size. Parquet output is often larger, so plan transfer and storage with headroom.

## 3. Profile the metadata

Start by building the metadata file:

```bash
lakefreight discover --database SALES --database HR --out metadata.csv
```

It reads tables, sizes, columns and types from `SNOWFLAKE.ACCOUNT_USAGE` (set `source = "information_schema"` under `[discover]` if you can't read that database). Sensitivity flags come from three places:

| Source | Sets | Configure with |
|---|---|---|
| Snowflake's sensitive data classification | Personal data: any column tagged `SNOWFLAKE.CORE.PRIVACY_CATEGORY` | On by default: `use_snowflake_classification` |
| Your own tags | Personal or restricted data | `pii_tags`, `restricted_tags`, as `DB.SCHEMA.TAG` or `DB.SCHEMA.TAG=VALUE` |
| Column-name patterns | Personal or restricted data | `pii_column_patterns`, `restricted_column_patterns` |

A column in a table Snowflake has classified, with no matching tag or pattern, is marked not personal. A column in a table that has never been classified is left unknown, and its table goes to the PII zone. Classify those tables in Snowflake, or add rules, until the zones look right. Only set `treat_unclassified_as_non_pii` if you know the estate holds no personal data.

Restricted flags come only from your tags and patterns, because only you know which columns hold tokens to swap for clear values.

`ACCOUNT_USAGE` lags changes by up to a few hours, and reading it needs access to the `SNOWFLAKE` database. Run `lakefreight discover --sql-only` to get the query and run it yourself, then export the result as CSV.

To track the plan in Snowflake as well, run `sql/ddl/01_control_tables.sql`, adapt and run `sql/ddl/02_seed_config.sql`. Then answer these:

- How is volume distributed? Usually a small number of tables holds most of the bytes. Those are your schedule.
- How many tables fall in each zone, and how many have missing flags?
- Which datatypes appear? Look for time-zone-aware timestamps, semi-structured and geospatial columns.
- Which large tables have no clustering key on a date or integer column?

**Check:** every table has a zone, and you have a list of the large tables with a proposed chunk column each.

## 4. Set up sensitivity zones

Create one zone per sensitivity class. Each zone has its own Snowflake stage, S3 location, KMS key and extract role.

| Zone | Contains | Rule |
|---|---|---|
| RESTRICTED | Tables with tokenized identifiers that must leave in clear form, such as government ID numbers | Own bucket; the only role that can read the token cross-reference |
| PII | Tables with personal data | Own prefix and key |
| INTERNAL | Everything explicitly flagged non-sensitive | Own prefix and key |

Two rules make routing safe:

- **Fail closed.** Only a table flagged `N`/`N` is INTERNAL. A missing flag is treated as sensitive. If a column is flagged but its table is not, the column wins.
- **Enforce with privileges, not code.** Each role can write only to its own stage, so a routing bug fails with a permission error and cannot write sensitive data to the wrong place.

**Check:** try to unload a PII table with the INTERNAL role. It should be refused.

## 5. Plan the chunks

Small tables are unloaded in one statement. Large tables are split into ranges of a date or an integer key, so that each piece finishes in reasonable time and a failure costs one piece.

1. **Pick a size threshold.** Start at 100 GB and adjust after the pilot.
2. **Pick a chunk column per large table.** It must be a column the table is clustered on: usually a load or event date, or for tables with no date, an integer key such as an entry or sequence number. If it is not, every chunk scans the whole table and chunking is slower than one pass. Check with `SYSTEM$CLUSTERING_INFORMATION`. Seeding picks the column the clustering key leads with when it is a date, timestamp or integer (`CHUNK_COLUMN_SOURCE = 'CLUSTERING_KEY'`). Otherwise it guesses a date column by name (`HEURISTIC`), which you must review.
3. **Profile each large table** with one query: row count by day for a date column, or by equal-width range for an integer key.
4. **Pack them into chunks** of roughly equal size. Fixed monthly or yearly chunks give uneven sizes when volume grew over the years.

The planner in `src/lakefreight/chunk_planner.py` does step 4. Dates are packed in whole months, and a month too big for one chunk is packed by day. Chunk IDs show the range: `0002_202401_202406` for months, `0003_20240301_20240313` for days, `0004_k1000000_k2000000` for integer keys. The predicates cannot miss a row:

| Chunk | Predicate |
|---|---|
| First | `col < end` (no lower bound) |
| Middle | `col >= start AND col < end` |
| Last | `col >= start` (no upper bound) |
| NULL | `col IS NULL` (always present) |

For a large table with no usable date or integer column, unload it in a single pass.

To generate the SQL without connecting to anything:

```bash
lakefreight plan --metadata my_metadata.csv --profiles my_profiles.csv --out generated
```

**Check:** for every chunked table, the chunk row estimates sum to the table's row count.

## 6. Handle tokenized columns

If sensitive columns are stored as tokens and must leave in clear form, replace each token by joining to a cross-reference table of token and clear value.

A join can change the row count without raising an error. Guard against it:

- **Use `LEFT JOIN`.** An inner join drops rows whose token is NULL or missing from the cross-reference.
- **Confirm the cross-reference is unique on the token** before you start. A duplicate token multiplies rows.
- **Compare against the un-joined source count.** This catches both dropped and duplicated rows.
- **Count unmatched tokens per chunk** and fail the chunk above an agreed threshold.
- **Purge clear files** on both sides as soon as the receiver confirms it has re-protected the data.

If detokenization instead calls an external service per row, measure its rate in the pilot. It will probably be slower than the unload.

## 7. Run a pilot

Pick about 20 tables that cover every case: each zone, a multi-terabyte table, a table with awkward datatypes, a table with tokenized columns. Take them all the way to the target account and load them into every consumer.

Measure and record:

| Measurement | Used for |
|---|---|
| TB/hour by warehouse size | Number and size of warehouses |
| Credits per TB | Compute budget |
| Parquet size vs Snowflake size | Transfer and storage estimates |
| Transfer rate to the target | Replication quota request |
| Type fidelity in each consumer | Cast rules |

**Check:** the measured rates support the design rate from step 2. If they do not, add capacity, extend the window or cut scope before starting the bulk run.

## 8. Run the bulk extraction

Deploy the pipeline with [`infra/`](../infra/README.md), create work items (`python -m lakefreight.planning`), then run one state-machine execution per lane.

| Lane | Tables | Why separate |
|---|---|---|
| Large | Chunked tables, oldest chunks first | These set the end date; start them on day one |
| Small | Everything under the threshold | Thousands of tables, little volume; finishes in days |
| Restricted | Restricted zone | Own role, warehouse and controls |

Each work item goes through: submit the `COPY`, poll until it finishes, reconcile, write the manifest. Reconciling compares row counts and a fingerprint of every value, read back from the files just written. A rerun picks up pending and failed items only.

By default a chunk whose timestamps have digits finer than a millisecond fails, because Parquet from Snowflake cannot keep them. The failure names the columns and counts the values. If milliseconds are enough, set `allow_timestamp_precision_loss = true` (the Terraform variable of the same name) and run the lane again.

Track daily unloaded volume against the design rate with the `EXTRACT_PROGRESS` view.

If source tables are still being written, set `STILL_WRITTEN = TRUE` on them in `EXTRACT_CONFIG` before planning. The planner then takes one snapshot, a UTC instant, and every query for those tables reads as of it through time travel: the profile, each chunk's unload, and every reconciliation check, retries included. All their chunks come from the same moment, and the extract reconciles however much the live table changes. Tables planned in the same run share the snapshot, so related tables stay consistent with each other.

- `snapshot = "all"` pins every table, and `"none"` turns pinning off.
- Time travel only reaches back as far as each table's retention: 1 day by default, up to 90 on Enterprise edition. The planner refuses a pinned table whose retention is shorter than `snapshot_window_hours` (12 by default), before it plans anything. Raise the table's `DATA_RETENTION_TIME_IN_DAYS` for the migration, lower the window, or extract from a zero-copy clone by setting `SOURCE_DATABASE`.
- The snapshot is recorded in each work item and manifest. Rows changed after it are what a delta pass must pick up.
- With `lakefreight plan`, pass `--snapshot '2026-01-31 12:00:00'` (UTC) to pin the generated SQL.

## 9. Transfer to the target account

- Keep the egress bucket in the same region as Snowflake, so Snowflake charges no cross-region egress. Let S3 carry the data across regions.
- Use S3 replication with a destination KMS key and destination-owned objects, or S3 Batch Operations if you need an explicit copy step.
- Treat the manifest as the commit marker. It is written last, only after reconciliation, and the receiver loads only prefixes that have one. Files from a failed attempt have no manifest and are ignored.
- Delete egress files once the receiver acknowledges them. Holding everything until the end is expensive.

## 10. Register Delta or Iceberg tables

Plain Parquet is readable everywhere, but readers then have to discover the files and infer the schema themselves. Registering a table format gives each table one schema, an exact file list, and row counts readers can use.

lakefreight writes the table metadata over the files already unloaded. Nothing is copied or rewritten.

| Format | Best for | Where to run it |
|---|---|---|
| Delta Lake | Databricks | Anywhere. The log uses paths relative to the table folder, so it stays valid after replication to another bucket. |
| Apache Iceberg | Mixed engines: Snowflake, Redshift, BigQuery, Databricks | Against the bucket the readers will use. Iceberg stores absolute paths. |

```bash
lakefreight register --format delta --table-root s3://ingress/internal/DB/SCHEMA/TABLE
```

```bash
lakefreight register --format iceberg --table-root s3://ingress/internal/DB/SCHEMA/TABLE --catalog glue --identifier db.table
```

Install the extra you need first: `pip install "lakefreight[delta]"` or `"lakefreight[iceberg]"`. The Iceberg catalog is configured the usual pyiceberg way, in `.pyiceberg.yaml` or with `--catalog-prop KEY=VALUE`.

Registration refuses to run when:

- a chunk has no manifest yet, unless you pass `--allow-partial`
- a listed file is missing or has the wrong size, which usually means replication hasn't finished
- chunks have different schemas, which means the source table changed during the extract
- a column holds nanosecond timestamps

If a chunk was extracted more than once, the newest manifest wins and the older run is reported.

### Updating a registered table

Registration runs once per table and refuses a table that already exists. When a chunk is extracted again after that, or arrives after an `--allow-partial` registration, pass `--update`:

```bash
lakefreight register --format delta --table-root s3://ingress/internal/DB/SCHEMA/TABLE --update
```

lakefreight compares the table's current files with the newest manifest of every chunk. In one new commit it removes the files of chunks that have a newer run and adds their new files, and adds chunks the table doesn't have yet. Unchanged chunks are left alone. If everything already matches, nothing is written, so `--update` is safe to run on every table after each wave. On a table that isn't registered yet, it registers it.

- **Delta:** a new log version with `remove` and `add` actions. Earlier versions stay readable for time travel.
- **Iceberg:** one `overwrite` snapshot, with the chunks in the snapshot summary under `lakefreight.added-chunks` and `lakefreight.replaced-chunks`.

An update refuses to run when:

- the table holds a file that no manifest lists, or uses row-level deletes or deletion vectors. Another writer has changed it, for example with `OPTIMIZE`, `DELETE` or `MERGE`, and swapping chunks could undo that work. Treat a table you want to keep updating as read-only.
- the newest runs have a different schema from the registered table. Register them as a new table.
- any of the checks above fails for the newest runs.

Run one registration per table at a time. The Delta log is written without a lock, so two concurrent updates of the same table could overwrite each other's commit. Iceberg commits go through the catalog and are safe.

Don't `VACUUM` a Delta table you still plan to update. Once the retention period passes, it deletes every file the log doesn't reference, including the manifests that `--update` relies on.

Delta logs include each file's null counts and, for numbers, dates and timestamps, its minimum and maximum, so engines skip files that cannot match a filter. Filters on the chunk column benefit most, since each file covers one date range.

**Check:** read the table from each target engine and compare its row count with the sum of the chunk manifests.

### Reading in Databricks

Read a registered Delta table by path, or copy it into a Unity Catalog managed table:

```sql
SELECT COUNT(*) FROM delta.`s3://ingress/internal/DB/SCHEMA/TABLE`;

CREATE TABLE main.migrated.table_name DEEP CLONE delta.`s3://ingress/internal/DB/SCHEMA/TABLE`;
```

To register the files in place as an external table instead, the bucket must be a Unity Catalog external location; `CREATE TABLE ... LOCATION` refuses a volume path. Snowflake writes every timestamp as a UTC instant, so all timestamp columns, `TIMESTAMP_NTZ` ones included, arrive as Databricks `TIMESTAMP`. Their values are right in a UTC session; set `spark.sql.session.timeZone` (or `SET TIME ZONE 'UTC'` in SQL) before comparing wall-clock values from `TIMESTAMP_NTZ` sources.

## 11. Reconcile and close out

| Level | Check |
|---|---|
| Work item | Rows unloaded equal the source count for the same predicate |
| Values | A `HASH_AGG` fingerprint of the source values, as unloaded, equals the same fingerprint read back from the files; the files hold as many rows as the `COPY` reported |
| Timestamps | No value has digits finer than a millisecond, or `allow_timestamp_precision_loss` is set |
| Chunked table | Chunk counts sum to the table count, NULL chunk included |
| Tokenized chunk | Unmatched tokens within threshold |
| Project | One report row per table: source rows, extracted rows, files, bytes, status |

Then purge the egress buckets, drop any clones, and hand over the snapshot times and per-chunk counts for the delta pass. Reset any retention you raised for time travel.

## 12. Common mistakes

| Mistake | What happens | Avoid it by |
|---|---|---|
| Pulling data through Python | Throughput is limited by one process | Letting Snowflake write to S3 with `COPY INTO` |
| Pre-generating and storing all the SQL | It goes stale when a column or flag changes | Rendering from metadata at run time |
| Chunking on an unclustered column | Each chunk scans the whole table | Checking clustering before choosing the column |
| Fixed monthly chunks | Recent months are far larger than old ones | Packing months to a target size |
| No NULL chunk | Rows with NULL dates are silently skipped | Always emitting one |
| Inner join to the token cross-reference | Rows disappear | `LEFT JOIN` plus count reconciliation |
| Treating a missing flag as non-sensitive | Unclassified data lands in the open zone | Failing closed |
| Starting small tables first | The largest tables finish after the deadline | Starting the largest tables on day one |
| Unloading time-zone-aware timestamps as they are | The unload fails or readers disagree | Casting to UTC `TIMESTAMP_NTZ` |
| Expecting sub-millisecond timestamps to survive | Snowflake writes Parquet timestamps in milliseconds; finer digits are lost | Reading the reconciliation failures from the pilot: they name each column that would lose digits |
| Reconciling on row counts only | A wrong cast or a broken join keeps the count and passes | Comparing a fingerprint of the values on both sides |
| Building a table from every file in the folder | Files from failed attempts appear as duplicate rows | Registering only files named in a manifest |
| Releasing files before reconciling | The receiver loads partial data | Writing the manifest last |
| Extracting a table that is still written without a snapshot | Chunks come from different moments, and reconciliation fails or misses moved rows | Setting `STILL_WRITTEN` so every query reads as of one snapshot |
