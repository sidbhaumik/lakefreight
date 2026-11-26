# Live validation on Snowflake

lakefreight's Snowflake path was run end to end against a Snowflake trial account on AWS (Snowflake 10.42) on 2026-11-26. This page records what was run, what passed, and what the run found.

The AWS half (S3, IAM, Lambda, Step Functions, replication) was not part of this run. Unloads went to Snowflake internal stages and were downloaded, in the same folder layout lakefreight uses on S3.

## The data

A demo database of synthetic travel data, built to include every case that matters:

| Table | Rows | Includes |
|---|---|---|
| `CATALOG.HOTEL_DIM` | 200 | `TIMESTAMP_LTZ(9)`, `TIME(9)` |
| `CRM.TRAVELER` | 1,000 | Personal data: name, email, date of birth; a `VARIANT` column |
| `BOOKINGS.RESERVATION` | 60,000 | A tokenized passport column; 2 years of growing volume; 37 rows with no load date; 5 rows with no token; 3 rows whose token is missing from the cross-reference |
| `WEB.CLICK_EVENTS` | 100,000 | `TIMESTAMP_TZ(9)`, `VARIANT`, 23 months |

A 100-row token cross-reference sat in a separate database.

## What was run

1. `lakefreight discover` against the account, with `INFORMATION_SCHEMA` as the source and column-name rules for personal and restricted data. The `ACCOUNT_USAGE` variant was also run; it compiled and executed, but new tables had not reached `ACCOUNT_USAGE` yet.
2. Month profiles and the chunk planner: 31 work items (2 full tables, 5 bookings chunks, 24 event chunks).
3. Each work item's generated `COPY INTO`, source count and unmatched-token count, then reconciliation, download and a manifest per chunk.
4. `lakefreight register` as Delta Lake for all four tables and as Iceberg for one, read back with DuckDB.
5. An asynchronous `COPY` of 4 million rows, started in one session that was then abandoned or closed, followed and read from a new session.

## Results

| Check | Result |
|---|---|
| Discovery flags | Name, email and birth date flagged as personal data; the passport column flagged restricted; tables routed to all three zones |
| Reconciliation | Rows unloaded equalled source rows in all 31 work items, including the NULL-date chunk and the 5 NULL tokens kept by the `LEFT JOIN` |
| Unmatched tokens | The chunk holding the 3 unknown tokens was refused and got no manifest |
| Completeness gate | Registration refused the bookings table while that chunk had no manifest |
| Rerun | After adding the missing token and re-extracting the one chunk under a new run ID, registration used the new run and ignored the failed run's file |
| Empty chunk | A chunk with no rows wrote no file and reconciled as 0 = 0 |
| Delta Lake | All four tables read back in DuckDB with exactly Snowflake's row counts |
| Iceberg | 100,000 rows from 23 files, read back in DuckDB with the exact count |
| Detokenization | No tokens left in the output; clear values in place |
| Asynchronous `COPY` | Kept running after its session was abandoned, and after it was closed; a new session read the full result |

## What the run found

**Bugs, now fixed:**

- **Column-name rules missed most columns.** Snowflake's `REGEXP_LIKE` must match the whole string, so a rule `PASSPORT` matched only a column named exactly `PASSPORT`. The offline tests ran the query in DuckDB, which matches anywhere, so they passed. Rules are now wrapped to match anywhere, with a test on the generated SQL.
- **The reconcile Lambda would have crashed.** With Snowflake connector 5.x, a cursor has no column descriptions after `get_results_from_sfqid` until its rows are fetched. The handler now fetches first, and new handler tests use a fake connection that behaves this way.

**Snowflake behaviour, now documented:**

- **Parquet timestamps are written in milliseconds.** This holds whatever the column's precision, whether it is cast to `TIMESTAMP_NTZ(6)`, and with `ENABLE_UNLOAD_PHYSICAL_TYPE_OPTIMIZATION` turned off. Anything finer than a millisecond is lost on unload. Timestamps are marked as UTC instants.
- **`TIMESTAMP_LTZ` and `TIMESTAMP_TZ` cannot be unloaded to Parquet at all.** lakefreight's conversion to UTC `TIMESTAMP_NTZ` is required, not optional.
- **`DETAILED_OUTPUT = TRUE` returns `FILE_NAME`, `FILE_SIZE` and `ROW_COUNT`**, with file names relative to the unload path. An empty unload returns no rows.
- **`ABORT_DETACHED_QUERY` was already `FALSE`** at the account level on this account.

## Not covered by this run

- S3 storage integrations, IAM trust and KMS, the Lambda functions in AWS, Step Functions and replication. These need an AWS account.
- Reads from Spark or Databricks, Snowflake external tables, Redshift and BigQuery.
- Snowflake's own sensitive data classification. Classification results take hours to reach `ACCOUNT_USAGE`, so this run used column-name rules.
