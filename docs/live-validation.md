# Live validation on Snowflake

lakefreight's Snowflake path was run end to end against a Snowflake trial account on AWS (Snowflake 10.42) on 2026-11-26. This page records what was run, what passed, and what the run found.

The AWS half (S3, IAM, Lambda, Step Functions, replication) was not part of this run. Unloads went to Snowflake internal stages and were downloaded, in the same folder layout lakefreight uses on S3. The AWS deployment is covered [further down](#live-deployment-on-aws).

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

- S3 storage integrations, IAM trust and KMS, the Lambda functions in AWS, Step Functions and replication. The [AWS deployment](#live-deployment-on-aws) below covers most of these.
- Reads from Spark or Databricks, Snowflake external tables, Redshift and BigQuery.
- Snowflake's own sensitive data classification. Classification results take hours to reach `ACCOUNT_USAGE`, so this run used column-name rules.

# Snapshots under concurrent writes

On 2026-11-27 a 2-million-row table, clustered on an integer key, was planned twice in the Snowflake trial: once flagged `STILL_WRITTEN` with `snapshot = "still_written"`, and once with `snapshot = "none"` as a control. Each plan was extracted through the real `submit`, `poll` and `reconcile` handlers while a second connection kept updating, deleting and inserting rows across the whole key range, pausing 3 seconds between each unload and its reconciliation.

| Run | Writes during the run | Result |
|---|---|---|
| Pinned | 42 statements | All 3 chunks reconciled. They held exactly 2,000,000 rows, the table's count at the snapshot, while the live table had moved to 1,999,384 |
| Unpinned | 60 statements | A chunk failed with `row count mismatch: unloaded 1048398, source 1048440`: rows changed between its unload and its reconciliation |

All work items of the pinned plan shared one snapshot, and its profile query read at it too. With the table's retention set to 0 days, and then to 1 day against a 30-hour window, the planner refused it with the retention, the window and the remedies in the message, and created no work items.

No bugs. The test database was dropped afterwards.

# Value reconciliation

On 2026-11-27 the demo tables were recreated with two more: one holding every column type the renderer handles (17, from `BINARY` and `GEOGRAPHY` to `TIMESTAMP_TZ`), and one with timestamps before 1970 that have sub-millisecond digits. They were discovered, seeded and planned into 20 work items. The real `submit`, `poll` and `reconcile` handlers then ran against the Snowflake trial, unloading to internal stages and keeping manifests in memory.

| Check | Result |
|---|---|
| Fingerprints | For every table and all 17 types, the source fingerprint equalled the one read back from the files, including detokenized passports and timestamps converted to UTC |
| Default run | 17 of 20 chunks failed, each naming the timestamp columns and how many values had digits below a millisecond: every `LOAD_TS`, `LISTED_AT` and `EVENT_TS` value in the demo data. The 3 chunks with none reconciled |
| Accepting the loss | With `allow_timestamp_precision_loss = true`, the 17 reconciled on the next run: all 20, 161,206 rows, with the fingerprint in every manifest |
| A changed value | Changing one email in the source after the unload changed its fingerprint; so did swapping two names between rows |
| A bad unload | An unload altered to write `UPPER(EMAIL)` kept the row count, failed with "values differ", and wrote no manifest |
| Empty chunk | A chunk with no rows and no files compared as equal |

**Found:** for timestamps before 1970, Snowflake's Parquet writer cuts toward zero on the epoch, so `1969-12-31 23:59:59.9995` became `1970-01-01 00:00:00.000`, a later instant, not an earlier one. The fingerprint models this, and design decision 14 records it.

Every live run before this one reconciled chunks whose timestamps had lost their sub-millisecond digits. Row counts alone could not see it.

# Chunking by integer key and by day

On 2026-11-27 two tables were built in the Snowflake trial to exercise the planner on data the earlier runs did not cover:

- `LEDGER`: 10 million rows, 206 MB, with no date column. It is clustered on an integer `ENTRY_ID` and has negative keys, gaps and 100 NULL keys.
- `EVENTS`: 5 million rows, 114 MB, clustered on `TO_DATE(EVENT_TS), REGION`, with 80% of rows in March 2024 and 100 NULL timestamps.

Discovery, seeding and planning ran as they would for a migration, with a 40 MB target chunk size. Then each chunk was checked in Snowflake.

| Check | Result |
|---|---|
| Clustering keys | Discovery recorded `LINEAR(ENTRY_ID)` and `LINEAR(TO_DATE(EVENT_TS), REGION)` |
| Chunk columns | Seeding picked `ENTRY_ID` and `EVENT_TS` from the clustering keys, including from inside `TO_DATE(...)` |
| `LEDGER` plan | Six integer ranges of about 1.94 million rows each, plus the NULL chunk |
| `EVENTS` plan | January to February as one chunk; March split into three day ranges; April to December as one chunk; the NULL chunk |
| Coverage | Every row of both tables matched exactly one chunk predicate, and chunk counts summed to the table counts |
| Pruning | Every chunk query scanned only part of the table: 1 or 2 of 4 partitions for `EVENTS`, 1 to 3 of 8 for `LEDGER` |
| Unload | One integer-range chunk and one day-range chunk unloaded through the renderer reconciled exactly: 1,937,981 and 1,703,273 rows |

No bugs. The test database was dropped afterwards.

# Live deployment on AWS

`infra/terraform` was applied to a new AWS account (us-east-2, the Snowflake account's region) and the same Snowflake trial on 2026-11-26, with OpenTofu 1.13, AWS provider 6 and Snowflake provider 2.24. The demo data above was recreated, then the full pipeline was run through Step Functions. Everything was destroyed afterwards.

It was run twice. The first account was on AWS's free plan, which stopped the run short of S3. The second, a standard paid account, ran all the way through: Snowflake wrote every file into S3, and the tables were registered and read back from there. That second run also found a gap in zone isolation, which is now fixed.

## First run: free-plan account

### What was run

1. `infra/build_lambda.sh x86_64` and `tofu apply` with the smallest warehouses and size thresholds lowered so the demo tables would chunk.
2. The service user's private key into the Secrets Manager secret; `01_control_tables.sql` as the admin role.
3. `lakefreight discover --table LAKEFREIGHT.CONTROL.SOURCE_METADATA` and `02_seed_config.sql`.
4. `python -m lakefreight.planning` as the pipeline's service user.
5. One Step Functions execution per lane.
6. `tofu destroy`.

### Results

| Check | Result |
|---|---|
| Apply | All 100 resources created on the first attempt, on AWS and in Snowflake |
| Storage integrations | Each integration's IAM user and external ID matched its role's trust policy exactly, resolved by Terraform in one plan |
| Seeding | Four tables routed to all three zones; `LOAD_TS` and `EVENT_TS` picked as chunk columns |
| Planning | 18 work items, created by the service user with a zone role |
| Lambda functions | Read the key from Secrets Manager, connected as the service user, submitted, polled and recorded every `COPY` |
| State machine | All three lanes ran every item through the Distributed Map |
| Unloads | **All 18 failed** with `Error assuming AWS_ROLE`: see below |
| Destroy | All 100 resources removed |

### What the run found

**The free plan blocks Snowflake.** Snowflake writes to S3 by assuming a role in your account from Snowflake's own AWS account. AWS free-plan accounts are members of an organization that AWS manages, with resource control policies that deny principals from outside that organization for services including STS and S3. Every `COPY` failed with:

```
Error assuming AWS_ROLE: User: arn:aws:iam::<snowflake account>:user/<id> is not authorized to perform: sts:AssumeRole on resource: arn:aws:iam::<your account>:role/lakefreight-snowflake-internal
```

The trust policies were correct, and the error persisted well beyond IAM's propagation delay. The account could not read its own organization policies to confirm the denying statement, so this cause is inferred from AWS's documentation of those policies. Lifting it requires upgrading to the paid plan and activating advanced features, both irreversible, so the run stopped here. `infra/README.md` now warns about it.

**Bugs, now fixed:**

- **A lane reported success when every item failed.** The Distributed Map tolerates failed items so each one gets its attempt, which also made the execution succeed. A `summarize` step now counts the run's items by status and fails the execution with `LaneHadFailures` if any did not reconcile.
- **The seed script ignored the configured size threshold.** It hard-coded the 100 GiB default, so a deployment with a different `full_threshold_bytes` seeded the wrong strategy. It now reads a variable set at the top of the script.
- **The seed script failed through the Python connector**, because it ended with comment-only lines, which the connector sends as an empty statement. The comments moved to the top.
- Provider 2.24 warned that the storage-integration preview flag is now stable; it is removed.

## Second run: paid account

The same configuration, with the fixes above, was applied to a standard AWS account in the same region on the same day.

### What was run

Steps 1 to 5 as above. The plan created 102 resources, including the `summarize` function. Then:

6. The RESTRICTED lane failed on purpose: see below. The missing token was added to the cross-reference and the lane run again.
7. `lakefreight register --format delta` on each table's S3 prefix, then a read back from S3 with delta-rs.
8. The zone-isolation check described below. After the fix, the stack was updated, the output and work items cleared, and the whole pipeline run again from planning.
9. The buckets emptied of every object version, then `tofu destroy`.

### Results

| Check | Result |
|---|---|
| Unloads | All 18 work items reconciled: 161,200 rows across the four tables, written by Snowflake through each zone's role |
| Zones | INTERNAL and PII tables in the egress bucket under their own prefixes; RESERVATION in the restricted bucket |
| Encryption | Every file encrypted with its zone's own KMS key |
| Failure gate | The RESTRICTED lane failed with `LaneHadFailures` when one chunk held 3 tokens with no match in the cross-reference. After the fix, the rerun retried only that chunk (2 attempts) and succeeded |
| Failed attempt | The failed attempt left a Parquet file with no manifest. Registration ignored it and counted exactly 60,000 rows |
| Registration on S3 | All four tables registered in place on S3, at Snowflake's row counts. CLICK_EVENTS: 13 chunks, 12 files (its NULL chunk is empty) |
| Read back from S3 | delta-rs read every table at the expected counts. No passport left tokenized; the 5 NULL tokens stayed NULL; 37 NULL load timestamps kept. Every CLICK_EVENTS file carries min/max statistics |
| Rerun without secondary roles | All 18 items reconciled on the first attempt, so each zone role holds every grant it needs |
| Destroy | Nothing left in the account; the secret and keys are in AWS's standard pending-deletion window |

### What the run found

**Zone roles did not isolate a session.** Snowflake activates every role granted to a user as a *secondary role* by default (`DEFAULT_SECONDARY_ROLES = ('ALL')`). The pipeline's service user holds all three zone roles, so a session opened as the INTERNAL role could also list the PII and RESTRICTED stages. The unloads still went to the right places, because the generated SQL picks each table's stage from its zone. But the second line of defence, a permission error if routing ever goes wrong, was missing. Now:

- Terraform sets `default_secondary_roles_option = "NONE"` on the service user.
- `connect()` runs `USE SECONDARY ROLES NONE` on every session, which also protects setups that do not use this Terraform.

After the fix, an INTERNAL session could list only its own stage; listing the PII or RESTRICTED stage failed.

**Timestamps without a zone** come back as UTC-adjusted timestamps (`LOAD_TS` is `timestamp[us, tz=UTC]` in Parquet). This is expected; see the Databricks section below.

**Teardown needs the buckets emptied first, including every object version**, since they are versioned and Terraform does not force-delete them. `infra/README.md` already says so.

## Not covered by these runs

- Cross-account replication, which needs a second account.
- Iceberg registration on S3; it was tested on local files above.

# Read back in Databricks

On 2026-11-26 the demo data was unloaded again from the Snowflake trial with the same steps as the first run, registered as Delta Lake with `lakefreight register`, uploaded to a Unity Catalog volume, and queried from a Databricks SQL warehouse. The unknown passport token was added to the cross-reference first, so every chunk would reconcile.

## Results

| Check | Result |
|---|---|
| Unload | 18 work items, all reconciled; 3 restricted chunks with 0 unmatched tokens |
| Row counts | `HOTEL_DIM` 200, `TRAVELER` 1,000, `RESERVATION` 60,000, `CLICK_EVENTS` 100,000: identical to Snowflake |
| Read by path | ``SELECT ... FROM delta.`/Volumes/...` `` worked on all four tables with no extra setup |
| Types | `NUMBER(38,0)` as `decimal(38,0)`, `DATE` as `date`, timestamps as `timestamp`, `VARIANT` as JSON text |
| `TIMESTAMP_LTZ` and `TIMESTAMP_TZ` | Arrived as the correct UTC instants, for example `-05:00` midnight as `05:00Z` |
| `TIMESTAMP_NTZ` | Correct wall-clock values in a UTC session, at millisecond precision |
| `TIME` | Text with all six fractional digits kept |
| Semi-structured | Databricks' `:` path syntax works on the JSON text directly, for example `PAYLOAD:page` |
| Detokenization | Clear passport values, no tokens left, and the 5 NULL tokens kept as NULL |
| NULL chunk | The 37 rows with no load date present |
| Unity Catalog | `DEEP CLONE` into a managed table copied all 12 files and 100,000 rows |
| Raw Parquet guard | `read_files` refused the table folder because it holds a Delta log, so the raw files, including any from a failed attempt, are not read by accident |

## What the run found

No bugs. One gap, now closed, and behaviour worth knowing, now in the migration guide:

- **`CREATE TABLE ... LOCATION` refuses a volume path.** Registering the files in place needs the bucket set up as a Unity Catalog external location; otherwise use `DEEP CLONE`.
- **`TIMESTAMP_NTZ` columns become Databricks `TIMESTAMP`**, which is time-zone aware, because Snowflake marks every Parquet timestamp as a UTC instant. Values display correctly in a UTC session, the SQL warehouse default. A session in another time zone shifts how they display.
- **The Delta log had no per-column statistics**, only row counts, so Databricks could not skip files on filters. `lakefreight register` now writes them from the Parquet footers. The same unloads, registered again and queried in Databricks, returned identical counts for filters on timestamps, IDs, NULLs and exact boundary values, and a filter on the last three months of `CLICK_EVENTS` read 3 of 12 files.

## An updated table

On 2026-11-27 a Delta table updated with `register --update` was read on the same SQL warehouse. Version 0 was a partial registration of 2 of 3 chunks. Version 1 replaced one chunk with a re-extract holding 200 more rows and added the missing chunk, so its log holds both `remove` and `add` actions. The data had Snowflake's Parquet types: decimals, strings and UTC timestamps.

| Check | Result |
|---|---|
| Current version | 1,710 rows and the exact ID sum; the 200 rows only in the re-extract present, the replaced file's data absent |
| Time travel | `VERSION AS OF 0` returned the original 1,010 rows and their exact ID sum |
| History | `DESCRIBE HISTORY` listed version 0 `CONVERT` and version 1 `REPLACE CHUNKS`, with the chunks in its parameters |
| Unity Catalog | `DEEP CLONE` copied the 3 live files, not the removed one, and matched the counts |
| File skipping | A filter only the added chunk can match read 1 file and pruned 2, from the statistics in the update commit |

No bugs.

## Not covered by this run

- Iceberg in Databricks. Iceberg metadata stores absolute paths, so it must be registered against the final bucket and read through a catalog; it was not tested here.
- Spark clusters other than a SQL warehouse, and reads directly from S3.
