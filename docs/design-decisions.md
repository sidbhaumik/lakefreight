# Design decisions

Each entry gives the decision, the alternative that was rejected, and the reason.

## 1. Snowflake writes the files; Python only coordinates

**Decision.** Extraction is `COPY INTO @stage` with Parquet output. Python submits statements, tracks state and reconciles.

**Rejected.** Reading rows through a Python client and writing Parquet from the client.

**Why.** At petabyte scale the client becomes the bottleneck and a point of failure. `COPY INTO` unloads in parallel across the warehouse and writes directly to S3.

## 2. SQL is rendered from metadata at run time

**Decision.** Each statement is built from the control tables when its work item runs. The TOML file holds settings only.

**Rejected.** Generating all statements up front and storing them in a config file.

**Why.** Thousands of stored statements go stale when a column is added or a sensitivity flag changes, and nothing tells you which ones. The offline generator exists for review and small jobs; the pipeline does not depend on its output.

## 3. Three zones, enforced by privileges

**Decision.** The `RESTRICTED`, `PII` and `INTERNAL` zones each get a stage, S3 location, KMS key and role. A role can write only to its own stage.

**Rejected.** One bucket with folders chosen by the code.

**Why.** With one writer role, a routing bug writes detokenized identifiers into the open folder and nothing stops it. With separate roles the same bug fails with a permission error. This holds only if each session holds just one zone role: Snowflake activates all of a user's granted roles as secondary roles by default, so the service user has them turned off and every connection runs `USE SECONDARY ROLES NONE`. Restricted data is kept apart from other personal data because clear identifiers usually carry stricter compliance rules and shorter retention.

## 4. Sensitivity fails closed

**Decision.** Only an explicit `N`/`N` is non-sensitive. Missing flags are treated as sensitive, and a flagged column outranks its table's flag.

**Why.** Metadata is never complete. Over-protecting an unclassified table costs nothing; under-protecting one is an incident.

## 5. Chunks are packed to a size, not fixed to a calendar period

**Decision.** One profile query per large table returns row counts by day for a date or timestamp chunk column, or by equal-width range for an integer one (up to 10,000 ranges between its minimum and maximum). Whole months are packed into chunks near a target size; a month too big for one chunk is packed by day instead. Integer ranges are packed the same way.

**Rejected.** Fixed monthly or yearly chunks.

**Why.** Volume is rarely even across many years of history. Fixed periods give tiny early chunks and oversized recent ones, which makes run time unpredictable. Packing whole months where they fit keeps chunk names readable and stable from one plan to the next; splitting only the months that need it keeps a single busy month from becoming one huge unload. Tables with no date column, such as ledgers keyed by a sequence, get the same even chunks from their integer key.

## 6. Chunk predicates are open-ended, and there is always a NULL chunk

**Decision.** The first chunk has no lower bound, the last has no upper bound, boundaries are shared between neighbours, and a `col IS NULL` chunk is always created.

**Rejected.** Closed ranges taken from the profiled minimum and maximum.

**Why.** Closed ranges miss rows that arrive after profiling or sit outside the expected range, and a range predicate never matches NULL. The open-ended form cannot leave a row out.

## 7. The chunk column must prune

**Decision.** A large table is chunked only on a column it is clustered on. Otherwise it is unloaded in a single pass. `discover` records each table's clustering key, and seeding picks the column the key leads with when it is a date, timestamp or integer, including one wrapped in an expression such as `TO_DATE(EVENT_TS)`. Only when there is no such key does it fall back to guessing a date column by name, marked for review.

**Why.** Without pruning, each of N chunks scans the full table, so the work is N times a single pass. Reading the clustering key turns this rule from a review step into the default.

## 8. Detokenization uses a LEFT JOIN, checked against the un-joined count

**Decision.** Tokenized columns are replaced by joining to a cross-reference. The reconciliation count comes from the source table without the join, and unmatched tokens are counted per chunk.

**Rejected.** An inner join; reconciling against a count of the joined query.

**Why.** An inner join drops rows with NULL or unknown tokens, and a non-unique cross-reference duplicates rows. A count of the same joined query would agree with the unload in both cases and hide the error.

## 9. The manifest is the commit marker

**Decision.** A manifest is written last, after reconciliation passes: row counts and values (decision 16). The receiver loads only prefixes that contain one. Each attempt writes to a new `run_id` prefix.

**Rejected.** Copying files to a separate "ready" location after validation.

**Why.** Petabytes of copy is slow and costly, and S3 has no atomic move. With a marker file, replication can run continuously and partial or failed attempts are simply never read.

## 10. Work is tracked per chunk

**Decision.** Every table or chunk is a row in `EXTRACT_WORK_ITEM` with its own status and attempt count.

**Rejected.** Tracking per table, or relying on the orchestrator's execution history.

**Why.** A failure in chunk 300 of a multi-terabyte table should cost one chunk. A table in Snowflake is also easy to query for progress and for the final reconciliation report.

## 11. Lanes by size and sensitivity

**Decision.** Separate state-machine executions, warehouses and concurrency limits for large tables, small tables and the restricted zone.

**Why.** In one queue, thousands of small tables delay the few large ones that decide the end date, or the large ones starve everything else. The restricted lane also needs its own role.

## 12. The egress bucket sits in Snowflake's region

**Decision.** Snowflake unloads to a bucket in its own region; S3 carries the data to the target region.

**Rejected.** Unloading straight to a bucket in the target region.

**Why.** Snowflake charges for cross-region unloads. In-region unloads avoid that charge, and S3's own transfer is the cheaper hop.

## 13. Table formats are registered over the unloaded files

**Decision.** `lakefreight register` writes Delta Lake or Iceberg metadata that points at the Parquet files Snowflake already wrote. It includes only files named in a committed manifest, picking the newest manifest per chunk.

**Rejected.** Rewriting the data through Spark into a new Delta or Iceberg table; converting the whole folder with a generic tool.

**Why.** Rewriting petabytes doubles the cost and time of the move. A generic folder conversion would pick up files from failed attempts, which have no manifest, and turn them into duplicate rows.

**Updates.** `register --update` swaps whole chunks: the table's files for a chunk are replaced by the newest run's files in one commit, so a reader sees either the old chunk or the new one, never both or neither. It updates only tables that lakefreight alone has written. Any file no manifest lists, or any row-level delete, means another engine has changed the table, and swapping chunks could silently undo that change, so the update refuses.

**Statistics.** Each Delta `add` action carries the file's row count, null counts and, for integers, decimals, dates and timestamps, its minimum and maximum, read from the Parquet footer. Engines use them to skip files on filters. Strings, floats and booleans get null counts only: writers may truncate string statistics, and a minimum or maximum that is wrong makes a filter silently drop rows. Timestamp maximums are rounded up to the millisecond, the precision Delta readers use.

## 14. Timestamps are converted to UTC, and leave at millisecond precision

**Decision.** Time-zone-aware timestamps are converted to UTC `TIMESTAMP_NTZ` before unloading, and `TIME` columns leave as text. Registration refuses files with nanosecond timestamps.

**Why.** Snowflake refuses to unload `TIMESTAMP_LTZ` and `TIMESTAMP_TZ` to Parquet, and Delta Lake has no `TIME` type. A live run also showed that Snowflake writes every Parquet timestamp in milliseconds, marked as UTC, whatever the column's precision or cast; no unload setting changes it. Digits finer than a millisecond are therefore lost. A lossless alternative, unloading timestamps as text or as epoch microseconds, would make every reader convert them back, so it is not the default.

The loss is not silent. Reconciliation counts the values in each timestamp column that have digits finer than a millisecond and fails the chunk, naming the columns, unless `allow_timestamp_precision_loss` is set. The writer cuts toward zero on the epoch, so a value before 1970 moves forward, for example `1969-12-31 23:59:59.9995` becomes `1970-01-01 00:00:00.000`.

## 15. Infrastructure is Terraform, covering both AWS and Snowflake

**Decision.** One Terraform/OpenTofu configuration creates the AWS side (buckets, keys, roles, Lambda functions, state machine, optional replication) and the Snowflake side (warehouses, roles, storage integrations, stages, grants, service user).

**Rejected.** AWS CDK or CloudFormation for AWS plus SQL scripts for Snowflake.

**Why.** The two sides reference each other: Snowflake's storage integration needs the AWS role's ARN, and the role's trust policy needs the integration's Snowflake identity. One tool resolves that in a single plan. Terraform is also what most data platform teams already run, and it works with either Terraform or OpenTofu.

## 16. Reconciliation compares values, not just counts

**Decision.** After each unload, one query fingerprints the chunk's values in the source with `HASH_AGG`, using the same expressions and joins as the unload. A second query computes the same fingerprint over the files that attempt wrote, read back through the zone's stage. The chunk passes only if the row counts and the fingerprints are equal, and no timestamp loses digits unless that was accepted. The fingerprint goes into the manifest.

**Rejected.** Row counts alone; sampling rows; comparing outside Snowflake.

**Why.** A count catches dropped and duplicated rows, but not changed values: a wrong cast, a timezone shift, a broken detokenizing join, or timestamps cut to the millisecond all keep the count and pass. `HASH_AGG` is order-independent and covers every row and column, and running both sides in Snowflake means one engine reads both, with no rows leaving it. Reading the files back through the zone's own stage uses the same role and key that wrote them. The extra cost is one scan of the chunk's source and one read of its files.

## 17. Tables still being written are read as of one snapshot

**Decision.** For tables flagged `STILL_WRITTEN` (or every table, with `snapshot = "all"`), the planner takes one UTC snapshot per run. The profile, every unload and every reconciliation query read `AT(TIMESTAMP => snapshot)`. The planner first checks that each pinned table's time-travel retention covers `snapshot_window_hours`, and refuses before planning anything if not.

**Rejected.** Reading the live table; a zero-copy clone per table as the only option.

**Why.** Chunks of a large table are unloaded hours apart. Read live, a row updated in between can land in two chunks or in none, while each chunk still reconciles; and rows that change between a chunk's unload and its reconciliation make good chunks fail. Time travel gives every query for a table the same moment with no extra storage or setup, and one snapshot for the whole run keeps related tables consistent too. Clones remain the fallback when retention cannot cover the extract.

## Known gaps

- Large tables whose clustering key leads with neither a date nor an integer, such as a text code, run as one `COPY`.
- A single day, or a single integer range, larger than twice the target chunk size is flagged but not split further.
- `infra/terraform` has been applied to a live AWS account and run end to end, with Snowflake writing into S3 through the zone roles. Cross-account replication is still untested.
- Snowflake's Parquet unload keeps timestamps only to the millisecond. Columns that need finer precision have no lossless path yet.
- Registered Delta tables have been read back in Databricks SQL, and Delta and Iceberg tables with delta-rs, pyiceberg and DuckDB, all from a live Snowflake unload. Iceberg in Databricks, and either format in Snowflake, Redshift or BigQuery, is untested.
- A table still being written but not flagged `STILL_WRITTEN` is read live, so its chunks can come from different moments and reconciliation can fail. Rows changed after a snapshot need a delta pass, which is not built yet.
- `register --update` writes the Delta log without a lock, so it relies on one registration per table at a time. Updated Delta tables have been read in Databricks SQL and with delta-rs.
