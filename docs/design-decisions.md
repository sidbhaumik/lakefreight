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

## 18. The delta pass reads Snowflake's change tracking between two snapshots

**Decision.** Once a table's bulk extract has reconciled, `python -m lakefreight.planning --changes` creates a change set: the rows that changed from the table's last snapshot to a new one, read with `CHANGES(INFORMATION => DEFAULT) AT(...) END(...)`. Each row carries `LF_ACTION` (`INSERT` or `DELETE`) and `LF_IS_UPDATE`; an update is a delete of the old row and an insert of the new one. Change sets go through the same handlers, detokenization and checks as chunks, under `<zone prefix>/_changes/`, and each pass starts where the last ended, so passes can repeat until cut-over. Applying them to the target is left to the engine.

**Rejected.** Comparing the two snapshots on a key; streams; applying changes in lakefreight.

**Why.** A real migration extracts at a snapshot and then catches up. Change tracking gives exactly the net changes between two instants, without a key and without scanning the table twice, and Snowflake refuses the query if tracking was off for any part of the range, so a gap cannot pass silently. The planner runs that check, and the retention check, before creating anything. Streams would add objects to manage and advance only when consumed; snapshots recorded in the work items make each pass reproducible.

A complete change set balances: the table's rows at the first snapshot, plus inserts, minus deletes, equal its rows at the second. Reconciliation checks that as well as the row count and fingerprint, so a change set that missed rows fails. Snowflake's `HASH_AGG` is not additive, so the fingerprints cannot prove that applying the set reproduces the second snapshot; the live test proved that end to end instead.

Change sets live outside the table's folder, so registering the table never picks them up. Registering a `_changes` folder gives a Delta or Iceberg change log, which `--update` extends with each pass, and which an engine can `MERGE` from.

## 19. Concurrent updates commit with a conditional put, and retry

**Decision.** A Delta commit is created only if its log version does not exist yet: a conditional `PutObject` (`If-None-Match: *`) on S3, a hard link from a temporary file on local disk. A writer whose commit is refused re-reads the table, then the manifests, plans again and retries with backoff, up to 10 times. Iceberg updates retry the same way when the catalog reports a conflict, and a new Iceberg table is created together with its files in one transaction.

**Rejected.** A lock table in DynamoDB; a lock file; one registration per table at a time.

**Why.** Lanes reconcile chunks of the same table independently, so updates naturally overlap. The Delta protocol only needs each version to be created once, and S3 has offered put-if-absent since 2024, so no lock service or stale-lock cleanup is needed. Re-planning from the new version, instead of replaying the old plan, means a losing writer never removes files the winner already swapped, and commits nothing if the winner covered its chunks. Reading the table before the manifests matters: every manifest that a commit in the snapshot was planned from is then in the listing, so a slow writer cannot put a chunk back to an older run.

## 20. AI prepares the inputs; deterministic code writes the SQL and moves the data

**Decision.** AI appears in one place: an optional Claude Code skill that turns a user's metadata export into lakefreight's format, runs `lakefreight plan`, and summarizes what needs a decision. The SQL comes from the generator's templates and rules, never from the model. Discovery, infrastructure, unloads, reconciliation, transfer, registration and delta passes are plain code with no model calls.

**Rejected.** Having a model write each table's SQL; calling a model at run time, for example to choose chunk columns or route tables.

**Why.**
- **Same input, same SQL:** with thousands of tables, each statement must follow from the metadata, so it can be reviewed, tested and regenerated. Model output can vary between runs, and an error in one table's SQL would be hard to spot among thousands.
- **Rules that are tested and that fail closed:** routing and detokenization are security rules, and they live in unit-tested code.
- **No model near the data:** the model sees metadata only, never row values, and the pipeline that moves data has no model in it to fail, drift or leak.
- **The work AI is good at:** what's left for AI is the part that varies: messy exports, unfamiliar header names, and turning the generator's notes into a clear list of decisions for a person.

## Known gaps

lakefreight does one job: a bulk move of Snowflake tables to open files on S3, with proof that every row and value arrived. Each item below is in one of four groups: outside that job by choice, a limit of a platform lakefreight depends on, not built yet, or built but not yet run on live services.

### Out of scope by design

- **Continuous replication or change data capture.** lakefreight moves each table as of one point in time, then catches up with delta passes until cut-over (decision 18). Keeping a target in sync after cut-over is a different tool's job.
- **Applying change sets.** lakefreight extracts and verifies each change set, and can register them as a change log. Merging them into the target table is left to the engine, which needs the table's key to do it.
- **Moving data through Python.** Snowflake writes every file, and Python only coordinates. Moving rows through a client would cap throughput at one process (decision 1).
- **Reshaping data.** Tables leave with the same columns and values. The only conversions are the ones every engine needs to read the files: timestamps to UTC, `TIME` as text, semi-structured values as JSON text, and geography as WKT (decision 14). Remodelling belongs downstream.
- **Loading into each engine's own storage.** lakefreight stops at Parquet files plus Delta or Iceberg metadata, which any engine can read in place. Copying them into managed tables is left to each engine. The migration guide shows how for Databricks, with `DEEP CLONE`.
- **Guessing which tables are still being written.** lakefreight reads the `STILL_WRITTEN` flag rather than inferring it. A table that is written but not flagged is read live, so its chunks can come from different moments and reconciliation can fail. Set `snapshot = "all"` to pin every table.

### Limits of the platforms

- **Timestamps keep only milliseconds.** Snowflake writes every Parquet timestamp in milliseconds, whatever the column's precision. lakefreight counts the affected values and fails the chunk unless `allow_timestamp_precision_loss` is set (decision 14).
- **Time-zone offsets are not kept.** Snowflake cannot unload `TIMESTAMP_LTZ` or `TIMESTAMP_TZ` to Parquet, so they leave as UTC instants. The instant is exact, but the original offset is lost.
- **Time travel reaches back only as far as retention allows.** That is 1 day on Standard edition, and up to 90 on Enterprise. The planner refuses a pinned table whose retention is shorter than `snapshot_window_hours`, and a change set whose first snapshot is older than retention allows. An extract that takes longer can read from a zero-copy clone instead, but then has no delta pass.
- **Change tracking must already be on.** A delta pass needs `CHANGE_TRACKING = TRUE` on the table from before its bulk snapshot. Turning it on later cannot recover the changes in between; the planner refuses such tables, and they need extracting again.

### Future work

- **Chunking on other keys.** A large table whose clustering key leads with neither a date nor an integer, such as a text code, runs as one `COPY`. Hash ranges on any column would split it, though the chunks would not prune.
- **Splitting oversized ranges.** A single day or integer range more than twice the target size is flagged for review, not split. Hour ranges, or narrower integer ranges, would split it.
- **Replication tracking and egress purge.** Terraform sets up cross-account replication, and registration already refuses a file whose size does not match its manifest. But nothing records when each chunk has fully replicated, and purging the egress buckets after hand-off is a manual step.
- **Lossless sub-millisecond timestamps.** An opt-in for the columns that need it, such as an extra column of epoch nanoseconds. It is not the default, because every reader would have to convert the values back.
- **One-step catalog registration.** Today tables are read by path, or copied in with `DEEP CLONE`. Registering them directly in Unity Catalog, AWS Glue or Snowflake's catalog would save that step.
- **Other clouds and sources.** Google Cloud Storage and Azure Data Lake Storage as targets, and warehouses other than Snowflake as sources.

### Built but not yet verified on live services

- **Cross-account replication.** Terraform creates it, and its plan tests pass, but it has never been applied across two accounts. Everything else in `infra/terraform` has run end to end on a live AWS account.
- **Some readers.** Verified so far: Delta in Databricks SQL, and Delta and Iceberg with delta-rs, pyiceberg and DuckDB, all from live Snowflake unloads. Not yet tested: Iceberg in Databricks, and either format in Snowflake, Redshift or BigQuery.
- **Snowflake's own sensitive-data classification.** Discovery reads it from `ACCOUNT_USAGE`, but results take hours to reach that view, so the live runs used column-name rules instead.
- **Production scale.** The largest live table had 10 million rows. Throughput and lane sizing come from the method in the migration guide, not from a measured run with thousands of tables.
