---
name: generate-extract-sql
description: Generate bulk Snowflake-to-S3 Parquet extract SQL (COPY INTO unloads) from a table/column metadata file, routed by sensitivity (personal and restricted data) and chunked by date or integer key for large tables. Use this whenever the user wants to unload, export, extract or migrate many Snowflake tables to S3, asks for extract or unload SQL for a list of tables, has a metadata or data-dictionary export and wants migration scripts from it, or asks how to split a large table into chunks, even if they don't say "COPY INTO" or name this skill.
---

# Generate extract SQL

Turn a metadata export (one row per column) into ready-to-run `COPY INTO` unload statements, one file per table or per date chunk, plus the reconciliation queries that prove each unload is complete.

The SQL is produced by this repo's generator, not written by hand or by you. With thousands of tables, hand-written SQL drifts and hides mistakes; the generator applies the same casting, routing and join rules to every table, and those rules are unit tested. Your job is to get the input into shape, run it, and tell the user what needs their attention.

Never write, edit or "fix" generated SQL yourself. If the output looks wrong, change the input metadata or the settings and run the generator again, so every statement still follows from the metadata. The project's documentation promises users this: AI shapes the input and summarizes the output, and the generator alone writes the SQL.

## Steps

### 1. Get the metadata into the expected shape

The generator reads a CSV with one row per column and these headers (case-insensitive):

| Header | Required | Meaning |
|---|---|---|
| `table_database`, `table_schema`, `table_name` | yes | Source table |
| `table_size_bytes` | yes | Table size in bytes |
| `table_pii_flag`, `table_restricted_flag` | yes | `Y` or `N`; blank means unknown |
| `column_name`, `column_datatype` | yes | As stored in Snowflake |
| `column_pii_flag`, `column_restricted_flag` | yes | `Y` or `N`; blank means unknown |
| `column_ordinal`, `column_length`, `column_description` | no | Column order and documentation |
| `chunk_column` | no | Date, timestamp or integer column to chunk on, overriding the clustering key and the name heuristic |

If the user's file uses other header names or units, write a normalized copy (a short script is fine) and leave their original untouched. Copy values across; don't invent flags. A blank sensitivity flag stays blank, so the generator treats it as sensitive. Convert sizes to bytes. Datatypes that contain commas, such as `NUMBER(38,0)`, must be quoted in the CSV.

If there is no file yet, use `lakefreight discover --database <DB> --out metadata.csv`, which builds it from Snowflake's metadata views and sensitive data classification. If you can't connect, `lakefreight discover --database <DB> --sql-only` prints the query for the user to run and export. Report how many tables discovery left with unknown personal-data flags: those go to the PII zone until they're classified or covered by a `[discover]` rule.

See `examples/metadata_sample.csv` for a working example.

### 2. Check the settings

The settings file holds the stage per zone, the xref table and its column names, the size threshold for chunking and the target chunk size. `lakefreight config` shows which file is in use; `lakefreight config --write` creates an editable `lakefreight.toml` in the current folder. Values marked `CHANGE-ME` are placeholders. Ask the user for real values if the SQL is going to be run; for a dry run or a review, the placeholders are fine, and say so in your summary.

### 3. Run the generator

```bash
lakefreight plan --metadata <metadata.csv> --out generated
```

Run it from the repo root after `pip install -e .`, which installs the `lakefreight` command. Without installing, use `PYTHONPATH=src python -m lakefreight plan ...`. Add `--profiles <profiles.csv>` when profiles exist (step 4). Add `--snapshot '<UTC timestamp>'` to read every table as of one moment through time travel, and `--changes-since '<UTC timestamp>'` with it to generate change sets for a delta pass instead.

It writes `generated/<ZONE>/<db>.<schema>.<table>/<chunk>.sql` and `generated/work_items.csv`. Each `.sql` file contains the unload, the source row count, for restricted tables the unmatched-token count, and the two fingerprint queries that compare the values in the source with the values in the unloaded files.

### 4. Resolve tables marked NEEDS_PROFILE

A table above the size threshold is split into ranges of its chunk column, and sizing the chunks needs row counts: per day for a date column, per equal-width range for an integer column. For each such table the generator writes a `profile.sql`. Ask the user to run those and collect the results into one CSV with headers `table_database, table_schema, table_name, bucket_start, row_count` (blank `bucket_start` for NULL values; profiles with `month_start` from older versions still work), then rerun with `--profiles`.

Before the user spends compute on profiles, show them the chunk column picked for each table and where it came from. The column must be one the table is clustered on: usually a load or event date, or an integer key. The generator prefers the column the clustering key leads with; a note asks the user to confirm any other pick. If the column doesn't prune, every chunk scans the whole table and chunking makes the extract slower, not faster. A wrong pick is fixed by adding `chunk_column` to the metadata CSV.

### 5. Report what you generated

Lead with the counts, then everything that needs a decision. Use this structure:

```
Generated <n> work items for <t> tables in generated/.

| Zone | Tables | Work items |
...

Needs attention
- <table>: <note from work_items.csv, in plain words>

Next steps
- ...
```

Read the `notes` column of `work_items.csv` and surface every distinct note. Do not paste the SQL into the chat; point to the files, and show one representative file only if the user asks.

## Rules the generator enforces, and why

Knowing these lets you explain the output and spot input problems.

- **Sensitivity fails closed.** Only a table explicitly flagged `N`/`N` is routed to the INTERNAL zone. Missing flags are treated as sensitive, and a column flagged restricted sends the whole table to the RESTRICTED zone even if the table flag disagrees. A misrouted sensitive table is a security incident; an over-protected table costs nothing.
- **Restricted columns are detokenized with a `LEFT JOIN` to the xref.** An inner join silently drops every row whose token is NULL or missing from the xref. The generator refuses to put clear values in any zone but RESTRICTED.
- **Chunk predicates cover every value.** The first chunk has no lower bound, the last has no upper bound, and there is always a NULL chunk, so rows with unexpected dates cannot be missed.
- **Every unload comes with its reconciliation queries.** The source count is taken without the join, so it catches both dropped rows and rows duplicated by a non-unique xref. The fingerprint queries compare every value, so a wrong cast or join that keeps the row count still fails.
- **Types are cast for Parquet.** Time-zone-aware timestamps are converted to UTC `TIMESTAMP_NTZ` (Snowflake refuses to unload them otherwise), `TIME` becomes text, semi-structured columns become JSON text, geospatial columns become WKT. Snowflake writes Parquet timestamps in milliseconds, and reconciliation fails a chunk whose timestamps would lose finer digits unless the user accepts it, so tell the user which timestamp columns are involved.

## What this skill does not do

It does not write SQL, run the generated SQL, or move data. The only command in it that connects to Snowflake is `lakefreight discover` (step 1), which reads metadata views through the user's own connection, never table rows. To execute at scale, the repo's Step Functions pipeline (`stepfunctions/` and `src/lakefreight/handlers.py`) renders the same SQL at run time from control tables, with no AI involved. For the full migration method, see `docs/migration-guide.md`.
