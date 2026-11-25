---
name: generate-extract-sql
description: Generate bulk Snowflake-to-S3 Parquet extract SQL (COPY INTO unloads) from a table/column metadata file, routed by sensitivity (personal and restricted data) and chunked by date for large tables. Use this whenever the user wants to unload, export, extract or migrate many Snowflake tables to S3, asks for extract or unload SQL for a list of tables, has a metadata or data-dictionary export and wants migration scripts from it, or asks how to split a large table into date-range chunks, even if they don't say "COPY INTO" or name this skill.
---

# Generate extract SQL

Turn a metadata export (one row per column) into ready-to-run `COPY INTO` unload statements, one file per table or per date chunk, plus the reconciliation queries that prove each unload is complete.

The SQL is produced by this repo's generator, not written by hand. With thousands of tables, hand-written SQL drifts and hides mistakes; the generator applies the same casting, routing and join rules to every table, and those rules are unit tested. Your job is to get the input into shape, run it, and tell the user what needs their attention.

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
| `chunk_column` | no | Date column to chunk on, overriding the heuristic |

If the user's file uses other header names or units, write a normalized copy (a short script is fine) and leave their original untouched. Convert sizes to bytes. Datatypes that contain commas, such as `NUMBER(38,0)`, must be quoted in the CSV.

If there is no file yet, give the user a query against their metadata table that produces these columns, and ask them to export the result.

See `examples/metadata_sample.csv` for a working example.

### 2. Check the settings

`config/defaults.toml` holds the stage per zone, the xref table and its column names, the size threshold for chunking and the target chunk size. Values marked `CHANGE-ME` are placeholders. Ask the user for real values if the SQL is going to be run; for a dry run or a review, the placeholders are fine, and say so in your summary.

### 3. Run the generator

```bash
python -m migrator.generate --metadata <metadata.csv> --out generated
```

Run it from the repo root with the package installed (`pip install -e .`) or with `PYTHONPATH=src`. Add `--profiles <profiles.csv>` when month profiles exist (step 4).

It writes `generated/<ZONE>/<db>.<schema>.<table>/<chunk>.sql` and `generated/work_items.csv`. Each `.sql` file contains the unload, the source row count, and for restricted tables the unmatched-token count.

### 4. Resolve tables marked NEEDS_PROFILE

A table above the size threshold is split by date, and sizing the chunks needs a row count per month. For each such table the generator writes a `profile.sql`. Ask the user to run those and collect the results into one CSV with headers `table_database, table_schema, table_name, month_start, row_count` (blank `month_start` for NULL dates), then rerun with `--profiles`.

Before the user spends compute on profiles, show them the chunk column picked for each table. The column must be one the table is clustered on, usually a load or event date. If it is not, every chunk scans the whole table and chunking makes the extract slower, not faster. A wrong pick is fixed by adding `chunk_column` to the metadata CSV.

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
- **Every unload comes with its reconciliation queries.** The source count is taken without the join, so it catches both dropped rows and rows duplicated by a non-unique xref.
- **Types are cast for Parquet.** Time-zone-aware timestamps become UTC `TIMESTAMP_NTZ`, semi-structured columns become JSON text, geospatial columns become WKT.

## What this skill does not do

It does not connect to Snowflake or run the SQL. To execute at scale, the repo's Step Functions pipeline (`stepfunctions/` and `src/migrator/handlers.py`) renders the same SQL at run time from control tables. For the full migration method, see `docs/migration-guide.md`.
