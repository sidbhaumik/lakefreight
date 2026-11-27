-- Control tables for the migration. Run once, in the control schema:
--
--   USE SCHEMA LAKEFREIGHT.CONTROL;   -- the schema infra/terraform creates,
--                                     -- or the control_schema in lakefreight.toml
--
-- With infra/terraform, run this after `terraform apply`, so the extract roles'
-- future grants apply to these tables. Without it, create the schema first.

-- One row per source table.
CREATE TABLE IF NOT EXISTS EXTRACT_CONFIG (
    DATABASE_NAME        STRING        NOT NULL,
    SCHEMA_NAME          STRING        NOT NULL,
    TABLE_NAME           STRING        NOT NULL,
    TABLE_BYTES          NUMBER(38,0),
    ZONE                 STRING        NOT NULL,   -- RESTRICTED | PII | INTERNAL
    STRATEGY             STRING        NOT NULL,   -- FULL | CHUNKED
    LANE                 STRING        NOT NULL,   -- LARGE | SMALL | RESTRICTED
    CHUNK_COLUMN         STRING,                   -- date, timestamp or integer column for CHUNKED tables
    CHUNK_COLUMN_SOURCE  STRING,                   -- CLUSTERING_KEY | HEURISTIC | REVIEWED
    SOURCE_DATABASE      STRING,                   -- set to a zero-copy clone database to extract from it
    STILL_WRITTEN        BOOLEAN       DEFAULT FALSE,
    ENABLED              BOOLEAN       DEFAULT TRUE,
    UPDATED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_EXTRACT_CONFIG PRIMARY KEY (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME)
);

-- One row per column of every source table, in ordinal order.
CREATE TABLE IF NOT EXISTS EXTRACT_COLUMN_CONFIG (
    DATABASE_NAME   STRING  NOT NULL,
    SCHEMA_NAME     STRING  NOT NULL,
    TABLE_NAME      STRING  NOT NULL,
    COLUMN_NAME     STRING  NOT NULL,
    ORDINAL         NUMBER  NOT NULL,
    DATA_TYPE       STRING  NOT NULL,
    DATA_LENGTH     NUMBER,
    DESCRIPTION     STRING,
    PII_FLAG        STRING,
    RESTRICTED_FLAG STRING,
    USE_XREF        BOOLEAN DEFAULT FALSE,          -- TRUE: replace the token with the xref clear value
    CONSTRAINT PK_EXTRACT_COLUMN_CONFIG PRIMARY KEY (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, COLUMN_NAME)
);

-- One row per table (FULL) or per chunk (CHUNKED).
CREATE TABLE IF NOT EXISTS EXTRACT_WORK_ITEM (
    WORK_ITEM_ID      STRING        NOT NULL,       -- DB.SCHEMA.TABLE#chunk_id
    DATABASE_NAME     STRING        NOT NULL,
    SCHEMA_NAME       STRING        NOT NULL,
    TABLE_NAME        STRING        NOT NULL,
    CHUNK_ID          STRING        NOT NULL,
    PREDICATE         STRING,                       -- NULL means the whole table
    ZONE              STRING        NOT NULL,
    LANE              STRING        NOT NULL,
    EST_BYTES         NUMBER(38,0),
    STATUS            STRING        DEFAULT 'PENDING',  -- PENDING | RUNNING | RECONCILED | REPLICATED | ACKED | FAILED
    RUN_ID            STRING,
    QUERY_ID          STRING,
    SNAPSHOT_TS       TIMESTAMP_NTZ,
    ROWS_UNLOADED     NUMBER(38,0),
    SOURCE_ROWS       NUMBER(38,0),
    FILE_COUNT        NUMBER,
    BYTES_UNLOADED    NUMBER(38,0),
    UNMATCHED_TOKENS  NUMBER(38,0),
    ATTEMPTS          NUMBER        DEFAULT 0,
    LAST_ERROR        STRING,
    UPDATED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_EXTRACT_WORK_ITEM PRIMARY KEY (WORK_ITEM_ID)
);

-- Daily progress against the throughput target.
CREATE OR REPLACE VIEW EXTRACT_PROGRESS AS
SELECT
    LANE,
    ZONE,
    STATUS,
    COUNT(*)                          AS WORK_ITEMS,
    SUM(EST_BYTES) / POWER(1024, 4)   AS EST_TB,
    SUM(BYTES_UNLOADED) / POWER(1024, 4) AS UNLOADED_TB
FROM EXTRACT_WORK_ITEM
GROUP BY LANE, ZONE, STATUS;
