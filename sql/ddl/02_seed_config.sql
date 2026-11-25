-- Seed the control tables from the metadata table.
--
-- CHANGE-ME: <METADATA_TABLE> and the column names on the right-hand side are
-- assumptions about your metadata table. Adjust them before running.
-- Assumed grain: one row per column, with the table-level values repeated.

USE SCHEMA MIGRATION.CONTROL;

-- Columns. A column flagged restricted is resolved through the xref.
INSERT INTO EXTRACT_COLUMN_CONFIG
    (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, ORDINAL, DATA_TYPE,
     DATA_LENGTH, DESCRIPTION, PII_FLAG, RESTRICTED_FLAG, USE_XREF)
SELECT
    m.TABLE_DATABASE,
    m.TABLE_SCHEMA,
    m.TABLE_NAME,
    m.COLUMN_NAME,
    ROW_NUMBER() OVER (
        PARTITION BY m.TABLE_DATABASE, m.TABLE_SCHEMA, m.TABLE_NAME
        ORDER BY m.COLUMN_NAME            -- CHANGE-ME: use the ordinal position if the metadata has one
    ),
    UPPER(m.COLUMN_DATATYPE),
    m.COLUMN_LENGTH,
    m.COLUMN_DESCRIPTION,
    m.COLUMN_PII_FLAG,
    m.COLUMN_RESTRICTED_FLAG,
    COALESCE(m.COLUMN_RESTRICTED_FLAG, 'N') = 'Y'
FROM <METADATA_TABLE> m;

-- Tables. Zone rules fail closed: only an explicit N/N is INTERNAL.
-- 107374182400 = full_threshold_bytes in lakefreight.toml (the default).
INSERT INTO EXTRACT_CONFIG
    (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, TABLE_BYTES, ZONE, STRATEGY, LANE)
WITH t AS (
    SELECT
        TABLE_DATABASE, TABLE_SCHEMA, TABLE_NAME,
        MAX(TABLE_SIZE)      AS TABLE_BYTES,     -- CHANGE-ME: convert to bytes if stored in another unit
        MAX(TABLE_PII_FLAG)  AS PII_FLAG,
        MAX(TABLE_RESTRICTED_FLAG)  AS RESTRICTED_FLAG
    FROM <METADATA_TABLE>
    GROUP BY 1, 2, 3
), z AS (
    SELECT
        t.*,
        CASE
            WHEN RESTRICTED_FLAG = 'Y' THEN 'RESTRICTED'
            WHEN RESTRICTED_FLAG = 'N' AND PII_FLAG = 'N' THEN 'INTERNAL'
            ELSE 'PII'
        END AS ZONE,
        IFF(TABLE_BYTES <= 107374182400, 'FULL', 'CHUNKED') AS STRATEGY
    FROM t
)
SELECT
    TABLE_DATABASE, TABLE_SCHEMA, TABLE_NAME, TABLE_BYTES, ZONE, STRATEGY,
    CASE
        WHEN ZONE = 'RESTRICTED' THEN 'RESTRICTED'
        WHEN STRATEGY = 'CHUNKED' THEN 'LARGE'
        ELSE 'SMALL'
    END
FROM z;

-- First guess at a chunk column for CHUNKED tables: a DATE/TIMESTAMP column
-- whose name looks like a load or event date. Review every one of these
-- (check clustering) and set CHUNK_COLUMN_SOURCE = 'REVIEWED'.
UPDATE EXTRACT_CONFIG c
SET CHUNK_COLUMN = g.COLUMN_NAME, CHUNK_COLUMN_SOURCE = 'HEURISTIC'
FROM (
    SELECT DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, COLUMN_NAME
    FROM EXTRACT_COLUMN_CONFIG
    WHERE (DATA_TYPE LIKE 'DATE%' OR DATA_TYPE LIKE 'TIMESTAMP%')
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY DATABASE_NAME, SCHEMA_NAME, TABLE_NAME
        ORDER BY
            CASE
                WHEN COLUMN_NAME ILIKE ANY ('%LOAD%', '%INSERT%', '%CREAT%') THEN 1
                WHEN COLUMN_NAME ILIKE ANY ('%TXN%', '%TRANS%', '%EVENT%', '%POST%') THEN 2
                WHEN COLUMN_NAME ILIKE ANY ('%UPDAT%', '%MODIF%') THEN 4
                ELSE 3
            END,
            COLUMN_NAME
    ) = 1
) g
WHERE c.STRATEGY = 'CHUNKED'
  AND c.DATABASE_NAME = g.DATABASE_NAME
  AND c.SCHEMA_NAME = g.SCHEMA_NAME
  AND c.TABLE_NAME = g.TABLE_NAME;

-- Checks to run after seeding.
-- 1. CHUNKED tables with no chunk column (these fall back to a single pass):
--    SELECT * FROM EXTRACT_CONFIG WHERE STRATEGY = 'CHUNKED' AND CHUNK_COLUMN IS NULL;
-- 2. The xref must be unique on the token (CHANGE-ME names):
--    SELECT TOKEN_VALUE, COUNT(*) FROM SECURE.PUBLIC.TOKEN_XREF GROUP BY 1 HAVING COUNT(*) > 1;
