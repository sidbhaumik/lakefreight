"""Snowflake connection and control-table access."""

from __future__ import annotations

import os

from .config import Settings
from .models import ColumnSpec, TableSpec


def connect(role: str, warehouse: str):
    """Key-pair connection from environment variables.

    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY_FILE.
    In Lambda, fetch the key from Secrets Manager to /tmp first (not wired here).
    """
    import snowflake.connector  # imported here so the pure modules need no connector

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_FILE"],
        role=role,
        warehouse=warehouse,
    )


def get_work_item(cur, settings: Settings, work_item_id: str) -> dict:
    cur.execute(
        f"SELECT * FROM {settings.control_schema}.EXTRACT_WORK_ITEM WHERE WORK_ITEM_ID = %s",
        (work_item_id,),
    )
    row = cur.fetchone()
    if row is None:
        raise KeyError(f"work item not found: {work_item_id}")
    return dict(zip([d[0] for d in cur.description], row))


def update_work_item(cur, settings: Settings, work_item_id: str, **fields) -> None:
    """Set columns on a work item. Keys are column names."""
    sets = ", ".join(f"{k} = %s" for k in fields)
    cur.execute(
        f"UPDATE {settings.control_schema}.EXTRACT_WORK_ITEM "
        f"SET {sets}, UPDATED_AT = CURRENT_TIMESTAMP() WHERE WORK_ITEM_ID = %s",
        (*fields.values(), work_item_id),
    )


def load_table_spec(cur, settings: Settings, database: str, schema: str, table: str) -> TableSpec:
    key = (database, schema, table)
    cur.execute(
        f"SELECT ZONE, SOURCE_DATABASE FROM {settings.control_schema}.EXTRACT_CONFIG "
        "WHERE DATABASE_NAME = %s AND SCHEMA_NAME = %s AND TABLE_NAME = %s",
        key,
    )
    row = cur.fetchone()
    if row is None:
        raise KeyError(f"no EXTRACT_CONFIG row for {'.'.join(key)}")
    zone, source_database = row
    cur.execute(
        "SELECT COLUMN_NAME, DATA_TYPE, USE_XREF, DATA_LENGTH, DESCRIPTION, PII_FLAG, RESTRICTED_FLAG "
        f"FROM {settings.control_schema}.EXTRACT_COLUMN_CONFIG "
        "WHERE DATABASE_NAME = %s AND SCHEMA_NAME = %s AND TABLE_NAME = %s ORDER BY ORDINAL",
        key,
    )
    columns = tuple(
        ColumnSpec(
            name=r[0], data_type=r[1], use_xref=bool(r[2]), length=r[3],
            description=r[4], pii_flag=r[5], restricted_flag=r[6],
        )
        for r in cur.fetchall()
    )
    return TableSpec(database, schema, table, zone, columns, source_database)
