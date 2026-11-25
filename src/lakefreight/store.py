"""Snowflake connection and control-table access."""

from __future__ import annotations

import json
import os
import tempfile

from .config import Settings
from .models import ColumnSpec, TableSpec

_KEY_FILE: str | None = None


def credentials() -> dict:
    """Account, user and private-key file for key-pair authentication.

    With SNOWFLAKE_SECRET_ARN set (the deployed Lambda functions), they come from
    a Secrets Manager secret holding JSON: {"account", "user", "private_key"},
    where private_key is the PEM text. The key is written once to a private
    temporary file. Otherwise they come from SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER
    and SNOWFLAKE_PRIVATE_KEY_FILE.
    """
    global _KEY_FILE
    arn = os.environ.get("SNOWFLAKE_SECRET_ARN")
    if not arn:
        return {
            "account": os.environ["SNOWFLAKE_ACCOUNT"],
            "user": os.environ["SNOWFLAKE_USER"],
            "private_key_file": os.environ["SNOWFLAKE_PRIVATE_KEY_FILE"],
        }
    import boto3

    secret = json.loads(
        boto3.client("secretsmanager").get_secret_value(SecretId=arn)["SecretString"]
    )
    missing = {"account", "user", "private_key"} - secret.keys()
    if missing:
        raise KeyError(f"Snowflake secret is missing {sorted(missing)}")
    if _KEY_FILE is None or not os.path.exists(_KEY_FILE):
        fd, path = tempfile.mkstemp(prefix="lakefreight-", suffix=".p8")  # mode 0600
        with os.fdopen(fd, "w") as f:
            f.write(secret["private_key"])
        _KEY_FILE = path
    return {"account": secret["account"], "user": secret["user"], "private_key_file": _KEY_FILE}


def connect(role: str, warehouse: str):
    """Key-pair connection using the given role and warehouse."""
    import snowflake.connector  # imported here so the pure modules need no connector

    return snowflake.connector.connect(**credentials(), role=role, warehouse=warehouse)


def get_work_item(cur, settings: Settings, work_item_id: str) -> dict:
    cur.execute(
        f"SELECT * FROM {settings.control_schema}.EXTRACT_WORK_ITEM WHERE WORK_ITEM_ID = %s",
        (work_item_id,),
    )
    row = cur.fetchone()
    if row is None:
        raise KeyError(f"work item not found: {work_item_id}")
    return dict(zip([d[0] for d in cur.description], row, strict=True))


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
            name=r[0],
            data_type=r[1],
            use_xref=bool(r[2]),
            length=r[3],
            description=r[4],
            pii_flag=r[5],
            restricted_flag=r[6],
        )
        for r in cur.fetchall()
    )
    return TableSpec(database, schema, table, zone, columns, source_database)
