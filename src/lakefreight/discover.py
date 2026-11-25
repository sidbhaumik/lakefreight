"""Build the metadata CSV that `lakefreight plan` reads, straight from Snowflake.

    lakefreight discover --database SALES --database HR --out metadata.csv
    lakefreight discover --database SALES --sql-only > discover.sql

Sizes, columns and types come from Snowflake's metadata views. Personal-data
flags come from Snowflake's sensitive data classification and from any tags or
column-name patterns set in the [discover] settings. Restricted flags come only
from your own tags or patterns.

Without --sql-only this connects with the Snowflake Python connector
(`pip install "lakefreight[snowflake]"`), using a connection from
~/.snowflake/connections.toml.
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from .config import load_raw
from .models import quote_ident

OUTPUT_COLUMNS = [
    "table_database",
    "table_schema",
    "table_name",
    "table_size_bytes",
    "table_pii_flag",
    "table_restricted_flag",
    "column_name",
    "column_ordinal",
    "column_datatype",
    "column_length",
    "column_pii_flag",
    "column_restricted_flag",
    "column_description",
]

SOURCES = ("account_usage", "information_schema")
CLASSIFICATION_TAG = "SNOWFLAKE.CORE.PRIVACY_CATEGORY"
_TAG = re.compile(
    r"^([A-Za-z_][A-Za-z0-9_$]*)\.([A-Za-z_][A-Za-z0-9_$]*)\.([A-Za-z_][A-Za-z0-9_$]*)$"
)


class DiscoverError(ValueError):
    """Settings or arguments that cannot produce a safe query."""


@dataclass(frozen=True)
class TagRule:
    database: str
    schema: str
    name: str
    value: str | None = None

    @classmethod
    def parse(cls, spec: str) -> TagRule:
        name, _, value = spec.partition("=")
        match = _TAG.match(name.strip())
        if not match:
            raise DiscoverError(f"tag must look like DATABASE.SCHEMA.TAG[=VALUE]: {spec!r}")
        db, schema, tag = (part.upper() for part in match.groups())
        return cls(db, schema, tag, value.strip() if value else None)

    def condition(self) -> str:
        parts = [
            f"TAG_DATABASE = {literal(self.database)}",
            f"TAG_SCHEMA = {literal(self.schema)}",
            f"TAG_NAME = {literal(self.name)}",
        ]
        if self.value is not None:
            parts.append(f"TAG_VALUE = {literal(self.value)}")
        return "(" + " AND ".join(parts) + ")"


@dataclass(frozen=True)
class DiscoverSettings:
    source: str = "account_usage"
    exclude_schemas: tuple[str, ...] = ("INFORMATION_SCHEMA",)
    use_snowflake_classification: bool = True
    pii_tags: tuple[TagRule, ...] = ()
    pii_column_patterns: tuple[str, ...] = ()
    restricted_tags: tuple[TagRule, ...] = ()
    restricted_column_patterns: tuple[str, ...] = ()
    treat_unclassified_as_non_pii: bool = False

    @classmethod
    def from_raw(cls, raw: dict) -> DiscoverSettings:
        d = raw.get("discover", {})
        source = d.get("source", "account_usage")
        if source not in SOURCES:
            raise DiscoverError(f"discover.source must be one of {SOURCES}, not {source!r}")
        for pattern in [
            *d.get("pii_column_patterns", []),
            *d.get("restricted_column_patterns", []),
        ]:
            try:
                re.compile(pattern)
            except re.error as exc:
                raise DiscoverError(f"bad column pattern {pattern!r}: {exc}") from exc
        return cls(
            source=source,
            exclude_schemas=tuple(
                s.upper() for s in d.get("exclude_schemas", ["INFORMATION_SCHEMA"])
            ),
            use_snowflake_classification=bool(d.get("use_snowflake_classification", True)),
            pii_tags=tuple(TagRule.parse(t) for t in d.get("pii_tags", [])),
            pii_column_patterns=tuple(d.get("pii_column_patterns", [])),
            restricted_tags=tuple(TagRule.parse(t) for t in d.get("restricted_tags", [])),
            restricted_column_patterns=tuple(d.get("restricted_column_patterns", [])),
            treat_unclassified_as_non_pii=bool(d.get("treat_unclassified_as_non_pii", False)),
        )

    @property
    def all_pii_tags(self) -> tuple[TagRule, ...]:
        if self.use_snowflake_classification:
            return (TagRule.parse(CLASSIFICATION_TAG), *self.pii_tags)
        return self.pii_tags

    @property
    def uses_tags(self) -> bool:
        return bool(self.all_pii_tags or self.restricted_tags)


def literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def _any(conditions: Iterable[str]) -> str:
    conditions = list(conditions)
    return " OR ".join(conditions) if conditions else "FALSE"


def _patterns(patterns: Iterable[str], column: str) -> list[str]:
    return [f"REGEXP_LIKE({column}, {literal(p)}, 'i')" for p in patterns]


def _columns_cte(source: str, databases: list[str], exclude_schemas: tuple[str, ...]) -> str:
    excluded = ", ".join(literal(s) for s in exclude_schemas) or "''"
    select = (
        "SELECT c.TABLE_CATALOG, c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME, c.ORDINAL_POSITION,\n"
        "         c.DATA_TYPE, c.NUMERIC_PRECISION, c.NUMERIC_SCALE, c.CHARACTER_MAXIMUM_LENGTH,\n"
        "         c.COMMENT, t.BYTES"
    )
    join = (
        "ON t.TABLE_CATALOG = c.TABLE_CATALOG AND t.TABLE_SCHEMA = c.TABLE_SCHEMA "
        "AND t.TABLE_NAME = c.TABLE_NAME"
    )
    if source == "account_usage":
        dbs = ", ".join(literal(d) for d in databases)
        return (
            f"  {select}\n"
            "  FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS c\n"
            f"  JOIN SNOWFLAKE.ACCOUNT_USAGE.TABLES t\n    {join}\n"
            "  WHERE c.DELETED IS NULL AND t.DELETED IS NULL\n"
            "    AND t.TABLE_TYPE = 'BASE TABLE'\n"
            f"    AND c.TABLE_CATALOG IN ({dbs})\n"
            f"    AND c.TABLE_SCHEMA NOT IN ({excluded})"
        )
    parts = []
    for db in databases:
        q = quote_ident(db)
        parts.append(
            f"  {select}\n"
            f"  FROM {q}.INFORMATION_SCHEMA.COLUMNS c\n"
            f"  JOIN {q}.INFORMATION_SCHEMA.TABLES t\n    {join}\n"
            "  WHERE t.TABLE_TYPE = 'BASE TABLE'\n"
            f"    AND c.TABLE_SCHEMA NOT IN ({excluded})"
        )
    return "\n  UNION ALL\n".join(parts)


def build_sql(databases: list[str], settings: DiscoverSettings) -> str:
    """One query whose result is the metadata CSV, one row per column."""
    if not databases:
        raise DiscoverError("name at least one database")
    # Unquoted Snowflake names are stored in upper case; keep mixed-case names as given.
    databases = [d.upper() if d.islower() else d for d in databases]

    pii_tag = _any(r.condition() for r in settings.all_pii_tags)
    restricted_tag = _any(r.condition() for r in settings.restricted_tags)
    pii_pattern = _any(_patterns(settings.pii_column_patterns, "c.COLUMN_NAME"))
    restricted_pattern = _any(_patterns(settings.restricted_column_patterns, "c.COLUMN_NAME"))

    ctes = [f"cols AS (\n{_columns_cte(settings.source, databases, settings.exclude_schemas)}\n)"]
    joins = []
    pii_hits = [pii_pattern]
    restricted_hits = [restricted_pattern]

    if settings.uses_tags:
        dbs = ", ".join(literal(d) for d in databases)
        ctes.append(
            "tags AS (\n"
            "  SELECT OBJECT_DATABASE, OBJECT_SCHEMA, OBJECT_NAME, COLUMN_NAME,\n"
            f"         MAX(IFF({pii_tag}, 1, 0)) AS PII_TAG,\n"
            f"         MAX(IFF({restricted_tag}, 1, 0)) AS RESTRICTED_TAG\n"
            "  FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES\n"
            "  WHERE DOMAIN = 'COLUMN' AND OBJECT_DELETED IS NULL\n"
            f"    AND OBJECT_DATABASE IN ({dbs})\n"
            "  GROUP BY 1, 2, 3, 4\n"
            ")"
        )
        joins.append(
            "LEFT JOIN tags g ON g.OBJECT_DATABASE = c.TABLE_CATALOG "
            "AND g.OBJECT_SCHEMA = c.TABLE_SCHEMA\n"
            "  AND g.OBJECT_NAME = c.TABLE_NAME AND g.COLUMN_NAME = c.COLUMN_NAME"
        )
        pii_hits.append("COALESCE(g.PII_TAG, 0) = 1")
        restricted_hits.append("COALESCE(g.RESTRICTED_TAG, 0) = 1")

    if settings.treat_unclassified_as_non_pii:
        otherwise = "'N'"
    elif settings.use_snowflake_classification:
        ctes.append(
            "classified AS (\n"
            "  SELECT DISTINCT DATABASE_NAME, SCHEMA_NAME, TABLE_NAME\n"
            "  FROM SNOWFLAKE.ACCOUNT_USAGE.DATA_CLASSIFICATION_LATEST\n"
            "  WHERE LAST_CLASSIFIED_ON IS NOT NULL\n"
            ")"
        )
        joins.append(
            "LEFT JOIN classified k ON k.DATABASE_NAME = c.TABLE_CATALOG "
            "AND k.SCHEMA_NAME = c.TABLE_SCHEMA AND k.TABLE_NAME = c.TABLE_NAME"
        )
        otherwise = "IFF(k.TABLE_NAME IS NOT NULL, 'N', NULL)"
    else:
        otherwise = "NULL"  # nothing says the column is safe: unknown, so treated as sensitive

    pii_rule = " OR ".join(h for h in pii_hits if h != "FALSE") or "FALSE"
    restricted_rule = " OR ".join(h for h in restricted_hits if h != "FALSE") or "FALSE"
    ctes.append(
        "flagged AS (\n"
        "  SELECT c.*,\n"
        f"         CASE WHEN {pii_rule} THEN 'Y' ELSE {otherwise} END AS COLUMN_PII_FLAG,\n"
        f"         IFF({restricted_rule}, 'Y', 'N') AS COLUMN_RESTRICTED_FLAG\n"
        "  FROM cols c\n" + "".join(f"  {j}\n" for j in joins) + ")"
    )

    by_table = "PARTITION BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME"
    return (
        "WITH " + ",\n".join(ctes) + "\nSELECT\n"
        "  TABLE_CATALOG AS TABLE_DATABASE,\n"
        "  TABLE_SCHEMA,\n"
        "  TABLE_NAME,\n"
        "  BYTES AS TABLE_SIZE_BYTES,\n"
        f"  CASE WHEN SUM(IFF(COLUMN_PII_FLAG = 'Y', 1, 0)) OVER ({by_table}) > 0 THEN 'Y'\n"
        f"       WHEN SUM(IFF(COLUMN_PII_FLAG IS NULL, 1, 0)) OVER ({by_table}) > 0 THEN NULL\n"
        "       ELSE 'N' END AS TABLE_PII_FLAG,\n"
        f"  IFF(SUM(IFF(COLUMN_RESTRICTED_FLAG = 'Y', 1, 0)) OVER ({by_table}) > 0, 'Y', 'N')"
        " AS TABLE_RESTRICTED_FLAG,\n"
        "  COLUMN_NAME,\n"
        "  ORDINAL_POSITION AS COLUMN_ORDINAL,\n"
        "  IFF(DATA_TYPE = 'NUMBER', 'NUMBER(' || NUMERIC_PRECISION || ',' || NUMERIC_SCALE || ')',"
        " DATA_TYPE) AS COLUMN_DATATYPE,\n"
        "  CHARACTER_MAXIMUM_LENGTH AS COLUMN_LENGTH,\n"
        "  COLUMN_PII_FLAG,\n"
        "  COLUMN_RESTRICTED_FLAG,\n"
        "  COMMENT AS COLUMN_DESCRIPTION\n"
        "FROM flagged\n"
        "ORDER BY TABLE_DATABASE, TABLE_SCHEMA, TABLE_NAME, COLUMN_ORDINAL"
    )


def write_csv(rows: Iterable[Iterable], out: Path) -> int:
    """Write query rows (in OUTPUT_COLUMNS order) as the metadata CSV."""
    count = 0
    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(OUTPUT_COLUMNS)
        for row in rows:
            writer.writerow(["" if v is None else v for v in row])
            count += 1
    return count


def run_query(sql: str, connection_name: str | None) -> list[tuple]:
    try:
        import snowflake.connector
    except ImportError as exc:  # pragma: no cover - depends on extras
        raise DiscoverError(
            'the Snowflake connector is required: pip install "lakefreight[snowflake]", '
            "or use --sql-only and run the query yourself"
        ) from exc
    kwargs = {"connection_name": connection_name} if connection_name else {}
    conn = snowflake.connector.connect(**kwargs)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
    finally:
        conn.close()


def summarize(path: Path) -> list[str]:
    """Counts by zone, for the message after a run."""
    from collections import Counter

    from .models import resolve_zone

    tables: dict[tuple[str, str, str], tuple[str, str]] = {}
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            key = (r["table_database"], r["table_schema"], r["table_name"])
            tables[key] = (r["table_restricted_flag"] or None, r["table_pii_flag"] or None)
    zones = Counter(resolve_zone(restricted, pii) for restricted, pii in tables.values())
    unknown = sum(1 for _, pii in tables.values() if pii is None)
    lines = ["  by zone: " + ", ".join(f"{z} {n}" for z, n in sorted(zones.items()))]
    if unknown:
        lines.append(
            f"  {unknown} tables have unclassified columns and go to the PII zone; "
            "classify them in Snowflake or add rules under [discover]"
        )
    return lines


def main(argv: list[str] | None = None, prog: str | None = None) -> int:
    parser = argparse.ArgumentParser(prog=prog, description=__doc__.splitlines()[0])
    parser.add_argument(
        "--database", action="append", required=True, help="database to scan; repeatable"
    )
    parser.add_argument("--out", default="metadata.csv", help="CSV to write")
    parser.add_argument("--sql-only", action="store_true", help="print the query and stop")
    parser.add_argument("--connection", help="connection name in ~/.snowflake/connections.toml")
    parser.add_argument("--config", help="settings TOML")
    args = parser.parse_args(argv)

    try:
        settings = DiscoverSettings.from_raw(load_raw(args.config))
        sql = build_sql(args.database, settings)
    except DiscoverError as exc:
        print(f"lakefreight discover: {exc}", file=sys.stderr)
        return 2
    if args.sql_only:
        print(sql + ";")
        return 0
    out = Path(args.out)
    try:
        rows = run_query(sql, args.connection)
    except DiscoverError as exc:
        print(f"lakefreight discover: {exc}", file=sys.stderr)
        return 1
    if not rows:
        print("lakefreight discover: no tables found; check the database names", file=sys.stderr)
        return 1
    count = write_csv(rows, out)
    tables = len({tuple(r[:3]) for r in rows})
    print(f"wrote {out}: {count} columns in {tables} tables")
    for line in summarize(out):
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
