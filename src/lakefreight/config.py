"""Load settings into typed values.

Settings are read from the first of these that exists:

1. the path passed in (the --config option)
2. the file named by the LAKEFREIGHT_CONFIG environment variable
3. the TOML text in the LAKEFREIGHT_CONFIG_TOML environment variable (used by the
   deployed Lambda functions)
4. lakefreight.toml in the current directory
5. the defaults shipped with the package (write a copy with `lakefreight config --write`)
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path

LOCAL_NAME = "lakefreight.toml"


def default_config_text() -> str:
    """The packaged defaults, as TOML text."""
    return files("lakefreight").joinpath("defaults.toml").read_text(encoding="utf-8")


@dataclass(frozen=True)
class Zone:
    stage: str
    bucket: str
    prefix: str
    role: str
    kms_key_id: str | None = None  # key for files lakefreight writes itself (manifests)


@dataclass(frozen=True)
class Lane:
    warehouse: str
    max_concurrency: int


@dataclass(frozen=True)
class Settings:
    control_schema: str
    full_threshold_bytes: int
    target_chunk_bytes: int
    max_file_size_bytes: int
    unmatched_token_threshold: int
    xref_table: str
    xref_token_column: str
    xref_clear_column: str
    zones: dict[str, Zone]
    lanes: dict[str, Lane]
    # Named Parquet file format used to read unloaded files back for reconciliation.
    parquet_file_format: str = ""
    # Accept timestamps losing digits finer than a millisecond (Snowflake's Parquet limit).
    allow_timestamp_precision_loss: bool = False


def config_source(path: str | Path | None = None) -> Path | None:
    """The settings file that will be used, or None for the packaged defaults."""
    if path:
        return Path(path)
    if os.environ.get("LAKEFREIGHT_CONFIG"):
        return Path(os.environ["LAKEFREIGHT_CONFIG"])
    if Path(LOCAL_NAME).is_file():
        return Path(LOCAL_NAME)
    return None


def load_raw(path: str | Path | None = None) -> dict:
    """The settings as parsed TOML, from the first source that exists."""
    if (
        not path
        and not os.environ.get("LAKEFREIGHT_CONFIG")
        and os.environ.get("LAKEFREIGHT_CONFIG_TOML")
    ):
        return tomllib.loads(os.environ["LAKEFREIGHT_CONFIG_TOML"])
    source = config_source(path)
    if source is None:
        return tomllib.loads(default_config_text())
    with open(source, "rb") as f:
        return tomllib.load(f)


def load_settings(path: str | Path | None = None) -> Settings:
    raw = load_raw(path)
    ex, xref = raw["extract"], raw["xref"]
    return Settings(
        control_schema=ex["control_schema"],
        full_threshold_bytes=int(ex["full_threshold_bytes"]),
        target_chunk_bytes=int(ex["target_chunk_bytes"]),
        max_file_size_bytes=int(ex["max_file_size_bytes"]),
        unmatched_token_threshold=int(ex["unmatched_token_threshold"]),
        xref_table=xref["table"],
        xref_token_column=xref["token_column"],
        xref_clear_column=xref["clear_column"],
        zones={k: Zone(**v) for k, v in raw["zones"].items()},
        lanes={k: Lane(**v) for k, v in raw["lanes"].items()},
        parquet_file_format=ex.get(
            "parquet_file_format", f"{ex['control_schema']}.LAKEFREIGHT_PARQUET"
        ),
        allow_timestamp_precision_loss=bool(ex.get("allow_timestamp_precision_loss", False)),
    )
