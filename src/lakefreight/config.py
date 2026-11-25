"""Load settings into typed values.

Settings are read from the first of these that exists:

1. the path passed in (the --config option)
2. the file named by the LAKEFREIGHT_CONFIG environment variable
3. lakefreight.toml in the current directory
4. the defaults shipped with the package (write a copy with `lakefreight config --write`)
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


def config_source(path: str | Path | None = None) -> Path | None:
    """The settings file that will be used, or None for the packaged defaults."""
    if path:
        return Path(path)
    if os.environ.get("LAKEFREIGHT_CONFIG"):
        return Path(os.environ["LAKEFREIGHT_CONFIG"])
    if Path(LOCAL_NAME).is_file():
        return Path(LOCAL_NAME)
    return None


def load_settings(path: str | Path | None = None) -> Settings:
    source = config_source(path)
    if source is None:
        raw = tomllib.loads(default_config_text())
    else:
        with open(source, "rb") as f:
            raw = tomllib.load(f)
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
    )
