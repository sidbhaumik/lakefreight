"""Load config/defaults.toml into typed settings."""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

DEFAULT_PATH = Path(__file__).resolve().parents[2] / "config" / "defaults.toml"


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


def load_settings(path: str | Path | None = None) -> Settings:
    path = Path(path or os.environ.get("LAKEFREIGHT_CONFIG") or DEFAULT_PATH)
    with open(path, "rb") as f:
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
