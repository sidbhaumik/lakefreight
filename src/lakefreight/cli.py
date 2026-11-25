"""lakefreight command line.

lakefreight plan --metadata tables.csv [--profiles months.csv] [--out generated]
lakefreight register --format delta|iceberg --table-root s3://bucket/zone/DB/SCHEMA/TABLE
lakefreight config [--write lakefreight.toml]
"""

from __future__ import annotations

import argparse
import sys
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from . import generate, register
from .config import LOCAL_NAME, config_source, default_config_text


def config_command(argv: list[str], prog: str | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog=prog, description="Show which settings file is used, or write a copy to edit"
    )
    parser.add_argument(
        "--write",
        nargs="?",
        const=LOCAL_NAME,
        metavar="PATH",
        help=f"write the default settings to PATH (default: {LOCAL_NAME})",
    )
    args = parser.parse_args(argv)
    if args.write:
        target = Path(args.write)
        if target.exists():
            print(f"lakefreight config: {target} already exists", file=sys.stderr)
            return 1
        target.write_text(default_config_text(), encoding="utf-8")
        print(f"wrote {target}; edit the CHANGE-ME values")
        return 0
    source = config_source()
    print(f"# settings in use: {source or 'built-in defaults'}")
    print(source.read_text(encoding="utf-8") if source else default_config_text(), end="")
    return 0


COMMANDS = {
    "plan": (generate.main, "Generate extract SQL and a work-item list from a metadata CSV"),
    "register": (register.main, "Register unloaded Parquet files as a Delta or Iceberg table"),
    "config": (config_command, "Show the settings in use, or write a copy to edit"),
}


def _version() -> str:
    try:
        return version("lakefreight")
    except PackageNotFoundError:
        return "unknown"


def _usage() -> str:
    lines = ["usage: lakefreight <command> [options]", "", "commands:"]
    lines += [f"  {name:<10}{desc}" for name, (_, desc) in COMMANDS.items()]
    lines += ["", "Run 'lakefreight <command> --help' for a command's options."]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if not argv or argv[0] in ("-h", "--help"):
        print(_usage())
        return 0
    if argv[0] in ("-V", "--version"):
        print(f"lakefreight {_version()}")
        return 0
    command = COMMANDS.get(argv[0])
    if command is None:
        print(f"lakefreight: unknown command '{argv[0]}'\n\n{_usage()}", file=sys.stderr)
        return 2
    return command[0](argv[1:], prog=f"lakefreight {argv[0]}")


if __name__ == "__main__":
    sys.exit(main())
