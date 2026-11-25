"""lakefreight command line.

lakefreight plan --metadata tables.csv [--profiles months.csv] [--out generated]
"""

from __future__ import annotations

import sys
from importlib.metadata import PackageNotFoundError, version

from . import generate

COMMANDS = {
    "plan": (generate.main, "Generate extract SQL and a work-item list from a metadata CSV"),
}


def _version() -> str:
    try:
        return version("lakefreight")
    except PackageNotFoundError:
        return "unknown"


def _usage() -> str:
    lines = ["usage: lakefreight <command> [options]", "", "commands:"]
    lines += [f"  {name:<8}{desc}" for name, (_, desc) in COMMANDS.items()]
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
