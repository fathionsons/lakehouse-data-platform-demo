from __future__ import annotations

import argparse
from collections.abc import Callable

from lakehouse import gold, ingest, mart, silver


def run_all() -> None:
    ingest.run()
    silver.run()
    gold.run()
    mart.run()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m lakehouse",
        description="Mini Lakehouse + Kimball demo CLI.",
    )
    parser.add_argument(
        "command",
        choices=["ingest", "silver", "gold", "mart", "all"],
        help="Pipeline stage to run.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    commands: dict[str, Callable[[], None]] = {
        "ingest": ingest.run,
        "silver": silver.run,
        "gold": gold.run,
        "mart": mart.run,
        "all": run_all,
    }
    commands[args.command]()
    return 0
