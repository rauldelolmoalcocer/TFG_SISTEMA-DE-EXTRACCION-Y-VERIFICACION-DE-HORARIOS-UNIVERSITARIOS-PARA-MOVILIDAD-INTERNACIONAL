"""CLI for stage 2: `pdf-extract-normalize [--output-dir DIR]`.

Reads the output_dir that `pdf-extract-diagnose` already wrote to. Thin
wrapper around `pdf_table_extractor.pipeline.run_normalization`.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from ..pipeline import run_normalization


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="Folder written by pdf-extract-diagnose, and where schedule.json/all_schedules.json are written (default: ./output)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    args = _parse_args(argv if argv is not None else sys.argv[1:])

    try:
        entries, skipped = run_normalization(args.output_dir)
    except FileNotFoundError as exc:
        logging.getLogger("pdf_table_extractor.cli.normalize").error(str(exc))
        return 1

    logging.getLogger("pdf_table_extractor.cli.normalize").info(
        "%d schedule entries, %d skipped tables/pages", len(entries), len(skipped)
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
