"""CLI for stage 1: `pdf-extract-diagnose [--input-dir DIR] [--output-dir DIR]`.

Thin wrapper around `pdf_table_extractor.pipeline.run_diagnostics` -- all
the actual logic lives there so it can be called directly from Python too.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from ..pipeline import run_diagnostics


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-dir", type=Path, default=Path("input_pdfs"), help="Folder of PDFs to analyze (default: ./input_pdfs)"
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("output"), help="Folder to write diagnostic JSON to (default: ./output)"
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    args = _parse_args(argv if argv is not None else sys.argv[1:])

    try:
        summary = run_diagnostics(args.input_dir, args.output_dir)
    except FileNotFoundError as exc:
        logging.getLogger("pdf_table_extractor.cli.diagnose").error(str(exc))
        return 1

    for pdf in summary["pdfs"]:
        report_path = args.output_dir / Path(pdf["source_file"]).stem / "diagnostic_report.txt"
        if report_path.exists():
            print(report_path.read_text(encoding="utf-8"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
