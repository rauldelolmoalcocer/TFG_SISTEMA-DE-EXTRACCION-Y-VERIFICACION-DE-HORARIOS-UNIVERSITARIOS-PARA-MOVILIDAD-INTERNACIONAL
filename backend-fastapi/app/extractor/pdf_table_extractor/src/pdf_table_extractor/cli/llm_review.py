"""CLI for the optional stage 3: `pdf-extract-llm-review --output-dir DIR`.

Reads output_dir/all_schedules.json (written by `pdf-extract-normalize`),
sends every flagged entry to a local Ollama model in batches, and writes
output_dir/all_schedules_reviewed.json -- the original file is never
overwritten, so re-running stage 2 always gives a clean slate to re-review.

Requires the `llm` extra (`pip install "pdf-table-extractor[llm]"`) and a
running local Ollama server with the target model pulled.
"""
from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import sys
from pathlib import Path

from ..models.schedule_models import ScheduleEntry


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="Folder written by pdf-extract-normalize (default: ./output)",
    )
    parser.add_argument("--model", default=None, help="Ollama model tag (default: qwen2.5:3b)")
    parser.add_argument("--host", default=None, help="Ollama server URL (default: http://localhost:11434)")
    parser.add_argument("--batch-size", type=int, default=None, help="Cells per call (default: 6)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    logger = logging.getLogger("pdf_table_extractor.cli.llm_review")
    args = _parse_args(argv if argv is not None else sys.argv[1:])

    try:
        import ollama  # noqa: F401  -- presence check; review_entries imports it itself
    except ImportError:
        logger.error(
            'The "llm" extra is not installed. Run: pip install "pdf-table-extractor[llm]"'
        )
        return 1

    from ..llm_review import DEFAULT_BATCH_SIZE, DEFAULT_HOST, DEFAULT_MODEL, review_entries

    input_path = args.output_dir / "all_schedules.json"
    if not input_path.exists():
        logger.error("%s not found -- run pdf-extract-normalize first", input_path)
        return 1

    raw_entries = json.loads(input_path.read_text(encoding="utf-8"))
    entries = [ScheduleEntry(**e) for e in raw_entries]

    reviewed = review_entries(
        entries,
        model=args.model or DEFAULT_MODEL,
        host=args.host or DEFAULT_HOST,
        batch_size=args.batch_size or DEFAULT_BATCH_SIZE,
    )

    output_path = args.output_dir / "all_schedules_reviewed.json"
    output_path.write_text(
        json.dumps([dataclasses.asdict(e) for e in reviewed], indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.info("%d entries -> %d after review -> %s", len(entries), len(reviewed), output_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
