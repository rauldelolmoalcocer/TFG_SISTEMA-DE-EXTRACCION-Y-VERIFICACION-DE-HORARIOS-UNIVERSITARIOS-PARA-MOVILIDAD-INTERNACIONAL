"""High-level entry points meant to be called from other code -- a larger
project embedding this package doesn't need to know about `extractor`,
`diagnostics`, or `normalizer` internals, just these two functions (plus
whatever it wants from `pdf_table_extractor.models` for typing).

`run_diagnostics` is stage 1 (structural diagnosis of raw PDFs) and
`run_normalization` is stage 2 (turning stage 1's output into schedule
records); the second only reads what the first already wrote to disk, so
they can run in separate processes/times as long as `output_dir` is shared.
"""
from __future__ import annotations

import logging
from pathlib import Path

from .diagnostics.analyzer import PdfAnalysis, analyze_pdf
from .diagnostics.report import build_summary, write_pdf_outputs
from .models.schedule_models import ScheduleEntry
from .normalizer.schedule_builder import build_entries_for_pdf
from .utils.json_utils import write_json

logger = logging.getLogger(__name__)


def run_diagnostics(input_dir: Path, output_dir: Path) -> dict:
    """Run the pdfplumber diagnostic stage against every PDF in `input_dir`.

    Writes output_dir/<pdf_stem>/{metadata,words,tables_*,diagnostic}.json
    and output_dir/summary.json, and returns that same summary dict.
    """
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)

    if not input_dir.exists():
        raise FileNotFoundError(f"Input folder not found: {input_dir}")

    pdf_paths = sorted(input_dir.glob("*.pdf"))
    analyses: list[PdfAnalysis] = []
    for pdf_path in pdf_paths:
        try:
            analysis = analyze_pdf(pdf_path)
        except Exception:
            logger.exception("Failed to analyze %s", pdf_path.name)
            continue
        write_pdf_outputs(analysis, output_dir)
        analyses.append(analysis)

    summary = build_summary(analyses)
    write_json(output_dir / "summary.json", summary)
    logger.info("Diagnosed %d/%d PDFs -> %s", len(analyses), len(pdf_paths), output_dir / "summary.json")
    return summary


def run_normalization(output_dir: Path) -> tuple[list[ScheduleEntry], list[dict]]:
    """Turn the diagnostic output/ that `run_diagnostics` already wrote into
    normalized schedule entries.

    Writes output_dir/<pdf_stem>/{schedule,skipped_tables}.json and the
    combined output_dir/{all_schedules,skipped_tables}.json, and returns
    (all_entries, all_skipped).
    """
    output_dir = Path(output_dir)
    if not output_dir.exists():
        raise FileNotFoundError(f"Output folder not found: {output_dir}")

    all_entries: list[ScheduleEntry] = []
    all_skipped: list[dict] = []

    for pdf_dir in sorted(p for p in output_dir.iterdir() if p.is_dir()):
        if not (pdf_dir / "diagnostic.json").exists():
            continue
        try:
            entries, skipped = build_entries_for_pdf(pdf_dir)
        except Exception:
            logger.exception("Failed to build schedule entries for %s", pdf_dir.name)
            continue

        write_json(pdf_dir / "schedule.json", entries)
        write_json(pdf_dir / "skipped_tables.json", skipped)
        all_entries.extend(entries)
        all_skipped.extend(skipped)

    write_json(output_dir / "all_schedules.json", all_entries)
    write_json(output_dir / "skipped_tables.json", all_skipped)
    logger.info("Normalized %d entries (%d skipped tables) -> %s", len(all_entries), len(all_skipped), output_dir)
    return all_entries, all_skipped
