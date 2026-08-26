"""Coordinates PDF reading, word extraction, and multi-strategy table
detection into a single diagnostic result per PDF.

No document-specific logic lives here: every PDF is run through every
strategy in `extractor.strategies.TABLE_STRATEGIES`, and a "best candidate"
strategy per page is picked using a generic structural-quality score (see
`_strategy_quality`) -- never a rule tied to a specific file or its
subject matter (day names, room codes, etc. are never referenced).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from ..extractor.pdf_reader import read_pdf_metadata
from ..extractor.strategies import TABLE_STRATEGIES
from ..extractor.table_detector import extract_lines_and_rects, run_strategy
from ..extractor.word_extractor import extract_words
from ..models.extraction_models import IntermediateTable, LineInfo, PdfMetadata, RectInfo, WordInfo

logger = logging.getLogger(__name__)


@dataclass
class PageAnalysis:
    page_number: int
    word_count: int
    line_count: int
    rect_count: int
    tables_by_strategy: dict[str, list[IntermediateTable]] = field(default_factory=dict)
    best_strategy: Optional[str] = None
    warnings: list[str] = field(default_factory=list)


@dataclass
class PdfAnalysis:
    source_file: str
    metadata: PdfMetadata
    words: list[WordInfo]
    lines: list[LineInfo]
    rects: list[RectInfo]
    pages: list[PageAnalysis]
    tables_by_strategy: dict[str, list[IntermediateTable]] = field(default_factory=dict)


# A page is only checked for "did the table capture the page's own text"
# once it has at least this many characters of extracted word text --
# below that, near-empty pages (a single title, a page number) would
# trigger spurious low-coverage warnings for no reason.
MIN_PAGE_CHARS_FOR_COVERAGE_CHECK = 30

# Below this fraction of the page's word-characters ending up inside the
# winning strategy's table(s), the table is flagged as probably not
# representing the page's actual content. This is a separate diagnostic
# from `_strategy_quality` (which compares strategies against each other,
# not against the page's full text -- see that function's docstring for why).
LOW_COVERAGE_THRESHOLD = 0.35


def _table_stats(table: IntermediateTable) -> dict[str, int]:
    """Non-null cell count, total characters and total word-tokens in one table."""
    non_null = chars = tokens = 0
    for row in table.rows:
        for cell in row:
            if cell:
                non_null += 1
                chars += len(cell)
                tokens += len(cell.split())
    jagged = sum(1 for w in table.warnings if w.code == "jagged_row")
    return {
        "non_null": non_null,
        "chars": chars,
        "tokens": tokens,
        "warnings": len(table.warnings),
        "jagged": jagged,
        "rows": table.n_rows,
    }


def _aggregate_stats(tables: list[IntermediateTable]) -> dict[str, int]:
    totals = {"non_null": 0, "chars": 0, "tokens": 0, "warnings": 0, "jagged": 0, "rows": 0}
    for t in tables:
        for key, value in _table_stats(t).items():
            totals[key] += value
    return totals


def _coverage(chars: int, page_char_total: int) -> float:
    """Fraction of the page's own word-characters that ended up inside the table."""
    if page_char_total <= 0:
        return 1.0
    return min(1.0, chars / page_char_total)


def _strategy_quality(tables: list[IntermediateTable]) -> float:
    """Generic, content-agnostic quality score for one strategy's tables on a page.

    This is only used to pick which strategy's output to *suggest first*
    for manual inspection -- all strategies' full results are still kept
    and written to disk regardless of this score.

    The deciding signal is **average characters per non-empty cell**: a
    genuine grid cell holds a short phrase (a subject name, a room code),
    while a mis-detected "one column per word" table averages a handful of
    characters per cell. This held up better, across many differently
    formatted PDFs, than two alternatives that were tried and discarded:

    - Rewarding raw non-empty-cell *count* rewards over-segmentation --
      splitting one real cell into ten fragments always looks "richer".
    - Comparing captured text against the *whole page's* word count
      unfairly punishes a correctly-scoped table for not also containing
      text that legitimately sits outside it (a title line above the
      grid, a note below it).

    `jagged_row` warnings (rows whose cell count doesn't match the
    header) reduce the score further -- they mark a genuinely inconsistent
    grid. Other warning types (multiline cells, repeated merged-cell
    content) are *not* penalized here: they mark structure the strategy
    correctly recognised, not a defect.
    """
    if not tables:
        return -1.0
    stats = _aggregate_stats(tables)
    if stats["non_null"] == 0:
        return -1.0

    avg_chars_per_cell = stats["chars"] / stats["non_null"]
    regularity = 1.0 - min(1.0, stats["jagged"] / max(1, stats["rows"]))

    return avg_chars_per_cell * regularity


def analyze_pdf(pdf_path: Path) -> PdfAnalysis:
    """Run the full diagnostic pipeline against a single PDF file."""
    logger.info("Analyzing %s", pdf_path.name)
    metadata, pdf = read_pdf_metadata(pdf_path)

    all_words: list[WordInfo] = []
    all_lines: list[LineInfo] = []
    all_rects: list[RectInfo] = []
    pages: list[PageAnalysis] = []
    tables_by_strategy: dict[str, list[IntermediateTable]] = {name: [] for name in TABLE_STRATEGIES}

    try:
        for i, page in enumerate(pdf.pages, start=1):
            words = extract_words(page, i)
            lines, rects = extract_lines_and_rects(page, i)
            all_words.extend(words)
            all_lines.extend(lines)
            all_rects.extend(rects)

            page_char_total = sum(len(w.text) for w in words)

            page_tables: dict[str, list[IntermediateTable]] = {}
            for strategy_name, settings in TABLE_STRATEGIES.items():
                tables = run_strategy(page, i, strategy_name, settings, pdf_path.name)
                page_tables[strategy_name] = tables
                tables_by_strategy[strategy_name].extend(tables)

            best_strategy: Optional[str] = max(
                page_tables,
                key=lambda name: _strategy_quality(page_tables[name]),
                default=None,
            )
            if best_strategy is not None and _strategy_quality(page_tables[best_strategy]) < 0:
                best_strategy = None  # no strategy found any usable table on this page

            page_warnings = [
                f"[{strategy_name}] {w.message}"
                for strategy_name, tables in page_tables.items()
                for t in tables
                for w in t.warnings
            ]

            if best_strategy is not None:
                coverage = _coverage(_aggregate_stats(page_tables[best_strategy])["chars"], page_char_total)
                if page_char_total >= MIN_PAGE_CHARS_FOR_COVERAGE_CHECK and coverage < LOW_COVERAGE_THRESHOLD:
                    page_warnings.append(
                        f"[{best_strategy}] Low text coverage ({coverage:.0%}) -- the detected table(s) "
                        "capture only part of this page's text; this page may not actually be a schedule "
                        "grid, or table detection may have failed here. Check the plain page text "
                        "(metadata.json) instead of trusting this table alone."
                    )

            pages.append(
                PageAnalysis(
                    page_number=i,
                    word_count=len(words),
                    line_count=len(lines),
                    rect_count=len(rects),
                    tables_by_strategy=page_tables,
                    best_strategy=best_strategy,
                    warnings=page_warnings,
                )
            )
    finally:
        pdf.close()

    return PdfAnalysis(
        source_file=pdf_path.name,
        metadata=metadata,
        words=all_words,
        lines=all_lines,
        rects=all_rects,
        pages=pages,
        tables_by_strategy=tables_by_strategy,
    )
