"""Builds normalized, per-class-block schedule records from the diagnostic
output/ that `main.py` already produced -- one record per (day, time-range,
subject) "cuadrante", ready to load into a database.

Only pages where the diagnostic stage confidently picked a strategy
(`diagnostic.json`'s `best_strategy`) are used; a page with no usable table
is skipped, never guessed. Nothing here is tied to one degree or PDF: day
names, time labels, and cell text are read verbatim from whatever the
winning strategy's table contains -- the only assumption is the schedule
grid's own generic shape (row 0 = day headers, column 0 = time labels),
which is the same shape the source spec used to describe the target
format itself.
"""
from __future__ import annotations

import json
import logging
import unicodedata
from pathlib import Path
from typing import Optional

from ..models.schedule_models import ScheduleEntry
from .cell_parser import parse_cell
from .header_parser import extract_header_info
from .time_parser import parse_time_range

logger = logging.getLogger(__name__)


def _load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _find_table(tables: list[dict], page: int, table_index: int) -> Optional[dict]:
    return next((t for t in tables if t["page"] == page and t["table_index"] == table_index), None)


# A table only gets treated as a time-based schedule grid once at least
# this fraction of its *labeled* rows (column 0 non-empty) parse as a time
# range. This is what stops a page that isn't really a schedule (e.g. a
# paragraph of prose that the "text" strategy mis-read as a wide, jagged
# table) from producing fabricated day/subject entries out of sentence
# fragments -- generic across any PDF, since it only looks at whether
# column 0 reads like times, never at what the text says. Rows are allowed
# to have an *empty* column 0 (a finer ruling grid can split one logical
# row into several physical ones, see `_time_blocks`) -- only a non-empty
# label that fails to parse counts against the table.
MIN_TIME_COLUMN_RATIO = 0.6

# The calendar's own day names, a closed and universal vocabulary (not
# tied to any degree, institution, or PDF) -- used only to check that row
# 0 is actually a day-name header before trusting it as one. Both the
# "text" strategy (which can pull a title line above the real grid into
# the table as an extra row) and even "lines" (when a table's real header
# sits outside the detected bbox) can otherwise hand back a row 0 that is
# actually subject content, silently mislabeling every entry's `day`.
_DAY_ROOTS = (
    "LUNES", "MARTES", "MIERCOLES", "JUEVES", "VIERNES", "SABADO", "DOMINGO",
    "MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY",
)
MIN_DAY_HEADER_RATIO = 0.5


def _strip_accents(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c))


def _looks_like_day_header(header_row: list[Optional[str]]) -> bool:
    day_cells = [c for c in header_row[1:] if c]
    if not day_cells:
        return False
    matches = sum(1 for c in day_cells if any(root in _strip_accents(c).upper() for root in _DAY_ROOTS))
    return matches / len(day_cells) >= MIN_DAY_HEADER_RATIO


def _looks_like_schedule_grid(rows: list[list[Optional[str]]]) -> bool:
    """Assumes `rows[0]` already passed `_looks_like_day_header`."""
    labels = [row[0] for row in rows[1:] if row and row[0]]
    if not labels:
        return False
    parseable = sum(1 for label in labels if parse_time_range(label))
    return parseable / len(labels) >= MIN_TIME_COLUMN_RATIO


def _time_blocks(rows: list[list[Optional[str]]]) -> list[tuple[int, list[int]]]:
    """Partition data rows (rows[1:]) into blocks anchored to each row whose
    column 0 holds a time label, absorbing any immediately-following rows
    with an empty column 0 into that same block.

    A ruling-line grid finer than the semantic one (e.g. a decorative
    underline between a subject's name and its room code) shows up exactly
    like this: one row carries the time label and the next one or two
    don't, even though they're still part of the same schedule slot. Rows
    before the first time label are dropped -- there is nothing to anchor
    them to.
    """
    blocks: list[tuple[int, list[int]]] = []
    current: Optional[list[int]] = None
    for idx in range(1, len(rows)):
        label = rows[idx][0] if rows[idx] else None
        if label:
            current = [idx]
            blocks.append((idx, current))
        elif current is not None:
            current.append(idx)
    return blocks


def _build_entries_for_table(
    table: dict,
    header_info: dict,
    source_file: str,
    page: int,
    strategy: str,
    inherited_header: bool = False,
) -> tuple[list[ScheduleEntry], Optional[dict]]:
    """Returns (entries, skip_info). skip_info is None unless the whole table
    was rejected, in which case it names which check failed and why.

    `inherited_header` is True when `rows[0]` isn't this table's own data --
    it was borrowed from a previous page by the caller (see
    `_try_inherit_header` in `build_entries_for_pdf`) because this table's
    real rows had no day-header row of their own. Every entry built this way
    carries a warning, so a human reviewer can double-check the borrowed
    header still applies before trusting it blindly.
    """
    rows: list[list[Optional[str]]] = table["rows"]
    if len(rows) < 2:
        return [], {
            "source_file": source_file,
            "page": page,
            "table_index": table["table_index"],
            "strategy": strategy,
            "reason": "too_few_rows",
            "detail": f"Table has only {len(rows)} row(s), nothing to build entries from.",
            "rows": rows,
        }
    if not _looks_like_day_header(rows[0]):
        detail = (
            "Row 0 doesn't look like a day-name header -- it's probably a title line or "
            f"subject content pdfplumber pulled into row 0: {rows[0]!r}"
        )
        logger.info("%s page %d table %d (%s): %s", source_file, page, table["table_index"], strategy, detail)
        return [], {
            "source_file": source_file,
            "page": page,
            "table_index": table["table_index"],
            "strategy": strategy,
            "reason": "no_day_header",
            "detail": detail,
            "rows": rows,
        }
    if not _looks_like_schedule_grid(rows):
        detail = "Column 0 doesn't look like a time axis -- this table is probably not a schedule grid."
        logger.info("%s page %d table %d (%s): %s", source_file, page, table["table_index"], strategy, detail)
        return [], {
            "source_file": source_file,
            "page": page,
            "table_index": table["table_index"],
            "strategy": strategy,
            "reason": "no_time_column",
            "detail": detail,
            "rows": rows,
        }

    header_row = rows[0]
    blocks = _time_blocks(rows)
    entries: list[ScheduleEntry] = []

    for col_idx in range(1, len(header_row)):
        day_label = header_row[col_idx]

        # One merged text per time block: join whatever non-empty cells
        # fell inside it (normally one, but a finer ruling grid can spread
        # a single logical entry, e.g. subject name then room code, across
        # a block's extra rows).
        block_texts: list[tuple[int, Optional[str]]] = []
        for anchor_idx, row_indices in blocks:
            parts = [
                rows[r][col_idx]
                for r in row_indices
                if col_idx < len(rows[r]) and rows[r][col_idx]
            ]
            block_texts.append((anchor_idx, "\n".join(parts) if parts else None))

        # Collapse consecutive blocks that repeat the same merged text --
        # pdfplumber's signature for a class spanning several time slots
        # (see repeated_cell_value in table_detector.py).
        i = 0
        while i < len(block_texts):
            anchor_i, text_i = block_texts[i]
            if text_i is None:
                i += 1
                continue
            j = i
            while j + 1 < len(block_texts) and block_texts[j + 1][1] == text_i:
                j += 1
            anchor_j = block_texts[j][0]

            time_start_raw = rows[anchor_i][0]
            time_end_raw = rows[anchor_j][0]
            start_range = parse_time_range(time_start_raw)
            end_range = parse_time_range(time_end_raw)

            warnings: list[str] = []
            if start_range is None:
                warnings.append(f"Could not parse a start time from '{time_start_raw}'")
            if end_range is None:
                warnings.append(f"Could not parse an end time from '{time_end_raw}'")

            parsed = parse_cell(text_i)
            if parsed["multiple_entries_suspected"]:
                warnings.append(
                    "Cell contains more than one room-like reference; it may hold two "
                    "overlapping classes that were not split (see cell_parser.py)."
                )
            if inherited_header:
                warnings.append(
                    "This table had no day-header row of its own; the day header and "
                    "degree/group/semester/course_year were inherited from the nearest "
                    "previous page with one, because this looks like a schedule grid that "
                    "the source PDF paginated across two pages. Please double-check."
                )

            entries.append(
                ScheduleEntry(
                    source_file=source_file,
                    page=page,
                    strategy=strategy,
                    table_index=table["table_index"],
                    row_span=[anchor_i, anchor_j],
                    degree=header_info.get("degree"),
                    group=header_info.get("group"),
                    semester=header_info.get("semester"),
                    course_year=header_info.get("course_year"),
                    day=day_label,
                    time_start=start_range[0] if start_range else None,
                    time_end=(end_range[1] if end_range else (start_range[1] if start_range else None)),
                    time_start_raw=time_start_raw,
                    time_end_raw=time_end_raw,
                    subject_raw=text_i,
                    subject_name=parsed["subject_name"],
                    rooms=parsed["rooms"],
                    non_room_annotations=parsed["non_room_annotations"],
                    multiple_entries_suspected=parsed["multiple_entries_suspected"],
                    warnings=warnings,
                )
            )
            i = j + 1

    return entries, None


def _header_info_is_empty(header_info: dict) -> bool:
    return not any(header_info.get(key) for key in ("degree", "group", "semester", "course_year"))


def _try_inherit_header(
    table: dict,
    skip_info: dict,
    header_info_is_empty: bool,
    last_header_row: Optional[list[Optional[str]]],
    last_header_info: Optional[dict],
    source_file: str,
    page: int,
    strategy: str,
) -> tuple[list[ScheduleEntry], Optional[dict]]:
    """Retries a table that failed the day-header check by borrowing the day
    header (and degree/group/semester/course_year) from the nearest earlier
    page that had its own valid one -- but only when every one of these
    structural signals lines up, never based on this table's content:

    - this table failed specifically because it has no day-header row
      (a genuine schedule grid with an unrelated problem, e.g. no usable
      time column, is left alone);
    - this page's own text yielded no degree/group/semester/course_year at
      all, meaning it isn't a fresh title/header page in its own right;
    - a previous page in this same PDF *did* have a valid day header to
      lend; and
    - that header has exactly as many columns as this table, so borrowing
      it can't misalign a day into the wrong column.

    This is how a source PDF paginating one wide schedule grid across two
    physical pages shows up: the continuation page repeats none of the
    header text and has no day-header row of its own, only more data rows.
    Returns (entries, skip_info) with the same contract as
    `_build_entries_for_table`; skip_info is the original one back if the
    conditions above aren't met or the retry doesn't pan out either.
    """
    rows = table["rows"]
    if (
        skip_info["reason"] != "no_day_header"
        or not header_info_is_empty
        or last_header_row is None
        or not rows
        or len(last_header_row) != len(rows[0])
    ):
        return [], skip_info

    inherited_table = dict(table, rows=[last_header_row, *rows])
    entries, retry_skip_info = _build_entries_for_table(
        inherited_table, last_header_info, source_file, page, strategy, inherited_header=True
    )
    if retry_skip_info is not None:
        return [], skip_info

    logger.info(
        "%s page %d table %d (%s): recovered %d entries by inheriting the day header "
        "from a previous page (this table had none of its own).",
        source_file, page, table["table_index"], strategy, len(entries),
    )
    return entries, None


def build_entries_for_pdf(pdf_output_dir: Path) -> tuple[list[ScheduleEntry], list[dict]]:
    """Read one PDF's diagnostic output/ folder and return (entries, skipped_tables).

    `skipped_tables` records every table that was rejected wholesale (bad
    header, bad time column, or a page the diagnostic stage never trusted
    any strategy for) -- so that discarding a table is traceable in a file,
    not only visible in the console log. Every skip record also carries the
    table's raw `rows`, when there were any, so nothing is lost even when
    it can't be recovered automatically -- a human reviewer can still read
    (and manually enter) exactly what pdfplumber saw.
    """
    diagnostic = _load_json(pdf_output_dir / "diagnostic.json")
    metadata = _load_json(pdf_output_dir / "metadata.json")
    source_file = diagnostic["source_file"]
    page_text_by_number = {p["page_number"]: p["text"] for p in metadata["pages"]}

    strategy_tables_cache: dict[str, list[dict]] = {}
    entries: list[ScheduleEntry] = []
    skipped: list[dict] = []

    # Carried across pages so a table with no day-header row of its own can
    # borrow one from the nearest earlier page that had one -- see
    # `_try_inherit_header`. Only ever overwritten by a table that passed on
    # its own merits, never by one that borrowed, so a run of several
    # consecutive continuation pages all correctly borrow from the same
    # original header.
    last_header_row: Optional[list[Optional[str]]] = None
    last_header_info: Optional[dict] = None

    for page_info in diagnostic["pages"]:
        strategy = page_info["best_strategy"]
        page_number = page_info["page_number"]
        if strategy is None:
            skipped.append(
                {
                    "source_file": source_file,
                    "page": page_number,
                    "table_index": None,
                    "strategy": None,
                    "reason": "no_strategy_selected",
                    "detail": "The diagnostic stage found no usable table with any strategy on this page.",
                }
            )
            continue

        if strategy not in strategy_tables_cache:
            strategy_tables_cache[strategy] = _load_json(pdf_output_dir / f"tables_{strategy}.json")
        tables = strategy_tables_cache[strategy]

        header_info = extract_header_info(page_text_by_number.get(page_number))
        header_info_is_empty = _header_info_is_empty(header_info)

        for table_meta in page_info["strategies"][strategy]["tables"]:
            table = _find_table(tables, page_number, table_meta["table_index"])
            if table is None:
                logger.warning(
                    "%s: table %d on page %d referenced in diagnostic.json but missing from tables_%s.json",
                    source_file,
                    table_meta["table_index"],
                    page_number,
                    strategy,
                )
                skipped.append(
                    {
                        "source_file": source_file,
                        "page": page_number,
                        "table_index": table_meta["table_index"],
                        "strategy": strategy,
                        "reason": "table_missing",
                        "detail": "Referenced in diagnostic.json but not found in the tables_<strategy>.json file.",
                    }
                )
                continue

            table_entries, skip_info = _build_entries_for_table(
                table, header_info, source_file, page_number, strategy
            )
            used_own_header = skip_info is None

            if skip_info is not None:
                table_entries, skip_info = _try_inherit_header(
                    table,
                    skip_info,
                    header_info_is_empty,
                    last_header_row,
                    last_header_info,
                    source_file,
                    page_number,
                    strategy,
                )

            entries.extend(table_entries)
            if skip_info is not None:
                skipped.append(skip_info)
            elif used_own_header:
                last_header_row = table["rows"][0]
                last_header_info = header_info

    return entries, skipped
