"""Runs each table-detection strategy against a page and converts the
results into the generic IntermediateTable representation.

Detection is structure-driven: nothing here hardcodes column positions or
day names. Merged-cell / multiline observations are recorded as warnings
(spec section 6), never silently "fixed" or guessed.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from pdfplumber.page import Page

from ..models.extraction_models import IntermediateTable, LineInfo, RectInfo, TableWarning

logger = logging.getLogger(__name__)


def extract_lines_and_rects(page: Page, page_number: int) -> tuple[list[LineInfo], list[RectInfo]]:
    """Report raw line/rectangle geometry found on the page.

    Kept as diagnostic data only -- used to *count* ruling lines, not to
    derive fixed coordinate rules.
    """
    lines: list[LineInfo] = []
    for l in page.lines:
        x0, x1, top, bottom = float(l["x0"]), float(l["x1"]), float(l["top"]), float(l["bottom"])
        if abs(top - bottom) < 0.5 and abs(x0 - x1) >= 0.5:
            orientation = "horizontal"
        elif abs(x0 - x1) < 0.5 and abs(top - bottom) >= 0.5:
            orientation = "vertical"
        else:
            orientation = "diagonal"
        lines.append(LineInfo(page=page_number, x0=x0, x1=x1, top=top, bottom=bottom, orientation=orientation))

    rects = [
        RectInfo(
            page=page_number,
            x0=float(r["x0"]),
            x1=float(r["x1"]),
            top=float(r["top"]),
            bottom=float(r["bottom"]),
        )
        for r in page.rects
    ]
    return lines, rects


def _detect_warnings(table: Any, rows: list[list[Optional[str]]], table_index: int) -> list[TableWarning]:
    """Detect (but do not repair) structural oddities in a detected table:
    multiline cells, jagged rows, and cells that look like they share a
    bounding box with the row above (a likely vertical merge / rowspan).
    """
    warnings: list[TableWarning] = []

    for r_idx, row in enumerate(rows):
        for c_idx, cell in enumerate(row):
            if cell and "\n" in cell:
                warnings.append(
                    TableWarning(
                        code="multiline_cell",
                        message=f"Multiline cell detected at row {r_idx}, col {c_idx}",
                        table_index=table_index,
                        row_index=r_idx,
                        col_index=c_idx,
                    )
                )

    expected_cols = len(rows[0]) if rows else 0
    for r_idx, row in enumerate(rows):
        if len(row) != expected_cols:
            warnings.append(
                TableWarning(
                    code="jagged_row",
                    message=f"Row {r_idx} has {len(row)} cells, expected {expected_cols}",
                    table_index=table_index,
                    row_index=r_idx,
                )
            )

    # Repeated content across consecutive rows in the same column: when a
    # ruled grid cuts a vertically-merged cell into several row bands (one
    # per horizontal line crossing it), pdfplumber duplicates that cell's
    # text into every band it overlaps rather than leaving it blank. This
    # is a strong, generic signal of a multi-slot merged cell.
    for c_idx in range(expected_cols):
        prev_value: Optional[str] = None
        for r_idx, row in enumerate(rows):
            cell = row[c_idx] if c_idx < len(row) else None
            if cell is not None and cell == prev_value:
                warnings.append(
                    TableWarning(
                        code="repeated_cell_value",
                        message=(
                            f"Column {c_idx}, row {r_idx} repeats the same non-empty content as "
                            "the row above -- likely part of a multi-slot merged cell"
                        ),
                        table_index=table_index,
                        row_index=r_idx,
                        col_index=c_idx,
                    )
                )
            prev_value = cell

    try:
        table_rows = table.rows
    except Exception:
        table_rows = []

    for c_idx in range(expected_cols):
        prev_bbox = None
        for r_idx, trow in enumerate(table_rows):
            cells = getattr(trow, "cells", None) or []
            cell_bbox = cells[c_idx] if c_idx < len(cells) else None
            if cell_bbox is not None and cell_bbox == prev_bbox:
                warnings.append(
                    TableWarning(
                        code="possible_merged_cell",
                        message=(
                            f"Cell at col {c_idx}, row {r_idx} shares its bounding box with the "
                            "row above -- likely a vertical merge (rowspan)"
                        ),
                        table_index=table_index,
                        row_index=r_idx,
                        col_index=c_idx,
                    )
                )
            prev_bbox = cell_bbox

    return warnings


def run_strategy(
    page: Page,
    page_number: int,
    strategy_name: str,
    table_settings: dict[str, Any],
    source_file: str,
) -> list[IntermediateTable]:
    """Detect and extract all tables on a page for one strategy."""
    try:
        found_tables = page.find_tables(table_settings=table_settings)
    except Exception:
        logger.exception(
            "Strategy '%s' failed to find tables on page %d of %s", strategy_name, page_number, source_file
        )
        return []

    results: list[IntermediateTable] = []
    for t_idx, table in enumerate(found_tables):
        try:
            raw_rows = table.extract()
        except Exception:
            logger.exception(
                "Strategy '%s' failed to extract table %d on page %d of %s",
                strategy_name,
                t_idx,
                page_number,
                source_file,
            )
            continue

        rows: list[list[Optional[str]]] = [[cell if cell else None for cell in row] for row in raw_rows]
        warnings = _detect_warnings(table, rows, t_idx)
        if warnings:
            logger.info(
                "%s (page %d, strategy %s, table %d): %d warning(s)",
                source_file,
                page_number,
                strategy_name,
                t_idx,
                len(warnings),
            )

        results.append(
            IntermediateTable(
                source_file=source_file,
                page=page_number,
                strategy=strategy_name,
                table_index=t_idx,
                bbox=tuple(round(float(v), 2) for v in table.bbox),
                n_rows=len(rows),
                n_cols=len(rows[0]) if rows else 0,
                rows=rows,
                warnings=warnings,
            )
        )
    return results
