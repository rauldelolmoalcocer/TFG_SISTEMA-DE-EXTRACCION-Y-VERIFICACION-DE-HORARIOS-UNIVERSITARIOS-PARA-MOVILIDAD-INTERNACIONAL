"""Table-detection strategy configurations to try against every PDF.

Nothing here is tuned for one specific document: each strategy is a generic
pdfplumber `table_settings` dict, tried against *every* PDF and *every*
page. We try more than one strategy because different faculties export
timetables with visible ruling lines, no ruling lines at all (columns only
implied by text alignment), or a mix of both.
"""
from __future__ import annotations

from typing import Any

TABLE_STRATEGIES: dict[str, dict[str, Any]] = {
    # Strategy A: rely purely on the ruling lines pdfplumber can detect
    # (drawn lines / cell borders). Works well for PDFs with visible grids.
    "lines": {
        "vertical_strategy": "lines",
        "horizontal_strategy": "lines",
    },
    # Strategy B: rely purely on text alignment to infer row/column
    # boundaries. Works for PDFs with no visible ruling lines at all.
    "text": {
        "vertical_strategy": "text",
        "horizontal_strategy": "text",
    },
    # Strategy C: combination. Some PDFs draw ruling lines on one axis only
    # (e.g. horizontal separators between time slots) and rely on text
    # alignment for the other (e.g. day columns with no vertical rules).
    "combined": {
        "vertical_strategy": "lines",
        "horizontal_strategy": "text",
    },
}
