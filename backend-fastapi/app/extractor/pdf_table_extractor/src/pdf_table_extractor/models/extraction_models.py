"""Dataclasses describing the generic, structure-agnostic extraction results.

These models intentionally avoid anything PDF-specific: no fixed column
positions, no day-of-week names, no absolute coordinate rules. They only
describe *what pdfplumber reported* for a given document, so later stages
can reason about structure without re-parsing raw pdfplumber objects.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PageSize:
    width: float
    height: float


@dataclass
class PageMetadata:
    page_number: int
    size: PageSize
    text: Optional[str]


@dataclass
class PdfMetadata:
    source_file: str
    page_count: int
    pages: list[PageMetadata]


@dataclass
class WordInfo:
    """One word as reported by pdfplumber's extract_words().

    Kept as diagnostic spatial data only (spec section 5) -- never used
    as a fixed/absolute coordinate rule.
    """

    page: int
    text: str
    x0: float
    x1: float
    top: float
    bottom: float


@dataclass
class LineInfo:
    page: int
    x0: float
    x1: float
    top: float
    bottom: float
    orientation: str  # "horizontal" | "vertical" | "diagonal"


@dataclass
class RectInfo:
    page: int
    x0: float
    x1: float
    top: float
    bottom: float


@dataclass
class TableWarning:
    """A recorded observation about a table's structure (e.g. a likely
    merged cell). Warnings are informational only -- nothing here rewrites
    or invents cell content."""

    code: str
    message: str
    table_index: int
    row_index: Optional[int] = None
    col_index: Optional[int] = None


@dataclass
class IntermediateTable:
    """Generic intermediate representation of one detected table.

    Mirrors the format requested in spec section 4: source file, page,
    strategy, table index and the raw rows, with cell content preserved
    verbatim (no invented data).
    """

    source_file: str
    page: int
    strategy: str
    table_index: int
    bbox: tuple[float, float, float, float]
    n_rows: int
    n_cols: int
    rows: list[list[Optional[str]]]
    warnings: list[TableWarning] = field(default_factory=list)
