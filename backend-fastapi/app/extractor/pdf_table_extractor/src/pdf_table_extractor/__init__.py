"""pdf_table_extractor: generic, structure-driven extraction of schedule
grids from PDFs using pdfplumber, in two stages.

    from pdf_table_extractor import run_diagnostics, run_normalization

    summary = run_diagnostics(Path("input_pdfs"), Path("output"))
    entries, skipped = run_normalization(Path("output"))

`run_diagnostics` tries several pdfplumber table-detection strategies
against every PDF in a folder and writes the raw structural findings
(metadata, words, tables per strategy, warnings) to `output/<pdf>/`.
`run_normalization` reads that same `output/` and turns it into flat
`ScheduleEntry` records -- one per day/time-range/subject "cuadrante" --
ready to load into a database.

Nothing in this package is tuned to a specific PDF, degree, or
institution: see each module's docstring for the generic, structural
signal it relies on instead.
"""
from __future__ import annotations

from .diagnostics.analyzer import PageAnalysis, PdfAnalysis, analyze_pdf
from .diagnostics.report import build_summary, write_pdf_outputs
from .models.extraction_models import (
    IntermediateTable,
    LineInfo,
    PageMetadata,
    PdfMetadata,
    RectInfo,
    TableWarning,
    WordInfo,
)
from .models.schedule_models import ScheduleEntry
from .normalizer.schedule_builder import build_entries_for_pdf
from .pipeline import run_diagnostics, run_normalization

__version__ = "0.1.0"

__all__ = [
    "run_diagnostics",
    "run_normalization",
    "analyze_pdf",
    "build_entries_for_pdf",
    "write_pdf_outputs",
    "build_summary",
    "PdfAnalysis",
    "PageAnalysis",
    "ScheduleEntry",
    "IntermediateTable",
    "PdfMetadata",
    "PageMetadata",
    "WordInfo",
    "LineInfo",
    "RectInfo",
    "TableWarning",
]
