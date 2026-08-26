"""Opens PDF files and reports low-level, per-page metadata.

Nothing here assumes a particular layout: page size and raw text are
reported as-is. Later stages decide what to do with them.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pdfplumber

from ..models.extraction_models import PageMetadata, PageSize, PdfMetadata

logger = logging.getLogger(__name__)


def read_pdf_metadata(pdf_path: Path) -> tuple[PdfMetadata, pdfplumber.PDF]:
    """Open `pdf_path` and return its metadata plus the open pdfplumber.PDF.

    The pdfplumber.PDF object is returned (still open) so callers can reuse
    the same `Page` objects for word/table extraction instead of re-parsing
    the file once per strategy. Callers are responsible for closing it.
    """
    pdf = pdfplumber.open(pdf_path)

    pages: list[PageMetadata] = []
    for i, page in enumerate(pdf.pages, start=1):
        try:
            text = page.extract_text()
        except Exception:
            logger.exception("Failed to extract text from page %d of %s", i, pdf_path.name)
            text = None

        pages.append(
            PageMetadata(
                page_number=i,
                size=PageSize(width=float(page.width), height=float(page.height)),
                text=text,
            )
        )
        logger.debug("Read page %d of %s (%d chars of text)", i, pdf_path.name, len(text or ""))

    metadata = PdfMetadata(source_file=pdf_path.name, page_count=len(pdf.pages), pages=pages)
    return metadata, pdf
