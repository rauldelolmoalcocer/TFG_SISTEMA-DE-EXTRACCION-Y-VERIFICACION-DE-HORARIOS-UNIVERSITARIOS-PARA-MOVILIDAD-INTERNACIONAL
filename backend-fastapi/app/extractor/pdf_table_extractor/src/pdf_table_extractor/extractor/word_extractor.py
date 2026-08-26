"""Extracts word-level spatial information via pdfplumber's extract_words().

This is diagnostic-only spatial data (spec section 5): x0/x1/top/bottom per
word. It is stored for later *relative* reasoning between elements -- it is
never turned into fixed/absolute coordinate rules.
"""
from __future__ import annotations

import logging

from pdfplumber.page import Page

from ..models.extraction_models import WordInfo

logger = logging.getLogger(__name__)


def extract_words(page: Page, page_number: int) -> list[WordInfo]:
    """Return every word pdfplumber detects on `page`, with its bounding box."""
    try:
        raw_words = page.extract_words()
    except Exception:
        logger.exception("Failed to extract words from page %d", page_number)
        return []

    words = [
        WordInfo(
            page=page_number,
            text=w["text"],
            x0=float(w["x0"]),
            x1=float(w["x1"]),
            top=float(w["top"]),
            bottom=float(w["bottom"]),
        )
        for w in raw_words
    ]
    logger.debug("Extracted %d words from page %d", len(words), page_number)
    return words
