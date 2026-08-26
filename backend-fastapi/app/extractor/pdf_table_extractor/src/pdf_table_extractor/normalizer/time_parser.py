"""Parses a time-range label (a table's time-column cell) into a
(start, end) pair of "HH:MM" strings.

Generic on purpose: the spec calls out that different faculties use
different hour formats ("8:00/8:55", "8:00-8:55", "15:00–16:00", with or
without leading zeros, with or without a space around the separator).
A single regex with an alternation of separators covers all of these
without hardcoding any one PDF's convention. Anything that doesn't match
returns None -- never a guessed time.

The character classes below (both the hour:minute separator and the
start/end separator) were widened after finding two real typos in the
sample PDFs, not to fit those PDFs specifically but because both are
plausible one-off substitutions in *any* scanned/authored document: a
colon typed where a dash was meant ("19:00:20:00" instead of
"19:00-20:00"), and an underscore where a colon was meant ("18_00"
instead of "18:00", most likely a font/glyph substitution from the
source PDF itself).
"""
from __future__ import annotations

import re
from typing import Optional

_TIME_RANGE_RE = re.compile(
    r"(\d{1,2})[:.,_](\d{2})\s*[/\-–—:]\s*(\d{1,2})[:.,_](\d{2})"
)


def parse_time_range(label: Optional[str]) -> Optional[tuple[str, str]]:
    """Return ("HH:MM", "HH:MM") for a time-range cell, or None if unparseable."""
    if not label:
        return None
    match = _TIME_RANGE_RE.search(label)
    if not match:
        return None
    h1, m1, h2, m2 = match.groups()
    return f"{int(h1):02d}:{m1}", f"{int(h2):02d}:{m2}"
