"""Parses one schedule cell's raw text into a subject name + room code(s).

The one structural convention this relies on -- observed across every
sample PDF, regardless of degree or faculty -- is that a room code sits in
its own trailing parentheses, e.g. "FÍSICA - 1A\\n(NA7)".

Not every parenthetical is a room, though: some PDFs annotate a class with
a shared-degree code ("(GII)"), a group-type marker ("(GG)"), or other
short acronym that structurally looks just like a room. The distinction
used here is generic, not a lookup against any fixed list of degrees or
institutions: across every sample PDF, a real room code always contains a
digit (NA7, OL24, SA1/OL12, PL21...) while an acronym/marker never does
(GII, GTIC, GIEAI, GG...). The one added vocabulary word is "lab" /
"laboratorio", a session-type marker seen across many different degrees'
PDFs, not tied to any one of them.

This module deliberately does NOT try to split a cell into two separate
class entries when two rooms are found (e.g. two overlapping lab groups
joined by pdfplumber into one multiline cell). Telling that apart from a
single subject with two legitimate room mentions is not reliable without
guessing -- so it is only flagged via `multiple_entries_suspected` for a
human (or a later, spatial-analysis iteration) to resolve.
"""
from __future__ import annotations

import re

_PAREN_RE = re.compile(r"\(([^()]*)\)")
_HAS_DIGIT_RE = re.compile(r"\d")
_GENERIC_ROOM_WORDS = {"lab", "laboratorio"}


def _looks_like_room(candidate: str) -> bool:
    """True for a parenthetical that structurally reads as a room code."""
    if _HAS_DIGIT_RE.search(candidate):
        return True
    return candidate.strip().strip(".").lower() in _GENERIC_ROOM_WORDS


def parse_cell(raw_text: str) -> dict[str, object]:
    """Return {"subject_name", "rooms", "non_room_annotations",
    "multiple_entries_suspected"} for one cell."""
    text = raw_text or ""
    all_parens = [g.strip() for g in _PAREN_RE.findall(text) if g.strip()]

    rooms = [g for g in all_parens if _looks_like_room(g)]
    non_room_annotations = [g for g in all_parens if not _looks_like_room(g)]

    subject_name = " ".join(_PAREN_RE.sub(" ", text).split()) or None

    return {
        "subject_name": subject_name,
        "rooms": rooms,
        "non_room_annotations": non_room_annotations,
        "multiple_entries_suspected": len(rooms) > 1,
    }
