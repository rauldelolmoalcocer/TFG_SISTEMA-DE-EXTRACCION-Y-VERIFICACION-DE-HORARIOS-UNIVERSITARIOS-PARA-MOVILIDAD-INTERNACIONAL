"""Extracts degree/group/semester/course-year from a page's plain text.

This is the one place in the whole pipeline that assumes a *language*
(Spanish academic vocabulary: "GRADO", "MASTER", "CUATRIMESTRE", "GRUPO")
-- unavoidable if we want these fields without an LLM. It does not assume
a specific degree name, faculty, or PDF: the patterns were checked against
several structurally different headers (grados, dobles grados, masters
with and without "GRUPO") before being fixed. A field this can't find in
the text is left as None rather than guessed.
"""
from __future__ import annotations

import re
from typing import Optional

_DEGREE_RE = re.compile(r"((?:DOBLE\s+)?(?:GRADO|M[ÁA]STER|MASTER)\b[^\n]*)", re.IGNORECASE)
_GROUP_RE = re.compile(r"GRUPO\s+([^\s.,;]+)", re.IGNORECASE)
_CUATRI_RE = re.compile(r"(?:CUATRIMESTRE|SEMESTRE)\s*(\d+)", re.IGNORECASE)
_ORDINAL_SEMESTER_RE = re.compile(
    r"(PRIMER|SEGUNDO|TERCER|CUARTO|QUINTO|SEXTO)\s+SEMESTRE", re.IGNORECASE
)
_COURSE_YEAR_RE = re.compile(r"Curso\s+(\d{4})\s*[/-]\s*(\d{2,4})", re.IGNORECASE)

_ORDINALS = {"PRIMER": 1, "SEGUNDO": 2, "TERCER": 3, "CUARTO": 4, "QUINTO": 5, "SEXTO": 6}


def _looks_like_group_value(value: str) -> bool:
    """`GRUPO\\s+(\\S+)` also matches inside ordinary prose that happens to
    contain the word "grupo" followed by another word -- most commonly a
    footnote like "(*) = Grupo con preferencia a INFOADE", which otherwise
    gets read as a group value of "con". A real group label in these PDFs
    is always either fully uppercase (e.g. "GRUPO A", "GRUPO UNICO") or
    contains a digit (e.g. "GRUPO 3ºA") -- prose picked up by accident is
    neither, so this rejects it without needing to hardcode any specific
    footnote wording."""
    return value.isupper() or any(ch.isdigit() for ch in value)


def _extract_semester(text: str) -> Optional[int]:
    match = _CUATRI_RE.search(text)
    if match:
        return int(match.group(1))
    match = _ORDINAL_SEMESTER_RE.search(text)
    if match:
        return _ORDINALS.get(match.group(1).upper())
    return None


def _extract_course_year(text: str) -> Optional[str]:
    match = _COURSE_YEAR_RE.search(text)
    if not match:
        return None
    year1, year2 = match.groups()
    if len(year2) == 2:
        year2 = year1[:2] + year2
    return f"{year1}/{year2}"


def extract_header_info(page_text: Optional[str]) -> dict[str, Optional[object]]:
    """Best-effort extraction of degree/group/semester/course_year from a
    page's raw extracted text (as stored in metadata.json)."""
    text = page_text or ""
    degree_match = _DEGREE_RE.search(text)
    group_match = _GROUP_RE.search(text)

    group = group_match.group(1).strip() if group_match else None
    if group and not _looks_like_group_value(group):
        group = None

    return {
        "degree": degree_match.group(1).strip() if degree_match else None,
        "group": group,
        "semester": _extract_semester(text),
        "course_year": _extract_course_year(text),
    }
