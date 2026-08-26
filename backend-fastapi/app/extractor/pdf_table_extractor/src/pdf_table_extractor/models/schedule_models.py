"""The final, normalized record produced by the normalizer stage: one
"cuadrante" (day + time-range + subject) ready to load into a database.

Unlike `extraction_models`, these fields *are* interpreted (a time range is
parsed, a room code is pulled out of the raw cell text) -- but only using
generic, format-agnostic rules. Nothing here is tied to a specific degree,
subject name, or PDF; a field that can't be confidently parsed is left as
`None` rather than guessed.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ScheduleEntry:
    # provenance -- always traceable back to the exact source table/cell
    source_file: str
    page: int
    strategy: str
    table_index: int
    row_span: list[int]

    # header metadata, extracted from the page's plain text (best-effort;
    # None when the page's text doesn't match any recognised pattern)
    degree: Optional[str]
    group: Optional[str]
    semester: Optional[int]
    course_year: Optional[str]

    # the schedule slot itself
    day: Optional[str]
    time_start: Optional[str]
    time_end: Optional[str]
    time_start_raw: Optional[str]
    time_end_raw: Optional[str]

    # the cell's content
    subject_raw: str
    subject_name: Optional[str]
    rooms: list[str]
    # parenthetical text found in the cell that did NOT structurally look
    # like a room (a shared-degree acronym, a group-type marker, etc.) --
    # kept rather than discarded, since it wasn't confidently a room either
    non_room_annotations: list[str]

    # true when the raw cell looks like it may hold more than one distinct
    # class (e.g. two room codes) that this stage did not attempt to split
    multiple_entries_suspected: bool

    warnings: list[str] = field(default_factory=list)

    # set only by the optional `llm_review` stage -- see that module's
    # docstring. Untouched (False/None) for every entry the rule-based
    # normalizer already produced.
    llm_reviewed: bool = False
    llm_confidence: Optional[str] = None
    llm_note: Optional[str] = None
