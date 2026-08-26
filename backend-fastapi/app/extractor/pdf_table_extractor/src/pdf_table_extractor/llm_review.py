"""Optional stage 3: ask a local, open-weight LLM (via Ollama) to resolve
the ambiguous cells the rule-based normalizer could only flag, not solve.

This is the one place in the package that uses an LLM, and it is entirely
opt-in -- nothing else here imports this module, and the base package
works with zero AI dependency. It requires the `ollama` Python package
(`pip install "pdf_table_extractor[llm]"` or `pip install ollama`) and a
running local Ollama server (default `http://localhost:11434`) with the
target model already pulled (`ollama pull qwen2.5:3b`).

Scope, deliberately narrow: only entries the rule-based stage already
flagged with a warning are sent to the model (mainly
`multiple_entries_suspected` -- a cell with more than one room-like
reference, which may be one subject with an extra annotation, like a
shared-degree code, or genuinely two overlapping classes; see
`normalizer/cell_parser.py`). Everything else is left untouched.

The model sees only each cell's own raw text (`subject_raw`) plus the
room/annotation candidates the rule-based parser already found -- never
outside knowledge about specific degrees or subjects -- and is asked to
either confirm it's one subject or split it into the distinct subjects it
actually contains, always citing its confidence. A low-confidence answer
keeps the original warning rather than silently clearing it: this stage
narrows the residue the README already documents, it doesn't claim to
eliminate it.

Small local models (the default, qwen2.5:3b, is 3B parameters) follow
formatting instructions far less reliably than a large hosted model, so
two things here exist specifically because of that: batches are small by
default (`DEFAULT_BATCH_SIZE`), and every call gets one retry with the
validation error fed back before the batch is given up on.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import replace
from typing import Literal, Optional

from pydantic import BaseModel, ValidationError

from .models.schedule_models import ScheduleEntry
from .normalizer.cell_parser import parse_cell

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "qwen2.5:3b"
DEFAULT_BATCH_SIZE = 6
DEFAULT_HOST = "http://localhost:11434"

_SYSTEM_PROMPT = """\
You are reviewing cells extracted from a university timetable PDF by an \
automated, rule-based pipeline. Each cell was flagged because it contains \
more than one room-like reference (a code with a letter and a digit, e.g. \
"NA7", "OL24"), which can mean one of two things:

1. It is genuinely ONE subject, and the extra parenthetical is something \
else entirely -- a shared-degree acronym (e.g. "(GII)"), a group-type \
marker (e.g. "(GG)"), a course-code annotation, etc.
2. It is actually TWO OR MORE distinct classes that happen to occupy the \
same table cell (joined by a newline because they fall in the same \
day/time slot).

For each cell, decide which case applies using ONLY the raw text given -- \
never any outside knowledge about what a specific degree or subject is \
called. If you cannot tell confidently, say so (confidence "low") and \
still give your best single interpretation -- never invent a subject name, \
room, or split that is not directly supported by the text.

Respond with ONLY a JSON object matching this exact shape, no other text:
{"reviews": [{"index": <int>, "is_single_subject": <bool>, \
"subjects": [{"subject_name": <string>, "room": <string or null>}], \
"confidence": "high"|"medium"|"low", "note": <string>}]}
"""

# Strips a leading <think>...</think> block some reasoning models (e.g.
# deepseek-r1) emit before their actual answer.
_THINK_BLOCK_RE = re.compile(r"<think>.*?</think>\s*", re.DOTALL)


class ResolvedSubject(BaseModel):
    subject_name: str
    room: Optional[str] = None


class ReviewedCell(BaseModel):
    index: int
    is_single_subject: bool
    subjects: list[ResolvedSubject]
    confidence: Literal["high", "medium", "low"]
    note: str


class BatchReview(BaseModel):
    reviews: list[ReviewedCell]


def _needs_review(entry: ScheduleEntry) -> bool:
    return bool(entry.warnings) and not entry.llm_reviewed


def _cell_payload(index: int, entry: ScheduleEntry) -> dict:
    return {
        "index": index,
        "subject_raw": entry.subject_raw,
        "day": entry.day,
        "time_start": entry.time_start,
        "time_end": entry.time_end,
        "rooms_already_detected": entry.rooms,
        "non_room_annotations": entry.non_room_annotations,
    }


def _resolve_name_and_room(subject: ResolvedSubject) -> tuple[str, list[str]]:
    """Small local models are reliable at *splitting* a cell but inconsistent
    at cleanly separating a trailing room from the subject name -- they
    often leave it embedded, e.g. subject_name="...4A1 (lab)", room=None.

    Rather than trust the model's `room` field blindly, re-run the same
    regex-based parser `cell_parser.py` already uses on real cells: it
    reliably pulls a trailing parenthetical room out of raw text, so
    applying it here recovers exactly what the model missed. Only falls
    back to the model's own (subject_name, room) split when the model
    *did* separate it and there is nothing left to re-parse.
    """
    if subject.room:
        return subject.subject_name, [subject.room]
    parsed = parse_cell(subject.subject_name)
    if parsed["rooms"]:
        return parsed["subject_name"] or subject.subject_name, parsed["rooms"]
    return subject.subject_name, []


def _apply_review(entry: ScheduleEntry, review: ReviewedCell) -> list[ScheduleEntry]:
    """Turn one entry + its model verdict into one or more ScheduleEntry."""
    if review.is_single_subject:
        subject = review.subjects[0] if review.subjects else None
        warnings = list(entry.warnings)
        if review.confidence != "low":
            warnings = [w for w in warnings if "more than one room-like reference" not in w]
        subject_name, rooms = _resolve_name_and_room(subject) if subject else (entry.subject_name, entry.rooms)
        return [
            replace(
                entry,
                subject_name=subject_name,
                rooms=rooms or entry.rooms,
                multiple_entries_suspected=review.confidence == "low",
                warnings=warnings,
                llm_reviewed=True,
                llm_confidence=review.confidence,
                llm_note=review.note,
            )
        ]

    results = []
    for subject in review.subjects:
        subject_name, rooms = _resolve_name_and_room(subject)
        results.append(
            replace(
                entry,
                subject_name=subject_name,
                rooms=rooms,
                multiple_entries_suspected=False,
                warnings=[w for w in entry.warnings if "more than one room-like reference" not in w],
                llm_reviewed=True,
                llm_confidence=review.confidence,
                llm_note=review.note,
            )
        )
    return results


def _extract_json_text(raw: str) -> str:
    """Strip a <think> block and any stray text around the JSON object --
    small local models often add a sentence before or after it despite
    being told not to."""
    text = _THINK_BLOCK_RE.sub("", raw).strip()
    start, end = text.find("{"), text.rfind("}")
    if start == -1 or end == -1 or end < start:
        return text
    return text[start : end + 1]


def _call_model(client, model: str, user_content: str) -> str:
    response = client.chat(
        model=model,
        format="json",
        options={"temperature": 0},
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
    )
    return response["message"]["content"]


def _review_batch(client, model: str, payload: list[dict]) -> BatchReview:
    """One attempt + one retry-with-error-feedback, per the module docstring."""
    user_content = "Review these cells and return one entry per index:\n\n" + json.dumps(payload)

    last_error: Exception | None = None
    for attempt in range(2):
        if attempt == 1:
            user_content += (
                f"\n\nYour previous reply was invalid: {last_error}. "
                "Reply again with ONLY the corrected JSON object, no other text."
            )
        raw = _call_model(client, model, user_content)
        try:
            data = json.loads(_extract_json_text(raw))
            return BatchReview.model_validate(data)
        except (json.JSONDecodeError, ValidationError) as exc:
            last_error = exc
            logger.warning("Attempt %d: model output failed to parse/validate: %s", attempt + 1, exc)

    raise ValueError(f"Model output invalid after retry: {last_error}")


def review_entries(
    entries: list[ScheduleEntry],
    *,
    client=None,
    model: str = DEFAULT_MODEL,
    host: str = DEFAULT_HOST,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> list[ScheduleEntry]:
    """Send every flagged entry to the local Ollama model in small batches
    and return a new list with resolved entries in place of the ambiguous
    ones.

    Entries with no warnings (already clean) pass through untouched. A
    batch that fails (server unreachable, model output still invalid after
    the retry) leaves its entries untouched too, with the failure logged --
    never silently dropped.
    """
    if client is None:
        import ollama  # local import: keep this an opt-in dependency

        client = ollama.Client(host=host)

    to_review = [(i, e) for i, e in enumerate(entries) if _needs_review(e)]
    if not to_review:
        logger.info("No entries need LLM review.")
        return list(entries)

    resolved: dict[int, list[ScheduleEntry]] = {}
    total_batches = (len(to_review) + batch_size - 1) // batch_size

    for batch_num in range(total_batches):
        chunk = to_review[batch_num * batch_size : (batch_num + 1) * batch_size]
        payload = [_cell_payload(i, e) for i, e in chunk]

        try:
            batch_review = _review_batch(client, model, payload)
        except Exception:
            logger.exception(
                "LLM review batch %d/%d failed -- leaving its %d entries unchanged",
                batch_num + 1,
                total_batches,
                len(chunk),
            )
            continue

        reviews_by_index = {r.index: r for r in batch_review.reviews}
        for original_index, entry in chunk:
            review = reviews_by_index.get(original_index)
            if review is None:
                logger.warning(
                    "Model response missing index %d -- leaving that entry unchanged", original_index
                )
                continue
            resolved[original_index] = _apply_review(entry, review)

        logger.info("LLM review batch %d/%d done (%d cells)", batch_num + 1, total_batches, len(chunk))

    output: list[ScheduleEntry] = []
    for i, entry in enumerate(entries):
        output.extend(resolved.get(i, [entry]))
    return output
