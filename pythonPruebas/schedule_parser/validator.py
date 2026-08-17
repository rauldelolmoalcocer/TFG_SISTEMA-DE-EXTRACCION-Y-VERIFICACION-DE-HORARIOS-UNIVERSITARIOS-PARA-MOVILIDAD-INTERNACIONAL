# validator.py

import re

from config import (
    DAYS,
    MIN_ACCEPTED_CONFIDENCE
)

from text_utils import (
    normalize_comparison_text
)


INVALID_SUBJECT_PATTERNS = [

    # 3A
    # 3A1
    r"^\d+[A-Z]\d*$",

    # (GIEAI)
    r"^\([A-Z0-9]+\)$",

    # - 1B
    r"^-\s*\d+[A-Z]\d*$",

    # únicamente números
    r"^\d+$"
]


def valid_subject_name(name):

    if not name:
        return False

    name = normalize_comparison_text(
        name
    ).strip()

    if len(name) < 3:
        return False

    # Una asignatura debería contener
    # al menos alguna letra.

    if not re.search(
        r"[A-ZÁÉÍÓÚÑ]",
        name
    ):
        return False

    for pattern in INVALID_SUBJECT_PATTERNS:

        if re.fullmatch(
            pattern,
            name,
            re.I
        ):
            return False

    return True


def valid_day(day):

    if not day:
        return False

    return (
        normalize_comparison_text(day)
        in DAYS
    )


def valid_time_range(start, end):

    if not start or not end:
        return False

    try:

        start_hour, start_minute = map(
            int,
            start.split(":")
        )

        end_hour, end_minute = map(
            int,
            end.split(":")
        )

        start_minutes = (
            start_hour * 60
            + start_minute
        )

        end_minutes = (
            end_hour * 60
            + end_minute
        )

        return end_minutes > start_minutes

    except Exception:

        return False


def calculate_confidence(record):

    score = 0.0

    if record["degree"]["name"]:
        score += 0.10

    if record["academic_year"]:
        score += 0.05

    if record["year"]:
        score += 0.05

    if record["group"]:
        score += 0.05

    if valid_day(
        record["day"]
    ):
        score += 0.15

    if valid_time_range(
        record["start_time"],
        record["end_time"]
    ):
        score += 0.20

    if valid_subject_name(
        record["subject"]["name"]
    ):
        score += 0.30

    if record["subgroup"]:
        score += 0.05

    if record["classroom"]:
        score += 0.05

    return round(
        min(score, 1.0),
        2
    )


def validate_record(record):

    errors = []

    if not valid_day(
        record["day"]
    ):

        errors.append(
            "invalid_day"
        )

    if not valid_time_range(
        record["start_time"],
        record["end_time"]
    ):

        errors.append(
            "invalid_time_range"
        )

    if not valid_subject_name(
        record["subject"]["name"]
    ):

        errors.append(
            "invalid_subject_name"
        )

    confidence = calculate_confidence(
        record
    )

    record["extraction"]["confidence"] = (
        confidence
    )

    accepted = (
        confidence
        >= MIN_ACCEPTED_CONFIDENCE
        and "invalid_subject_name"
        not in errors
    )

    return {
        "accepted": accepted,
        "errors": errors,
        "record": record
    }
