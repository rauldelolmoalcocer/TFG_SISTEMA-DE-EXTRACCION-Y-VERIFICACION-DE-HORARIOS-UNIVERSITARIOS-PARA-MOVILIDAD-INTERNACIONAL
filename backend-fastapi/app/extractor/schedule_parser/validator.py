# validator.py

import re

from .config import (
    DAYS,
    MIN_ACCEPTED_CONFIDENCE
)

from .text_utils import (
    GROUP_PATTERN,
    normalize_comparison_text
)


# ==========================================
# SEVERIDAD
# ==========================================
#
# ERROR   -> fuerza el rechazo del registro, independientemente de la
#            confianza calculada.
# WARNING -> resta confianza pero no rechaza el registro por sí solo.

ERROR_ISSUES = {
    "missing_subject",
    "invalid_subject_name",
    "invalid_day",
    "invalid_time",
    "invalid_classroom",
    "invalid_academic_year"
}

ISSUE_PENALTIES = {

    # ERRORES
    "missing_subject": 0.4,
    "invalid_subject_name": 0.4,
    "invalid_day": 0.4,
    "invalid_time": 0.4,
    "invalid_classroom": 0.4,
    "invalid_academic_year": 0.4,

    # WARNINGS
    "fragmented_subject": 0.6,
    "suspicious_classroom": 0.15,
    "unknown_classroom": 0.15,
    "missing_classroom": 0.15,
    "missing_degree": 0.15,
    "missing_academic_year": 0.10,
    "incompatible_subgroup": 0.15
}

DEFAULT_ISSUE_PENALTY = 0.10


# ==========================================
# ASIGNATURA
# ==========================================

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

# Una asignatura real debería contener al menos una "palabra" de 3 o
# más letras. Cubre de forma general casos como "2A (*) 2A2", "2A1 Y
# 2A2", "I-2", "1A", "16:30" o "*" sin necesidad de listas ad-hoc.
ALPHA_WORD_PATTERN = re.compile(r"[A-Z]{3,}")

# Nombres que empiezan por una preposición/artículo suelto suelen ser
# fragmentos de un nombre más largo (p.ej. "DE CALIDAD" viniendo de
# "GESTIÓN DE CALIDAD" recortado). No se rechazan automáticamente,
# solo se marcan como sospechosos.
STOPWORD_START_PATTERN = re.compile(
    r"^(DE|DEL|LA|EL|LOS|LAS|EN|Y|A|AL|CON|PARA|POR)\b"
)


def classify_subject_name(name):

    if not name:
        return ["missing_subject"]

    normalized = normalize_comparison_text(
        name
    ).strip()

    if len(normalized) < 3:
        return ["invalid_subject_name"]

    if not ALPHA_WORD_PATTERN.search(normalized):
        return ["invalid_subject_name"]

    for pattern in INVALID_SUBJECT_PATTERNS:

        if re.fullmatch(
            pattern,
            normalized,
            re.I
        ):
            return ["invalid_subject_name"]

    issues = []

    words = normalized.split()

    if (
        STOPWORD_START_PATTERN.match(normalized)
        and len(words) <= 3
    ):
        issues.append("fragmented_subject")

    return issues


# ==========================================
# AULA
# ==========================================

# Valores que indican explícitamente "sin aula asignada".
SUSPICIOUS_CLASSROOM_VALUES = {
    "*", "-", "--", "", "N/A", "NA"
}

PENDING_CLASSROOM_PATTERN = re.compile(
    r"^PEND\.?(IENTE)?\s*(DE\s*)?ASIG"
)

TIME_LIKE_PATTERN = re.compile(
    r"\d{1,2}[:.]\d{2}"
)

# Permisivo a propósito: solo detecta falsos positivos evidentes.
# Cubre NA7, SA1, SA5B, EA1B, SA5B/OL8 y también variantes reales
# unidas con guion como SA5A-SL11.
VALID_CLASSROOM_PATTERN = re.compile(
    r"^[A-ZÑ]{1,4}\d{1,3}[A-Z]?"
    r"(?:[/-][A-ZÑ]{1,4}\d{1,3}[A-Z]?)*$"
)


def classify_classroom(value):

    if not value:
        return None, "missing_classroom"

    cleaned = value.strip()

    if not cleaned:
        return None, "missing_classroom"

    normalized = normalize_comparison_text(cleaned)

    if (
        normalized in SUSPICIOUS_CLASSROOM_VALUES
        or PENDING_CLASSROOM_PATTERN.match(normalized)
    ):
        return None, "suspicious_classroom"

    if TIME_LIKE_PATTERN.search(cleaned):
        return None, "invalid_classroom"

    if VALID_CLASSROOM_PATTERN.fullmatch(normalized):
        return cleaned, None

    # Aula con formato no reconocido pero no evidentemente falsa
    # (p.ej. "LAB", "GTIC", "E.F."): se conserva el valor original,
    # solo se marca como desconocida.
    return cleaned, "unknown_classroom"


# ==========================================
# CURSO ACADÉMICO
# ==========================================

ACADEMIC_YEAR_PATTERN = re.compile(
    r"^(\d{4})/(\d{4})$"
)


def academic_year_issue(value):

    if not value:
        return "missing_academic_year"

    match = ACADEMIC_YEAR_PATTERN.fullmatch(
        value.strip()
    )

    if not match:
        return "invalid_academic_year"

    first = int(match.group(1))
    second = int(match.group(2))

    if second != first + 1:
        return "invalid_academic_year"

    return None


# ==========================================
# GRUPO / SUBGRUPO
# ==========================================

def classify_subgroup(group, subgroup):

    if not group or not subgroup:
        return None

    if not subgroup.upper().startswith(
        group.upper()
    ):
        return "incompatible_subgroup"

    return None


# ==========================================
# DÍA / HORA
# ==========================================

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


# ==========================================
# CONFIANZA
# ==========================================

def calculate_confidence(issues):

    score = 1.0

    for issue in issues:

        score -= ISSUE_PENALTIES.get(
            issue,
            DEFAULT_ISSUE_PENALTY
        )

    return round(
        max(0.0, min(score, 1.0)),
        2
    )


# ==========================================
# VALIDACIÓN DE REGISTRO
# ==========================================

def validate_record(record):

    issues = []

    if not valid_day(
        record["day"]
    ):

        issues.append(
            "invalid_day"
        )

    if not valid_time_range(
        record["start_time"],
        record["end_time"]
    ):

        issues.append(
            "invalid_time"
        )

    issues.extend(
        classify_subject_name(
            record["subject"]["name"]
        )
    )

    normalized_classroom, classroom_issue = classify_classroom(
        record["classroom"]
    )

    record["classroom"] = normalized_classroom

    if classroom_issue:
        issues.append(classroom_issue)

    year_issue = academic_year_issue(
        record["academic_year"]
    )

    if year_issue:
        issues.append(year_issue)

    if not record["degree"]["name"]:

        issues.append(
            "missing_degree"
        )

    subgroup_issue = classify_subgroup(
        record["group"],
        record["subgroup"]
    )

    if subgroup_issue:
        issues.append(subgroup_issue)

    confidence = calculate_confidence(
        issues
    )

    record["extraction"]["confidence"] = (
        confidence
    )

    record["extraction"]["issues"] = (
        issues
    )

    has_error = any(
        issue in ERROR_ISSUES
        for issue in issues
    )

    accepted = (
        not has_error
        and confidence >= MIN_ACCEPTED_CONFIDENCE
    )

    return {
        "accepted": accepted,
        "errors": issues,
        "record": record
    }
