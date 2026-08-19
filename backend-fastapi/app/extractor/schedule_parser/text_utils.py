# text_utils.py

import re
import unicodedata


# Patrón de grupo/subgrupo compartido entre subject_parser.py y validator.py
# (p.ej. "3A", "3A1").
GROUP_PATTERN = r"\d+[A-Z](?:\d+)?"


def clean_text(text):

    if not text:
        return ""

    text = text.replace("\xa0", " ")
    text = text.replace("–", "-")
    text = text.replace("—", "-")

    text = re.sub(r"\s+", " ", text)

    return text.strip()


def remove_accents(text):

    if not text:
        return ""

    normalized = unicodedata.normalize("NFD", text)

    return "".join(
        char
        for char in normalized
        if unicodedata.category(char) != "Mn"
    )


def normalize_comparison_text(text):

    text = clean_text(text)
    text = remove_accents(text)

    return text.upper()


def normalize_hour(hour):

    if not hour:
        return None

    hour = clean_text(hour)

    match = re.fullmatch(
        r"(\d{1,2}):(\d{2})",
        hour
    )

    if not match:
        return hour

    h = int(match.group(1))
    m = int(match.group(2))

    return f"{h:02d}:{m:02d}"


def remove_time_ranges(text):

    if not text:
        return ""

    return clean_text(
        re.sub(
            r"\b\d{1,2}:\d{2}\s*/\s*\d{1,2}:\d{2}\b",
            " ",
            text
        )
    )


def normalize_academic_year(value):

    if not value:
        return None

    value = clean_text(value)

    # OJO: el orden importa. Con "\d{2}|\d{4}" la alternancia probaba
    # primero \d{2} y, al no haber ancla de fin, se quedaba con los dos
    # primeros dígitos de un año de 4 dígitos (p.ej. "2026" -> "20"),
    # produciendo años como "2025/2020". "\d{2,4}" es un cuantificador
    # (no una alternancia) y captura codicioso hasta 4 dígitos primero.
    match = re.match(
        r"(\d{4})/(\d{2,4})",
        value
    )

    if not match:
        return value

    first = int(match.group(1))
    second = match.group(2)

    if len(second) == 2:

        century = (first // 100) * 100
        second = century + int(second)

    else:
        second = int(second)

    return f"{first}/{second}"


def normalize_group(value):

    if not value:
        return None

    value = normalize_comparison_text(value)

    value = value.replace("º", "")
    value = value.replace(" ", "")

    return value


def normalize_classroom(value):

    if not value:
        return None

    value = clean_text(value)

    # "OA 4" -> "OA4"
    value = re.sub(
        r"\b([A-Za-z]{1,3})\s+(\d+)\b",
        r"\1\2",
        value
    )

    return value.upper()
