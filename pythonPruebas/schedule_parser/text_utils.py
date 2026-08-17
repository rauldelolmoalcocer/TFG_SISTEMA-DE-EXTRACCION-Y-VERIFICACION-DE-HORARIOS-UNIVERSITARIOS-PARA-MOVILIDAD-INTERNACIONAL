# text_utils.py

import re
import unicodedata


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

    match = re.match(
        r"(\d{4})/(\d{2}|\d{4})",
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