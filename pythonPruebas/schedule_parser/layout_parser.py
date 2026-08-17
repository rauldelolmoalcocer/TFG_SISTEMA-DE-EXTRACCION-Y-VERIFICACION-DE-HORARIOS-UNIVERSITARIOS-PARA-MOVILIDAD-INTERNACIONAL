# layout_parser.py

import re

from config import DAYS, LINE_Y_TOLERANCE

from text_utils import (
    clean_text,
    normalize_comparison_text,
    normalize_hour
)


TIME_PATTERN = re.compile(
    r"(\d{1,2}:\d{2})\s*/\s*(\d{1,2}:\d{2})"
)


# ==========================================
# WORD EXTRACTION
# ==========================================

def extract_words(page):

    result = []

    for word in page.get_text("words"):

        x0, y0, x1, y1, text, block_no, line_no, word_no = word

        text = clean_text(text)

        if not text:
            continue

        result.append({

            "text": text,

            "x0": x0,
            "y0": y0,
            "x1": x1,
            "y1": y1,

            "cx": (x0 + x1) / 2,
            "cy": (y0 + y1) / 2,

            "block_no": block_no,
            "line_no": line_no,
            "word_no": word_no
        })

    return result


# ==========================================
# DAY COLUMNS
# ==========================================

def find_day_columns(words, page_width):

    detected = {}

    for word in words:

        normalized = normalize_comparison_text(
            word["text"]
        )

        if normalized in DAYS:

            # Nos quedamos con el encabezado más alto
            if (
                normalized not in detected
                or word["cy"] < detected[normalized]["cy"]
            ):

                detected[normalized] = word

    if len(detected) < 3:
        return None

    # Si tenemos los cinco días, perfecto.

    if all(day in detected for day in DAYS):

        centers = [
            detected[day]["cx"]
            for day in DAYS
        ]

        columns = {}

        for i, day in enumerate(DAYS):

            center = centers[i]

            if i == 0:
                left = max(
                    0,
                    center - (centers[i + 1] - center) / 2
                )
            else:
                left = (
                    centers[i - 1] + center
                ) / 2

            if i == len(DAYS) - 1:
                right = min(
                    page_width,
                    center + (center - centers[i - 1]) / 2
                )
            else:
                right = (
                    center + centers[i + 1]
                ) / 2

            columns[day] = {
                "x0": left,
                "x1": right,
                "center": center
            }

        return columns

    # ======================================
    # FALLBACK:
    # aproximamos usando los días encontrados
    # ======================================

    ordered = sorted(
        detected.items(),
        key=lambda item: item[1]["cx"]
    )

    first_x = ordered[0][1]["cx"]
    last_x = ordered[-1][1]["cx"]

    if len(ordered) <= 1:
        return None

    estimated_spacing = (
        last_x - first_x
    ) / (len(ordered) - 1)

    start = first_x

    columns = {}

    for index, day in enumerate(DAYS):

        center = start + index * estimated_spacing

        columns[day] = {
            "x0": center - estimated_spacing / 2,
            "x1": center + estimated_spacing / 2,
            "center": center
        }

    return columns


# ==========================================
# TIME ROWS
# ==========================================

def find_time_rows(page):

    rows = []

    for block in page.get_text("blocks"):

        x0, y0, x1, y1, text, *_ = block

        text = clean_text(text)

        for match in TIME_PATTERN.finditer(text):

            rows.append({

                "hora_inicio": normalize_hour(
                    match.group(1)
                ),

                "hora_fin": normalize_hour(
                    match.group(2)
                ),

                "y": (y0 + y1) / 2
            })

    rows.sort(
        key=lambda row: row["y"]
    )

    # Eliminamos duplicados muy próximos.

    clean_rows = []

    for row in rows:

        duplicated = False

        for existing in clean_rows:

            if (
                row["hora_inicio"]
                == existing["hora_inicio"]
                and row["hora_fin"]
                == existing["hora_fin"]
                and abs(row["y"] - existing["y"]) < 8
            ):

                duplicated = True
                break

        if not duplicated:
            clean_rows.append(row)

    return clean_rows


# ==========================================
# ROW BOUNDS
# ==========================================

def build_row_bounds(rows, page_height):

    result = []

    if not rows:
        return result

    for index, row in enumerate(rows):

        if index == 0:

            if len(rows) > 1:

                upper = row["y"] - (
                    rows[index + 1]["y"]
                    - row["y"]
                ) / 2

            else:
                upper = row["y"] - 20

        else:

            upper = (
                rows[index - 1]["y"]
                + row["y"]
            ) / 2

        if index == len(rows) - 1:

            if len(rows) > 1:

                lower = row["y"] + (
                    row["y"]
                    - rows[index - 1]["y"]
                ) / 2

            else:
                lower = row["y"] + 20

        else:

            lower = (
                row["y"]
                + rows[index + 1]["y"]
            ) / 2

        result.append({

            **row,

            "y0": max(0, upper),
            "y1": min(page_height, lower)
        })

    return result


# ==========================================
# RECONSTRUCT TEXT
# ==========================================

def reconstruct_text(words):

    if not words:
        return ""

    words = sorted(
        words,
        key=lambda w: (w["cy"], w["x0"])
    )

    lines = []

    current_line = []
    current_y = None

    for word in words:

        if current_y is None:

            current_line = [word]
            current_y = word["cy"]

            continue

        if abs(word["cy"] - current_y) <= LINE_Y_TOLERANCE:

            current_line.append(word)

            current_y = sum(
                w["cy"] for w in current_line
            ) / len(current_line)

        else:

            current_line.sort(
                key=lambda w: w["x0"]
            )

            lines.append(
                " ".join(
                    w["text"]
                    for w in current_line
                )
            )

            current_line = [word]
            current_y = word["cy"]

    if current_line:

        current_line.sort(
            key=lambda w: w["x0"]
        )

        lines.append(
            " ".join(
                w["text"]
                for w in current_line
            )
        )

    return clean_text(
        " ".join(lines)
    )


# ==========================================
# BUILD CELLS
# ==========================================

def build_schedule_cells(page):

    words = extract_words(page)

    columns = find_day_columns(
        words,
        page.rect.width
    )

    rows = find_time_rows(page)

    if not columns or not rows:
        return []

    row_bounds = build_row_bounds(
        rows,
        page.rect.height
    )

    cells = []

    for row in row_bounds:

        for day in DAYS:

            column = columns[day]

            cell_words = []

            for word in words:

                # Centro de la palabra dentro de la celda
                if not (
                    column["x0"]
                    <= word["cx"]
                    <= column["x1"]
                ):
                    continue

                if not (
                    row["y0"]
                    <= word["cy"]
                    <= row["y1"]
                ):
                    continue

                # Evitamos que las horas entren
                # como contenido de la asignatura.

                if TIME_PATTERN.search(word["text"]):
                    continue

                cell_words.append(word)

            text = reconstruct_text(
                cell_words
            )

            if not text:
                continue

            cells.append({

                "dia": day,

                "hora_inicio": row["hora_inicio"],
                "hora_fin": row["hora_fin"],

                "text": text,

                "bbox": {
                    "x0": column["x0"],
                    "x1": column["x1"],
                    "y0": row["y0"],
                    "y1": row["y1"]
                }
            })

    return cells
