import os
import re
import json
import pymupdf
from datetime import datetime

# ==========================================
# CONFIG
# ==========================================

INPUT_DIR = r"C:\Users\rauldelolmo123\Desktop\Pruebas\downloads_test"
OUTPUT_DIR = r"C:\Users\rauldelolmo123\Desktop\Pruebas\parsed_output"

DAYS = ["LUNES", "MARTES", "MIÉRCOLES", "JUEVES", "VIERNES"]


# ==========================================
# HELPERS
# ==========================================

def clean_text(text):
    if not text:
        return ""

    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)

    return text.strip()


def normalize_hour(hour):
    hour = hour.strip()

    if re.match(r"^\d:\d{2}$", hour):
        return "0" + hour

    return hour


# ==========================================
# METADATA
# ==========================================

def extract_metadata(text):
    grado = re.search(r"GRADO EN (.*?)\s*\((G\d+)\)", text, re.I)

    master = re.search(
        r"MASTER UNIVERSITARIO EN (.*?)(?:CUATRIMESTRE|\()",
        text,
        re.I
    )

    curso_academico = re.search(
        r"Curso\s+(\d{4}/\d{2,4})",
        text,
        re.I
    )

    cuatri = re.search(
        r"CUATRIMESTRE\s+(\d+º).*?Curso\s+(\d+º)",
        text,
        re.I
    )

    grupo = re.search(
        r"GRUPO\s+([0-9]º[A-Z])",
        text,
        re.I
    )

    titulacion = None
    codigo = None

    if grado:
        titulacion = clean_text(grado.group(1))
        codigo = grado.group(2)

    elif master:
        titulacion = clean_text(master.group(1))

    return {
        "titulacion": titulacion,
        "codigo_titulacion": codigo,
        "curso_academico": curso_academico.group(1) if curso_academico else None,
        "cuatrimestre": cuatri.group(1) if cuatri else None,
        "curso": cuatri.group(2) if cuatri else None,
        "grupo": grupo.group(1) if grupo else None
    }


# ==========================================
# RAW EXTRACTION
# ==========================================

def extract_raw_page(page, page_number):

    blocks = []

    for block in page.get_text("blocks"):

        x0, y0, x1, y1, text, block_no, block_type = block

        text = clean_text(text)

        if not text:
            continue

        blocks.append({
            "page": page_number,
            "block_number": block_no,
            "type": block_type,
            "text": text,
            "bbox": {
                "x0": x0,
                "y0": y0,
                "x1": x1,
                "y1": y1,
                "cx": (x0 + x1) / 2,
                "cy": (y0 + y1) / 2
            }
        })

    return blocks


# ==========================================
# LAYOUT DETECTION
# ==========================================

def find_day_columns(blocks):

    header_blocks = []

    for b in blocks:

        text = b["text"].upper()

        found_days = [
            day for day in DAYS
            if day in text
        ]

        if len(found_days) >= 3:
            header_blocks.append(b)

    if not header_blocks:
        return None

    header = header_blocks[0]

    x0 = header["bbox"]["x0"]
    x1 = header["bbox"]["x1"]

    width = x1 - x0

    start_x = x0 + width * 0.13

    day_width = (x1 - start_x) / 5

    columns = {}

    for i, day in enumerate(DAYS):

        columns[day] = {
            "x0": start_x + i * day_width,
            "x1": start_x + (i + 1) * day_width
        }

    return columns


def find_time_rows(blocks):

    rows = []

    pattern = re.compile(
        r"(\d{1,2}:\d{2})\s*/\s*(\d{1,2}:\d{2})"
    )

    for b in blocks:

        matches = pattern.findall(b["text"])

        for start, end in matches:

            rows.append({
                "hora_inicio": normalize_hour(start),
                "hora_fin": normalize_hour(end),
                "y": b["bbox"]["cy"]
            })

    rows = sorted(rows, key=lambda r: r["y"])

    clean_rows = []
    seen = set()

    for r in rows:

        key = (r["hora_inicio"], r["hora_fin"])

        if key not in seen:
            clean_rows.append(r)
            seen.add(key)

    return clean_rows


def assign_day(block, columns):

    if not columns:
        return None

    cx = block["bbox"]["cx"]

    for day, col in columns.items():

        if col["x0"] <= cx <= col["x1"]:
            return day

    return None


def assign_time(block, rows):

    if not rows:
        return None

    cy = block["bbox"]["cy"]

    nearest = min(
        rows,
        key=lambda r: abs(r["y"] - cy)
    )

    if abs(nearest["y"] - cy) < 30:
        return nearest

    return None


# ==========================================
# FILTERS
# ==========================================

def is_noise(text):

    upper = text.upper()

    noise_patterns = [
        "APROBADO EN",
        "CONSULTE VERSIÓN",
        "LOS HORARIOS PODRÁN",
        "CADA ALUMNO",
        "CUATRIMESTRE",
        "CURSO 2025",
        "AULAS",
        "LABORATORIOS",
    ]

    if any(p in upper for p in noise_patterns):
        return True

    if upper in DAYS:
        return True

    if re.fullmatch(
        r"\d{1,2}:\d{2}\s*/\s*\d{1,2}:\d{2}",
        upper
    ):
        return True

    return False


# ==========================================
# SUBJECT PARSER
# ==========================================

def parse_subject(text):

    text = clean_text(text)

    patterns = [

        r"^(?P<codigo>\d{6})[- ]+(?P<nombre>.+?)\s*-\s*(?P<subgrupo>[A-Z0-9º]+)\*?\s*\((?P<aula>[^)]+)\)",

        r"^(?P<nombre>.+?)\s*-\s*(?P<subgrupo>[A-Z0-9º]+)\*?\s*\((?P<aula>[^)]+)\)",

        r"^(?P<codigo>\d{6})[- ]+(?P<nombre>.+?)\s*\((?P<aula>[^)]+)\)",

        r"^(?P<nombre>.+?)\s*\((?P<aula>[^)]+)\)",
    ]

    for pattern in patterns:

        m = re.search(pattern, text)

        if not m:
            continue

        data = m.groupdict()

        nombre = clean_text(data.get("nombre"))
        aula = clean_text(data.get("aula"))

        if not nombre:
            continue

        return {
            "codigo_asignatura": data.get("codigo"),
            "nombre": nombre.upper(),
            "subgrupo": data.get("subgrupo"),
            "aula": aula,
            "texto_original": text
        }

    return None


# ==========================================
# PAGE NORMALIZATION
# ==========================================

def normalize_page(page, page_number, pdf_path):

    text = page.get_text("text")

    metadata = extract_metadata(text)

    blocks = extract_raw_page(page, page_number)

    columns = find_day_columns(blocks)

    rows = find_time_rows(blocks)

    records = []

    for block in blocks:

        text = block["text"]

        if is_noise(text):
            continue

        day = assign_day(block, columns)

        time = assign_time(block, rows)

        if not day or not time:
            continue

        subject = parse_subject(text)

        if not subject:
            continue

        records.append({

            "source": {
                "archivo": os.path.basename(pdf_path),
                "pagina": page_number
            },

            "academic_context": metadata,

            "schedule": {
                "dia": day,
                "hora_inicio": time["hora_inicio"],
                "hora_fin": time["hora_fin"]
            },

            "subject": subject
        })

    return {
        "page": page_number,
        "metadata": metadata,
        "records": records,
        "raw": {
            "text": clean_text(text),
            "blocks": blocks
        }
    }


# ==========================================
# PDF PROCESSOR
# ==========================================

def process_pdf(pdf_path):

    print(f"\nProcesando PDF: {pdf_path}")

    doc = pymupdf.open(pdf_path)

    pages = []
    all_records = []

    for page_number, page in enumerate(doc, start=1):

        page_data = normalize_page(
            page,
            page_number,
            pdf_path
        )

        pages.append(page_data)

        all_records.extend(page_data["records"])

    result = {

        "schema_version": "1.0",

        "processed_at": datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        ),

        "input_file": {
            "path": pdf_path,
            "name": os.path.basename(pdf_path),
            "total_pages": len(doc)
        },

        "standard_schedule": all_records,

        "debug": {
            "pages": pages
        }
    }

    base_name = os.path.splitext(
        os.path.basename(pdf_path)
    )[0]

    output_path = os.path.join(
        OUTPUT_DIR,
        f"{base_name}.json"
    )

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(
            result,
            f,
            ensure_ascii=False,
            indent=4
        )

    print(f"JSON generado: {output_path}")
    print(f"Registros encontrados: {len(all_records)}")


# ==========================================
# MAIN
# ==========================================

def main():

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    pdf_files = []

    for root, dirs, files in os.walk(INPUT_DIR):

        for file_name in files:

            if file_name.lower().endswith(".pdf"):

                full_path = os.path.join(
                    root,
                    file_name
                )

                pdf_files.append(full_path)

    print(f"PDFs encontrados: {len(pdf_files)}")

    for pdf_path in pdf_files:

        try:

            process_pdf(pdf_path)

        except Exception as e:

            print(f"ERROR procesando {pdf_path}")
            print(e)


if __name__ == "__main__":
    main()