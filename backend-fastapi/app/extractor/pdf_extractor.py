import os
import re
import json
import time
import threading
from dataclasses import dataclass, field
from datetime import datetime

import pymupdf


DAYS = ["LUNES", "MARTES", "MIÉRCOLES", "JUEVES", "VIERNES"]


# ==========================================
# STATE
# ==========================================

@dataclass
class ExtractorState:
    running: bool = False
    logs: list = field(default_factory=list)
    errors: list = field(default_factory=list)
    total_files: int = 0
    processed_files: int = 0
    current_file: str = ""
    output_files: list = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def add_log(self, message: str):
        timestamp = time.strftime("%H:%M:%S")
        with self.lock:
            self.logs.append(f"[{timestamp}] {message}")

    def add_error(self, message: str):
        timestamp = time.strftime("%H:%M:%S")
        line = f"[{timestamp}] ERROR: {message}"
        with self.lock:
            self.errors.append(line)
            self.logs.append(line)

    @property
    def progress_percent(self) -> int:
        if self.total_files == 0:
            return 0
        return int(self.processed_files / self.total_files * 100)


# ==========================================
# HELPERS
# ==========================================

def _clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _normalize_hour(hour: str) -> str:
    hour = hour.strip()
    if re.match(r"^\d:\d{2}$", hour):
        return "0" + hour
    return hour


# ==========================================
# METADATA
# ==========================================

def _extract_metadata(text: str) -> dict:
    grado = re.search(r"GRADO EN (.*?)\s*\((G\d+)\)", text, re.I)
    master = re.search(r"MASTER UNIVERSITARIO EN (.*?)(?:CUATRIMESTRE|\()", text, re.I)
    curso_academico = re.search(r"Curso\s+(\d{4}/\d{2,4})", text, re.I)
    cuatri = re.search(r"CUATRIMESTRE\s+(\d+º).*?Curso\s+(\d+º)", text, re.I)
    grupo = re.search(r"GRUPO\s+([0-9]º[A-Z])", text, re.I)

    titulacion = None
    codigo = None

    if grado:
        titulacion = _clean_text(grado.group(1))
        codigo = grado.group(2)
    elif master:
        titulacion = _clean_text(master.group(1))

    return {
        "titulacion": titulacion,
        "codigo_titulacion": codigo,
        "curso_academico": curso_academico.group(1) if curso_academico else None,
        "cuatrimestre": cuatri.group(1) if cuatri else None,
        "curso": cuatri.group(2) if cuatri else None,
        "grupo": grupo.group(1) if grupo else None,
    }


# ==========================================
# LAYOUT DETECTION
# ==========================================

def _extract_raw_page(page, page_number: int) -> list:
    blocks = []
    for block in page.get_text("blocks"):
        x0, y0, x1, y1, text, block_no, block_type = block
        text = _clean_text(text)
        if not text:
            continue
        blocks.append({
            "page": page_number,
            "block_number": block_no,
            "type": block_type,
            "text": text,
            "bbox": {
                "x0": x0, "y0": y0, "x1": x1, "y1": y1,
                "cx": (x0 + x1) / 2,
                "cy": (y0 + y1) / 2,
            },
        })
    return blocks


def _find_day_columns(blocks: list) -> dict | None:
    header_blocks = [
        b for b in blocks
        if sum(1 for d in DAYS if d in b["text"].upper()) >= 3
    ]
    if not header_blocks:
        return None

    header = header_blocks[0]
    x0 = header["bbox"]["x0"]
    x1 = header["bbox"]["x1"]
    start_x = x0 + (x1 - x0) * 0.13
    day_width = (x1 - start_x) / 5

    return {
        day: {
            "x0": start_x + i * day_width,
            "x1": start_x + (i + 1) * day_width,
        }
        for i, day in enumerate(DAYS)
    }


def _find_time_rows(blocks: list) -> list:
    pattern = re.compile(r"(\d{1,2}:\d{2})\s*/\s*(\d{1,2}:\d{2})")
    rows = []
    seen = set()

    for b in blocks:
        for start, end in pattern.findall(b["text"]):
            key = (_normalize_hour(start), _normalize_hour(end))
            if key not in seen:
                rows.append({
                    "hora_inicio": key[0],
                    "hora_fin": key[1],
                    "y": b["bbox"]["cy"],
                })
                seen.add(key)

    return sorted(rows, key=lambda r: r["y"])


def _assign_day(block: dict, columns: dict | None) -> str | None:
    if not columns:
        return None
    cx = block["bbox"]["cx"]
    for day, col in columns.items():
        if col["x0"] <= cx <= col["x1"]:
            return day
    return None


def _assign_time(block: dict, rows: list) -> dict | None:
    if not rows:
        return None
    cy = block["bbox"]["cy"]
    nearest = min(rows, key=lambda r: abs(r["y"] - cy))
    return nearest if abs(nearest["y"] - cy) < 30 else None


# ==========================================
# FILTERS + SUBJECT PARSER
# ==========================================

_NOISE_PATTERNS = [
    "APROBADO EN", "CONSULTE VERSIÓN", "LOS HORARIOS PODRÁN",
    "CADA ALUMNO", "CUATRIMESTRE", "CURSO 2025", "AULAS", "LABORATORIOS",
]


def _is_noise(text: str) -> bool:
    upper = text.upper()
    if any(p in upper for p in _NOISE_PATTERNS):
        return True
    if upper in DAYS:
        return True
    if re.fullmatch(r"\d{1,2}:\d{2}\s*/\s*\d{1,2}:\d{2}", upper):
        return True
    return False


def _parse_subject(text: str) -> dict | None:
    text = _clean_text(text)
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
        nombre = _clean_text(data.get("nombre", ""))
        if not nombre:
            continue
        return {
            "codigo_asignatura": data.get("codigo"),
            "nombre": nombre.upper(),
            "subgrupo": data.get("subgrupo"),
            "aula": _clean_text(data.get("aula", "")),
            "texto_original": text,
        }
    return None


# ==========================================
# PAGE + PDF PROCESSING
# ==========================================

def _normalize_page(page, page_number: int, pdf_path: str) -> dict:
    text = page.get_text("text")
    metadata = _extract_metadata(text)
    blocks = _extract_raw_page(page, page_number)
    columns = _find_day_columns(blocks)
    rows = _find_time_rows(blocks)

    records = []
    for block in blocks:
        if _is_noise(block["text"]):
            continue
        day = _assign_day(block, columns)
        slot = _assign_time(block, rows)
        if not day or not slot:
            continue
        subject = _parse_subject(block["text"])
        if not subject:
            continue
        records.append({
            "source": {"archivo": os.path.basename(pdf_path), "pagina": page_number},
            "academic_context": metadata,
            "schedule": {
                "dia": day,
                "hora_inicio": slot["hora_inicio"],
                "hora_fin": slot["hora_fin"],
            },
            "subject": subject,
        })

    return {
        "page": page_number,
        "metadata": metadata,
        "records": records,
        "raw": {"text": _clean_text(text), "blocks": blocks},
    }


def process_pdf(pdf_path: str, output_dir: str) -> str:
    doc = pymupdf.open(pdf_path)
    pages = []
    all_records = []

    for page_number, page in enumerate(doc, start=1):
        page_data = _normalize_page(page, page_number, pdf_path)
        pages.append(page_data)
        all_records.extend(page_data["records"])

    result = {
        "schema_version": "1.0",
        "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "input_file": {
            "path": pdf_path,
            "name": os.path.basename(pdf_path),
            "total_pages": len(doc),
        },
        "standard_schedule": all_records,
        "debug": {"pages": pages},
    }

    base_name = os.path.splitext(os.path.basename(pdf_path))[0]
    output_path = os.path.join(output_dir, f"{base_name}.json")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=4)

    return output_path


# ==========================================
# BATCH RUNNER
# ==========================================

def run_extraction(input_dir: str, output_dir: str, state: ExtractorState):
    os.makedirs(output_dir, exist_ok=True)

    pdf_files = [
        os.path.join(input_dir, f)
        for f in os.listdir(input_dir)
        if f.lower().endswith(".pdf")
    ]

    with state.lock:
        state.running = True
        state.total_files = len(pdf_files)
        state.processed_files = 0
        state.output_files = []
        state.logs = []
        state.errors = []

    state.add_log(f"Iniciando extracción. {len(pdf_files)} PDFs encontrados.")

    for pdf_path in pdf_files:
        filename = os.path.basename(pdf_path)

        with state.lock:
            state.current_file = filename

        state.add_log(f"Procesando: {filename}")

        try:
            output_path = process_pdf(pdf_path, output_dir)
            with state.lock:
                state.processed_files += 1
                state.output_files.append(os.path.basename(output_path))
            state.add_log(f"OK → {os.path.basename(output_path)}")

        except Exception as e:
            with state.lock:
                state.processed_files += 1
            state.add_error(f"Error en {filename}: {e}")

    with state.lock:
        state.running = False
        state.current_file = ""

    state.add_log(
        f"Extracción finalizada. {state.processed_files}/{state.total_files} procesados. "
        f"JSONs en: {output_dir}"
    )
