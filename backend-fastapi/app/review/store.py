# app/review/store.py
#
# Capa de persistencia y consulta de la revisión manual.
#
# all_schedules.json (o all_schedules_reviewed.json si la etapa LLM
# corrió) es el resultado ORIGINAL de la extracción y no se toca jamás
# desde aquí (solo lectura), igual que skipped_tables.json y
# extraction_run.json. reviewed_schedules.json es la copia editable que
# esta capa construye, filtra, pagina y persiste.

import copy
import hashlib
import json
import os
import time
import uuid

ALL_SCHEDULES_FILENAME = "all_schedules.json"
ALL_SCHEDULES_REVIEWED_FILENAME = "all_schedules_reviewed.json"
SKIPPED_TABLES_FILENAME = "skipped_tables.json"
EXTRACTION_RUN_FILENAME = "extraction_run.json"
REVIEWED_FILENAME = "reviewed_schedules.json"

VALID = "VALID"
WARNING = "WARNING"
INVALID = "INVALID"

RECORD_FIELDS = [
    "degree", "course_year", "semester", "group", "day",
    "time_start", "time_end", "subject_name", "rooms",
    "multiple_entries_suspected", "notes",
]


class ReviewError(Exception):
    """Error de dominio de la capa de revisión (payload inválido, id no
    encontrado, etc.). Se traduce a una respuesta HTTP en app/main.py."""


# ==========================================
# RUTAS + IO ATÓMICA
# ==========================================

def _reviewed_path(output_dir: str) -> str:
    return os.path.join(output_dir, REVIEWED_FILENAME)


def _atomic_write_json(path: str, data: dict):
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def _load_json(path: str):
    if not os.path.isfile(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_all_schedules(output_dir: str):
    """Lista de entradas (dict con la forma de ScheduleEntry). Prefiere
    la versión revisada por LLM si la etapa 3 llegó a ejecutarse."""
    reviewed = _load_json(os.path.join(output_dir, ALL_SCHEDULES_REVIEWED_FILENAME))
    if reviewed is not None:
        return reviewed
    return _load_json(os.path.join(output_dir, ALL_SCHEDULES_FILENAME))


def load_skipped_tables(output_dir: str):
    return _load_json(os.path.join(output_dir, SKIPPED_TABLES_FILENAME))


def load_extraction_run(output_dir: str):
    return _load_json(os.path.join(output_dir, EXTRACTION_RUN_FILENAME))


def load_reviewed(output_dir: str):
    return _load_json(_reviewed_path(output_dir))


def save_reviewed(output_dir: str, data: dict):
    _atomic_write_json(_reviewed_path(output_dir), data)


# ==========================================
# CONSTRUCCIÓN DE reviewed_schedules.json
# A PARTIR DE all_schedules.json + skipped_tables.json
# ==========================================

def _make_id(origin: str, file: str, page, seq: int) -> str:
    raw = f"{origin}|{file}|{page}|{seq}"
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]
    prefix = "e" if origin == "entry" else "s"
    return f"{prefix}{digest}"


def _current_from_entry(entry: dict) -> dict:
    return {
        "degree": entry.get("degree"),
        "course_year": entry.get("course_year"),
        "semester": entry.get("semester"),
        "group": entry.get("group"),
        "day": entry.get("day"),
        "time_start": entry.get("time_start"),
        "time_end": entry.get("time_end"),
        "subject_name": entry.get("subject_name"),
        "rooms": list(entry.get("rooms") or []),
        "multiple_entries_suspected": bool(entry.get("multiple_entries_suspected")),
        "notes": [],
    }


def _empty_current() -> dict:
    return {
        "degree": None,
        "course_year": None,
        "semester": None,
        "group": None,
        "day": None,
        "time_start": None,
        "time_end": None,
        "subject_name": None,
        "rooms": [],
        "multiple_entries_suspected": False,
        "notes": [],
    }


def _compute_entry_status(current: dict, issues: list) -> str:
    """A cell the rule-based (or LLM) stage still believes mixes more than
    one class is wrong data, not merely uncertain -- degree/day/time apply
    to both subjects at once, none of which the reviewer can trust as-is.
    That earns INVALID (the "Incorrecto" tab), same as a table that
    couldn't be parsed at all, rather than a soft WARNING. Any other
    original issue (e.g. an unparseable time) still only warrants WARNING."""
    if current.get("multiple_entries_suspected"):
        return INVALID
    return WARNING if issues else VALID


def _build_entry_item(entry: dict, seq: int) -> dict:
    warnings = list(entry.get("warnings") or [])
    current = _current_from_entry(entry)
    status = _compute_entry_status(current, warnings)

    return {
        "id": _make_id("entry", entry.get("source_file"), entry.get("page"), seq),
        "origin": "entry",
        "status": status,
        "original_status": status,
        "reviewed": False,
        "manually_modified": False,
        "original_issues": warnings,
        "original_reason": warnings,
        "current": current,
        "original": copy.deepcopy(current),
        "source": {
            "file": entry.get("source_file"),
            "page": entry.get("page"),
            "raw_text": entry.get("subject_raw"),
        },
        "extraction": {
            "strategy": entry.get("strategy"),
            "table_index": entry.get("table_index"),
            "row_span": entry.get("row_span"),
            "time_start_raw": entry.get("time_start_raw"),
            "time_end_raw": entry.get("time_end_raw"),
            "non_room_annotations": list(entry.get("non_room_annotations") or []),
            "llm_reviewed": bool(entry.get("llm_reviewed")),
            "llm_confidence": entry.get("llm_confidence"),
            "llm_note": entry.get("llm_note"),
        },
    }


def _build_skipped_item(skipped: dict, seq: int) -> dict:
    reason = skipped.get("reason")
    detail = skipped.get("detail")
    issues = [reason] if reason else []
    current = _empty_current()

    return {
        "id": _make_id("skipped", skipped.get("source_file"), skipped.get("page"), seq),
        "origin": "skipped",
        "status": INVALID,
        "original_status": INVALID,
        "reviewed": False,
        "manually_modified": False,
        "original_issues": issues,
        "original_reason": issues,
        "current": current,
        "original": copy.deepcopy(current),
        "source": {
            "file": skipped.get("source_file"),
            "page": skipped.get("page"),
            "raw_text": detail,
        },
        "extraction": {
            "strategy": skipped.get("strategy"),
            "table_index": skipped.get("table_index"),
            "row_span": None,
            "time_start_raw": None,
            "time_end_raw": None,
            "non_room_annotations": [],
            "llm_reviewed": False,
            "llm_confidence": None,
            "llm_note": None,
            "reason": reason,
            "detail": detail,
            "rows": skipped.get("rows"),
        },
    }


def build_reviewed_from_all_schedules(entries: list, skipped: list, run_manifest: dict = None) -> dict:
    items = []
    seq_counter = {}

    for entry in entries:
        key = ("entry", entry.get("source_file"), entry.get("page"))
        seq_counter[key] = seq_counter.get(key, 0) + 1
        items.append(_build_entry_item(entry, seq_counter[key]))

    for skip in skipped:
        key = ("skipped", skip.get("source_file"), skip.get("page"))
        seq_counter[key] = seq_counter.get(key, 0) + 1
        items.append(_build_skipped_item(skip, seq_counter[key]))

    return {
        "schema_version": "2.0",
        "source_generated_at": (run_manifest or {}).get("processed_at"),
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "items": items,
    }


def refresh_reviewed_from_extraction(output_dir: str) -> dict:
    """Reconstruye reviewed_schedules.json desde el all_schedules.json
    (+ skipped_tables.json) recién generados. Se llama siempre al
    terminar una extracción, para que una nueva extracción nunca mezcle
    revisiones de otra anterior. La versión previa (si existía) se
    conserva renombrada, nunca se borra silenciosamente."""

    entries = load_all_schedules(output_dir)
    if entries is None:
        raise ReviewError("No existe all_schedules.json en la carpeta de extracción.")

    skipped = load_skipped_tables(output_dir) or []
    run_manifest = load_extraction_run(output_dir)

    reviewed_path = _reviewed_path(output_dir)
    if os.path.isfile(reviewed_path):
        backup_suffix = time.strftime("%Y%m%d%H%M%S")
        backup_path = os.path.join(
            output_dir, f"reviewed_schedules.{backup_suffix}.bak.json"
        )
        os.replace(reviewed_path, backup_path)

    reviewed = build_reviewed_from_all_schedules(entries, skipped, run_manifest)
    save_reviewed(output_dir, reviewed)
    return reviewed


def ensure_reviewed(output_dir: str) -> dict:
    """Devuelve reviewed_schedules.json, generándolo si no existe todavía
    (p.ej. tras un reinicio del backend con una extracción ya hecha)."""

    reviewed = load_reviewed(output_dir)
    if reviewed is not None:
        return reviewed
    return refresh_reviewed_from_extraction(output_dir)


# ==========================================
# CONSULTA: FILTROS / BÚSQUEDA / PAGINACIÓN
# ==========================================

def _matches_search(item: dict, query: str) -> bool:
    query = query.strip().lower()
    if not query:
        return True

    current = item["current"]
    haystack = " ".join(str(v) for v in [
        current["subject_name"],
        ", ".join(current["rooms"]),
        current["group"],
        item["source"]["raw_text"],
        item["source"]["file"],
        current["degree"],
    ] if v).lower()

    return query in haystack


def apply_filters(items: list, filters: dict) -> list:
    result = items

    def eq_filter(field_getter, value):
        nonlocal result
        if value is None or value == "":
            return
        if value == "__NULL__":
            result = [i for i in result if not field_getter(i)]
            return
        result = [i for i in result if str(field_getter(i)) == str(value)]

    status = filters.get("status")
    if status:
        result = [i for i in result if i["status"] == status]

    eq_filter(lambda i: i["current"]["degree"], filters.get("degree"))
    eq_filter(lambda i: i["current"]["course_year"], filters.get("course_year"))
    eq_filter(lambda i: i["current"]["semester"], filters.get("semester"))
    eq_filter(lambda i: i["current"]["group"], filters.get("group"))
    eq_filter(lambda i: i["current"]["day"], filters.get("day"))
    eq_filter(lambda i: i["source"]["file"], filters.get("pdf"))

    issue = filters.get("issue")
    if issue:
        result = [i for i in result if issue in i["original_issues"]]

    llm = filters.get("llm")
    if llm == "yes":
        result = [i for i in result if i["extraction"].get("llm_reviewed")]
    elif llm == "no":
        result = [i for i in result if not i["extraction"].get("llm_reviewed")]

    search = filters.get("search")
    if search:
        result = [i for i in result if _matches_search(i, search)]

    return result


def sort_items(items: list) -> list:
    def key(i):
        c = i["current"]
        return (
            c["degree"] or "",
            c["course_year"] or "",
            c["semester"] if c["semester"] is not None else 99,
            c["group"] or "",
            c["day"] or "",
            c["time_start"] or "",
            c["subject_name"] or "",
        )
    return sorted(items, key=key)


def paginate(items: list, page: int, page_size: int):
    page = max(1, page)
    page_size = max(1, min(page_size, 200))
    total = len(items)
    start = (page - 1) * page_size
    end = start + page_size
    return items[start:end], total


# ==========================================
# RESUMEN / FILTROS DISPONIBLES / ÁRBOL
# ==========================================

def compute_summary(output_dir: str) -> dict:
    entries = load_all_schedules(output_dir)
    if entries is None:
        return None

    skipped = load_skipped_tables(output_dir) or []
    run_manifest = load_extraction_run(output_dir) or {}
    reviewed = load_reviewed(output_dir)

    if reviewed:
        # Fuente de verdad: el estado EN VIVO de cada item, no los ficheros
        # crudos de la extracción. item["status"] ya se recalcula en cada
        # edición manual, separación/duplicado, restauración y revisión con
        # IA (ver apply_edit / _apply_llm_entry_to_item / restore_item más
        # arriba) -- así que si algo cambió su estado desde que se extrajo
        # (p.ej. la IA separó una celda mezclada en dos registros válidos),
        # el resumen tiene que reflejarlo, no quedarse con la foto de la
        # extracción original.
        items = reviewed["items"]
        entry_like = [i for i in items if i["origin"] != "skipped"]
        skipped_like = [i for i in items if i["origin"] == "skipped"]

        total_records = len(items)
        valid_records = sum(1 for i in items if i["status"] == VALID)
        records_with_warnings = sum(1 for i in items if i["status"] == WARNING)
        # "Incorrectos" se sigue mostrando como dos motivos distintos
        # (mezcla sin resolver vs. tabla que no se pudo leer), pero ambos
        # cuentan solo mientras SIGAN sin resolver -- un skipped rescatado
        # a mano y marcado válido deja de contar aquí y pasa a valid_records
        # /records_with_warnings arriba, igual que cualquier otro registro.
        invalid_records = sum(1 for i in entry_like if i["status"] == INVALID)
        skipped_records = sum(1 for i in skipped_like if i["status"] == INVALID)
        reviewed_count = sum(1 for i in items if i["reviewed"])
        # Cuenta cualquier registro que haya pasado por la IA, tanto si fue
        # en la etapa de extracción (run_manifest["llm_review"], opcional y
        # desactivada por defecto) como si fue a demanda desde "Revisar con
        # IA" / "Revisar todo con IA".
        llm_reviewed_count = sum(1 for i in items if i["extraction"].get("llm_reviewed"))
    else:
        # Red de seguridad: no debería pasar en condiciones normales
        # (refresh_reviewed_from_extraction se llama siempre al terminar una
        # extracción), pero si por lo que sea reviewed_schedules.json no
        # existe todavía, se recalcula desde los ficheros crudos.
        total_records = len(entries) + len(skipped)
        invalid_records = sum(1 for entry in entries if entry.get("multiple_entries_suspected"))
        records_with_warnings = sum(
            1 for entry in entries
            if entry.get("warnings") and not entry.get("multiple_entries_suspected")
        )
        valid_records = len(entries) - invalid_records - records_with_warnings
        skipped_records = len(skipped)
        reviewed_count = 0
        llm_reviewed_count = 0

    return {
        "processed_at": run_manifest.get("processed_at"),
        "pdfs_found": run_manifest.get("pdfs_found", 0),
        "pdfs_processed": run_manifest.get("pdfs_processed", 0),
        "pdfs_failed": run_manifest.get("pdfs_failed", 0),
        "failed_files": run_manifest.get("failed_files", []),
        "valid_records": valid_records,
        "records_with_warnings": records_with_warnings,
        "invalid_records": invalid_records,
        "skipped_records": skipped_records,
        "total_records": total_records,
        "reviewed_records": reviewed_count,
        "llm_reviewed_records": llm_reviewed_count,
        "llm_review": run_manifest.get("llm_review"),
    }


def compute_filter_options(items: list) -> dict:
    degrees = set()
    course_years = set()
    semesters = set()
    groups = set()
    days = set()
    files = set()
    issues = set()

    for i in items:
        c = i["current"]
        if c["degree"]:
            degrees.add(c["degree"])
        if c["course_year"]:
            course_years.add(c["course_year"])
        if c["semester"] is not None:
            semesters.add(c["semester"])
        if c["group"]:
            groups.add(c["group"])
        if c["day"]:
            days.add(c["day"])
        if i["source"]["file"]:
            files.add(i["source"]["file"])
        for issue in i["original_issues"]:
            issues.add(issue)

    return {
        "degrees": sorted(degrees),
        "course_years": sorted(course_years),
        "semesters": sorted(semesters),
        "groups": sorted(groups),
        "days": sorted(days),
        "pdfs": sorted(files),
        "issues": sorted(issues),
        "statuses": [VALID, WARNING, INVALID],
    }


def compute_tree(items: list) -> list:
    tree = {}

    for i in items:
        c = i["current"]
        degree_key = c["degree"] or "Sin titulación"
        year_key = c["course_year"] or "Sin año académico"
        semester_key = c["semester"] if c["semester"] is not None else "Sin cuatrimestre"
        group_key = c["group"] or "Sin grupo"

        degree_node = tree.setdefault(degree_key, {
            "degree_name": degree_key,
            "count": 0,
            "years": {},
        })
        degree_node["count"] += 1

        year_node = degree_node["years"].setdefault(year_key, {
            "course_year": year_key,
            "count": 0,
            "semesters": {},
        })
        year_node["count"] += 1

        semester_node = year_node["semesters"].setdefault(semester_key, {
            "semester": semester_key,
            "count": 0,
            "groups": {},
        })
        semester_node["count"] += 1

        group_node = semester_node["groups"].setdefault(group_key, {
            "group": group_key,
            "count": 0,
        })
        group_node["count"] += 1

    def sort_key(v):
        return (v is None, str(v))

    result = []
    for degree_key in sorted(tree.keys()):
        degree_node = tree[degree_key]
        years_out = []
        for year_key in sorted(degree_node["years"].keys(), key=sort_key):
            year_node = degree_node["years"][year_key]
            semesters_out = []
            for semester_key in sorted(year_node["semesters"].keys(), key=sort_key):
                semester_node = year_node["semesters"][semester_key]
                groups_out = [
                    semester_node["groups"][g]
                    for g in sorted(semester_node["groups"].keys(), key=sort_key)
                ]
                semesters_out.append({
                    "semester": semester_node["semester"],
                    "count": semester_node["count"],
                    "groups": groups_out,
                })
            years_out.append({
                "course_year": year_node["course_year"],
                "count": year_node["count"],
                "semesters": semesters_out,
            })
        result.append({
            "degree_name": degree_node["degree_name"],
            "count": degree_node["count"],
            "years": years_out,
        })

    return result


# ==========================================
# EDICIÓN DE REGISTROS
# ==========================================

def find_item(reviewed: dict, item_id: str):
    for item in reviewed["items"]:
        if item["id"] == item_id:
            return item
    return None


def _validate_current_payload(payload: dict):
    if not isinstance(payload, dict):
        raise ReviewError("Formato de registro inválido.")

    for field_name in RECORD_FIELDS:
        if field_name not in payload:
            raise ReviewError(f"Falta el campo '{field_name}'.")

    if not isinstance(payload.get("rooms"), list):
        raise ReviewError("El campo 'rooms' debe ser una lista.")

    if not isinstance(payload.get("notes"), list):
        raise ReviewError("El campo 'notes' debe ser una lista.")

    if not isinstance(payload.get("multiple_entries_suspected"), bool):
        raise ReviewError("El campo 'multiple_entries_suspected' debe ser un booleano.")


def apply_edit(item: dict, payload: dict):
    _validate_current_payload(payload)

    item["current"] = {
        "degree": payload.get("degree"),
        "course_year": payload.get("course_year"),
        "semester": payload.get("semester"),
        "group": payload.get("group"),
        "day": payload.get("day"),
        "time_start": payload.get("time_start"),
        "time_end": payload.get("time_end"),
        "subject_name": payload.get("subject_name"),
        "rooms": list(payload.get("rooms") or []),
        "multiple_entries_suspected": bool(payload.get("multiple_entries_suspected")),
        "notes": list(payload.get("notes") or []),
    }

    item["manually_modified"] = item["current"] != item["original"]
    item["reviewed"] = True

    # Un registro "entry"/"manual" trae consigo su propio dato editable de
    # multiple_entries_suspected -- si el revisor lo destilda al separar las
    # asignaturas mezcladas, el estado debe reflejarlo sin que haga falta un
    # segundo clic en "Marcar como válido". Los "skipped" no tienen ese
    # campo con sentido propio (nunca hubo nada parseado), así que su
    # estado se deja tal cual hasta que el revisor lo marque a mano.
    if item["origin"] in ("entry", "manual"):
        item["status"] = _compute_entry_status(item["current"], item["original_issues"])


def duplicate_item(reviewed: dict, item_id: str) -> dict:
    """Clone `item_id` into a brand-new, independent item (origin "manual")
    with the same editable fields -- used to split a cell the extractor
    flagged as mixing two classes into two clean records, by hand: edit the
    original down to one subject, duplicate it, then edit the copy to be
    the other one.

    The new item keeps the same `source`/`extraction` (day/page/etc. --
    it *is* the same original cell) so both halves stay traceable to where
    they came from, but starts fully independent: editing one never
    touches the other."""
    source_item = find_item(reviewed, item_id)
    if source_item is None:
        raise ReviewError("Registro no encontrado.")

    new_current = copy.deepcopy(source_item["current"])
    status = _compute_entry_status(new_current, [])

    new_item = {
        "id": f"m{uuid.uuid4().hex[:20]}",
        "origin": "manual",
        "status": status,
        "original_status": status,
        "reviewed": False,
        "manually_modified": False,
        "original_issues": [],
        "original_reason": [
            f"Creado a mano duplicando el registro {item_id} (separación de una celda con varias asignaturas)."
        ],
        "current": new_current,
        "original": copy.deepcopy(new_current),
        "source": copy.deepcopy(source_item["source"]),
        "extraction": copy.deepcopy(source_item["extraction"]),
        "duplicated_from": item_id,
    }
    reviewed["items"].append(new_item)
    return new_item


def set_status(item: dict, status: str):
    if status not in (VALID, WARNING, INVALID):
        raise ReviewError(f"Estado inválido: {status}")
    item["status"] = status
    item["reviewed"] = True


def restore_item(item: dict):
    item["current"] = copy.deepcopy(item["original"])
    item["status"] = item["original_status"]
    item["manually_modified"] = False
    item["reviewed"] = True


# ==========================================
# REVISIÓN CON IA (OLLAMA), BAJO DEMANDA
# ==========================================
#
# La etapa 3 del paquete (pdf_table_extractor.llm_review.review_entries)
# normalmente solo corre una vez, dentro de la extracción, sobre los
# ScheduleEntry recién normalizados. Aquí la reconstruimos a demanda sobre
# los items YA guardados en reviewed_schedules.json -- para que el
# revisor pueda pedir "inténtalo con IA" en cualquier momento (uno a uno o
# en bloque) sin perder ediciones manuales que ya haya hecho en otros
# registros: solo se tocan los items seleccionados, nunca se reconstruye
# el fichero entero.

def _entry_from_item(item: dict):
    from pdf_table_extractor.models.schedule_models import ScheduleEntry

    c = item["current"]
    ex = item["extraction"]
    return ScheduleEntry(
        source_file=item["source"].get("file"),
        page=item["source"].get("page"),
        strategy=ex.get("strategy"),
        table_index=ex.get("table_index"),
        row_span=list(ex.get("row_span") or []),
        degree=c.get("degree"),
        group=c.get("group"),
        semester=c.get("semester"),
        course_year=c.get("course_year"),
        day=c.get("day"),
        time_start=c.get("time_start"),
        time_end=c.get("time_end"),
        time_start_raw=ex.get("time_start_raw"),
        time_end_raw=ex.get("time_end_raw"),
        subject_raw=item["source"].get("raw_text") or c.get("subject_name") or "",
        subject_name=c.get("subject_name"),
        rooms=list(c.get("rooms") or []),
        non_room_annotations=list(ex.get("non_room_annotations") or []),
        multiple_entries_suspected=bool(c.get("multiple_entries_suspected")),
        warnings=list(item.get("original_issues") or []),
        llm_reviewed=bool(ex.get("llm_reviewed")),
        llm_confidence=ex.get("llm_confidence"),
        llm_note=ex.get("llm_note"),
    )


def _apply_llm_entry_to_item(item: dict, entry) -> None:
    """Updates `item` in place with the (first) resolved ScheduleEntry."""
    item["current"]["subject_name"] = entry.subject_name
    item["current"]["rooms"] = list(entry.rooms)
    item["current"]["multiple_entries_suspected"] = entry.multiple_entries_suspected
    item["extraction"]["non_room_annotations"] = list(entry.non_room_annotations)
    item["extraction"]["llm_reviewed"] = entry.llm_reviewed
    item["extraction"]["llm_confidence"] = entry.llm_confidence
    item["extraction"]["llm_note"] = entry.llm_note
    item["reviewed"] = True
    item["manually_modified"] = item["current"] != item["original"]
    item["status"] = _compute_entry_status(item["current"], item["original_issues"])


def _new_item_from_llm_entry(source_item: dict, entry) -> dict:
    """Like duplicate_item, but populated with one of the extra subjects an
    LLM split produced instead of a raw copy of the source."""
    current = {
        "degree": entry.degree,
        "course_year": entry.course_year,
        "semester": entry.semester,
        "group": entry.group,
        "day": entry.day,
        "time_start": entry.time_start,
        "time_end": entry.time_end,
        "subject_name": entry.subject_name,
        "rooms": list(entry.rooms),
        "multiple_entries_suspected": entry.multiple_entries_suspected,
        "notes": [],
    }
    status = _compute_entry_status(current, [])

    return {
        "id": f"l{uuid.uuid4().hex[:20]}",
        "origin": "entry",
        "status": status,
        "original_status": status,
        "reviewed": True,
        "manually_modified": False,
        "original_issues": [],
        "original_reason": [f"Separado por IA a partir del registro {source_item['id']}."],
        "current": current,
        "original": copy.deepcopy(current),
        "source": copy.deepcopy(source_item["source"]),
        "extraction": {
            "strategy": source_item["extraction"].get("strategy"),
            "table_index": source_item["extraction"].get("table_index"),
            "row_span": source_item["extraction"].get("row_span"),
            "time_start_raw": source_item["extraction"].get("time_start_raw"),
            "time_end_raw": source_item["extraction"].get("time_end_raw"),
            "non_room_annotations": list(entry.non_room_annotations),
            "llm_reviewed": True,
            "llm_confidence": entry.llm_confidence,
            "llm_note": entry.llm_note,
        },
        "llm_split_from": source_item["id"],
    }


# Cuánto esperamos como máximo por la respuesta de Ollama a UN registro
# antes de darlo por fallido y seguir con el siguiente. Sin este límite,
# si el servidor de Ollama se cae o se cuelga a media respuesta, la
# llamada se queda esperando para siempre y todo el proceso con ella (nos
# pasó: un lote de 355 se quedó colgado en el registro 351, sin ninguna
# forma de recuperarlo porque tampoco había guardado incremental).
OLLAMA_CALL_TIMEOUT_SECONDS = 180


def llm_review_items(
    reviewed: dict,
    item_ids,
    host: str,
    model: str,
    on_item_done=None,
    should_continue=None,
) -> dict:
    """Sends the given items through the optional Ollama review stage, one
    at a time so each result can be attributed back precisely: a cell
    either gets resolved in place or split into extra new items. Only ever
    touches the selected items -- any other manual review work already
    done elsewhere in `reviewed` is left untouched.

    `item_ids`: list of item ids to target, or None for every entry-origin
    item still flagged `multiple_entries_suspected` (the ones that need
    it). `model`: the Ollama model to use (from app.ai_settings, editable
    from the AI admin panel). `on_item_done(done, total, item_id, outcome)`
    is called after each attempt if given, for progress reporting (and,
    on the caller's side, for saving progress incrementally).

    `should_continue()` (optional): called before each item. It should
    block internally while the caller wants this paused, and return False
    to stop the loop early (cancelled) or True to keep going. Checked only
    *between* items, never mid-call -- an in-flight Ollama request isn't
    safely interruptible, which is exactly what OLLAMA_CALL_TIMEOUT_SECONDS
    is for.

    Returns {"attempted", "resolved", "split", "errors", "cancelled"}. A
    per-item failure (Ollama unreachable, timeout, invalid model output)
    is recorded in "errors" and does not stop the rest of the batch.
    """
    try:
        from pdf_table_extractor.llm_review import review_entries
        import ollama
    except ImportError as e:
        return {
            "attempted": 0, "resolved": 0, "split": 0, "cancelled": False,
            "errors": [{"id": None, "error": f"Etapa LLM no disponible: {e}"}],
        }

    # Un único cliente, reutilizado en todas las llamadas de este lote --
    # y con timeout, a diferencia del que construiría review_entries() por
    # su cuenta si no le pasamos ninguno (ese no tiene límite de tiempo).
    client = ollama.Client(host=host, timeout=OLLAMA_CALL_TIMEOUT_SECONDS)

    if item_ids is None:
        candidates = [
            i for i in reviewed["items"]
            if i["origin"] == "entry" and i["current"].get("multiple_entries_suspected")
        ]
    else:
        wanted = set(item_ids)
        candidates = [i for i in reviewed["items"] if i["id"] in wanted]

    total = len(candidates)
    attempted = 0
    resolved = 0
    split = 0
    errors = []
    cancelled = False

    for item in candidates:
        if should_continue is not None and not should_continue():
            cancelled = True
            break

        attempted += 1
        outcome = "error"
        try:
            entry = _entry_from_item(item)
            results = review_entries([entry], client=client, model=model)

            if not results or not results[0].llm_reviewed:
                errors.append({"id": item["id"], "error": "Ollama no respondió (o tardó más de lo permitido) para este registro."})
            else:
                _apply_llm_entry_to_item(item, results[0])
                if len(results) == 1:
                    resolved += 1
                    outcome = "resolved"
                else:
                    split += 1
                    outcome = f"split into {len(results)}"
                    for extra_entry in results[1:]:
                        reviewed["items"].append(_new_item_from_llm_entry(item, extra_entry))

        except Exception as e:
            errors.append({"id": item["id"], "error": str(e)})

        if on_item_done:
            on_item_done(attempted, total, item["id"], outcome)

    return {
        "attempted": attempted, "resolved": resolved, "split": split,
        "errors": errors, "cancelled": cancelled,
    }
