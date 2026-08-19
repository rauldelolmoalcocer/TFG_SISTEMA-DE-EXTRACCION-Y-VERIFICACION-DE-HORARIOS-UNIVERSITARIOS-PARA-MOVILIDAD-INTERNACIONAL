# app/review/store.py
#
# Capa de persistencia y consulta de la revisión manual.
#
# all_schedules.json es el resultado ORIGINAL de la extracción y no se
# toca jamás desde aquí (solo lectura). reviewed_schedules.json es la
# copia editable que esta capa construye, filtra, pagina y persiste.

import copy
import hashlib
import json
import os
import time

ALL_SCHEDULES_FILENAME = "all_schedules.json"
REVIEWED_FILENAME = "reviewed_schedules.json"

VALID = "VALID"
WARNING = "WARNING"
INVALID = "INVALID"

RECORD_FIELDS = [
    "degree", "academic_year", "semester", "year", "group", "day",
    "start_time", "end_time", "subject", "subgroup", "classroom", "notes",
]


class ReviewError(Exception):
    """Error de dominio de la capa de revisión (payload inválido, id no
    encontrado, etc.). Se traduce a una respuesta HTTP en app/main.py."""


# ==========================================
# RUTAS + IO ATÓMICA
# ==========================================

def _all_schedules_path(output_dir: str) -> str:
    return os.path.join(output_dir, ALL_SCHEDULES_FILENAME)


def _reviewed_path(output_dir: str) -> str:
    return os.path.join(output_dir, REVIEWED_FILENAME)


def _atomic_write_json(path: str, data: dict):
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def load_all_schedules(output_dir: str):
    path = _all_schedules_path(output_dir)
    if not os.path.isfile(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_reviewed(output_dir: str):
    path = _reviewed_path(output_dir)
    if not os.path.isfile(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_reviewed(output_dir: str, data: dict):
    _atomic_write_json(_reviewed_path(output_dir), data)


# ==========================================
# CONSTRUCCIÓN DE reviewed_schedules.json
# A PARTIR DE all_schedules.json
# ==========================================

def _make_id(origin: str, file: str, page, seq: int) -> str:
    raw = f"{origin}|{file}|{page}|{seq}"
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]
    prefix = "a" if origin == "accepted" else "r"
    return f"{prefix}{digest}"


def _extract_current(record: dict) -> dict:
    return {
        "degree": {
            "code": record.get("degree", {}).get("code"),
            "name": record.get("degree", {}).get("name"),
        },
        "academic_year": record.get("academic_year"),
        "semester": record.get("semester"),
        "year": record.get("year"),
        "group": record.get("group"),
        "day": record.get("day"),
        "start_time": record.get("start_time"),
        "end_time": record.get("end_time"),
        "subject": {
            "code": record.get("subject", {}).get("code"),
            "name": record.get("subject", {}).get("name"),
        },
        "subgroup": record.get("subgroup"),
        "classroom": record.get("classroom"),
        "notes": list(record.get("notes") or []),
    }


def _build_item(origin: str, record: dict, reason: list, seq: int) -> dict:
    source = record.get("source", {})
    extraction = record.get("extraction", {})
    issues = list(extraction.get("issues") or [])

    if origin == "rejected":
        status = INVALID
    elif issues:
        status = WARNING
    else:
        status = VALID

    current = _extract_current(record)

    return {
        "id": _make_id(origin, source.get("file"), source.get("page"), seq),
        "origin": origin,
        "status": status,
        "original_status": status,
        "reviewed": False,
        "manually_modified": False,
        "original_issues": issues,
        "original_reason": list(reason or issues),
        "confidence": extraction.get("confidence"),
        "current": current,
        "original": copy.deepcopy(current),
        "source": {
            "file": source.get("file"),
            "page": source.get("page"),
            "raw_text": source.get("raw_text"),
        },
    }


def build_reviewed_from_all_schedules(all_schedules: dict) -> dict:
    items = []
    seq_counter = {}

    for record in all_schedules.get("records", []):
        source = record.get("source", {})
        key = ("accepted", source.get("file"), source.get("page"))
        seq_counter[key] = seq_counter.get(key, 0) + 1
        items.append(_build_item("accepted", record, [], seq_counter[key]))

    for wrapper in all_schedules.get("rejected_records", []):
        record = wrapper.get("record", {})
        source = record.get("source", {})
        key = ("rejected", source.get("file"), source.get("page"))
        seq_counter[key] = seq_counter.get(key, 0) + 1
        items.append(
            _build_item("rejected", record, wrapper.get("reason"), seq_counter[key])
        )

    return {
        "schema_version": "1.0",
        "source_generated_at": all_schedules.get("processed_at"),
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "items": items,
    }


def refresh_reviewed_from_extraction(output_dir: str) -> dict:
    """Reconstruye reviewed_schedules.json desde el all_schedules.json
    recién generado. Se llama siempre al terminar una extracción, para
    que una nueva extracción nunca mezcle revisiones de otra anterior.
    La versión previa (si existía) se conserva renombrada, nunca se
    borra silenciosamente."""

    all_schedules = load_all_schedules(output_dir)
    if all_schedules is None:
        raise ReviewError("No existe all_schedules.json en la carpeta de extracción.")

    reviewed_path = _reviewed_path(output_dir)
    if os.path.isfile(reviewed_path):
        backup_suffix = time.strftime("%Y%m%d%H%M%S")
        backup_path = os.path.join(
            output_dir, f"reviewed_schedules.{backup_suffix}.bak.json"
        )
        os.replace(reviewed_path, backup_path)

    reviewed = build_reviewed_from_all_schedules(all_schedules)
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
        current["subject"]["name"],
        current["subject"]["code"],
        current["classroom"],
        current["group"],
        current["subgroup"],
        item["source"]["raw_text"],
        item["source"]["file"],
        current["degree"]["name"],
        current["degree"]["code"],
    ] if v is not None).lower()

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

    eq_filter(lambda i: i["current"]["degree"]["code"] or i["current"]["degree"]["name"], filters.get("degree"))
    eq_filter(lambda i: i["current"]["year"], filters.get("year"))
    eq_filter(lambda i: i["current"]["semester"], filters.get("semester"))
    eq_filter(lambda i: i["current"]["group"], filters.get("group"))
    eq_filter(lambda i: i["current"]["day"], filters.get("day"))
    eq_filter(lambda i: i["source"]["file"], filters.get("pdf"))

    issue = filters.get("issue")
    if issue:
        result = [i for i in result if issue in i["original_issues"]]

    search = filters.get("search")
    if search:
        result = [i for i in result if _matches_search(i, search)]

    return result


def sort_items(items: list) -> list:
    def key(i):
        c = i["current"]
        return (
            c["degree"]["name"] or "",
            c["year"] if c["year"] is not None else 99,
            c["semester"] if c["semester"] is not None else 99,
            c["group"] or "",
            c["day"] or "",
            c["start_time"] or "",
            c["subject"]["name"] or "",
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
    all_schedules = load_all_schedules(output_dir)
    if all_schedules is None:
        return None

    stats = all_schedules.get("statistics", {})
    reviewed = load_reviewed(output_dir)

    reviewed_count = 0
    if reviewed:
        reviewed_count = sum(1 for i in reviewed["items"] if i["reviewed"])

    return {
        "processed_at": all_schedules.get("processed_at"),
        "pdfs_found": stats.get("pdfs_found", 0),
        "pdfs_processed": stats.get("pdfs_processed", 0),
        "pdfs_failed": stats.get("pdfs_failed", 0),
        "failed_files": all_schedules.get("failed_files", []),
        "accepted_records": stats.get("accepted_records", 0),
        "records_with_warnings": stats.get("records_with_warnings", 0),
        "rejected_records": stats.get("rejected_records", 0),
        "total_records": stats.get("accepted_records", 0) + stats.get("rejected_records", 0),
        "reviewed_records": reviewed_count,
    }


def compute_filter_options(items: list) -> dict:
    degrees = {}
    years = set()
    semesters = set()
    groups = set()
    days = set()
    files = set()
    issues = set()

    for i in items:
        c = i["current"]
        if c["degree"]["code"] or c["degree"]["name"]:
            degrees[c["degree"]["code"] or c["degree"]["name"]] = c["degree"]["name"] or c["degree"]["code"]
        if c["year"] is not None:
            years.add(c["year"])
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
        "degrees": [{"code": code, "name": name} for code, name in sorted(degrees.items(), key=lambda x: x[1] or "")],
        "years": sorted(years, key=lambda v: (v is None, v)),
        "semesters": sorted(semesters, key=lambda v: (v is None, v)),
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
        degree_key = c["degree"]["name"] or "Sin titulación"
        year_key = c["year"] if c["year"] is not None else "Sin curso"
        semester_key = c["semester"] if c["semester"] is not None else "Sin cuatrimestre"
        group_key = c["group"] or "Sin grupo"

        degree_node = tree.setdefault(degree_key, {
            "degree_code": c["degree"]["code"],
            "degree_name": degree_key,
            "count": 0,
            "years": {},
        })
        degree_node["count"] += 1

        year_node = degree_node["years"].setdefault(year_key, {
            "year": year_key,
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
                "year": year_node["year"],
                "count": year_node["count"],
                "semesters": semesters_out,
            })
        result.append({
            "degree_code": degree_node["degree_code"],
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

    if not isinstance(payload.get("degree"), dict):
        raise ReviewError("El campo 'degree' debe ser un objeto {code, name}.")

    if not isinstance(payload.get("subject"), dict):
        raise ReviewError("El campo 'subject' debe ser un objeto {code, name}.")

    if not isinstance(payload.get("notes"), list):
        raise ReviewError("El campo 'notes' debe ser una lista.")


def apply_edit(item: dict, payload: dict):
    _validate_current_payload(payload)

    item["current"] = {
        "degree": {
            "code": payload["degree"].get("code"),
            "name": payload["degree"].get("name"),
        },
        "academic_year": payload.get("academic_year"),
        "semester": payload.get("semester"),
        "year": payload.get("year"),
        "group": payload.get("group"),
        "day": payload.get("day"),
        "start_time": payload.get("start_time"),
        "end_time": payload.get("end_time"),
        "subject": {
            "code": payload["subject"].get("code"),
            "name": payload["subject"].get("name"),
        },
        "subgroup": payload.get("subgroup"),
        "classroom": payload.get("classroom"),
        "notes": list(payload.get("notes") or []),
    }

    item["manually_modified"] = item["current"] != item["original"]
    item["reviewed"] = True


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
