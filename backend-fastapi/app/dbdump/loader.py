# app/dbdump/loader.py
#
# Vuelca reviewed_schedules.json (la copia YA revisada de la extracción) a
# las tablas relacionales de PostgreSQL descritas en db/createdatabase.sql.
#
# Estrategia: VACIAR Y RECARGAR. Cada volcado hace TRUNCATE de las tablas
# de horario + catálogos y las reconstruye enteras desde el JSON, así el
# resultado nunca depende de lo que hubiera antes ni acumula duplicados.
# Todo ocurre dentro de una única transacción: si algo falla a mitad, se
# hace ROLLBACK y la base de datos se queda exactamente como estaba.
#
# Corre en segundo plano (thread) igual que la extracción o la revisión
# con IA, informando progreso para la barra del panel "Volcado a BD".

import re
import threading
import time
import unicodedata

from app.review import store as review_store

VALID = "VALID"
WARNING = "WARNING"

# Sólo se vuelca lo que el revisor ha dado por bueno o por bueno-con-reparos.
# INVALID y las tablas omitidas (origin == "skipped") se quedan fuera y se
# informan en el resumen como "descartados".
DUMPABLE_STATUSES = (VALID, WARNING)

# Orden: hijas antes que padres. Todas las PK son uuid (no hay identidad que
# reiniciar) pero CASCADE cubre cualquier FK que se nos escape. demo y
# usuarios NO se tocan.
TRUNCATE_TABLES = [
    "class_session_rooms",
    "extraction_issues",
    "extraction_metadata",
    "class_sessions",
    "subjects",
    "groups",
    "sources",
    "rooms",
    "academic_years",
    "degrees",
    "universities",
]

UNIVERSITY_NAME = "Universidad de Alcalá"
UNIVERSITY_ACRONYM = "UAH"

_DAY_CANON = {
    "lunes": "Lunes",
    "martes": "Martes",
    "miercoles": "Miércoles",
    "jueves": "Jueves",
    "viernes": "Viernes",
    "sabado": "Sábado",
    "domingo": "Domingo",
}


# ==========================================
# NORMALIZACIÓN DE CAMPOS DEL JSON
# ==========================================

def _strip_accents(text: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFKD", str(text))
        if not unicodedata.combining(c)
    )


def _canon_day(value):
    """"LUNES" / "lunes" / "Lunes" -> "Lunes". Cualquier otra cosa se deja
    en Title case; None si viene vacío."""
    if not value:
        return None
    key = _strip_accents(value).strip().lower()
    if key in _DAY_CANON:
        return _DAY_CANON[key]
    return str(value).strip().title() or None


def _normalize_subject(name):
    """Nombre de asignatura en MAYÚSCULAS, sin acentos y con espacios
    colapsados -- para agrupar la misma asignatura escrita de varias
    formas bajo una sola fila en `subjects`."""
    if not name:
        return None
    return re.sub(r"\s+", " ", _strip_accents(name).upper()).strip() or None


def _parse_year_range(course_year):
    """"2026/2027" -> (2026, 2027). Formato inesperado -> (None, None)."""
    if not course_year:
        return None, None
    m = re.match(r"\s*(\d{4})\s*/\s*(\d{4})\s*$", str(course_year))
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))


def _leading_int(value):
    """"3ºB" -> 3, "1A" -> 1, "SA1" -> None."""
    if not value:
        return None
    m = re.match(r"\s*(\d+)", str(value))
    return int(m.group(1)) if m else None


def _clean_time(value):
    """"15:00" -> "15:00", "9:5" no válido -> None. Deja que psycopg2
    haga el cast a TIME."""
    if not value:
        return None
    m = re.match(r"\s*(\d{1,2}):(\d{2})", str(value))
    if not m:
        return None
    hh = int(m.group(1))
    if hh > 23:
        return None
    return f"{hh:02d}:{m.group(2)}"


def _as_int(value):
    return value if isinstance(value, int) else None


# La etapa LLM guarda la confianza como etiqueta ("high"/"medium"/"low"),
# no como número, pero las columnas confidence / llm_confidence son
# numeric(5,4). Traducimos la etiqueta a un valor representativo; si ya
# viene un número real se respeta; cualquier otra cosa -> NULL.
_CONFIDENCE_LEVELS = {"high": 0.9, "medium": 0.6, "low": 0.3}


def _as_numeric(value):
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    key = str(value).strip().lower()
    if key in _CONFIDENCE_LEVELS:
        return _CONFIDENCE_LEVELS[key]
    try:
        return float(key)
    except ValueError:
        return None


# ==========================================
# ESTADO COMPARTIDO CON LOS ENDPOINTS
# ==========================================

class DumpState:
    def __init__(self):
        self.lock = threading.Lock()
        self.running = False
        self.total = 0
        self.processed = 0
        self.logs = []
        self.errors = []
        self.finished_at = None
        self.counts = {}
        self.cancel_requested = False

    def add_log(self, msg):
        line = f"[{time.strftime('%H:%M:%S')}] {msg}"
        with self.lock:
            self.logs.append(line)

    def add_error(self, msg):
        line = f"[{time.strftime('%H:%M:%S')}] ERROR: {msg}"
        with self.lock:
            self.errors.append(line)
            self.logs.append(line)

    @property
    def progress_percent(self):
        if not self.total:
            return 0
        return int(self.processed / self.total * 100)

    def reset_for_run(self):
        with self.lock:
            self.running = True
            self.total = 0
            self.processed = 0
            self.logs = []
            self.errors = []
            self.finished_at = None
            self.counts = {}
            self.cancel_requested = False

    def snapshot(self):
        with self.lock:
            return {
                "running": self.running,
                "total": self.total,
                "processed": self.processed,
                "progress_percent": self.progress_percent,
                "logs": list(self.logs),
                "errors": list(self.errors),
                "finished_at": self.finished_at,
                "counts": dict(self.counts),
                "cancel_requested": self.cancel_requested,
            }


# ==========================================
# VOLCADO
# ==========================================

def run_dump(output_dir: str, get_connection, state: DumpState):
    """`output_dir`: carpeta de extracción (la que tiene
    reviewed_schedules.json). `get_connection`: callable que devuelve una
    conexión psycopg2 a la BD (se le pasa el mismo get_connection de
    main.py). `state`: DumpState que alimenta /db-dump/status."""

    state.reset_for_run()
    conn = None

    try:
        reviewed = review_store.load_reviewed(output_dir)
        if reviewed is None:
            # Lanza review_store.ReviewError si ni siquiera hay extracción.
            reviewed = review_store.ensure_reviewed(output_dir)

        all_items = reviewed.get("items", [])
        items = [
            it for it in all_items
            if it.get("origin") != "skipped"
            and it.get("status") in DUMPABLE_STATUSES
        ]
        discarded = len(all_items) - len(items)

        with state.lock:
            state.total = len(items)

        state.add_log(
            f"{len(items)} registros a volcar (VALID + WARNING). "
            f"{discarded} descartados (INVALID / tablas omitidas)."
        )

        if not items:
            state.add_log("Nada que volcar. La base de datos no se ha tocado.")
            with state.lock:
                state.counts = {"discarded": discarded}
            return

        conn = get_connection()
        conn.autocommit = False
        cur = conn.cursor()

        state.add_log("Vaciando tablas de horario y catálogos (TRUNCATE CASCADE)...")
        cur.execute("TRUNCATE TABLE " + ", ".join(TRUNCATE_TABLES) + " CASCADE;")

        cur.execute(
            "INSERT INTO universities (name, acronym) VALUES (%s, %s) RETURNING id;",
            (UNIVERSITY_NAME, UNIVERSITY_ACRONYM),
        )
        university_id = cur.fetchone()[0]

        # Cachés en memoria: primera vez que aparece un nombre -> INSERT y
        # se guarda el id; siguientes veces se reutiliza. Evita ir a la BD
        # a comprobar existencia en cada uno de los ~2500 registros.
        degrees, years, groups, subjects, rooms, sources = {}, {}, {}, {}, {}, {}

        def get_degree(name):
            if not name:
                return None
            if name not in degrees:
                cur.execute(
                    "INSERT INTO degrees (university_id, name) VALUES (%s, %s) RETURNING id;",
                    (university_id, name),
                )
                degrees[name] = cur.fetchone()[0]
            return degrees[name]

        def get_year(course_year):
            if not course_year:
                return None
            if course_year not in years:
                start, end = _parse_year_range(course_year)
                cur.execute(
                    "INSERT INTO academic_years (name, start_year, end_year) "
                    "VALUES (%s, %s, %s) RETURNING id;",
                    (str(course_year)[:20], start, end),
                )
                years[course_year] = cur.fetchone()[0]
            return years[course_year]

        def get_group(name, degree_id, year_id, semester):
            if not name:
                return None
            key = (name, degree_id, year_id)
            if key not in groups:
                cur.execute(
                    "INSERT INTO groups (degree_id, academic_year_id, name, course, semester) "
                    "VALUES (%s, %s, %s, %s, %s) RETURNING id;",
                    (degree_id, year_id, str(name)[:100], _leading_int(name), semester),
                )
                groups[key] = cur.fetchone()[0]
            return groups[key]

        def get_subject(name, degree_id, semester):
            if not name:
                return None
            norm = _normalize_subject(name)
            key = (norm, degree_id)
            if key not in subjects:
                cur.execute(
                    "INSERT INTO subjects (degree_id, name, normalized_name, semester, status) "
                    "VALUES (%s, %s, %s, %s, 'RESOLVED') RETURNING id;",
                    (degree_id, name, norm, semester),
                )
                subjects[key] = cur.fetchone()[0]
            return subjects[key]

        def get_room(code):
            if not code:
                return None
            if code not in rooms:
                cur.execute(
                    "INSERT INTO rooms (university_id, code) VALUES (%s, %s) RETURNING id;",
                    (university_id, str(code)[:100]),
                )
                rooms[code] = cur.fetchone()[0]
            return rooms[code]

        def get_source(file_name, year_id):
            key = file_name or "(origen desconocido)"
            if key not in sources:
                cur.execute(
                    "INSERT INTO sources (university_id, file_name, academic_year_id) "
                    "VALUES (%s, %s, %s) RETURNING id;",
                    (university_id, key, year_id),
                )
                sources[key] = cur.fetchone()[0]
            return sources[key]

        n_sessions = n_room_links = n_issues = 0

        for it in items:
            if state.cancel_requested:
                state.add_log("Cancelado por el usuario. Deshaciendo cambios (ROLLBACK)...")
                conn.rollback()
                return

            c = it.get("current") or {}
            ex = it.get("extraction") or {}
            src = it.get("source") or {}

            semester = _as_int(c.get("semester"))
            degree_id = get_degree(c.get("degree"))
            year_id = get_year(c.get("course_year"))
            group_id = get_group(c.get("group"), degree_id, year_id, semester)
            subject_id = get_subject(c.get("subject_name"), degree_id, semester)
            source_id = get_source(src.get("file"), year_id)

            room_codes = [str(r) for r in (c.get("rooms") or []) if r]
            annos = ex.get("non_room_annotations") or []
            class_type = str(annos[0])[:50] if annos else None

            cur.execute(
                """
                INSERT INTO class_sessions
                    (subject_id, degree_id, group_id, academic_year_id,
                     subject_name_raw, group_name_raw, room_raw,
                     day_of_week, time_start, time_end, semester,
                     class_type, status, confidence, multiple_entries_suspected)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
                """,
                (
                    subject_id, degree_id, group_id, year_id,
                    src.get("raw_text") or c.get("subject_name"),
                    c.get("group"),
                    ", ".join(room_codes) or None,
                    _canon_day(c.get("day")),
                    _clean_time(c.get("time_start")),
                    _clean_time(c.get("time_end")),
                    semester,
                    class_type,
                    it.get("status"),
                    _as_numeric(ex.get("llm_confidence")),
                    bool(c.get("multiple_entries_suspected")),
                ),
            )
            session_id = cur.fetchone()[0]
            n_sessions += 1

            for code in room_codes:
                room_id = get_room(code)
                cur.execute(
                    "INSERT INTO class_session_rooms (class_session_id, room_id) "
                    "VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                    (session_id, room_id),
                )
                n_room_links += 1

            row_span = ex.get("row_span") or []
            row_start = row_span[0] if len(row_span) > 0 else None
            row_end = row_span[1] if len(row_span) > 1 else None

            cur.execute(
                """
                INSERT INTO extraction_metadata
                    (class_session_id, source_id, page, raw_text,
                     extraction_strategy, table_index, row_start, row_end,
                     time_start_raw, time_end_raw,
                     llm_reviewed, llm_confidence, llm_note)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    session_id, source_id, _as_int(src.get("page")), src.get("raw_text"),
                    ex.get("strategy"), _as_int(ex.get("table_index")), row_start, row_end,
                    ex.get("time_start_raw"), ex.get("time_end_raw"),
                    bool(ex.get("llm_reviewed")), _as_numeric(ex.get("llm_confidence")), ex.get("llm_note"),
                ),
            )

            for issue in it.get("original_issues") or []:
                cur.execute(
                    "INSERT INTO extraction_issues (class_session_id, severity, message) "
                    "VALUES (%s, 'WARNING', %s);",
                    (session_id, str(issue)),
                )
                n_issues += 1
            for note in c.get("notes") or []:
                cur.execute(
                    "INSERT INTO extraction_issues (class_session_id, severity, message) "
                    "VALUES (%s, 'INFO', %s);",
                    (session_id, str(note)),
                )
                n_issues += 1

            with state.lock:
                state.processed += 1

        conn.commit()

        counts = {
            "universities": 1,
            "degrees": len(degrees),
            "academic_years": len(years),
            "groups": len(groups),
            "subjects": len(subjects),
            "rooms": len(rooms),
            "sources": len(sources),
            "class_sessions": n_sessions,
            "class_session_rooms": n_room_links,
            "extraction_issues": n_issues,
            "discarded": discarded,
        }
        with state.lock:
            state.counts = counts

        state.add_log(
            f"Volcado completado y confirmado (COMMIT): {n_sessions} sesiones, "
            f"{len(subjects)} asignaturas, {len(degrees)} titulaciones, "
            f"{len(rooms)} aulas, {n_issues} incidencias."
        )

    except review_store.ReviewError as e:
        state.add_error(f"No hay datos revisados que volcar: {e}")
        if conn:
            conn.rollback()
    except Exception as e:
        state.add_error(f"Volcado abortado. Se ha hecho ROLLBACK, la BD no ha cambiado: {e}")
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        with state.lock:
            state.running = False
            state.finished_at = time.strftime("%Y-%m-%d %H:%M:%S")
