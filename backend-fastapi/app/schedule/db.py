# app/schedule/db.py
#
# Lectura de horarios YA volcados en PostgreSQL para el "Gestor de
# horarios" del frontend. Sustituye a los datos simulados que tenía
# horarios.js.
#
# La unidad que consume el gestor es (asignatura + grupo): una tarjeta
# por cada combinación, con su lista de sesiones (día / hora / aula).
# Varias sesiones de la misma asignatura y grupo se CONDENSAN en una sola
# entrada con varias sesiones dentro.
#
# Cualquier campo puede venir a NULL en la BD (hora, día, grupo,
# asignatura...). Las sesiones sin día u hora completos no se pueden
# pintar en la rejilla: se dejan fuera de "sessions" y se cuentan en
# "unscheduled_count" para que el frontend pueda avisar.


class ScheduleError(Exception):
    """Fallo al leer los horarios de la BD (conexión caída, consulta
    inválida...). main.py lo traduce a un HTTP 503."""


_SUBJECTS_SQL = """
    SELECT
        cs.id                                       AS session_id,
        cs.subject_id                               AS subject_id,
        COALESCE(s.name, cs.subject_name_raw)       AS subject_name,
        cs.degree_id                                AS degree_id,
        d.name                                      AS degree_name,
        COALESCE(g.name, cs.group_name_raw)         AS group_name,
        COALESCE(s.semester, cs.semester)           AS semester,
        cs.day_of_week                              AS day_of_week,
        cs.time_start                               AS time_start,
        cs.time_end                                 AS time_end,
        cs.status                                   AS status,
        COALESCE(
            NULLIF(string_agg(DISTINCT r.code, ', '), ''),
            cs.room_raw
        )                                           AS rooms
    FROM class_sessions cs
    LEFT JOIN subjects            s   ON s.id  = cs.subject_id
    LEFT JOIN degrees             d   ON d.id  = cs.degree_id
    LEFT JOIN groups              g   ON g.id  = cs.group_id
    LEFT JOIN class_session_rooms csr ON csr.class_session_id = cs.id
    LEFT JOIN rooms               r   ON r.id  = csr.room_id
    WHERE (%(degree_id)s::uuid IS NULL OR cs.degree_id = %(degree_id)s::uuid)
    GROUP BY cs.id, s.name, s.semester, d.name, g.name
"""

_DEGREES_SQL = """
    SELECT d.id, d.name, count(cs.id) AS session_count
    FROM degrees d
    LEFT JOIN class_sessions cs ON cs.degree_id = d.id
    GROUP BY d.id, d.name
    ORDER BY d.name
"""


def _fmt_time(value):
    """datetime.time -> 'HH:MM'; None -> None."""
    if value is None:
        return None
    return value.strftime("%H:%M")


def list_degrees(get_connection):
    """[{id, name, session_count}] ordenado por nombre. Solo titulaciones
    que tienen al menos una sesión volcada."""
    try:
        conn = get_connection()
    except Exception as e:
        raise ScheduleError(f"No se pudo conectar con la base de datos: {e}")

    try:
        cur = conn.cursor()
        cur.execute(_DEGREES_SQL)
        rows = cur.fetchall()
    except Exception as e:
        raise ScheduleError(f"Error consultando las titulaciones: {e}")
    finally:
        conn.close()

    return [
        {"id": str(row[0]), "name": row[1], "session_count": row[2]}
        for row in rows
        if row[2] > 0
    ]


def list_subjects(get_connection, degree_id=None):
    """Lista condensada de (asignatura + grupo) con sus sesiones.

    `degree_id`: str uuid o None (None -> todas las titulaciones).

    Devuelve [] si no hay nada; que eso sea un 404 o no lo decide main.py.
    """
    try:
        conn = get_connection()
    except Exception as e:
        raise ScheduleError(f"No se pudo conectar con la base de datos: {e}")

    try:
        cur = conn.cursor()
        cur.execute(_SUBJECTS_SQL, {"degree_id": degree_id})
        rows = cur.fetchall()
    except Exception as e:
        raise ScheduleError(f"Error consultando los horarios: {e}")
    finally:
        conn.close()

    # key -> entrada condensada
    condensed = {}

    for (session_id, subject_id, subject_name, deg_id, degree_name,
         group_name, semester, day_of_week, time_start, time_end,
         status, rooms) in rows:

        # Sin nombre de asignatura no hay nada que ofrecer al usuario.
        if not subject_name:
            continue

        subject_key = str(subject_id) if subject_id else f"raw:{subject_name.strip().lower()}"
        group_key = group_name or ""
        entry_key = f"{subject_key}|{group_key}"

        entry = condensed.get(entry_key)
        if entry is None:
            entry = {
                "id": entry_key,
                "subject_id": str(subject_id) if subject_id else None,
                "degree_id": str(deg_id) if deg_id else None,
                "degree_name": degree_name,
                "name": subject_name,
                "group": group_name,
                "semester": semester,
                "sessions": [],
                "unscheduled_count": 0,
            }
            condensed[entry_key] = entry

        start = _fmt_time(time_start)
        end = _fmt_time(time_end)

        # Sin día u hora completos no se puede colocar en la rejilla.
        if not day_of_week or not start or not end:
            entry["unscheduled_count"] += 1
            continue

        entry["sessions"].append({
            "day": day_of_week,
            "start": start,
            "end": end,
            "classroom": rooms or "",
            "status": status,
        })

    result = list(condensed.values())
    result.sort(key=lambda e: (
        (e["degree_name"] or ""),
        (e["name"] or ""),
        (e["group"] or ""),
    ))
    return result
