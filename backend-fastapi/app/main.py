from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import psycopg2
import os
import requests
import threading
import time
from typing import Optional

from app.crawler.crawler import CrawlerConfig, CrawlerState, PdfCrawler
from app.extractor.pdf_extractor import ExtractorState, run_extraction
from app.review import store as review_store
from app.dbdump import loader as db_dump_loader
from app.schedule import db as schedule_db
from app import ai_settings


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================================================
# MODELOS
# =========================================================

class LoginRequest(BaseModel):
    username: str
    password: str


class DownloadRequest(BaseModel):
    url: str


class ExtractStartRequest(BaseModel):
    # None -> usa ENABLE_LLM_REVIEW del entorno como valor por defecto
    # (ver run_extraction). true/false -> lo fuerza para esta extracción,
    # sea cual sea la variable de entorno.
    enable_llm_review: Optional[bool] = None


class RecordEditRequest(BaseModel):
    degree: Optional[str] = None
    course_year: Optional[str] = None
    semester: Optional[int] = None
    group: Optional[str] = None
    day: Optional[str] = None
    time_start: Optional[str] = None
    time_end: Optional[str] = None
    subject_name: Optional[str] = None
    rooms: list[str] = []
    multiple_entries_suspected: bool = False
    notes: list[str] = []


class LlmReviewRequest(BaseModel):
    # None -> todos los registros con asignaturas mezcladas sin resolver.
    # Una lista -> solo esos ids (revisión de un único registro).
    item_ids: Optional[list[str]] = None


class AiSettingsRequest(BaseModel):
    ollama_host: str
    model: Optional[str] = None


class RecordStatusRequest(BaseModel):
    status: str


# =========================================================
# BASE DE DATOS
# =========================================================

def get_connection():
    return psycopg2.connect(
        host="db",
        dbname="tfg",
        user="postgres",
        password="postgres",
        port=5432,
    )


# =========================================================
# CONFIG DESCARGAS
# =========================================================

DOWNLOAD_FOLDER = "/data/downloaded_pdfs"
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

EXTRACT_FOLDER = "/data/extracted_json"
os.makedirs(EXTRACT_FOLDER, exist_ok=True)

extract_state = ExtractorState()
extract_lock = threading.Lock()
extract_thread = None

review_lock = threading.Lock()

db_dump_state = db_dump_loader.DumpState()
db_dump_lock = threading.Lock()
db_dump_thread = None

llm_review_lock = threading.Lock()
llm_review_thread = None
llm_review_state = {
    "running": False,
    "paused": False,
    "attempted": 0,
    "total": 0,
    "resolved": 0,
    "split": 0,
    "errors": [],
    "logs": [],
}

# Coordinan pausa/cancelación del bucle en _run_llm_review_background sin
# interrumpir una llamada a Ollama en curso (eso no es seguro -- para eso
# está el timeout en review_store.OLLAMA_CALL_TIMEOUT_SECONDS). resume_event
# SET = corriendo con normalidad, CLEAR = pausado (el bucle espera aquí).
# cancel_event SET = parar en el próximo punto de control entre registros.
llm_review_resume_event = threading.Event()
llm_review_resume_event.set()
llm_review_cancel_event = threading.Event()


def _llm_review_should_continue() -> bool:
    llm_review_resume_event.wait()
    return not llm_review_cancel_event.is_set()

download_state = {
    "running": False,
    "paused": False,
    "logs": [],
    "errors": [],
    "files": [],
    "total_pages_crawled": 0,
    "total_pdfs_found": 0,
    "total_pdfs_downloaded": 0,
    "last_activity": None,
}

state_lock = threading.Lock()
download_thread = None
current_crawler_state: CrawlerState | None = None


def refresh_files():
    files = []

    if os.path.exists(DOWNLOAD_FOLDER):
        for filename in os.listdir(DOWNLOAD_FOLDER):
            if filename.lower().endswith(".pdf"):
                files.append(filename)

    files.sort()

    with state_lock:
        download_state["files"] = files


def sync_state_from_crawler(crawler_state: CrawlerState):
    with crawler_state.lock:
        logs = list(crawler_state.logs)
        errors = list(crawler_state.errors)
        total_pages_crawled = crawler_state.total_pages_crawled
        total_pdfs_found = crawler_state.total_pdfs_found
        total_pdfs_downloaded = crawler_state.total_pdfs_downloaded

    refresh_files()

    with state_lock:
        download_state["logs"] = logs
        download_state["errors"] = errors
        download_state["total_pages_crawled"] = total_pages_crawled
        download_state["total_pdfs_found"] = total_pdfs_found
        download_state["total_pdfs_downloaded"] = total_pdfs_downloaded
        download_state["last_activity"] = time.strftime("%H:%M:%S")


def real_download_process(url: str):
    global current_crawler_state

    crawler_state = CrawlerState()

    with state_lock:
        current_crawler_state = crawler_state
        download_state["running"] = True
        download_state["paused"] = False
        download_state["logs"] = []
        download_state["errors"] = []
        download_state["files"] = []
        download_state["total_pages_crawled"] = 0
        download_state["total_pdfs_found"] = 0
        download_state["total_pdfs_downloaded"] = 0
        download_state["last_activity"] = time.strftime("%H:%M:%S")

    original_add_log = crawler_state.add_log
    original_add_error = crawler_state.add_error

    def bridged_add_log(message: str):
        original_add_log(message)
        sync_state_from_crawler(crawler_state)

    def bridged_add_error(message: str):
        original_add_error(message)
        sync_state_from_crawler(crawler_state)

    crawler_state.add_log = bridged_add_log
    crawler_state.add_error = bridged_add_error

    try:
        config = CrawlerConfig(
            start_url=url,
            download_folder=DOWNLOAD_FOLDER,
            max_depth=2,
            max_pages=50,
            max_download_workers=4,
            request_timeout=15,
            delay_between_requests=0.8,
            same_domain_only=True,
            verify_ssl=True,
            overwrite_files=False
        )

        crawler = PdfCrawler(config, crawler_state)
        crawler.run()

    except Exception as e:
        with state_lock:
            download_state["logs"].append(f"Error general del crawler: {str(e)}")
            download_state["last_activity"] = time.strftime("%H:%M:%S")

    finally:
        try:
            crawler_state.add_log(
                f"Crawler finalizado. Páginas rastreadas: {crawler_state.total_pages_crawled}, "
                f"PDFs encontrados: {crawler_state.total_pdfs_found}, "
                f"PDFs descargados: {crawler_state.total_pdfs_downloaded}"
            )
        except Exception:
            pass

        sync_state_from_crawler(crawler_state)

        with state_lock:
            current_crawler_state = None
            download_state["running"] = False
            download_state["paused"] = False
            download_state["last_activity"] = time.strftime("%H:%M:%S")


# =========================================================
# ENDPOINTS EXISTENTES
# =========================================================

@app.get("/demo")
def demo():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT mensaje FROM demo LIMIT 1;")
    row = cur.fetchone()

    cur.close()
    conn.close()

    return {"mensaje": row[0] if row else "Sin datos"}


@app.post("/login")
def login(data: LoginRequest):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        "SELECT id, username FROM usuarios WHERE username = %s AND password = %s",
        (data.username, data.password)
    )
    user = cur.fetchone()

    cur.close()
    conn.close()

    if user:
        return {
            "success": True,
            "user": {
                "id": user[0],
                "username": user[1]
            }
        }

    return {
        "success": False,
        "message": "Usuario o contraseña incorrectos"
    }


# =========================================================
# ENDPOINTS DE DESCARGA
# =========================================================

@app.post("/download/start")
def start_download(data: DownloadRequest):
    global download_thread

    url = data.url.strip()

    if not url:
        return {
            "success": False,
            "message": "La URL está vacía"
        }

    if not url.startswith(("http://", "https://")):
        return {
            "success": False,
            "message": "La URL debe empezar por http:// o https://"
        }

    with state_lock:
        if download_state["running"]:
            return {
                "success": False,
                "message": "Ya hay un proceso en ejecución"
            }

    download_thread = threading.Thread(
        target=real_download_process,
        args=(url,),
        daemon=True
    )
    download_thread.start()

    return {
        "success": True,
        "message": "Crawler iniciado"
    }


@app.post("/download/pause")
def pause_download():
    with state_lock:
        if not download_state["running"]:
            return {"success": False, "message": "No hay proceso en ejecución"}
        if download_state["paused"]:
            return {"success": False, "message": "El proceso ya está pausado"}
        download_state["paused"] = True

    if current_crawler_state:
        current_crawler_state.pause_event.clear()
        current_crawler_state.add_log("Proceso pausado por el usuario.")

    return {"success": True, "message": "Proceso pausado"}


@app.post("/download/resume")
def resume_download():
    with state_lock:
        if not download_state["running"]:
            return {"success": False, "message": "No hay proceso en ejecución"}
        if not download_state["paused"]:
            return {"success": False, "message": "El proceso no está pausado"}
        download_state["paused"] = False

    if current_crawler_state:
        current_crawler_state.pause_event.set()
        current_crawler_state.add_log("Proceso reanudado por el usuario.")

    return {"success": True, "message": "Proceso reanudado"}


@app.get("/download/status")
def download_status():
    refresh_files()

    with state_lock:
        return {
            "running": download_state["running"],
            "paused": download_state["paused"],
            "thread_alive": download_thread.is_alive() if download_thread else False,
            "last_activity": download_state["last_activity"],
            "logs": download_state["logs"],
            "errors": download_state["errors"],
            "files": download_state["files"],
            "total_pages_crawled": download_state["total_pages_crawled"],
            "total_pdfs_found": download_state["total_pdfs_found"],
            "total_pdfs_downloaded": download_state["total_pdfs_downloaded"],
        }


# =========================================================
# SERVIR PDF INDIVIDUAL
# =========================================================

@app.get("/download/file/{filename:path}")
def download_file(filename: str):
    file_path = os.path.join(DOWNLOAD_FOLDER, filename)

    if not os.path.isfile(file_path):
        raise HTTPException(status_code=404, detail="PDF no encontrado")

    return FileResponse(
        path=file_path,
        media_type="application/pdf",
        filename=filename
    )


# =========================================================
# ENDPOINTS DE EXTRACCIÓN
# =========================================================

@app.post("/extract/start")
def start_extraction(data: ExtractStartRequest = ExtractStartRequest()):
    global extract_thread

    with extract_lock:
        if extract_state.running:
            return {"success": False, "message": "Ya hay una extracción en curso"}

    pdf_files = [
        f for f in os.listdir(DOWNLOAD_FOLDER)
        if f.lower().endswith(".pdf")
    ] if os.path.exists(DOWNLOAD_FOLDER) else []

    if not pdf_files:
        return {"success": False, "message": "No hay PDFs en la carpeta de descargas"}

    ai_conf = ai_settings.load_ai_settings(EXTRACT_FOLDER)

    extract_thread = threading.Thread(
        target=run_extraction,
        args=(DOWNLOAD_FOLDER, EXTRACT_FOLDER, extract_state, data.enable_llm_review, ai_conf["ollama_host"], ai_conf["model"]),
        daemon=True,
    )
    extract_thread.start()

    return {"success": True, "message": f"Extracción iniciada sobre {len(pdf_files)} PDFs"}


@app.get("/extract/status")
def extraction_status():
    with extract_state.lock:
        return {
            "running": extract_state.running,
            "total_files": extract_state.total_files,
            "processed_files": extract_state.processed_files,
            "progress_percent": extract_state.progress_percent,
            "current_file": extract_state.current_file,
            "output_files": list(extract_state.output_files),
            "logs": list(extract_state.logs),
            "errors": list(extract_state.errors),
        }


# =========================================================
# VOLCADO A BASE DE DATOS
# =========================================================
#
# Coge reviewed_schedules.json (la copia YA revisada de la extracción) y
# reconstruye las tablas relacionales de Postgres desde cero (vaciar y
# recargar, todo en una transacción). Corre en segundo plano como la
# extracción; el progreso se consulta con /db-dump/status.

@app.post("/db-dump/start")
def db_dump_start():
    global db_dump_thread

    with db_dump_lock:
        if db_dump_state.running:
            return {"success": False, "message": "Ya hay un volcado en curso."}
        db_dump_state.running = True  # cerrar la ventana entre este check y el arranque del hilo

    db_dump_thread = threading.Thread(
        target=db_dump_loader.run_dump,
        args=(EXTRACT_FOLDER, get_connection, db_dump_state),
        daemon=True,
    )
    db_dump_thread.start()

    return {"success": True, "message": "Volcado a base de datos iniciado."}


@app.get("/db-dump/status")
def db_dump_status():
    return db_dump_state.snapshot()


@app.post("/db-dump/cancel")
def db_dump_cancel():
    with db_dump_state.lock:
        if not db_dump_state.running:
            return {"success": False, "message": "No hay ningún volcado en curso."}
        db_dump_state.cancel_requested = True
    return {"success": True, "message": "Cancelando volcado (se deshace lo hecho hasta ahora)."}


# =========================================================
# GESTOR DE HORARIOS: DATOS REALES DESDE LA BD
# =========================================================
#
# Sustituyen a los datos simulados de horarios.js. Leen de las tablas que
# rellena el volcado. Si la BD no responde -> 503; si no hay ni un solo
# horario volcado -> 404 (para que el frontend diga "ejecuta el volcado
# primero" en vez de mostrar una pantalla vacía sin explicación).

@app.get("/schedule/degrees")
def schedule_degrees():
    try:
        degrees = schedule_db.list_degrees(get_connection)
    except schedule_db.ScheduleError as e:
        raise HTTPException(status_code=503, detail=str(e))

    if not degrees:
        raise HTTPException(
            status_code=404,
            detail="No hay horarios en la base de datos. Ejecuta el volcado primero.",
        )
    return degrees


@app.get("/schedule/subjects")
def schedule_subjects(degree_id: Optional[str] = None):
    try:
        subjects = schedule_db.list_subjects(get_connection, degree_id or None)
    except schedule_db.ScheduleError as e:
        raise HTTPException(status_code=503, detail=str(e))

    if not subjects:
        raise HTTPException(
            status_code=404,
            detail=(
                "No hay asignaturas para esa titulación en la base de datos."
                if degree_id else
                "No hay horarios en la base de datos. Ejecuta el volcado primero."
            ),
        )
    return subjects


# =========================================================
# ENDPOINTS DE REVISIÓN MANUAL
# =========================================================
#
# Trabajan siempre sobre reviewed_schedules.json (copia editable).
# all_schedules.json (resultado original de la extracción) nunca se
# modifica desde aquí, solo se lee para construir/reconstruir la
# copia editable.

def _require_reviewed():
    """Carga reviewed_schedules.json o lanza 404 si todavía no existe
    ninguna extracción (o falló antes de generar all_schedules.json)."""
    reviewed = review_store.load_reviewed(EXTRACT_FOLDER)
    if reviewed is None:
        try:
            reviewed = review_store.ensure_reviewed(EXTRACT_FOLDER)
        except review_store.ReviewError:
            raise HTTPException(
                status_code=404,
                detail="Todavía no hay ninguna extracción con resultados que revisar.",
            )
    return reviewed


@app.get("/review/summary")
def review_summary():
    summary = review_store.compute_summary(EXTRACT_FOLDER)
    if summary is None:
        raise HTTPException(
            status_code=404,
            detail="Todavía no hay ninguna extracción con resultados que revisar.",
        )
    return summary


@app.get("/review/filters")
def review_filters():
    reviewed = _require_reviewed()
    return review_store.compute_filter_options(reviewed["items"])


@app.get("/review/tree")
def review_tree(
    status: Optional[str] = None,
    degree: Optional[str] = None,
    course_year: Optional[str] = None,
    semester: Optional[str] = None,
    group: Optional[str] = None,
    day: Optional[str] = None,
    pdf: Optional[str] = None,
    issue: Optional[str] = None,
    llm: Optional[str] = None,
    search: Optional[str] = None,
):
    reviewed = _require_reviewed()
    filters = {
        "status": status, "degree": degree, "course_year": course_year, "semester": semester,
        "group": group, "day": day, "pdf": pdf, "issue": issue, "llm": llm, "search": search,
    }
    filtered = review_store.apply_filters(reviewed["items"], filters)
    return {"tree": review_store.compute_tree(filtered), "total": len(filtered)}


@app.get("/review/records")
def review_records(
    page: int = 1,
    page_size: int = 50,
    status: Optional[str] = None,
    degree: Optional[str] = None,
    course_year: Optional[str] = None,
    semester: Optional[str] = None,
    group: Optional[str] = None,
    day: Optional[str] = None,
    pdf: Optional[str] = None,
    issue: Optional[str] = None,
    llm: Optional[str] = None,
    search: Optional[str] = None,
):
    reviewed = _require_reviewed()
    filters = {
        "status": status, "degree": degree, "course_year": course_year, "semester": semester,
        "group": group, "day": day, "pdf": pdf, "issue": issue, "llm": llm, "search": search,
    }
    filtered = review_store.apply_filters(reviewed["items"], filters)
    ordered = review_store.sort_items(filtered)
    page_items, total = review_store.paginate(ordered, page, page_size)

    return {
        "items": page_items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if page_size else 0,
    }


@app.get("/review/records/{item_id}")
def review_record_detail(item_id: str):
    reviewed = _require_reviewed()
    item = review_store.find_item(reviewed, item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Registro no encontrado.")
    return item


@app.put("/review/records/{item_id}")
def review_record_edit(item_id: str, payload: RecordEditRequest):
    with review_lock:
        reviewed = _require_reviewed()
        item = review_store.find_item(reviewed, item_id)
        if item is None:
            raise HTTPException(status_code=404, detail="Registro no encontrado.")

        try:
            review_store.apply_edit(item, payload.model_dump())
        except review_store.ReviewError as e:
            raise HTTPException(status_code=400, detail=str(e))

        review_store.save_reviewed(EXTRACT_FOLDER, reviewed)
        return {"success": True, "message": "Cambios guardados.", "item": item}


@app.post("/review/records/{item_id}/status")
def review_record_status(item_id: str, payload: RecordStatusRequest):
    with review_lock:
        reviewed = _require_reviewed()
        item = review_store.find_item(reviewed, item_id)
        if item is None:
            raise HTTPException(status_code=404, detail="Registro no encontrado.")

        try:
            review_store.set_status(item, payload.status)
        except review_store.ReviewError as e:
            raise HTTPException(status_code=400, detail=str(e))

        review_store.save_reviewed(EXTRACT_FOLDER, reviewed)
        return {"success": True, "message": "Estado actualizado.", "item": item}


@app.post("/review/records/{item_id}/restore")
def review_record_restore(item_id: str):
    with review_lock:
        reviewed = _require_reviewed()
        item = review_store.find_item(reviewed, item_id)
        if item is None:
            raise HTTPException(status_code=404, detail="Registro no encontrado.")

        review_store.restore_item(item)

        review_store.save_reviewed(EXTRACT_FOLDER, reviewed)
        return {"success": True, "message": "Registro restaurado al original.", "item": item}


@app.post("/review/records/{item_id}/duplicate")
def review_record_duplicate(item_id: str):
    with review_lock:
        reviewed = _require_reviewed()

        try:
            new_item = review_store.duplicate_item(reviewed, item_id)
        except review_store.ReviewError as e:
            raise HTTPException(status_code=404, detail=str(e))

        review_store.save_reviewed(EXTRACT_FOLDER, reviewed)
        return {"success": True, "message": "Registro duplicado.", "item": new_item}


# =========================================================
# REVISIÓN CON IA BAJO DEMANDA (uno o todos los registros)
# =========================================================
#
# Puede tardar bastante (un registro por llamada a Ollama) así que corre
# en segundo plano igual que la extracción o el crawler. Mantiene
# review_lock tomado durante TODO el proceso -- no solo para leer/guardar
# -- para que ninguna edición manual concurrente se pise con esto; a
# cambio, mientras esté corriendo, guardar una edición de OTRO registro
# esperará a que termine (aceptable: en este proyecto solo hay un
# revisor a la vez).
#
# Guarda reviewed_schedules.json después de CADA registro (no solo al
# final): si Ollama se cuelga o el proceso se corta a mitad, lo ya hecho
# hasta ese punto no se pierde. Antes no era así y nos costó perder un
# lote entero -- ver review_store.OLLAMA_CALL_TIMEOUT_SECONDS para el
# otro lado de ese mismo arreglo (el timeout que evita que se cuelgue).

def _run_llm_review_background(item_ids):
    ai_conf = ai_settings.load_ai_settings(EXTRACT_FOLDER)
    host = ai_conf["ollama_host"]
    model = ai_conf["model"]

    llm_review_resume_event.set()
    llm_review_cancel_event.clear()

    with llm_review_lock:
        llm_review_state["running"] = True
        llm_review_state["paused"] = False
        llm_review_state["attempted"] = 0
        llm_review_state["total"] = 0
        llm_review_state["resolved"] = 0
        llm_review_state["split"] = 0
        llm_review_state["errors"] = []
        llm_review_state["logs"] = [f"Iniciando revisión con IA ({model}) vía {host}..."]

    try:
        with review_lock:
            reviewed = _require_reviewed()

            def on_item_done(done, total, item_id, outcome):
                # Ya estamos dentro del review_lock de arriba -- no hace
                # falta (ni se puede, threading.Lock no es reentrante)
                # volver a adquirirlo aquí.
                review_store.save_reviewed(EXTRACT_FOLDER, reviewed)
                with llm_review_lock:
                    llm_review_state["attempted"] = done
                    llm_review_state["total"] = total
                    llm_review_state["logs"].append(f"[{done}/{total}] {item_id}: {outcome}")

            result = review_store.llm_review_items(
                reviewed, item_ids, host, model,
                on_item_done=on_item_done,
                should_continue=_llm_review_should_continue,
            )
            review_store.save_reviewed(EXTRACT_FOLDER, reviewed)

        with llm_review_lock:
            llm_review_state["resolved"] = result["resolved"]
            llm_review_state["split"] = result["split"]
            llm_review_state["errors"] = result["errors"]
            nota = " (cancelado por el usuario)" if result.get("cancelled") else ""
            llm_review_state["logs"].append(
                f"Terminado{nota}. {result['resolved']} resueltos, {result['split']} separados en varios registros, "
                f"{len(result['errors'])} con error."
            )

    except Exception as e:
        with llm_review_lock:
            llm_review_state["errors"].append({"id": None, "error": str(e)})
            llm_review_state["logs"].append(f"Error general: {e}")

    finally:
        with llm_review_lock:
            llm_review_state["running"] = False
            llm_review_state["paused"] = False


@app.post("/review/llm-review")
def start_llm_review(data: LlmReviewRequest = LlmReviewRequest()):
    global llm_review_thread

    with llm_review_lock:
        if llm_review_state["running"]:
            return {"success": False, "message": "Ya hay una revisión con IA en curso."}

    try:
        _require_reviewed()
    except review_store.ReviewError:
        raise HTTPException(
            status_code=404,
            detail="Todavía no hay ninguna extracción con resultados que revisar.",
        )

    llm_review_thread = threading.Thread(
        target=_run_llm_review_background,
        args=(data.item_ids,),
        daemon=True,
    )
    llm_review_thread.start()

    return {"success": True, "message": "Revisión con IA iniciada."}


@app.get("/review/llm-review/status")
def llm_review_status():
    with llm_review_lock:
        return dict(llm_review_state)


@app.post("/review/llm-review/pause")
def pause_llm_review():
    with llm_review_lock:
        if not llm_review_state["running"]:
            return {"success": False, "message": "No hay ninguna revisión con IA en curso."}
        llm_review_state["paused"] = True
        llm_review_state["logs"].append("Pausado por el usuario (se pausa entre registros, nunca a mitad de una llamada).")
    llm_review_resume_event.clear()
    return {"success": True, "message": "Revisión pausada."}


@app.post("/review/llm-review/resume")
def resume_llm_review():
    with llm_review_lock:
        if not llm_review_state["running"]:
            return {"success": False, "message": "No hay ninguna revisión con IA en curso."}
        llm_review_state["paused"] = False
        llm_review_state["logs"].append("Reanudado por el usuario.")
    llm_review_resume_event.set()
    return {"success": True, "message": "Revisión reanudada."}


@app.post("/review/llm-review/cancel")
def cancel_llm_review():
    with llm_review_lock:
        if not llm_review_state["running"]:
            return {"success": False, "message": "No hay ninguna revisión con IA en curso."}
        llm_review_state["logs"].append("Cancelación solicitada por el usuario...")
    llm_review_cancel_event.set()
    llm_review_resume_event.set()  # por si estaba pausado, para que pueda salir del bucle y ver la cancelación
    return {"success": True, "message": "Cancelando (se detiene tras el registro en curso, lo hecho hasta ahora ya está guardado)."}


# =========================================================
# ADMINISTRACIÓN DE IA (Ollama): dirección + estado de conexión
# =========================================================

@app.get("/ai/settings")
def get_ai_settings():
    return ai_settings.load_ai_settings(EXTRACT_FOLDER)


@app.put("/ai/settings")
def update_ai_settings(data: AiSettingsRequest):
    try:
        return ai_settings.save_ai_settings(EXTRACT_FOLDER, data.ollama_host, data.model)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/ai/status")
def ai_status():
    host = ai_settings.load_ai_settings(EXTRACT_FOLDER)["ollama_host"]

    start = time.monotonic()
    try:
        response = requests.get(f"{host.rstrip('/')}/api/tags", timeout=5)
        ping_ms = round((time.monotonic() - start) * 1000)

        if response.status_code != 200:
            return {
                "connected": False,
                "host": host,
                "ping_ms": ping_ms,
                "error": f"Ollama respondió con HTTP {response.status_code}",
            }

        data = response.json()
        models = [m.get("name") for m in data.get("models", []) if m.get("name")]

        return {"connected": True, "host": host, "ping_ms": ping_ms, "models": models}

    except Exception as e:
        ping_ms = round((time.monotonic() - start) * 1000)
        return {"connected": False, "host": host, "ping_ms": ping_ms, "error": str(e)}