from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import psycopg2
import os
import threading
import time

from app.crawler.crawler import CrawlerConfig, CrawlerState, PdfCrawler
from app.extractor.pdf_extractor import ExtractorState, run_extraction


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
def start_extraction():
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

    extract_thread = threading.Thread(
        target=run_extraction,
        args=(DOWNLOAD_FOLDER, EXTRACT_FOLDER, extract_state),
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