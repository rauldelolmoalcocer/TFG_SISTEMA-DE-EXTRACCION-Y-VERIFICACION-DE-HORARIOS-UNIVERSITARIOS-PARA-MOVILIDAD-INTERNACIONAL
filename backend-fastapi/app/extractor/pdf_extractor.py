import os
import threading
import time
import dataclasses
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from pdf_table_extractor import analyze_pdf, write_pdf_outputs, build_summary, run_normalization
from pdf_table_extractor.utils.json_utils import write_json

from app.review.store import refresh_reviewed_from_extraction


EXTRACTION_RUN_FILENAME = "extraction_run.json"


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
# ETAPA 1 (diagnóstico) CON PROGRESO POR ARCHIVO
# ==========================================
#
# pdf_table_extractor.run_diagnostics() procesa todos los PDFs de un
# tirón y solo devuelve un resumen final. Para poder informar progreso
# por archivo (ExtractorState.on_file_done, que alimenta la barra de
# progreso del panel de descargas), replicamos aquí su mismo bucle
# usando los bloques que el paquete expone justo para eso
# (analyze_pdf / write_pdf_outputs / build_summary), en vez de llamar a
# run_diagnostics() directamente.

def _run_diagnostics_with_progress(input_dir: str, output_dir: str, on_file_done=None):
    input_path = Path(input_dir)
    output_path = Path(output_dir)

    pdf_paths = sorted(input_path.glob("*.pdf")) if input_path.exists() else []

    analyses = []
    failed_files = []

    for pdf_path in pdf_paths:
        filename = pdf_path.name

        try:
            analysis = analyze_pdf(pdf_path)
            write_pdf_outputs(analysis, output_path)
            analyses.append(analysis)

            if on_file_done:
                on_file_done(filename, None)

        except Exception as error:
            failed_files.append({"file": filename, "error": str(error)})

            if on_file_done:
                on_file_done(filename, str(error))

    summary = build_summary(analyses)
    write_json(output_path / "summary.json", summary)

    return summary, failed_files, len(pdf_paths)


# ==========================================
# ETAPA 3 (opcional): REVISIÓN CON LLM LOCAL (OLLAMA)
# ==========================================
#
# Desactivada por defecto: si Ollama no está levantado, review_entries()
# ya devuelve las entradas sin tocar y sin lanzar excepción, así que el
# try/except de aquí cubre sobre todo el caso de que el extra [llm] no
# esté instalado (ImportError) u otros fallos duros de configuración.
#
# El "on/off" de cada ejecución concreta llega desde el checkbox del
# panel de descargas (enable_llm_review, ver run_extraction). Si esa
# extracción no especifica nada (llamadas antiguas, scripts, etc.), cae
# a la variable de entorno ENABLE_LLM_REVIEW como valor por defecto.

def _run_llm_review_stage(entries, output_dir: str, state: ExtractorState, enabled: bool):
    if not enabled:
        return None

    host = os.environ.get("OLLAMA_HOST", "http://localhost:11434")

    try:
        from pdf_table_extractor.llm_review import review_entries

        state.add_log(f"Etapa LLM: revisando registros con warnings vía {host}...")
        reviewed = review_entries(entries, host=host)

        output_path = Path(output_dir) / "all_schedules_reviewed.json"
        write_json(output_path, [dataclasses.asdict(entry) for entry in reviewed])

        reviewed_count = sum(1 for entry in reviewed if entry.llm_reviewed)
        state.add_log(f"Etapa LLM completada. {reviewed_count} registros revisados por el modelo.")

        return {"enabled": True, "host": host, "reviewed_count": reviewed_count}

    except Exception as error:
        state.add_log(f"Etapa LLM omitida: {error}")
        return {"enabled": True, "host": host, "error": str(error)}


# ==========================================
# BATCH RUNNER
# ==========================================

def run_extraction(input_dir: str, output_dir: str, state: ExtractorState, enable_llm_review: Optional[bool] = None):
    if enable_llm_review is None:
        enable_llm_review = os.environ.get("ENABLE_LLM_REVIEW", "false").strip().lower() == "true"

    os.makedirs(output_dir, exist_ok=True)

    pdf_files = [
        f for f in os.listdir(input_dir)
        if f.lower().endswith(".pdf")
    ] if os.path.exists(input_dir) else []

    with state.lock:
        state.running = True
        state.total_files = len(pdf_files)
        state.processed_files = 0
        state.output_files = []
        state.logs = []
        state.errors = []

    state.add_log(f"Iniciando extracción. {len(pdf_files)} PDFs encontrados.")

    def on_file_done(filename, error):
        with state.lock:
            state.current_file = filename
            state.processed_files += 1
            if not error:
                state.output_files.append(filename)

        if error:
            state.add_error(f"Error en {filename}: {error}")
        else:
            state.add_log(f"Diagnosticado → {filename}")

    try:
        _summary, failed_files, pdfs_found = _run_diagnostics_with_progress(
            input_dir, output_dir, on_file_done=on_file_done
        )

        state.add_log("Normalizando tablas detectadas...")
        entries, skipped = run_normalization(output_dir)

        entries_with_warnings = sum(1 for entry in entries if entry.warnings)

        per_file_skipped = defaultdict(int)
        for item in skipped:
            per_file_skipped[item.get("source_file")] += 1

        per_file_entries = defaultdict(int)
        for entry in entries:
            per_file_entries[entry.source_file] += 1

        for filename in sorted(set(per_file_entries) | set(per_file_skipped)):
            state.add_log(
                f"OK → {filename} "
                f"({per_file_entries.get(filename, 0)} registros, "
                f"{per_file_skipped.get(filename, 0)} tablas omitidas)"
            )

        state.add_log(
            f"Extracción finalizada. {pdfs_found - len(failed_files)}/{pdfs_found} PDFs procesados "
            f"({len(failed_files)} fallidos). Registros: {len(entries)} "
            f"(con warnings: {entries_with_warnings}). Tablas omitidas: {len(skipped)}."
        )

        llm_result = _run_llm_review_stage(entries, output_dir, state, enable_llm_review)

        run_manifest = {
            "schema_version": "3.0",
            "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "pdfs_found": pdfs_found,
            "pdfs_processed": pdfs_found - len(failed_files),
            "pdfs_failed": len(failed_files),
            "failed_files": failed_files,
            "llm_review": llm_result,
        }
        write_json(Path(output_dir) / EXTRACTION_RUN_FILENAME, run_manifest)

        try:
            refresh_reviewed_from_extraction(output_dir)
            state.add_log("reviewed_schedules.json inicializado para la nueva extracción.")
        except Exception as e:
            state.add_error(f"No se pudo inicializar reviewed_schedules.json: {e}")

    except Exception as e:
        state.add_error(f"Error general de la extracción: {e}")

    finally:
        with state.lock:
            state.running = False
            state.current_file = ""
