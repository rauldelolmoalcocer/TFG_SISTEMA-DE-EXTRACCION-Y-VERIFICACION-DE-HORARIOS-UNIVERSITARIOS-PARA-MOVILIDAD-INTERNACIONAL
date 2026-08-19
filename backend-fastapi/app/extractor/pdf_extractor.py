import os
import threading
import time
from dataclasses import dataclass, field

from app.extractor.schedule_parser.pipeline import run_pipeline
from app.review.store import refresh_reviewed_from_extraction


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
# BATCH RUNNER
# ==========================================

def run_extraction(input_dir: str, output_dir: str, state: ExtractorState):
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

    def on_file_done(filename, accepted, rejected, error):
        with state.lock:
            state.current_file = filename
            state.processed_files += 1
            if not error:
                state.output_files.append(
                    os.path.splitext(filename)[0] + ".json"
                )

        if error:
            state.add_error(f"Error en {filename}: {error}")
        else:
            state.add_log(
                f"OK → {filename} "
                f"({accepted} registros, {rejected} rechazados)"
            )

    try:
        result = run_pipeline(input_dir, output_dir, on_file_done=on_file_done)

        stats = result["statistics"]
        state.add_log(
            f"Extracción finalizada. {stats['pdfs_processed']}/{stats['pdfs_found']} PDFs procesados "
            f"({stats['pdfs_failed']} fallidos). Registros válidos: {stats['accepted_records']} "
            f"(con warnings: {stats['records_with_warnings']}). Rechazados: {stats['rejected_records']}. "
            f"JSON global: all_schedules.json"
        )

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
