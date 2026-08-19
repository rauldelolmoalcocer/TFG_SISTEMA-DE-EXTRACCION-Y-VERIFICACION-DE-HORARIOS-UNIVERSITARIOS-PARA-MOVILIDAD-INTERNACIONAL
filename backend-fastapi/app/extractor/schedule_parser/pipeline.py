# pipeline.py
#
# Punto de entrada del algoritmo de extracción (schedule_parser)
# adaptado para ejecutarse dentro del backend, sobre las carpetas
# ya usadas por el proyecto (DOWNLOAD_FOLDER / EXTRACT_FOLDER) en
# lugar de las rutas absolutas hardcodeadas del script standalone
# original. La lógica de parsing/validación (pdf_processor,
# layout_parser, subject_parser, normalizer, validator) no se
# modifica.

import os
import json

from datetime import datetime

from .pdf_processor import process_pdf


GLOBAL_OUTPUT_FILE = "all_schedules.json"


def find_pdf_files(input_dir):

    pdf_files = []

    for root, dirs, files in os.walk(input_dir):

        for file_name in files:

            if not file_name.lower().endswith(".pdf"):
                continue

            pdf_files.append(
                os.path.join(root, file_name)
            )

    return sorted(pdf_files)


def run_pipeline(input_dir, output_dir, on_file_done=None):
    """Procesa todos los PDFs de ``input_dir`` y escribe el JSON global
    ``all_schedules.json`` en ``output_dir``.

    ``on_file_done`` (opcional) se invoca tras procesar cada PDF con
    ``(filename, accepted, rejected, error)`` para permitir reportar
    progreso al llamador (p.ej. ExtractorState) sin acoplar este
    módulo a ningún mecanismo de logging concreto.

    Devuelve el diccionario ``all_schedules.json`` ya generado.
    """

    os.makedirs(output_dir, exist_ok=True)

    pdf_files = find_pdf_files(input_dir)

    global_records = []
    global_rejected = []

    processed_files = []
    failed_files = []

    for pdf_path in pdf_files:

        filename = os.path.basename(pdf_path)

        try:

            result = process_pdf(pdf_path, output_dir)

            global_records.extend(result["records"])
            global_rejected.extend(result["rejected_records"])

            processed_files.append({
                "file": result["source"]["file"],
                "pages": result["source"]["pages"],
                "records": len(result["records"]),
                "rejected": len(result["rejected_records"])
            })

            if on_file_done:
                on_file_done(
                    filename,
                    len(result["records"]),
                    len(result["rejected_records"]),
                    None
                )

        except Exception as error:

            failed_files.append({
                "file": filename,
                "error": str(error)
            })

            if on_file_done:
                on_file_done(filename, 0, 0, str(error))

    global_result = {

        "schema_version": "2.0",

        "processed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),

        "statistics": {

            "pdfs_found": len(pdf_files),
            "pdfs_processed": len(processed_files),
            "pdfs_failed": len(failed_files),

            "accepted_records": len(global_records),
            "rejected_records": len(global_rejected),

            "records_with_warnings": sum(
                1
                for record in global_records
                if record["extraction"]["issues"]
            )
        },

        "files": processed_files,

        "records": global_records,

        "rejected_records": global_rejected,

        "failed_files": failed_files
    }

    output_path = os.path.join(output_dir, GLOBAL_OUTPUT_FILE)
    tmp_path = output_path + ".tmp"

    with open(tmp_path, "w", encoding="utf-8") as file:
        json.dump(global_result, file, ensure_ascii=False, indent=4)

    os.replace(tmp_path, output_path)

    return global_result
