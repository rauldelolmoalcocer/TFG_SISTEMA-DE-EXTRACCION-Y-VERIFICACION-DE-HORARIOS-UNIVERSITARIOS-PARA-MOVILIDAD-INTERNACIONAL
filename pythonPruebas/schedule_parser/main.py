# main.py

import os
import json

from datetime import datetime

from config import (
    INPUT_DIR,
    OUTPUT_DIR,
    GLOBAL_OUTPUT_FILE
)

from pdf_processor import (
    process_pdf
)


def find_pdf_files():

    pdf_files = []

    for root, dirs, files in os.walk(
        INPUT_DIR
    ):

        for file_name in files:

            if not file_name.lower().endswith(
                ".pdf"
            ):
                continue

            pdf_files.append(
                os.path.join(
                    root,
                    file_name
                )
            )

    return sorted(
        pdf_files
    )


def main():

    os.makedirs(
        OUTPUT_DIR,
        exist_ok=True
    )

    pdf_files = find_pdf_files()

    print(
        f"PDFs encontrados: {len(pdf_files)}"
    )

    global_records = []
    global_rejected = []

    processed_files = []
    failed_files = []

    for pdf_path in pdf_files:

        try:

            result = process_pdf(
                pdf_path
            )

            global_records.extend(
                result["records"]
            )

            global_rejected.extend(
                result["rejected_records"]
            )

            processed_files.append({

                "file":
                    result["source"]["file"],

                "pages":
                    result["source"]["pages"],

                "records":
                    len(result["records"]),

                "rejected":
                    len(
                        result[
                            "rejected_records"
                        ]
                    )
            })

        except Exception as error:

            print(
                f"ERROR procesando {pdf_path}"
            )

            print(error)

            failed_files.append({

                "file": pdf_path,
                "error": str(error)
            })

    global_result = {

        "schema_version": "2.0",

        "processed_at":
            datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            ),

        "statistics": {

            "pdfs_found":
                len(pdf_files),

            "pdfs_processed":
                len(processed_files),

            "pdfs_failed":
                len(failed_files),

            "accepted_records":
                len(global_records),

            "rejected_records":
                len(global_rejected)
        },

        "files":
            processed_files,

        "records":
            global_records,

        "rejected_records":
            global_rejected,

        "failed_files":
            failed_files
    }

    output_path = os.path.join(
        OUTPUT_DIR,
        GLOBAL_OUTPUT_FILE
    )

    with open(
        output_path,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            global_result,
            file,
            ensure_ascii=False,
            indent=4
        )

    print()
    print(
        "======================================"
    )
    print(
        "PROCESO FINALIZADO"
    )
    print(
        "======================================"
    )

    print(
        f"PDFs procesados: "
        f"{len(processed_files)}"
    )

    print(
        f"PDFs fallidos: "
        f"{len(failed_files)}"
    )

    print(
        f"Registros válidos: "
        f"{len(global_records)}"
    )

    print(
        f"Registros rechazados: "
        f"{len(global_rejected)}"
    )

    print(
        f"JSON global: {output_path}"
    )


if __name__ == "__main__":
    main()
