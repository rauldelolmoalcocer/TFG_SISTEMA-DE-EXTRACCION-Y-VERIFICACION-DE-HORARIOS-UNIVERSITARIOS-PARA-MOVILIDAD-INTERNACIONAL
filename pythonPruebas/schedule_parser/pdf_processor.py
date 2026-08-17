# pdf_processor.py

import os
import json
import pymupdf

from metadata_parser import extract_metadata

from layout_parser import (
    build_schedule_cells
)

from subject_parser import (
    parse_subjects
)

from normalizer import (
    normalize_record
)

from validator import (
    validate_record
)

from config import OUTPUT_DIR


def process_page(
    page,
    page_number,
    pdf_path
):

    page_text = page.get_text(
        "text"
    )

    metadata = extract_metadata(
        page_text,
        pdf_path
    )

    cells = build_schedule_cells(
        page
    )

    accepted_records = []
    rejected_records = []

    for cell in cells:

        subjects = parse_subjects(
            cell["text"]
        )

        # Si la celda no se ha podido
        # interpretar, la guardamos para debug.

        if not subjects:

            rejected_records.append({

                "reason": [
                    "subject_not_parsed"
                ],

                "source": {
                    "file": os.path.basename(
                        pdf_path
                    ),
                    "page": page_number
                },

                "schedule": {
                    "day": cell["dia"],
                    "start_time": cell["hora_inicio"],
                    "end_time": cell["hora_fin"]
                },

                "raw_text": cell["text"]
            })

            continue

        for subject in subjects:

            raw_record = {

                **metadata,

                "dia": cell["dia"],
                "hora_inicio": cell["hora_inicio"],
                "hora_fin": cell["hora_fin"],

                **subject,

                "archivo": os.path.basename(
                    pdf_path
                ),

                "pagina": page_number
            }

            normalized = normalize_record(
                raw_record
            )

            validation = validate_record(
                normalized
            )

            if validation["accepted"]:

                accepted_records.append(
                    validation["record"]
                )

            else:

                rejected_records.append({

                    "reason": validation[
                        "errors"
                    ],

                    "record": validation[
                        "record"
                    ]
                })

    return {
        "accepted": accepted_records,
        "rejected": rejected_records,
        "cells_detected": len(cells)
    }


def process_pdf(pdf_path):

    print()
    print(
        f"Procesando PDF: {pdf_path}"
    )

    document = pymupdf.open(
        pdf_path
    )

    all_records = []
    rejected_records = []

    page_stats = []

    for page_number, page in enumerate(
        document,
        start=1
    ):

        result = process_page(
            page,
            page_number,
            pdf_path
        )

        all_records.extend(
            result["accepted"]
        )

        rejected_records.extend(
            result["rejected"]
        )

        page_stats.append({

            "page": page_number,

            "cells_detected":
                result["cells_detected"],

            "accepted":
                len(result["accepted"]),

            "rejected":
                len(result["rejected"])
        })

    result = {

        "source": {

            "file": os.path.basename(
                pdf_path
            ),

            "path": pdf_path,

            "pages": len(document)
        },

        "statistics": {

            "accepted_records":
                len(all_records),

            "rejected_records":
                len(rejected_records),

            "pages": page_stats
        },

        "records": all_records,

        "rejected_records":
            rejected_records
    }

    base_name = os.path.splitext(
        os.path.basename(pdf_path)
    )[0]

    output_path = os.path.join(
        OUTPUT_DIR,
        f"{base_name}.json"
    )

    with open(
        output_path,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            result,
            file,
            ensure_ascii=False,
            indent=4
        )

    print(
        f"  OK: {len(all_records)}"
    )

    print(
        f"  Rechazados: {len(rejected_records)}"
    )

    return result
