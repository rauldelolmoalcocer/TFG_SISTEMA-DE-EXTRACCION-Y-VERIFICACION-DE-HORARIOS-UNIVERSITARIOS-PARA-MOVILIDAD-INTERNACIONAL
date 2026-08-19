# normalizer.py

from text_utils import (
    clean_text,
    normalize_comparison_text,
    normalize_hour,
    normalize_group,
    normalize_classroom
)


def normalize_subject_name(value):

    if not value:
        return None

    value = clean_text(value)

    value = value.strip(" -*")

    return value.upper()


def normalize_record(record):

    result = {

        "degree": {

            "code": record.get(
                "codigo_titulacion"
            ),

            "name": record.get(
                "titulacion"
            )
        },

        "academic_year": record.get(
            "curso_academico"
        ),

        "semester": record.get(
            "cuatrimestre"
        ),

        "year": record.get(
            "curso"
        ),

        "group": normalize_group(
            record.get("grupo")
        ),

        "day": normalize_comparison_text(
            record.get("dia")
        ),

        "start_time": normalize_hour(
            record.get("hora_inicio")
        ),

        "end_time": normalize_hour(
            record.get("hora_fin")
        ),

        "subject": {

            "code": record.get(
                "codigo_asignatura"
            ),

            "name": normalize_subject_name(
                record.get("nombre")
            )
        },

        "subgroup": normalize_group(
            record.get("subgrupo")
        ),

        "classroom": normalize_classroom(
            record.get("aula")
        ),

        "notes": record.get("notas") or [],

        "source": {

            "file": record.get(
                "archivo"
            ),

            "page": record.get(
                "pagina"
            ),

            "raw_text": clean_text(
                record.get("texto_original")
            )
        },

        "extraction": {

            "degree_source": record.get(
                "titulacion_source"
            )
        }
    }

    return result
