# metadata_parser.py

import os
import re

from .text_utils import (
    clean_text,
    normalize_academic_year,
    normalize_comparison_text,
    normalize_group
)


DEGREE_CATALOG = {

    "GISI": {
        "codigo": "G581",
        "nombre": "INGENIERÍA EN SISTEMAS DE INFORMACIÓN"
    },

    "GII": {
        "codigo": "G781",
        "nombre": "INGENIERÍA INFORMÁTICA"
    },

    "GIT": {
        "codigo": "G380",
        "nombre": "INGENIERÍA TELEMÁTICA"
    },

    "GIEC": {
        "codigo": "G370",
        "nombre": "INGENIERÍA ELECTRÓNICA DE COMUNICACIONES"
    },

    "GIEAI": {
        "codigo": "G60",
        "nombre": "INGENIERIA EN ELECTRONICA Y AUTOMATICA INDUSTRIAL"
    }
}


def infer_degree_from_filename(pdf_path):

    filename = normalize_comparison_text(
        os.path.basename(pdf_path)
    )

    # Importante: primero códigos largos
    # para evitar que GIEAI coincida parcialmente con otros.

    degree_codes = sorted(
        DEGREE_CATALOG.keys(),
        key=len,
        reverse=True
    )

    for code in degree_codes:

        if code in filename:

            degree = DEGREE_CATALOG[code]

            return {
                "titulacion": degree["nombre"],
                "codigo_titulacion": degree["codigo"],
                "source": "filename"
            }

    return {
        "titulacion": None,
        "codigo_titulacion": None,
        "source": None
    }


def extract_metadata(text, pdf_path=None):

    text = clean_text(text)

    grado = re.search(
        r"GRADO EN (.*?)\s*\((G\d+)\)",
        text,
        re.I
    )

    master = re.search(
        r"MASTER UNIVERSITARIO EN (.*?)(?:CUATRIMESTRE|\()",
        text,
        re.I
    )

    curso_academico = re.search(
        r"Curso\s+(\d{4}/\d{2,4})",
        text,
        re.I
    )

    cuatri = re.search(
        r"CUATRIMESTRE\s+(\d+º).*?Curso\s+(\d+º)",
        text,
        re.I
    )

    grupo = re.search(
        r"GRUPO\s+([0-9]+º?\s*[A-Z])",
        text,
        re.I
    )

    titulacion = None
    codigo = None
    degree_source = None

    if grado:

        titulacion = clean_text(grado.group(1)).upper()
        codigo = grado.group(2).upper()
        degree_source = "document"

    elif master:

        titulacion = clean_text(master.group(1)).upper()
        degree_source = "document"

    # =====================================
    # FALLBACK POR NOMBRE DE ARCHIVO
    # =====================================

    if not titulacion and pdf_path:

        fallback = infer_degree_from_filename(
            pdf_path
        )

        titulacion = fallback["titulacion"]
        codigo = fallback["codigo_titulacion"]

        degree_source = fallback["source"]

    return {

        "titulacion": titulacion,

        "codigo_titulacion": codigo,

        "titulacion_source": degree_source,

        "curso_academico": normalize_academic_year(
            curso_academico.group(1)
            if curso_academico
            else None
        ),

        "cuatrimestre": (
            int(cuatri.group(1).replace("º", ""))
            if cuatri
            else None
        ),

        "curso": (
            int(cuatri.group(2).replace("º", ""))
            if cuatri
            else None
        ),

        "grupo": normalize_group(
            grupo.group(1)
            if grupo
            else None
        )
    }
