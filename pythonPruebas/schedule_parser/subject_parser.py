# subject_parser.py

import re

from text_utils import (
    GROUP_PATTERN,
    clean_text,
    remove_time_ranges,
    normalize_group,
    normalize_classroom
)


CLASSROOM_PATTERN = r"[^()]{1,50}"

# Anotaciones de redirección entre grupos, p.ej.:
#
# PROGRAMACIÓN - 1A → 1A(GII) (NA7)
#
# El fragmento "1A → 1A(GII)" no forma parte del nombre de la
# asignatura ni es un aula: se extrae como nota y se retira del texto
# antes de aplicar los parsers de formato.
REDIRECT_NOTE_PATTERN = re.compile(

    rf"""
    {GROUP_PATTERN}
    \s*(?:→|->)\s*
    {GROUP_PATTERN}
    \s*\([A-Z0-9]{{1,10}}\)
    """,

    re.I | re.X
)

# Prefijos de código interno de tipo "abreviatura + dígitos + guion"
# que a veces preceden al nombre real de la asignatura, p.ej.:
#
# GD. 202012-SISTEMAS ELÉCTRICOS DE POTENCIA
LEADING_CODE_PATTERN = re.compile(
    r"^[A-ZÁÉÍÓÚÑ]{1,6}\.?\s*\d{3,8}\s*-\s*",
    re.I
)


# ==========================================
# NOTES EXTRACTION
# ==========================================

def extract_redirect_notes(text):

    if not text:
        return text, []

    notes = [
        clean_text(match.group(0))
        for match in REDIRECT_NOTE_PATTERN.finditer(text)
    ]

    if not notes:
        return text, []

    cleaned = clean_text(
        REDIRECT_NOTE_PATTERN.sub(" ", text)
    )

    return cleaned, notes


# ==========================================
# NAME CLEANING
# ==========================================

def clean_subject_name(name):

    if not name:
        return None

    name = clean_text(name)

    # Elimina grupos/aulas huérfanos al principio:
    #
    # 3D3 (EL7) Programación Avanzada
    #
    # ->
    #
    # Programación Avanzada

    name = re.sub(
        rf"^{GROUP_PATTERN}\s*\([^)]+\)\s*",
        "",
        name,
        flags=re.I
    )

    # Elimina basura como:
    #
    # (SA6) Electrónica Analógica

    name = re.sub(
        r"^\([^)]+\)\s*",
        "",
        name
    )

    # Elimina prefijos de código interno:
    #
    # GD. 202012-SISTEMAS ELÉCTRICOS DE POTENCIA
    #
    # ->
    #
    # SISTEMAS ELÉCTRICOS DE POTENCIA

    name = LEADING_CODE_PATTERN.sub(
        "",
        name
    )

    name = clean_text(name)

    name = name.strip(" -*")

    return name if name else None


# ==========================================
# FORMAT 1
#
# Informática - 1A1 (NL5)
# ==========================================

def parse_standard_format(text):

    pattern = re.compile(

        rf"""
        (?P<nombre>.+?)
        \s*-\s*
        (?P<subgrupo>{GROUP_PATTERN})
        \*?
        \s*
        \(
            (?P<aula>{CLASSROOM_PATTERN})
        \)
        """,

        re.I | re.X
    )

    results = []

    position = 0

    while position < len(text):

        match = pattern.search(
            text,
            position
        )

        if not match:
            break

        nombre = clean_subject_name(
            match.group("nombre")
        )

        results.append({

            "codigo_asignatura": None,

            "nombre": nombre,

            "subgrupo": normalize_group(
                match.group("subgrupo")
            ),

            "aula": normalize_classroom(
                match.group("aula")
            ),

            "texto_original": clean_text(
                match.group(0)
            )
        })

        position = match.end()

    return results


# ==========================================
# FORMAT 2
#
# Computación Ubicua (E.F.) - 3A
# ==========================================

def parse_classroom_before_group(text):

    pattern = re.compile(

        rf"""
        (?P<nombre>.+?)
        \s*
        \(
            (?P<aula>{CLASSROOM_PATTERN})
        \)
        \s*-\s*
        (?P<subgrupo>{GROUP_PATTERN})
        """,

        re.I | re.X
    )

    results = []

    for match in pattern.finditer(text):

        results.append({

            "codigo_asignatura": None,

            "nombre": clean_subject_name(
                match.group("nombre")
            ),

            "subgrupo": normalize_group(
                match.group("subgrupo")
            ),

            "aula": normalize_classroom(
                match.group("aula")
            ),

            "texto_original": clean_text(
                match.group(0)
            )
        })

    return results


# ==========================================
# FORMAT 3
#
# 3A1 (EL5) Sistemas Operativos Avanzados
# ==========================================

def parse_group_first(text):

    pattern = re.compile(

        rf"""
        ^
        (?P<subgrupo>{GROUP_PATTERN})
        \s*
        \(
            (?P<aula>{CLASSROOM_PATTERN})
        \)
        \s+
        (?P<nombre>.+?)
        \s*[-]?
        $
        """,

        re.I | re.X
    )

    match = pattern.search(text)

    if not match:
        return []

    return [{

        "codigo_asignatura": None,

        "nombre": clean_subject_name(
            match.group("nombre")
        ),

        "subgrupo": normalize_group(
            match.group("subgrupo")
        ),

        "aula": normalize_classroom(
            match.group("aula")
        ),

        "texto_original": clean_text(text)
    }]


# ==========================================
# FALLBACK
#
# Nombre (Aula)
# ==========================================

def parse_simple_format(text):

    pattern = re.compile(

        r"""
        ^
        (?P<nombre>.+?)
        \s*
        \(
            (?P<aula>[^)]+)
        \)
        $
        """,

        re.I | re.X
    )

    match = pattern.search(text)

    if not match:
        return []

    return [{

        "codigo_asignatura": None,

        "nombre": clean_subject_name(
            match.group("nombre")
        ),

        "subgrupo": None,

        "aula": normalize_classroom(
            match.group("aula")
        ),

        "texto_original": clean_text(text)
    }]


# ==========================================
# MAIN PARSER
# ==========================================

def parse_subjects(text):

    text = remove_time_ranges(text)
    text = clean_text(text)

    text, notes = extract_redirect_notes(text)

    if not text:
        return []

    parsers = [

        parse_standard_format,
        parse_classroom_before_group,
        parse_group_first,
        parse_simple_format
    ]

    for parser in parsers:

        results = parser(text)

        if results:

            for result in results:
                result["notas"] = notes

            return results

    return []
