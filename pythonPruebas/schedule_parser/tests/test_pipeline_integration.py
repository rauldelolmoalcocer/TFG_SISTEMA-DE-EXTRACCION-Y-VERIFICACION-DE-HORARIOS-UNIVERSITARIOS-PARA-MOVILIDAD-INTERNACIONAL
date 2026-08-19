import unittest

import _pathfix  # noqa: F401

from subject_parser import parse_subjects
from normalizer import normalize_record
from validator import validate_record


def build_record(cell_text, metadata, dia, hora_inicio, hora_fin,
                  archivo="test.pdf", pagina=1):
    """
    Replica lo que hace pdf_processor.process_page: interpreta el
    texto de una celda y construye el registro tal y como lo vería
    normalize_record/validate_record en el pipeline real.
    """

    subjects = parse_subjects(cell_text)

    if not subjects:
        subjects = [{
            "codigo_asignatura": None,
            "nombre": None,
            "subgrupo": None,
            "aula": None,
            "texto_original": cell_text,
            "notas": []
        }]

    subject = subjects[0]

    raw_record = {
        **metadata,
        "dia": dia,
        "hora_inicio": hora_inicio,
        "hora_fin": hora_fin,
        **subject,
        "archivo": archivo,
        "pagina": pagina
    }

    normalized = normalize_record(raw_record)

    return validate_record(normalized)


# Metadatos tomados de un PDF real (Semestre1-GIEAI.pdf).
GIEAI_METADATA = {
    "titulacion": "INGENIERIA EN ELECTRONICA Y AUTOMATICA INDUSTRIAL",
    "codigo_titulacion": "G60",
    "titulacion_source": "document",
    "curso_academico": "2025/2026",
    "cuatrimestre": 1,
    "curso": 1,
    "grupo": "1A"
}


class TestRealRecordsAccepted(unittest.TestCase):

    def test_clean_standard_record_is_accepted(self):
        result = build_record(
            "Informática - 1A3 (NL5)",
            GIEAI_METADATA,
            "JUEVES",
            "08:00",
            "08:55"
        )

        self.assertTrue(result["accepted"])
        self.assertEqual(result["record"]["academic_year"], "2025/2026")
        self.assertEqual(result["record"]["subject"]["name"], "INFORMÁTICA")
        self.assertEqual(result["record"]["classroom"], "NL5")
        self.assertEqual(result["errors"], [])

    def test_unknown_classroom_is_still_accepted_with_warning(self):
        result = build_record(
            "Programación Avanzada - 1A1 (LAB)",
            GIEAI_METADATA,
            "LUNES",
            "10:00",
            "10:55"
        )

        self.assertTrue(result["accepted"])
        self.assertIn(
            "unknown_classroom",
            result["record"]["extraction"]["issues"]
        )
        self.assertEqual(result["record"]["classroom"], "LAB")


class TestRealRecordsRejected(unittest.TestCase):

    def test_junk_subject_name_is_rejected_but_kept(self):
        result = build_record(
            "- 2A (*) 2A2 (*)",
            GIEAI_METADATA,
            "VIERNES",
            "16:00",
            "16:55"
        )

        self.assertFalse(result["accepted"])
        self.assertIn("invalid_subject_name", result["errors"])

        # El registro se conserva con toda la información parcial:
        # día, hora y source.raw_text no deben perderse.
        record = result["record"]

        self.assertEqual(record["day"], "VIERNES")
        self.assertEqual(record["start_time"], "16:00")
        self.assertEqual(
            record["source"]["raw_text"],
            "- 2A (*) 2A2 (*)"
        )

    def test_time_as_classroom_is_rejected(self):
        result = build_record(
            "Economía De La Empresa - 1A (19:00-19:30)",
            GIEAI_METADATA,
            "JUEVES",
            "19:00",
            "19:55"
        )

        self.assertFalse(result["accepted"])
        self.assertIn("invalid_classroom", result["errors"])
        self.assertIsNone(result["record"]["classroom"])

    def test_unparsed_cell_keeps_partial_information(self):
        result = build_record(
            "algo totalmente irreconocible sin formato",
            GIEAI_METADATA,
            "MARTES",
            "09:00",
            "09:55"
        )

        self.assertFalse(result["accepted"])
        self.assertIn("missing_subject", result["errors"])

        record = result["record"]

        self.assertEqual(record["day"], "MARTES")
        self.assertEqual(record["academic_year"], "2025/2026")
        self.assertEqual(
            record["source"]["raw_text"],
            "algo totalmente irreconocible sin formato"
        )


class TestAcademicYearRegressionAcrossPipeline(unittest.TestCase):

    def test_full_pipeline_does_not_corrupt_academic_year(self):
        result = build_record(
            "Estadística - 1A1 (NL7)",
            GIEAI_METADATA,
            "MIERCOLES",
            "08:00",
            "08:55"
        )

        self.assertEqual(
            result["record"]["academic_year"],
            "2025/2026"
        )
        self.assertNotIn(
            "invalid_academic_year",
            result["record"]["extraction"]["issues"]
        )


if __name__ == "__main__":
    unittest.main()
