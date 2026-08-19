import unittest

import _pathfix  # noqa: F401

from validator import classify_subject_name


class TestValidSubjectNames(unittest.TestCase):

    def test_programacion(self):
        self.assertEqual(
            classify_subject_name("PROGRAMACIÓN"),
            []
        )

    def test_sistemas_operativos_avanzados(self):
        self.assertEqual(
            classify_subject_name(
                "SISTEMAS OPERATIVOS AVANZADOS"
            ),
            []
        )

    def test_teoria_economica(self):
        self.assertEqual(
            classify_subject_name("TEORÍA ECONÓMICA II"),
            []
        )


class TestInvalidSubjectNames(unittest.TestCase):

    def test_group_and_subgroup_combo(self):
        self.assertEqual(
            classify_subject_name("2A (*) 2A2"),
            ["invalid_subject_name"]
        )

    def test_group_only(self):
        self.assertEqual(
            classify_subject_name("1A"),
            ["invalid_subject_name"]
        )

    def test_time_as_name(self):
        self.assertEqual(
            classify_subject_name("16:30"),
            ["invalid_subject_name"]
        )

    def test_symbol_only(self):
        self.assertEqual(
            classify_subject_name("*"),
            ["invalid_subject_name"]
        )

    def test_group_combination(self):
        self.assertEqual(
            classify_subject_name("2A1 Y 2A2"),
            ["invalid_subject_name"]
        )

    def test_short_code(self):
        self.assertEqual(
            classify_subject_name("I-2"),
            ["invalid_subject_name"]
        )

    def test_missing_name(self):
        self.assertEqual(
            classify_subject_name(None),
            ["missing_subject"]
        )


class TestSuspiciousSubjectNames(unittest.TestCase):

    def test_fragment_starting_with_preposition(self):
        # Sospechoso (probable fragmento), pero no se rechaza como un
        # error duro automáticamente.
        issues = classify_subject_name("DE CALIDAD")
        self.assertEqual(issues, ["fragmented_subject"])


if __name__ == "__main__":
    unittest.main()
