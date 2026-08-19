import unittest

import _pathfix  # noqa: F401

from subject_parser import parse_subjects


class TestStandardFormat(unittest.TestCase):

    def test_name_subgroup_classroom(self):
        results = parse_subjects(
            "Seguridad En Sistemas Distribuidos - 4A1 (SA1)"
        )

        self.assertEqual(len(results), 1)

        result = results[0]

        self.assertEqual(
            result["nombre"],
            "Seguridad En Sistemas Distribuidos"
        )
        self.assertEqual(result["subgrupo"], "4A1")
        self.assertEqual(result["aula"], "SA1")
        self.assertEqual(result["notas"], [])


class TestRedirectNotes(unittest.TestCase):

    def test_redirect_annotation_is_extracted_as_note(self):
        results = parse_subjects(
            "PROGRAMACIÓN - 1A → 1A(GII) (NA7)"
        )

        self.assertEqual(len(results), 1)

        result = results[0]

        self.assertEqual(result["nombre"], "PROGRAMACIÓN")
        self.assertEqual(result["aula"], "NA7")
        self.assertTrue(result["notas"])
        self.assertIn("1A", result["notas"][0])
        self.assertIn("GII", result["notas"][0])


class TestFragmentedCode(unittest.TestCase):

    def test_leading_internal_code_is_stripped(self):
        results = parse_subjects(
            "GD. 202012-SISTEMAS ELÉCTRICOS DE POTENCIA (EA6)"
        )

        self.assertEqual(len(results), 1)

        result = results[0]

        self.assertEqual(
            result["nombre"],
            "SISTEMAS ELÉCTRICOS DE POTENCIA"
        )
        self.assertEqual(result["aula"], "EA6")


class TestJunkStillParses(unittest.TestCase):

    def test_junk_cell_is_still_parsed_for_later_validation(self):
        # subject_parser solo interpreta el texto; es validator quien
        # decide si "2A (*) 2A2" es un nombre de asignatura válido.
        results = parse_subjects("- 2A (*) 2A2 (*)")

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["nombre"], "2A (*) 2A2")


if __name__ == "__main__":
    unittest.main()
