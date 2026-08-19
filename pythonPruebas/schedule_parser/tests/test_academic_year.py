import unittest

import _pathfix  # noqa: F401

from text_utils import normalize_academic_year
from validator import academic_year_issue


class TestAcademicYearNormalization(unittest.TestCase):

    def test_four_digit_year_is_not_truncated(self):
        # Regresión: "\d{2}|\d{4}" colapsaba "2026" -> "20" -> "2020".
        self.assertEqual(
            normalize_academic_year("2025/2026"),
            "2025/2026"
        )

    def test_two_digit_year_is_expanded(self):
        self.assertEqual(
            normalize_academic_year("2025/26"),
            "2025/2026"
        )


class TestAcademicYearValidation(unittest.TestCase):

    def test_valid_year(self):
        self.assertIsNone(
            academic_year_issue("2025/2026")
        )

    def test_invalid_year_not_consecutive(self):
        self.assertEqual(
            academic_year_issue("2025/2020"),
            "invalid_academic_year"
        )

    def test_missing_year(self):
        self.assertEqual(
            academic_year_issue(None),
            "missing_academic_year"
        )

    def test_malformed_year(self):
        self.assertEqual(
            academic_year_issue("2025-2026"),
            "invalid_academic_year"
        )


if __name__ == "__main__":
    unittest.main()
