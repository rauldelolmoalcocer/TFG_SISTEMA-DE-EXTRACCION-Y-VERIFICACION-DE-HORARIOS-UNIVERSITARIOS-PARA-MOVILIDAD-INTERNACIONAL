import unittest

import _pathfix  # noqa: F401

from validator import classify_classroom


class TestValidClassrooms(unittest.TestCase):

    def test_simple_classroom(self):
        self.assertEqual(
            classify_classroom("NA7"),
            ("NA7", None)
        )

    def test_multi_room_slash(self):
        self.assertEqual(
            classify_classroom("SA5B/OL8"),
            ("SA5B/OL8", None)
        )

    def test_multi_room_dash(self):
        self.assertEqual(
            classify_classroom("SA5A-SL11"),
            ("SA5A-SL11", None)
        )


class TestInvalidClassrooms(unittest.TestCase):

    def test_time_as_classroom(self):
        value, issue = classify_classroom("16:30")
        self.assertIsNone(value)
        self.assertEqual(issue, "invalid_classroom")

    def test_time_range_as_classroom(self):
        value, issue = classify_classroom("19:00-19:30")
        self.assertIsNone(value)
        self.assertEqual(issue, "invalid_classroom")


class TestSuspiciousClassrooms(unittest.TestCase):

    def test_asterisk_becomes_null(self):
        self.assertEqual(
            classify_classroom("*"),
            (None, "suspicious_classroom")
        )

    def test_dash_placeholder_becomes_null(self):
        self.assertEqual(
            classify_classroom("--"),
            (None, "suspicious_classroom")
        )

    def test_pending_assignment_becomes_null(self):
        value, issue = classify_classroom("PEND. ASIG.")
        self.assertIsNone(value)
        self.assertEqual(issue, "suspicious_classroom")

    def test_missing_classroom(self):
        self.assertEqual(
            classify_classroom(None),
            (None, "missing_classroom")
        )


class TestUnknownButRealClassrooms(unittest.TestCase):

    def test_unknown_format_is_kept(self):
        value, issue = classify_classroom("LAB")
        self.assertEqual(value, "LAB")
        self.assertEqual(issue, "unknown_classroom")

    def test_unknown_format_is_kept_gtic(self):
        value, issue = classify_classroom("GTIC")
        self.assertEqual(value, "GTIC")
        self.assertEqual(issue, "unknown_classroom")


if __name__ == "__main__":
    unittest.main()
