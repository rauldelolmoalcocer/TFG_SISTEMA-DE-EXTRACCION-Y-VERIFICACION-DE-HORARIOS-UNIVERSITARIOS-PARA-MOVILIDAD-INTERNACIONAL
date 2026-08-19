import unittest

import _pathfix  # noqa: F401

from validator import classify_subgroup


class TestGroupSubgroupCoherence(unittest.TestCase):

    def test_coherent_1a(self):
        self.assertIsNone(
            classify_subgroup("1A", "1A1")
        )

    def test_coherent_2b(self):
        self.assertIsNone(
            classify_subgroup("2B", "2B2")
        )

    def test_incompatible_subgroup(self):
        self.assertEqual(
            classify_subgroup("4B", "4A1"),
            "incompatible_subgroup"
        )

    def test_missing_values_are_not_flagged(self):
        self.assertIsNone(
            classify_subgroup(None, None)
        )
        self.assertIsNone(
            classify_subgroup("1A", None)
        )
        self.assertIsNone(
            classify_subgroup(None, "1A1")
        )


if __name__ == "__main__":
    unittest.main()
