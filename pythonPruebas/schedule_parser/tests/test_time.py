import unittest

import _pathfix  # noqa: F401

from validator import valid_time_range


class TestTimeRange(unittest.TestCase):

    def test_valid_range(self):
        self.assertTrue(
            valid_time_range("10:00", "10:55")
        )

    def test_invalid_range_end_before_start(self):
        self.assertFalse(
            valid_time_range("18:00", "17:55")
        )

    def test_missing_values(self):
        self.assertFalse(
            valid_time_range(None, "10:00")
        )
        self.assertFalse(
            valid_time_range("10:00", None)
        )


if __name__ == "__main__":
    unittest.main()
