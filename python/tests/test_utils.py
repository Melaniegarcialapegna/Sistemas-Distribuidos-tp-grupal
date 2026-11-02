import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from common.utils import fnv1a_32, FNV32_OFFSET


class TestUtils(unittest.TestCase):
    def test_fnv1a_32_deterministic(self):
        self.assertEqual(fnv1a_32(b"hello"), fnv1a_32(b"hello"))
        self.assertEqual(fnv1a_32(b"world"), fnv1a_32(b"world"))

    def test_fnv1a_32_different_inputs(self):
        self.assertNotEqual(fnv1a_32(b"apple"), fnv1a_32(b"banana"))

    def test_fnv1a_32_empty_bytes(self):
        self.assertEqual(fnv1a_32(b""), FNV32_OFFSET)

    def test_fnv1a_32_range(self):
        for sample in [b"a", b"long string with many characters", b"\x00\xff"]:
            h = fnv1a_32(sample)
            self.assertGreaterEqual(h, 0)
            self.assertLessEqual(h, 0xFFFFFFFF)


if __name__ == "__main__":
    unittest.main()

