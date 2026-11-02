import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from common.word_item import WordItem


class TestWordItem(unittest.TestCase):
    def test_init_and_total_count(self):
        item = WordItem("apple", 3, 4)
        self.assertEqual(item.word, "apple")
        self.assertEqual(item.eng_count, 3)
        self.assertEqual(item.fre_count, 4)
        self.assertEqual(item.total_count(), 7)

    def test_create_in_language_word_count_en(self):
        item = WordItem.create_in_language_word_count("apple", "en")
        self.assertEqual(item.word, "apple")
        self.assertEqual(item.eng_count, 1)
        self.assertEqual(item.fre_count, 0)
        self.assertEqual(item.total_count(), 1)
        self.assertFalse(item.intersection_reached())

    def test_create_in_language_word_count_fr(self):
        item = WordItem.create_in_language_word_count("pomme", "fr")
        self.assertEqual(item.word, "pomme")
        self.assertEqual(item.eng_count, 0)
        self.assertEqual(item.fre_count, 1)
        self.assertEqual(item.total_count(), 1)
        self.assertFalse(item.intersection_reached())

    def test_create_in_language_unsupported_language_raises(self):
        with self.assertRaises(ValueError):
            WordItem.create_in_language_word_count("hola", "es")

    def test_intersection_reached(self):
        self.assertFalse(WordItem("test", 0, 0).intersection_reached())
        self.assertFalse(WordItem("test", 1, 0).intersection_reached())
        self.assertFalse(WordItem("test", 0, 1).intersection_reached())
        self.assertTrue(WordItem("test", 1, 1).intersection_reached())
        self.assertTrue(WordItem("test", 5, 2).intersection_reached())

    def test_add_same_word(self):
        item1 = WordItem("apple", 2, 1)
        item2 = WordItem("apple", 3, 4)
        combined = item1 + item2
        self.assertEqual(combined.word, "apple")
        self.assertEqual(combined.eng_count, 5)
        self.assertEqual(combined.fre_count, 5)
        self.assertEqual(combined.total_count(), 10)

    def test_add_different_words_raises(self):
        item1 = WordItem("apple", 1, 0)
        item2 = WordItem("banana", 0, 1)
        with self.assertRaises(ValueError):
            _ = item1 + item2

    def test_comparisons(self):
        small = WordItem("a", 1, 1)   # total = 2
        medium = WordItem("b", 3, 2)  # total = 5
        large = WordItem("c", 4, 4)   # total = 8
        same_as_medium = WordItem("d", 5, 0)  # total = 5

        self.assertTrue(small < medium)
        self.assertTrue(medium > small)
        self.assertFalse(large < medium)
        self.assertTrue(medium == same_as_medium)
        self.assertFalse(medium < same_as_medium)
        self.assertFalse(medium > same_as_medium)

    def test_sorting(self):
        items = [
            WordItem("low", 1, 0),
            WordItem("high", 10, 5),
            WordItem("mid", 3, 3),
        ]
        sorted_desc = sorted(items, reverse=True)
        self.assertEqual([w.word for w in sorted_desc], ["high", "mid", "low"])


if __name__ == "__main__":
    unittest.main()

