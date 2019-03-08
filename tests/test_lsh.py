import unittest
from crawler.lsh import LocalitySensitiveHashing


class TestLocalitySensitiveHashing(unittest.TestCase):
    def setUp(self):
        self.lsh_obj = LocalitySensitiveHashing(vocab=["a", "b", "c", "d", "e"],
                                                num_hash=2,
                                                hash_funcs=[
                                                    lambda idx: (idx + 1) % 5,
                                                    lambda idx: (3 * idx + 1) % 5
                                                ],
                                                num_bands=2)

    def testInits(self):
        with self.assertRaises(ValueError):
            # `num_bands` must be lower or equal than `num_hash`
            LocalitySensitiveHashing(vocab=["a", "b", "c", "d", "e"],
                                     num_hash=2,
                                     hash_funcs=[
                                         lambda idx: (idx + 1) % 5,
                                         lambda idx: (3 * idx + 1) % 5
                                     ],
                                     num_bands=10)

            # `num_bands` must divide `num_hash` perfectly
            LocalitySensitiveHashing(vocab=["a", "b", "c", "d", "e"],
                                     num_hash=7,
                                     hash_funcs=[
                                         lambda idx: (idx + 1) % 5,
                                         lambda idx: (3 * idx + 1) % 5
                                     ],
                                     num_bands=5)

    def testSparseEncoding(self):
        self.assertSetEqual(self.lsh_obj.get_repr("acd"), {0, 2, 3})
        self.assertSetEqual(self.lsh_obj.get_repr("a"), {0})
        self.assertSetEqual(self.lsh_obj.get_repr(""), set({}))
        # units not in the vocabulary should be skipped
        self.assertSetEqual(self.lsh_obj.get_repr("afb"), {0, 1})
