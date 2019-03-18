import unittest
from crawler.links import Links


class TestLocalitySensitiveHashing(unittest.TestCase):
    def testUrlQuerySanitizationSanitizingCorrectly(self):
        dangerous_link = 'http://www.Python.org/doc/neki?hack=* from USERS; DROP * WHERE user.id < 300000&hack2=yeet;#ezgg'
        sanitized = Links.sanitize_url_query(dangerous_link)
        self.assertEqual(
            sanitized,
            'http://www.Python.org/doc/neki?hack=%2A%20from%20USERS%3B%20DROP%20%2A%20WHERE%20user.id%20%3C%20300000&hack2=yeet%3B#ezgg')

    def testUrlQuerySanitizationLeavingNiceURLAlone(self):
        safe_link = 'http://www.Python.org/ena/dva/tri/stiri/pet/sest'
        sanitized = Links.sanitize_url_query(safe_link)
        self.assertEqual(sanitized, safe_link)

    def testUrlPruningPruningCorrectly(self):
        sanitized_long_link = Links.sanitize(
            'http://www.Python.org/ena/dva/tri/stiri/pet/sest?kveri=parameter#l')

        pruned = Links.prune_to_max_depth(sanitized_long_link, 4)

        self.assertEqual(
            pruned, 'http://www.Python.org/ena/dva/tri/stiri?kveri=parameter#l')

    def testUrlPruningLeavingShortURLsAlone(self):
        sanitized_long_link = Links.sanitize(
            'http://www.Python.org/ena/dva/tri/stiri/pet/sest?kveri=parameter#l')

        pruned = Links.prune_to_max_depth(sanitized_long_link, 10)

        self.assertEqual(
            pruned, sanitized_long_link)

        sanitized_short_link = Links.sanitize(
            'http://www.Python.org/ena/')

        pruned = Links.prune_to_max_depth(sanitized_short_link, 4)

        self.assertEqual(
            pruned, sanitized_short_link)
