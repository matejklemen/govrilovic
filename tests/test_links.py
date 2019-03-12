import unittest
from crawler.links import Links


class TestLocalitySensitiveHashing(unittest.TestCase):
    def testUrlQuerySanitization(self):
        dangerous_link = 'http://www.Python.org/doc/neki?hack=* from USERS; DROP * WHERE user.id < 300000;#ezgg'
        sanitized = Links.sanitize_url_query(dangerous_link)
        self.assertEqual(
            sanitized,
            'http://www.Python.org/doc/neki?hack%3D%2A%20from%20USERS%3B%20DROP%20%2A%20WHERE%20user.id%20%3C%20300000%3B#ezgg')
