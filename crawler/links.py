"""
This file contains utility functions for working with URLs.
"""
from urllib.parse import urlsplit
import requests


class Links:

    @staticmethod
    def sanitize(url):
        """
        Removes unnecessary symbols from the URL and fixes
        any capitalization inconsistencies.

        Example:

        >>> url = 'HTTP://www.Python.org/doc/#'
        >>> Links.sanitize(url)
        'http://www.Python.org/doc/'

        HTTP gets fixed as capitalization there doesn't matter
        whereas the actual URL's are case-sensitive.
        """
        return urlsplit(url).getUrl()

    @staticmethod
    def has_parsable_content(url):
        """
        Tells whether a URL leads to a webpage. E.g. if the 
        URL leads to a .pptx file, we don't consider adding
        it to the frontier.

        Examples:

        >>> Links.has_parsable_content('http://docs.python-requests.org/en/master/user/quickstart/#response-status-codes')
        True

        >>> Links.has_parsable_content('http://www.africau.edu/images/default/sample.pdf')
        False
        """
        return 'text/html' in requests.head(url).headers['Content-Type']
