"""
This file contains utility functions for working with URLs.
"""
from urllib.parse import urlsplit, quote
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
        return urlsplit(url).geturl()

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

    @staticmethod
    def sanitize_url_query(url):
        """
        Returns the url with the sanitized query parameters and
        a sanitized fragment (the # parameter for autoscrolling).

        Example:

        >>> Links.sanitize_url_query('http://www.Python.org/doc/neki?hack=* from USERS; DROP * WHERE user.id < 300000&hack2=yeet;#ezgg')
        'http://www.Python.org/doc/neki?hack=%2A%20from%20USERS%3B%20DROP%20%2A%20WHERE%20user.id%20%3C%20300000&hack2=yeet%3B#ezgg'
        """
        url = urlsplit(url)
        return "".join([url.scheme,
                        "://", url.netloc, url.path,
                        "?", quote(url.query, safe="=&"),
                        "#", quote(url.fragment, safe="=&")])
