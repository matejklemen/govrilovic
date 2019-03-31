from bs4 import BeautifulSoup
import requests
from urllib.parse import urlparse


class Sitemap:
    """
    Sitemap parser utililty class.
    """

    def __init__(self, url, parse_nested_sitemaps=False):
        """
        Parameters
        ----------
        url: str
            URL of page where the sitemap is supposed to be located.

        parse_nested_sitemaps: boolean
            If set to true, it will also parse nested sitemaps.
            e.g. If in the sitemap, there is a URL like 'http://someurl.com/nested/sitemap.xml',
            the util will parse the links from it as well and add it to self.urls

        Returns
        -------
        Sitemap:
            Instance of the Sitemap, with the attribute (self.)urls, where all the urls from
            the sitemaps are listed in.
        """
        self.raw = ""
        self.url = url
        self.parse_recursive_sitemaps = parse_nested_sitemaps

        if self.parse_recursive_sitemaps:
            self.urls = self.parse_sitemap_nested()
        else:
            self.urls = self.parse_sitemap_simple()

    def __str__(self):
        return self.raw

    def fetch_sitemap(self, url):
        """
        Parameters
        ----------
        url: str
            URL of page where the sitemap is supposed to be located.

        Returns
        -------
        raw_response: str:
            Raw XML from the sitemap.

        Raises
        -------
        requests.HTTPError
            If the requests response returns a code that is not 200, 
            we throw an exception, as we kind of cannot expect to find
            a sitemap on this URL for sure.
        """
        response = requests.get(url)

        if response.status_code == 200:
            self.raw = response.text
            return response.text
        else:
            raise requests.HTTPError(
                'Could not fetch sitemap on URL: {}'.format(url))

    @staticmethod
    def process_sitemap(raw):
        """
        Parameters
        ----------
        raw: str
            Raw XML string of the sitemap.

        Returns
        -------
        urls: str:
            List of parsed urls from the sitemap.
        """
        soup = BeautifulSoup(raw, 'lxml')
        result = []

        for loc in soup.findAll('loc'):
            result.append(loc.text)

        return result

    @staticmethod
    def url_leads_to_subsitemap(url):
        """
        Parameters
        ----------
        url: str
            URL where a sitemap is supposed to be located.

        Returns
        -------
        url_leads_to_subsitemap: boolean:
            Returns True if the link could feature a nested sitemap.xml.
            E.g.: http://someurl.com/nested/sitemap.xml.
        """
        parts = urlparse(url)
        return parts.path.endswith('.xml') and 'sitemap' in parts.path

    def parse_sitemap_simple(self):
        """
        Returns
        -------
        urls: str:
            List of parsed urls from the sitemap (not including the links
            from nested subsitemaps).
        """
        return self.process_sitemap(self.fetch_sitemap(self.url))

    def parse_sitemap_nested(self):
        """
        Returns
        -------
        urls: str:
            List of parsed urls from the sitemap (including the links
            from nested subsitemaps).
        """
        sitemap = self.process_sitemap(self.fetch_sitemap(self.url))
        result = []

        while sitemap:
            current_url = sitemap.pop()

            # if the url from the sitemap leads to another sitemap
            if self.url_leads_to_subsitemap(current_url):

                # get the links from the nested sitemap and add it
                # to the current (flattened) sitemap link queue
                for url in self.process_sitemap(self.fetch_sitemap(current_url)):
                    sitemap.append(url)

            # else just add it to the results
            else:
                result.append(current_url)

        return result


if __name__ == '__main__':
    try:
        sm = Sitemap('https://www.sitemaps.org/sitemap.xml')
        print(sm.urls)
    except requests.HTTPError:
        print('Caught exception , so ... let\'s not use the sitemap.')
