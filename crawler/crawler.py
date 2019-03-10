import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

"""
This file contains the main crawler class along with
its core functionalities (methods for parsing, accessing
the frontier, adding links to the crawled set, etc.).
"""

# In order: '.pptx', '.pdf', '.doc', '.ppt' and '.docx' files
DOWNLOADABLE_CONTENT_TYPES = {"application/vnd.openxmlformats-officedocument.presentationml.presentation",
                              "application/pdf", "application/msword", "application/vnd.ms-powerpoint",
                              "application/vnd.openxmlformats-officedocument.wordprocessingml.document"}
# how long the crawler waits before giving up on a page (in seconds)
TIMEOUT_PERIOD = 5.0


def find_links(current_url, soup_obj):
    """ Find links inside <a> tags.

    Parameters
    ----------
    current_url: str
        URL of page whose content `soup_obj` contains

    soup_obj: bs4.BeautifulSoup
        BeautifulSoup's object, containing the response for `current_url`

    Returns
    -------
    list of str:
        List of absolute URLs
    """
    a_tags = soup_obj.find_all("a")
    links = []

    for anchor in a_tags:
        link = anchor.get("href")
        if link:
            processed_link = link
            if link[0] == "/":
                processed_link = urljoin(current_url, link)
            elif link[0] == "#":
                continue

            links.append(processed_link)

    return links


class Agent:
    # ["http://evem.gov.si", "http://e-uprava.gov.si", "http://podatki.gov.si", "http://e-prostor.gov.si"] + 5 other gov sites
    # contains links for the next level of crawling (using BFS strategy)
    link_queue = set(["http://podatki.gov.si"])
    visited = set()
    user_agent = "govrilovic-crawler/v0.1"

    def __init__(self):
        pass

    @staticmethod
    def crawl_level():
        # get pages for the next level and clear the queue
        curr_level_links = Agent.link_queue
        Agent.link_queue = set()

        # remove duplicate links before dividing among workers so that the tasks
        # are more evenly split
        relevant_links = [link for link in curr_level_links if link not in Agent.visited]

        # TODO: divide `relevant_links` among workers
        # ...

        pass

    @staticmethod
    def crawl_page(url):
        """ Crawl a single web page denoted by `url`. The URL is expected to be preprocessed
        (if needed) and VALID.

        Parameters
        ----------
        url: str
            URL of web page.

        Returns
        -------
        TODO
            decide on what is to be returned (need links, HTML content, images, documents)
        """
        links = []

        if url not in Agent.visited:
            print("Crawling page '%s'..." % url)
            response = requests.get(url, headers={"User-Agent": Agent.user_agent},
                                    timeout=TIMEOUT_PERIOD)

            # TODO: there are other status codes that indicate success
            if not response or response.status_code != 200:
                return

            # if Content-Type is not present in header (is this even possible?), assume it's HTML
            content_type = response.headers.get("Content-Type", "text/html")

            # https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Complete_list_of_MIME_types
            # possible to have {"Content-Type": "text/html; charset=utf-8"}
            if "text/html" in content_type:
                soup = BeautifulSoup(response.content, "html.parser")

                # TODO: detect unrendered javascript and process it using headless browser
                # ...

                # find links on current site
                links = find_links(url, soup)

                # TODO: only keep links that point to '.gov.si' websites
                # ...

                # TODO: parse HTML content
                # ...

            elif content_type in DOWNLOADABLE_CONTENT_TYPES:
                # TODO: store the document that is present in response
                pass

            Agent.link_queue.remove(url)
            Agent.visited.add(url)

        return links


if __name__ == "__main__":
    l = Agent.crawl_page("http://podatki.gov.si")
    print(l)
