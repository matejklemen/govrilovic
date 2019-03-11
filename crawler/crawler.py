import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import multiprocessing as mp
from time import sleep
import time
# from selenium import webdriver
# import selenium
from urllib.parse import urlparse
from os.path import splitext

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


def get_url_extension(url):
    # Returns the filename extension or '' if none. For example "png", "html", ...

    parsed = urlparse(url)
    root, extension = splitext(parsed.path)
    return extension[1:]


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
            if link[0] in {"/", "?"}:
                processed_link = urljoin(current_url, link)
            elif link[0] == "#":
                continue

            links.append(processed_link)

    return links

def find_images(current_url, soup_obj):
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
    images = []
    for image in soup_obj.find_all('img'):
        src = image.get('src')
        if src:
            processed_src = src     
            if src[0] in {"/", "?"}:
                processed_src = urljoin(current_url, src)
            elif src[0] == "#":
                continue
            if get_url_extension(processed_src) in ["png", "jpeg", "jpg"]:
                # Download the image?
                images.append(processed_src)

    return images

class Agent:
    USER_AGENT = "govrilovic-crawler/v0.1"

    def __init__(self, seed_pages, num_workers=None, sleep_period=5):
        # contains links for the next level of crawling (using BFS strategy)
        self.link_queue = set(seed_pages)
        self.visited = set()
        self.num_workers = num_workers if num_workers is not None else mp.cpu_count()
        self.sleep_period = sleep_period

    def crawl(self, max_level=2):
        """ Performs breadth-first search up to a certain level or while there are links to be
        crawled (if `max_level` is None).

        Parameters
        ----------
        max_level: int, optional
            Max depth up to which to perform breadth first search
        """
        curr_level = 0
        if max_level is None:
            # set the depth limit ridiculously high which essentially means 'no limit'
            max_level = 2 ** 31 - 1

        while self.link_queue:
            if curr_level == max_level:
                print("Reached specified maximal level. Exiting...")
                break
            print("[Level %d] Links to be crawled: %d..." % (curr_level, len(self.link_queue)))
            self.crawl_level()
            print("[Level %d] New links produced: %d..." % (curr_level, len(self.link_queue)))
            curr_level += 1

    def crawl_level(self):
        """ Performs a single level of breadth-first search."""
        # get pages for the next level and clear the queue
        curr_level_links = self.link_queue
        self.link_queue = set()

        # remove duplicate links before dividing among workers so that the tasks
        # are more evenly split
        relevant_links = [link for link in curr_level_links if link not in self.visited]
        num_links = len(relevant_links)

        next_level_links = set()
        # TODO: concurrency
        # divide relevant links among workers (as evenly as possible)
        for id_worker in range(self.num_workers):
            idx_start = int(float(id_worker) * num_links / self.num_workers)
            idx_end = int(float(id_worker + 1) * num_links / self.num_workers)

            links_to_crawl = relevant_links[idx_start: idx_end + 1]
            new_links = self.worker_task(links_to_crawl, id_worker=id_worker)
            next_level_links.update(new_links)

        self.visited.update(relevant_links)
        self.link_queue = next_level_links

    def worker_task(self, urls, id_worker=None):
        """ Work to be done in a single worker (thread/process).

        Parameters
        ----------
        urls: list of str
            URLs to be crawled by current worker

        id_worker: int, optional
            Unique identifier for current worker

        Returns
        -------
        TODO:
            decide (see `crawl_page(...)` TODO)
        """
        produced_links = set()
        for url in urls:
            new_urls = self.crawl_page(url=url)
            # Insert new data into the database
            produced_links.update(new_urls)
            sleep(self.sleep_period)

        return produced_links

    def crawl_page(self, url):
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

        if url not in self.visited:
            print("Crawling '%s'..." % url)
            start = time.time()
            response = requests.get(url, headers={"User-Agent": Agent.USER_AGENT},
                                    timeout=TIMEOUT_PERIOD)
            end = time.time()
            print("Request time: ", round(end - start, 2), " seconds.")
            # Selenium
            # start = time.time()
            # chromedriver = '/usr/local/bin/chromedriver'
            # chrome_options = webdriver.ChromeOptions()
            # chrome_options.add_argument('--headless')
            # driver = webdriver.Chrome(chrome_options=chrome_options, executable_path=chromedriver)
            # driver.get(url)
            # driver.close()
            # end = time.time()
            # print("Selenium time: ", end - start)
            
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

                # find images on the current site
                images = find_images(url, soup)

                # find links on current site
                links = find_links(url, soup)

                # only keep links that point to '.gov.si' websites
                links = [l for l in links if ".gov.si" in l]

                # TODO: parse HTML content
                # ...

            elif content_type in DOWNLOADABLE_CONTENT_TYPES:
                # TODO: store the document that is present in response
                pass

        # TODO: should return more data than just links
        # e.g. a 4-tuple (links, HTML, images, documents) <- is there a nicer way?
        return links


if __name__ == "__main__":
    SEED_PAGES_ALL = ["http://evem.gov.si", "http://e-uprava.gov.si", "http://podatki.gov.si",
                      "http://e-prostor.gov.si", "http://www.mz.gov.si/", "http://www.mnz.gov.si/",
                      "http://www.up.gov.si/", "http://www.ti.gov.si/", "http://www.mf.gov.si/"]
    SEED_PAGES_SAMPLE = SEED_PAGES_ALL[:3]

    a = Agent(seed_pages=SEED_PAGES_SAMPLE,
              num_workers=1)
    a.crawl()

