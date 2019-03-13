import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import multiprocessing as mp
from time import sleep
import time
from selenium import webdriver
import selenium
from selenium.common.exceptions import TimeoutException
from urllib.parse import urlparse
from os.path import splitext
from os import environ
import sys

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

    def __init__(self, seed_pages, num_workers=None, sleep_period=5, get_images=False):
        # contains links for the next level of crawling (using BFS strategy)
        self.link_queue = set(seed_pages)
        self.visited = set()
        self.num_workers = num_workers if num_workers is not None else mp.cpu_count()
        self.sleep_period = sleep_period
        self.get_images = get_images

        # Selenium webdriver initialization
        chromedriver = environ["CHROME_DRIVER"]
        chrome_options = webdriver.ChromeOptions()
        # Accepts untrusted certificates and thus prevents some SSLErrors
        chrome_options.accept_untrusted_certs = True
        chrome_options.add_argument('--headless')
        self.driver = webdriver.Chrome(chrome_options=chrome_options, executable_path=chromedriver)
        # Set timeout for the request
        self.driver.set_page_load_timeout(TIMEOUT_PERIOD)


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

        # TODO - if url in self.visited, make a link between them.
        if url not in self.visited:
            try:
                # Will be omitted in next version. Use only HEAD to determine status_code and headers.
                response = requests.get(url, headers={"User-Agent": Agent.USER_AGENT},
                                        timeout=TIMEOUT_PERIOD)
                # We can not read head using Selenium, hence use requests.head -- produces weird status codes often - differ from response.status_code
                head = requests.head(url)
            except Exception as e:
                print("Requests error - ", e)
                return links

            # Selenium
            print("Selenium crawling '%s'..." % url)
            try:
                start = time.time()
                self.driver.get(url)
                page_source = self.driver.page_source
                end = time.time()
                print("Request time: ", round(end - start, 2), " seconds.")
            except TimeoutException:
                print("Timeout for this request reached.")
                return links
            except Exception as e:
                # Exception for everything else: bad handshakes, various errors
                print("Unexpected error:", e)
                return links

            
            # In the case of a redirect, head returns 302, while response returns 200. In one specific case, head even returned 403, while response was 200.
            print("Head: ", head.status_code )
            print("Response ", response.status_code)

            # TODO: there are other status codes that indicate success
            if not response or response.status_code not in [200, 201, 203]:
                return links


            # if Content-Type is not present in header (is this even possible?), assume it's HTML
            content_type = response.headers.get("Content-Type", "text/html")

            # https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Complete_list_of_MIME_types
            # possible to have {"Content-Type": "text/html; charset=utf-8"}
            if "text/html" in content_type:
                soup = BeautifulSoup(page_source, "html.parser")

                # TODO: detect unrendered javascript and process it using headless browser
                # ...

                # find images on the current site
                if self.get_images:
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

    # Check if environment variable is set
    try:  
        environ["CHROME_DRIVER"]
    except KeyError: 
        print("Please set the environment variable CHROME_DRIVER and reopen terminal. Read README.md for more information.")
        sys.exit(1)

    # We will first run our crawler with these seed pages only. Crawler will download images and binary data here.
    SEED_PAGES_THAT_REQUIRE_IMAGE_DOWNLOADS = ["http://evem.gov.si", "http://e-uprava.gov.si", "http://podatki.gov.si",
                      "http://e-prostor.gov.si"]

    # Crawler will not download images and binary data here.
    # No need to even include image links and binary files in the database.
    # Only data from tables site, page, link and page_type will be present in the actual DB dump
    SEED_PAGES_ALL = ["http://evem.gov.si", "http://e-uprava.gov.si", "http://podatki.gov.si",
                      "http://e-prostor.gov.si", "http://www.mz.gov.si/", "http://www.mnz.gov.si/",
                      "http://www.up.gov.si/", "http://www.ti.gov.si/", "http://www.mf.gov.si/"]
    SEED_PAGES_SAMPLE = SEED_PAGES_ALL[:3]

    a = Agent(seed_pages=SEED_PAGES_SAMPLE,
              num_workers=1, get_images=True)
    # TODO: On specific key press, stop the script and save current state
    a.crawl()

    


