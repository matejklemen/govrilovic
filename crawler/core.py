import requests
import re
import threading
import selenium
import sys

from queue import Queue
from time import sleep, time
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from urllib.request import urlretrieve
from urllib.parse import urlparse, urljoin
from os import environ, makedirs
from os.path import splitext, exists, abspath, join, dirname

from crawler import db, lsh
from crawler import robots as rb
from crawler import sitemap as sm
from crawler.links import Links


"""
This file contains the main crawler class along with
its core functionalities (methods for parsing, accessing
the frontier, adding links to the crawled set, etc.).
"""

# In order: '.pptx', '.pdf', '.doc', '.ppt' and '.docx' files
DOWNLOADABLE_CONTENT_TYPES = {"application/vnd.openxmlformats-officedocument.presentationml.presentation": "PPTX",
                              "application/pdf": "PDF",
                              "application/msword": "DOC",
                              "application/vnd.ms-powerpoint": "PPT",
                              "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "DOCX"}
# how long the crawler waits before giving up on a page (in seconds)
TIMEOUT_PERIOD = 10.0


def get_url_extension(url):
    # Returns the filename extension or '' if none. For example "png", "html", ...

    parsed = urlparse(url)
    root, extension = splitext(parsed.path)
    return extension[1:]


def get_base_href(soup_obj, fallback):
    """ Gets the href from the <base href="some_url.com" /> tag.
    Returns the fallback if no base element found.

    Parameters
    ----------
    soup_obj: bs4.BeautifulSoup
        BeautifulSoup's object, containing the response for `current_url`

    fallback: str
        Current site's URL to use if a base href is not found in the soup_obj.

    Returns
    -------
    str:
        Base href or default site's url from which we can download files/images.
    """

    if not fallback:
        raise ValueError("Fallback url should be provided in case a base href is not found.")

    href = fallback
    try:
        href = soup_obj.base.get('href')
    except Exception as e:
        pass

    return href


def find_links(base_url, soup_obj, parse_js_redirects=False):
    """ Find links inside <a> tags.

    Parameters
    ----------
    base_url: str
        URL of page whose content `soup_obj` contains

        Should be passed from the <base href=".."> element if it exists.

    soup_obj: bs4.BeautifulSoup
        BeautifulSoup's object, containing the response for `base_url`

    parse_js_redirects: bool
        Boolean which tells the function whether it should also look for javascript
        redirects inside onclick="window.location = 'somelink'"-like objects.

    Returns
    -------
    list of str:
        List of absolute URLs
    """

    """
    This regexp will extract links from value assignments of kind window.location(.href/.assign) = "link"
    E.g. this example will result in "link".
    """
    regexp_set_value = r"window.location(\.href|\.assign)?\s*=\s*'(.+)'"

    """
    This regexp will extract links from value of kind self/top.location = "link" (could be .replace)
    E.g. this example will result in "link".
    """
    regexp_self_top = r"(self|top)\.location\s*=\s*'(.+)'"

    """
    This regexp will extract links from functional calls of kind window.location.assign("yeet") (could be .replace)
    E.g. this example will result in "yeet".
    """
    regexp_func_call = r"window.location(\.assign|\.replace)\('(.+)'"

    a_tags = soup_obj.find_all("a")
    button_tags = soup_obj.find_all("button")
    links = []

    def normalize_url(url): return Links.remove_www(
        Links.prune_to_max_depth(Links.sanitize(url), 10))

    for anchor in a_tags:
        link = anchor.get("href")
        onclick = anchor.get("onclick")
        if link:
            processed_link = link
            if link[0] in {"/", "?"}:
                processed_link = urljoin(base_url, link)
            elif link[0] == "#":
                continue

            links.append(normalize_url(processed_link))
        if onclick and parse_js_redirects:
            links.extend([normalize_url(second_el)
                          for _, second_el in re.findall(regexp_set_value, onclick)])
            links.extend([normalize_url(second_el)
                          for _, second_el in re.findall(regexp_self_top, onclick)])
            links.extend([normalize_url(second_el)
                          for _, second_el in re.findall(regexp_func_call, onclick)])

    if parse_js_redirects:
        for button in button_tags:
            onclick = button.get("onclick")
            if onclick:
                links.extend([normalize_url(second_el)
                              for _, second_el in re.findall(regexp_set_value, onclick)])
                links.extend([normalize_url(second_el)
                              for _, second_el in re.findall(regexp_self_top, onclick)])
                links.extend([normalize_url(second_el)
                              for _, second_el in re.findall(regexp_func_call, onclick)])

    return links


def save_image(base_url, image_src):
    """
    Saves an image to the disk under the current crawled page's URL. The image's
    path will look something like '../files/images/example.com/image_name.png'

    Parameters
    ----------
    base_url: str
        URL of the page which we are currently crawling so we can
        locate the correct folder we need to save the image in.

        Should be passed from the <base href=".."> element if it exists.

    image_src: str
        URL of image which you want to download.

    Returns
    -------
    TODO
    """
    # cross-platform path to the file/images directory
    images_dir = abspath(join(dirname(__file__), '..', 'files', 'images'))

    # the path to the folder of the current crawled page
    current_url_directory = images_dir + '/' + urlparse(base_url).netloc

    # getting the URL element from the last slash onwards and treating it as the filename
    image_filename = image_src.rsplit('/', 1)[-1]

    # filename (path) of the downloaded image
    image_destination = current_url_directory + '/' + image_filename

    if not exists(current_url_directory):
        makedirs(current_url_directory)

    abs_image_url = base_url + '/' + image_src
    try:
        urlretrieve(image_src, image_destination)
        print("Got image: ", image_filename)
    except Exception as e:
        try:
            # Try with absolute URL
            urlretrieve(abs_image_url, image_destination)
            print("Got image (abs): ", image_filename)
        except Exception as e2:
            print(abs_image_url)
            print("Failed to retrieve image")
            # print(image_src)
            # print(abs_image_url)
            print(e)
            print(e2)

    return image_destination, image_filename


def save_file(base_url, file_src, file_extension, db):
    """
    Saves a file to the disk under the current crawled page's URL. The file's
    path will look something like '../files/pptx/example.com/some_pres.pptx'

    Parameters
    ----------
    base_url: str
        URL of the page which we are currently crawling so we can
        locate the correct folder we need to save the file in.

        Should be passed from the <base href=".."> element if it exists.

    file_src: str
        URL of the file from which you want to download it.

    file_extension: str
        The extension of the file that you have detected from the response's
        content-type. It will be used to save the file under a correct folder
        inside the files directory.
    db: 
        Database connection

        E.g. files/pptx/example.com/pres.pptx or files/pdf/example.com/pricelist.pdf

    Returns
    -------
    TODO
    """
    # cross-platform path to the files/extension directory
    files_dir = abspath(join(dirname(__file__), '..', 'files', file_extension))

    # the path to the folder of the current crawled page
    current_url_directory = files_dir + '/' + base_url

    # getting the URL element from the last slash onwards and treating it as the filename
    file_filename = file_src.rsplit('/', 1)[-1]

    # filename (path) of the downloaded file
    file_destination = current_url_directory + '/' + file_filename

    if not exists(current_url_directory):
        makedirs(current_url_directory)

    try:
        urlretrieve(file_src, file_destination)
        print("Got file: ", file_filename)
    except Exception as e:
        print("Failed to retrieve file.")
        print(e)

    # TODO: Save the file into the database (link was saved already - foreign key)
    # ...

    return file_destination, file_filename


def find_images(current_url, soup_obj, db):
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
                # Download the image to the disk
                (image_path, image_name) = save_image(current_url, processed_src)
                # Save the image information to DB
                db.add_image(current_url, image_name, get_url_extension(processed_src), image_path)

                # TODO: check whether we need to return the images links list at all
                # we could extract the save_image call to another function called save images,
                # which would accept a list of links to images to save (but that calls for
                # another iteration over all links)
                images.append(processed_src)
                # Probably no need to even return images. They will be saved into db in this function.

    return images


def read_vocab_file(name):
    with open(name) as f:
        content = f.readlines()
    # Remove whitespace characters like `\n` at the end of each line
    content = [x.strip() for x in content]
    return content


def triples(big_string):
    """
    input: Big string, for example html_content
    return: list of triples from that string
    """
    for i in range(len(big_string) - 3 + 1):
        yield big_string[i: i + 3]


class Agent:
    USER_AGENT = "govrilovic-crawler/v0.1"
    MAX_CRAWLED_PAGES = 100000

    def __init__(self, seed_pages, num_workers=None, sleep_period=5, get_files=False):
        # contains links for the next level of crawling (using BFS strategy)
        self.link_queue = set(seed_pages)
        self.visited = set()
        # Unique sites, each has its own (possibly) robots.txt file etc.
        self.sites = set()
        self.num_workers = num_workers if num_workers is not None else 1
        self.sleep_period = sleep_period
        self.get_files = get_files
        # Each root URL gets its own robots_file. Check this to see if new url is allowed.
        self.robots_file = {}
        self.last_crawled = {}
        self.thread_res_queue = Queue()

        # number of visited unique links
        self.visited_uniq_links = 0

        # Selenium webdriver initialization
        chromedriver = environ["CHROME_DRIVER"]
        chrome_options = webdriver.ChromeOptions()
        # Accepts untrusted certificates and thus prevents some SSLErrors
        chrome_options.accept_untrusted_certs = True
        chrome_options.add_argument('--headless')
        self.driver = webdriver.Chrome(
            chrome_options=chrome_options, executable_path=chromedriver)
        # Set timeout for the request
        self.driver.set_page_load_timeout(TIMEOUT_PERIOD)

        # LSH object
        vocab = read_vocab_file("./data/triples_etc.txt")
        self.lsh_obj = lsh.LocalitySensitiveHashing(vocab,
                                                    num_hash=4,
                                                    hash_funcs=[
                                                        lambda idx: (idx + 1) % 5,
                                                        lambda idx: (3 * idx + 1) % 5,
                                                        lambda idx: hash(idx),
                                                        lambda idx: hash(3*idx)
                                                    ],
                                                    num_bands=4,
                                                    repr_func=triples)

        # Database
        self.pool = db.Pool()
        self.db = None # gets initialized by thread worker

    def insert_page_into_db(self, url, content_type, html_content, status_code, site_url, page_type="HTML"):
        """ Inserts page into the database.

        Parameters
        ----------
        url: str

        content_type: str
            available content types are PDF, DOC, DOCX, PPT, PPTX
            
        html_content: str or None
            if it is not html, this is None. If it is a duplicate also None.

        status_code: int
            200, other 200 codes

        site_url: str
            ROOT url of the website. This page is connected to site table with site_url value.

        page_type: str
            available page types are HTML, BINARY, DUPLICATE and FRONTIER
        """

        root_site_id = self.db.root_site_id(site_url)
        if content_type == "HTML":
            lsh_hash = "".join(map(str, self.lsh_obj.compute_signature(str(html_content))))
            self.db.add_page(root_site_id, page_type, url, html_content, status_code, lsh_hash)

            if page_type == "DUPLICATE":
                # We will probably insert this somewhere else. Two URLS are needed.
                # TODO: Make an insertion into "link" table
                # TODO: Have the information about the site, this one was equal to - link them
                pass

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
            print("[Level %d] Links to be crawled: %d..." %
                  (curr_level, len(self.link_queue)))
            self.crawl_level()
            print("[Level %d] New links produced: %d..." %
                  (curr_level, len(self.link_queue)))
            curr_level += 1

    def crawl_level(self):
        """ Performs a single level of breadth-first search."""
        # get pages for the next level and clear the queue
        relevant_links = list(self.link_queue)

        # if current level contains an amount of links that would bring us over maximum,
        # only take a part of the links to be crawled in the next level
        if self.visited_uniq_links + len(relevant_links) > Agent.MAX_CRAWLED_PAGES:
            relevant_links = relevant_links[: (Agent.MAX_CRAWLED_PAGES - self.visited_uniq_links)]

        num_links = len(relevant_links)
        self.link_queue = set()
        effective_workers = min(self.num_workers, num_links)

        workers = []
        next_level_links = set()
        print("[crawl_level] Creating {} workers...".format(effective_workers))
        # divide relevant links among workers (as evenly as possible)
        for id_worker in range(effective_workers):
            idx_start = int(float(id_worker) * num_links / effective_workers)
            idx_end = int(float(id_worker + 1) * num_links / effective_workers)

            links_to_crawl = relevant_links[idx_start: idx_end]
            workers.append(threading.Thread(target=self.worker_task,
                                            args=(links_to_crawl, id_worker)))

        for id_worker in range(effective_workers):
            workers[id_worker].start()

        # Wait for all threads to finish so we have all links for current depth obtained
        for id_worker in range(effective_workers):
            workers[id_worker].join()

        # Deduplicate obtained links by workers because multiple workers might have extracted
        # the same link twice independently
        while not self.thread_res_queue.empty():
            curr_res = self.thread_res_queue.get()

            for link in curr_res:
                if link not in next_level_links:
                    next_level_links.add(link)

        self.visited_uniq_links += num_links

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

        """
        self.db = db.Database(self.pool)
        idx_curr_page = 0
        produced_links = set()
        for url in urls:
            print("[worker_task] Worker with ID={} crawling '{}'... {} pages left "
                  "for this thread".format(id_worker, url, len(urls) - idx_curr_page))
            new_urls = self.crawl_page(url=url)
            # Insert new data into the database
            produced_links.update(new_urls)
            sleep(self.sleep_period)
            idx_curr_page += 1

        self.thread_res_queue.put(produced_links)

    def crawl_page(self, url):
        """ Crawl a single web page denoted by `url`. The URL is expected to be preprocessed
        (if needed) and VALID.

        Parameters
        ----------
        url: str
            URL of web page.

        Returns
        -------
        list
            Obtained links
        """
        links = []
        duplicate_page = False
        # Parsing URL
        parsed_url = urlparse(url)
        # URL of the site. This is the base url, which possibly has robots.txt etc.
        site_url = parsed_url.netloc
        # Relative path
        path_url = parsed_url.path

        if url not in self.visited:
            # Check if you can crawl this page in robots file.
            if site_url in self.robots_file and not self.robots_file[site_url].can_fetch(path_url):
                return links

            if site_url in self.last_crawled:
                cooldown = 3  # default
                if site_url in self.robots_file:
                    cooldown = self.robots_file[site_url].crawl_delay()
                cooldown_so_far = time() - self.last_crawled[site_url]
                if cooldown_so_far < cooldown:
                    sleep(cooldown-cooldown_so_far)
                    print("[crawl_page] Waited %f second before crawling website..." %
                          (cooldown - cooldown_so_far))
            try:
                response = requests.get(url, headers={"User-Agent": Agent.USER_AGENT},
                                        timeout=TIMEOUT_PERIOD)
            except Exception as e:
                print("[crawl_page] Requests error - ", e)
                return links


            # TODO: LSH compare here
            '''
            check if for lsh match
                if any matches in lsh, check for exact match of text???
                    if matches, then ENTRY INTO LINKS
                        duplicate_page = True
                        self.insert_page_into_db(url, "HTML", None, response.status_code, site_url, "DUPLICATE")

            NO NEED FOR ANY SELENIUM CRAWLING -> html_content is set to None
            return links
            '''

            # Selenium
            print("[crawl_page] Passed duplicate checks, crawling '%s'..." % url)
            try:
                start = time()
                self.driver.get(url)
                page_source = self.driver.page_source
                self.last_crawled[site_url] = start
                end = time()
                print("[crawl_page] Request time: ", round(end - start, 2), " seconds...")
            except TimeoutException:
                print("[crawl_page] Timeout for request to '{}' reached...".format(url))
                return links
            except Exception as e:
                # Exception for everything else: bad handshakes, various errors
                print("[crawl_page] Unexpected error for '{}'...{}".format(url, e))
                return links

            print("[crawl_page] Response code for request to '{}': {}".format(
                url, response.status_code))

            if not response or response.status_code not in [200, 203, 302]:
                return links

            # if Content-Type is not present in header (is this even possible?), assume it's HTML
            content_type = response.headers.get("Content-Type", "text/html")

            if site_url not in self.sites:
                robots = None
                sitemap = None
                try:
                    print(site_url)
                    robots = rb.Robots(parsed_url.scheme + '://' + site_url)
                    print("[crawl_page] Found robots for '{}'...".format(url))
                except:
                    print("[crawl_page] No robots file found for '{}'...".format(url))
                    # Robots failed.
                try:
                    sitemap = sm.Sitemap(robots.sitemap_location)
                    # Add entire sitemap to 'links' array
                    links.extend(sitemap.urls)
                    print("[crawl_page] Found sitemap for '{}'...".format(url))
                except:
                    # Sitemap from robots failed.
                    try:
                        print("[crawl_page] Robots for '{}' didn't contain sitemap url. Trying "
                              "default one ...".format(url))
                        sitemap = sm.Sitemap(
                            parsed_url.scheme + '://' + site_url + '/sitemap.xml')
                        # Add entire sitemap to 'links' array
                        links.extend(sitemap.urls)
                        print("[crawl_page] Found sitemap at default location for '{}'...".format(
                            url))
                    except Exception as e:
                        print("[crawl_page] No sitemap found ANYWHERE for '{}'...".format(url))
                        # Sitemap failed.

                # Insert this new Site into the DB
                self.db.add_site_info_to_db(
                    site_url, str(robots), str(sitemap))
                # Add the new site into the set.
                self.sites.add(site_url)
                print("[crawl_page] New root website added: {}".format(site_url))

            # https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Complete_list_of_MIME_types
            # possible to have {"Content-Type": "text/html; charset=utf-8"}
            if "text/html" in content_type:
                soup = BeautifulSoup(page_source, "lxml")

                # Check if there is a base href url from which we have to assemble file paths
                base_url = get_base_href(soup, fallback=url)

                # Insert page into the database
                if not duplicate_page:
                    self.insert_page_into_db(url, "HTML", str(
                        soup), response.status_code, site_url, "HTML")

                # find images on the current site. Save to FS and DB
                if self.get_files:
                    find_images(base_url, soup, self.db)

                # find links on current site
                found_links = find_links(url, soup)

                # TODO: only keep links that point to '.gov.si' websites
                # CURRENT: only keep links that point to evem.gov.si and e-prostor.gov.si
                found_links = [
                    l for l in found_links if "evem.gov.si" in l or "e-prostor.gov.si" in l]

                # Extend to links. There might be some from sitemap.
                links.extend(found_links)

            # Check if content is downloadable AND we are downloading files
            elif content_type in DOWNLOADABLE_CONTENT_TYPES.keys() and self.get_files:
                file_extension = DOWNLOADABLE_CONTENT_TYPES[content_type]

                # Insert page into the database. Html_content is NULL
                self.insert_page_into_db(
                    url, file_extension, None, response.status_code, site_url, "BINARY")

                save_file(site_url, url, file_extension, self.db)

        return links


if __name__ == "__main__":
    # Check if environment variable is set
    try:
        print()
        environ["CHROME_DRIVER"]
    except KeyError:
        print("Please set the environment variable CHROME_DRIVER and reopen terminal. "
              "Read README.md for more information.")
        sys.exit(1)

    # We will first run our crawler with these seed pages only.
    # Crawler will download images and binary data here.
    SEED_PAGES_THAT_REQUIRE_DOWNLOADS = [
        "http://evem.gov.si", "http://e-prostor.gov.si"]

    # Crawler will not download images and binary data here.
    # No need to even include image links and binary files in the database.
    # Only data from tables site, page, link and page_type will be present in the actual DB dump
    SEED_PAGES_ALL = ["http://evem.gov.si", "http://e-uprava.gov.si", "http://podatki.gov.si",
                      "http://e-prostor.gov.si", "http://www.mz.gov.si/", "http://www.mnz.gov.si/",
                      "http://www.up.gov.si/", "http://www.ti.gov.si/", "http://www.mf.gov.si/"]
    SEED_PAGES_SAMPLE = SEED_PAGES_ALL[:3]

    a = Agent(seed_pages=SEED_PAGES_THAT_REQUIRE_DOWNLOADS,
              num_workers=4, get_files=True)
    # TODO: On specific key press, stop the script and save current state

    # Truncates every table except data_type, page_type --- they have fixed types in them
    # WARNING: disable this when you want to start from a saved state
    temp_db = db.Database(a.pool)
    temp_db.truncate_everything()
    temp_db.pool.pool.putconn(temp_db.connection)

    crawl_start = time()
    try:
        a.crawl(max_level=None)
    except KeyboardInterrupt:
        pass
    crawl_end = time()

    print("Visited {} links in {} seconds...".format(a.visited_uniq_links, crawl_end - crawl_start))
