import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import multiprocessing as mp
from time import sleep, time
from selenium import webdriver
import selenium
from selenium.common.exceptions import TimeoutException
from urllib.parse import urlparse
from os.path import splitext, exists, abspath, join, dirname
from os import environ, makedirs
import sys
from urllib.request import urlretrieve
from datetime import datetime
from crawler import db, lsh
from crawler import robots as rb
from crawler import sitemap as sm
from crawler.links import Links
import re


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
TIMEOUT_PERIOD = 5.0


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

    for anchor in a_tags:
        link = anchor.get("href")
        onclick = anchor.get("onclick")
        if link:
            processed_link = link
            if link[0] in {"/", "?"}:
                processed_link = urljoin(base_url, link)
            elif link[0] == "#":
                continue

            processed_link = Links.sanitize(processed_link)
            processed_link = Links.prune_to_max_depth(processed_link, 10)
            links.append(processed_link)
        if onclick and parse_js_redirects:
            links.extend([Links.prune_to_max_depth(Links.sanitize(second_el), 10)
                          for _, second_el in re.findall(regexp_set_value, onclick)])
            links.extend([Links.prune_to_max_depth(Links.sanitize(second_el), 10)
                          for _, second_el in re.findall(regexp_self_top, onclick)])
            links.extend([Links.prune_to_max_depth(Links.sanitize(second_el), 10)
                          for _, second_el in re.findall(regexp_func_call, onclick)])

    if parse_js_redirects:
        for button in button_tags:
            onclick = button.get("onclick")
            if onclick:
                links.extend([Links.prune_to_max_depth(Links.sanitize(second_el), 10)
                              for _, second_el in re.findall(regexp_set_value, onclick)])
                links.extend([Links.prune_to_max_depth(Links.sanitize(second_el), 10)
                              for _, second_el in re.findall(regexp_self_top, onclick)])
                links.extend([Links.prune_to_max_depth(Links.sanitize(second_el), 10)
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


class Agent:
    USER_AGENT = "govrilovic-crawler/v0.1"

    def __init__(self, seed_pages, num_workers=None, sleep_period=5, get_files=False):
        # contains links for the next level of crawling (using BFS strategy)
        self.link_queue = set(seed_pages)
        self.visited = set()
        # Unique sites, each has its own (possibly) robots.txt file etc.
        self.sites = set()
        self.num_workers = num_workers if num_workers is not None else mp.cpu_count()
        self.sleep_period = sleep_period
        self.get_files = get_files
        # Each root URL gets its own robots_file. Check this to see if new url is allowed.
        self.robots_file = {}

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

        # Database
        self.db = db.Database()

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
            self.db.add_page(root_site_id, page_type, url, html_content, status_code)
            if page_type == "DUPLICATE":
                # html_content will be None
                self.db.add_page(root_site_id, page_type, url, html_content, status_code)
                # TODO: Make an insertion into "link" table
                # TODO: Have the information about the site, this one was equal to - link them
                print("dup")
        else:
            pass
            # TODO: Make in insertion into page_data
            # content_type tells us if it is .doc, .pptx etc.

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
        curr_level_links = self.link_queue
        self.link_queue = set()

        # remove duplicate links before dividing among workers so that the tasks
        # are more evenly split
        relevant_links = [
            link for link in curr_level_links if link not in self.visited]
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
        set:
            Obtained unique* links (there might be a link that is a duplicate of link
            obtained from another worker!)
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
        list
            Obtained links
        """
        links = []
        
        # Parsing URL
        parsed_url = urlparse(url)
        # URL of the site. This is the base url, which possibly has robots.txt etc.
        site_url = parsed_url.netloc
        # Relative path
        path_url = parsed_url.path

        if url not in self.visited:
            # TODO: LSH compare here
            # self.insert_page_into_db(url, "HTML", None, response.status_code, site_url, "DUPLICATE")

            # Check if you can crawl this page in robots file.
            if site_url in self.robots_file and not self.robots_file[site_url].can_fetch(path_url):
                return links
            try:
                response = requests.get(url, headers={"User-Agent": Agent.USER_AGENT},
                                        timeout=TIMEOUT_PERIOD)
            except Exception as e:
                print("Requests error - ", e)
                return links

            # Selenium
            print("Crawling '%s'..." % url)
            try:
                start = time()
                self.driver.get(url)
                page_source = self.driver.page_source
                end = time()
                print("Request time: ", round(end - start, 2), " seconds.")
            except TimeoutException:
                print("Timeout for this request reached.")
                return links
            except Exception as e:
                # Exception for everything else: bad handshakes, various errors
                print("Unexpected error:", e)
                return links

            print("Response code ", response.status_code)

            # TODO: there are other status codes that indicate success
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
                    print("Found robots")
                except:
                    print("No robots file found.")
                    # Robots failed.
                try:
                    sitemap = sm.Sitemap(robots.sitemap_location)
                    # Add entire sitemap to 'links' array
                    links.extend(sitemap.urls)
                    print("Found sitemap")
                except:
                    # Sitemap from robots failed.
                    try:
                        sitemap = sm.Sitemap(
                            parsed_url.scheme + '://' + site_url)
                        # Add entire sitemap to 'links' array
                        links.extend(sitemap.urls)
                        print("Found sitemap")
                    except Exception as e:
                        print("No sitemap found.")
                        # Sitemap failed.

                # Insert this new Site into the DB
                self.db.add_site_info_to_db(
                    site_url, str(robots), str(sitemap))
                # Add the new site into the set.
                self.sites.add(site_url)
                print("New root website added: ", site_url)

            # https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Complete_list_of_MIME_types
            # possible to have {"Content-Type": "text/html; charset=utf-8"}
            if "text/html" in content_type:
                soup = BeautifulSoup(page_source, "lxml")

                # Check if there is a base href url from which we have to assemble file paths
                base_url = get_base_href(soup, fallback=url)

                # Insert page into the database
                self.insert_page_into_db(url, "HTML", str(
                    soup), response.status_code, site_url, "HTML")

                # find images on the current site. Save to FS and DB
                if self.get_files:
                    find_images(base_url, soup, self.db)

                # find links on current site
                found_links = find_links(url, soup)

                # LATER: only keep links that point to '.gov.si' websites
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
              num_workers=1, get_files=True)
    # TODO: On specific key press, stop the script and save current state

    # Truncates every table except data_type, page_type --- they have fixed types in them
    # WARNING: disable this when you want to start from a saved state
    a.db.truncate_everything()

    a.crawl()
