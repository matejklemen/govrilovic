from urllib.robotparser import RobotFileParser, Entry, RuleLine, RequestRate
import urllib


class Robots(RobotFileParser):
    """
    A simple wrapper around urllib's robots.txt parser
    that reads the robots.txt on creation of the object.

    To call custom functions needed from urllib.robotsparser.RobotsFileParser,
    call it in the same way you do with the RobotsFileParser as we are extending
    the class.
    """

    def __init__(self, website_url):
        """
        Initializes the Robots object and parses the robots.txt file if it exists.
        If the file doesn't exist, it will respond with True on every can_fetch() call.

        Parameters
        ----------
        website_url: str
            The url of the website from which we will read the robots.txt file
        """
        super().__init__()
        self.url = website_url + '/robots.txt'
        self.sitemap_location = None
        self.read()

    def parse(self, lines):
        """Parse the input lines from a robots.txt file.

        We allow that a user-agent: line is not preceded by
        one or more blank lines.

        Most of the functionality is copied from the original RobotsFileParser
        with the addition of the Sitemap rule.
        """
        # states:
        #   0: start state
        #   1: saw user-agent line
        #   2: saw an allow or disallow line
        state = 0
        entry = Entry()

        self.modified()
        for line in lines:
            if not line:
                if state == 1:
                    entry = Entry()
                    state = 0
                elif state == 2:
                    self._add_entry(entry)
                    entry = Entry()
                    state = 0
            # remove optional comment and strip line
            i = line.find('#')
            if i >= 0:
                line = line[:i]
            line = line.strip()
            if not line:
                continue
            line = line.split(':', 1)
            if len(line) == 2:
                line[0] = line[0].strip().lower()
                line[1] = urllib.parse.unquote(line[1].strip())
                if line[0] == "user-agent":
                    if state == 2:
                        self._add_entry(entry)
                        entry = Entry()
                    entry.useragents.append(line[1])
                    state = 1
                elif line[0] == "disallow":
                    if state != 0:
                        entry.rulelines.append(RuleLine(line[1], False))
                        state = 2
                elif line[0] == "allow":
                    if state != 0:
                        entry.rulelines.append(RuleLine(line[1], True))
                        state = 2
                elif line[0] == "crawl-delay":
                    if state != 0:
                        # before trying to convert to int we need to make
                        # sure that robots.txt has valid syntax otherwise
                        # it will crash
                        if line[1].strip().isdigit():
                            entry.delay = int(line[1])
                        state = 2
                elif line[0] == "request-rate":
                    if state != 0:
                        numbers = line[1].split('/')
                        # check if all values are sane
                        if (len(numbers) == 2 and numbers[0].strip().isdigit()
                                and numbers[1].strip().isdigit()):
                            entry.req_rate = RequestRate(
                                int(numbers[0]), int(numbers[1]))
                        state = 2

                elif line[0] == "sitemap":
                    '''
                    This is our addition. We could do this in a separate method, but that would mean
                    that we would need to parse the contents again and search for a Sitemap entry.
                    '''
                    if state != 0:
                        self.sitemap_location = line[1]
                        state = 2
        if state == 2:
            self._add_entry(entry)

    def crawl_delay(self):
        """
        Returns
        ----------
        crawl_delay: int
            The delay between two requests to the current domain in seconds.
        """

        return super().crawl_delay('*')

    def can_fetch(self, page):
        """
        Tells whether the agent (*) is allowed to crawl a page from the current domain.

        Parameters
        ----------
        page: str
            The page relative URL that one is trying to get permission to crawl.

        Returns
        ----------
        can_fetch: boolean
            True if the agent is allowed to crawl the page, else False.
        """
        return super().can_fetch('*', page)


if __name__ == '__main__':

    r = Robots('https://www.coca-cola.si')
    print(r.can_fetch('/content/dam/'))  # isn't allowed
    print(r.can_fetch('/should_be_ok'))  # is allowed
    print(r.sitemap_location)

    r = Robots('https://podatki.gov.si')
    print(r.can_fetch('/knjiznica'))

    if not r.crawl_delay():
        print('Do as many requests as you want.')
    else:
        print('Wait for {} seconds between requests'.format(r.crawl_delay()))
