# govrilovic
![Too much?](sigil.png)
## What is this?
Have you ever searched for a form on any of the Slovenian government's sites and could not find it?  
Do you always print the wrong form for scholarship application?  
  
We have a simple solution - just use this tool to crawl all the .gov.si websites, extract any
pdfs, ppts, docxs, print them all and bring them with you to the administrative office - one of
them is bound to be correct!

**JK** - this is an implementation of a relatively simple crawler that crawls through
predefined .gov.si websites and extracts data from them. It is implemented for the purposes of
first assignment in the **Web information extraction and retrieval** course.

## Setup
**[Optional]** Create a virtualenv and activate it.
```
$ virtualenv --python=python3 --system-site-packages wiervenv
$ source wiervenv/bin/activate
```
  
Install required dependencies.
```
$ pip3 install -r requirements.txt
```
  
Install in dev mode.
```
$ python3 setup.py develop
```
Download [ChromeDriver](http://chromedriver.chromium.org).

Create environment variable for the location of Chrome Webdriver.
```
$CHROME_DRIVER="/path/to/chromedriver"
```
  
Note: you might need to replace `pip3` and `python3` with `pip` and `python` (respectively) if you 
are using Windows or just do not have these aliases on your system.  
(And no, this does not mean this works with Python 2.)

## Running the crawler
Running `docker-compose up` in the `/docker` directory sets up the database and runs `baza.sql` script (only the first time).
PostgreSQL is then available on port 5432 and mounted locally in `/db_data` directory.
After the database is set up and running, we need to run the crawler.
```
cd crawler
python3 core.py
```

## Folder structure
```
.
├── ...
├── crawler                    # Web crawler
│   ├── core.py                # Core crawler function
│   └── ...                    # etc.
├── docker                     # Database setup
├── ...                        # etc.
├── db                         # Database dump for 2 seed URLs
└──report.pdf                  # Final report PDF

```


## Brief guidelines (for development)
1. When writing regex, either explain it (comment the code) like you would explain it to a monkey
or use something other than regex for solving the problem.
2. Comment non-obvious code.
