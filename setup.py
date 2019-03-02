from setuptools import setup
from warnings import warn
import sys

if sys.version_info.major < 3:
    warn("This implementation was made for Python 3, so it might not work with Python 2")

setup(
   name="govrilovic",
   version="0.1",
   description="Crawler for .gov.si websites.",
   author="Andraz Povse, Matej Klemen, Jaka Stavanja",
   packages=["crawler"]
)
