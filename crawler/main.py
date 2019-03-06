"""
This is the crawler's entrypoint which should
be used to run the whole crawling process.
"""
from settings import SettingsReader

if __name__ == "__main__":
    config = SettingsReader('./config.yaml').config

    # TODO: initialize workers here
