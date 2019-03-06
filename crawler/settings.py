"""
This is a YAML reading util which reads a file that includes 
the configuration options. It then constructs a settings
object which is used to set different parameters of the crawler.

Example usage:

##### config.yaml: #####

threads: 2
some_other_parameter: foobar

##### example.py #####

from settings import SettingsReader

> config = SettingsReader('./config.yaml').config
> config

{'threads': 2, 'some_other_parameter': 'foobar'}

"""

from yaml import load


class SettingsReader:

    """
    Default settings for the crawler.
    """
    config = {
        'threads': 5
    }

    def __init__(self, config_file_path):
        with open(config_file_path, 'r') as config_file:
            """
            Merges the default configuration with additional
            parameters from the config file (which override defaults).
            """
            custom_config = load(config_file)
            self.config = {**self.config, **custom_config}
