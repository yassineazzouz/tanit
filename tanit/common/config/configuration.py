import logging as lg
import os
import os.path as osp
from threading import RLock
from six.moves.configparser import ConfigParser
from .configuration_keys import DEFAULT_PROPERTIES, ConfigurationKey

_glock = RLock()

_logger = lg.getLogger(__name__)

SITE_PROPERTIES_FILE = "tanit-site.ini"
CONFIG_DIR = osp.expanduser("/etc/tanit/conf")


class TanitConfiguration(object):
    __instance = None

    @staticmethod
    def getInstance():
        with _glock:
            if TanitConfiguration.__instance is None:
                TanitConfiguration.__instance = TanitConfiguration()
        return TanitConfiguration.__instance

    def __init__(self, path=None):
        if TanitConfiguration.__instance is not None:
            raise Exception("Only one instance of Client Factory is allowed!")
        self.properties = {}
        self.conf_file = osp.join(
            path or os.getenv("TANIT_CONF_DIR", CONFIG_DIR),
            SITE_PROPERTIES_FILE
        )

        self.reload()

    def reload(self):
        # clear configuration
        self.properties = {}
        # load defaults parameters
        for prop in DEFAULT_PROPERTIES:
            self.set(prop.key, prop.default_value)

        # load config file TANIT_CONF_DIR
        config = ConfigParser()

        if osp.exists(self.conf_file):
            try:
                config.read(self.conf_file)
            except Exception as e:
                raise TanitConfigurationException(
                    "Exception while loading configuration file %s.", self.conf_file, e
                )

            _logger.info("Instantiated configuration from %s.", self.conf_file)
        else:
            raise TanitConfigurationException(
                "Invalid configuration file %s.", self.conf_file
            )

        # load site parameters
        for section in config.sections():
            for (key, value) in config.items(section=section):
                self.set(ConfigurationKey(section, key), value)

    def set(self, key, value):
        self.properties[key] = value

    def unset(self, key):
        self.properties.pop(key)

    def key_set(self):
        return self.properties.keys()

    def get(self, key):
        if key in self.properties:
            return self.properties[key]
        else:
            return None

    def get_int(self, key):
        try:
            return int(self.get(key))
        except ValueError as e:
            raise InvalidConfigurationKeyException("Configuration key '%s' can not be converted to int.", e)

    def get_float(self, key):
        try:
            return float(self.get(key))
        except ValueError as e:
            raise InvalidConfigurationKeyException("Configuration key '%s' can not be converted to float.", e)


class TanitConfigurationException(Exception):
    pass


class InvalidConfigurationKeyException(Exception):
    pass
