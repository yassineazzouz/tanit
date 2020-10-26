import json
import logging as lg
import os

_logger = lg.getLogger(__name__)


class FileSystemsConfig(object):
    config_file = "filesystems.json"
    default_conf_dir = os.path.expanduser("/etc/tanit/conf")

    def __init__(self, path=None):
        self._filesystems = {}

        self.conf_dir = path or os.getenv("TANIT_CONF_DIR", self.default_conf_dir)
        self.conf_file = os.path.join(self.conf_dir, self.config_file)

        if os.path.exists(self.conf_file):
            try:
                self.config = json.loads(open(self.conf_file).read())
            except Exception as e:
                raise TanitConfigurationException(
                    "Exception while loading configuration file %s.", self.conf_file, e
                )

            _logger.info(
                "Instantiated filesystems configuration from %s.", self.conf_file
            )
        else:
            _logger.warning(
                "Could not find filesystems configuration file, "
                + "instantiating empty configuration."
            )
            self.config = None

    def get_config(self):
        return self.config


class TanitConfigurationException(Exception):
    pass
