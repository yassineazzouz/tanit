#!/usr/bin/env python

from ConfigParser import ConfigParser
import os
import os.path as osp
import logging as lg

_logger = lg.getLogger(__name__)

class Config(object):

    config_file = "kraken.conf" 
    default_conf_dir = osp.expanduser('/etc/kraken/conf')
        
    def __init__(self, path=None):
        self.config = ConfigParser()
        
        self.conf_dir = path or os.getenv('KRAKEN_CONF_DIR', self.default_conf_dir)
        self.conf_file = osp.join(self.conf_dir, self.config_file)
        
        if osp.exists(self.conf_file):
            try:
                self.config.read(self.conf_file)
                try:
                    self.load()
                except KrakenConfigurationException as e:
                    _logger.error("Error loading configuration from file %s.", self.conf_file)
                    raise e

            except Exception as e:
                raise KrakenConfigurationException('Exception while loading configuration file %s.', self.conf_file, e)

            _logger.info('Instantiated configuration from %s.', self.conf_file)
        else:
            raise KrakenConfigurationException('Invalid configuration file %s.', self.conf_file)
    
class KrakenConfigurationException(Exception):
    pass