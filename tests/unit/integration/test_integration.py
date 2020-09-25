import pytest
import time
import os
from threading import Thread
from kraken.master.config.config import MasterConfig
from kraken.master.server.server import MasterServer
from kraken.master.client.client import ClientFactory

from ..resources import conf

config_dir = os.path.dirname(os.path.abspath(conf.__file__))

def test_integration():
    def get_user_client():
        pass