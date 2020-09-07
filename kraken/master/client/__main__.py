#!/usr/bin/env python
# encoding: utf-8

"""kraken-client: A simple thrift client for kraken service.

Usage:
  kraken-client [-v...] [--status jid] [--list]
  kraken-client (--version | -h)

Options:
  --version                     Show version and exit.
  -h --help                     Show help and exit.
  -v --verbose                  Enable log output. Can be specified up to three
                                times (increasing verbosity each time).
  --status JOB_ID               Disable checksum check prior to file transfer. This will force
                                overwrite.
  --list                        Do not create the same directory strecture at the destination and copy
                                files only under DEST_PATH.

Examples:
  pydistcp -s prod -d preprod -v /tmp/src /tmp/dest

"""

from ... import __version__
from .client import MasterClient
from ..config.config import MasterConfig
import requests as rq
import logging as lg
from docopt import docopt

def configure_logging():
    # capture warnings issued by the warnings module  
    try:
      # This is not available in python 2.6
      lg.captureWarnings(True)
    except:
      # disable annoying url3lib warnings
      rq.packages.urllib3.disable_warnings()
      pass

    logger = lg.getLogger()
    logger.setLevel(lg.DEBUG)
    lg.getLogger('requests_kerberos.kerberos_').setLevel(lg.CRITICAL)
    lg.getLogger('requests').setLevel(lg.ERROR)

    # Configure stream logging if applicable
    stream_handler = lg.StreamHandler() 
    stream_handler.setLevel(lg.INFO)

    fmt = '%(levelname)s\t%(message)s'
    stream_handler.setFormatter(lg.Formatter(fmt))
    logger.addHandler(stream_handler)
    
def main(argv=None):
    
    args = docopt(__doc__, argv=argv, version=__version__)
    
    configure_logging()
    
    config = MasterConfig()
    client = MasterClient(config.client_service_host, config.client_service_port)
    client.start()
    if (args['--list']):
        for job in client.list_jobs():
            print("id: {}, state: {}".format(job.id, job.state))
    else:
        client.dummy_job()
    client.stop()

if __name__ == '__main__':
    main()