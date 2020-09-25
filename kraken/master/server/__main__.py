#!/usr/bin/env python
# encoding: utf-8

"""kraken-server: Kraken master.

Usage:
  kraken-client [-v...] [--standalone]
  kraken-client (--version | -h)

Options:
  --version                     Show version and exit.
  -h --help                     Show help and exit.
  -v --verbose                  Enable log output. Can be specified up to three
                                times (increasing verbosity each time).
  --standalone                  Run in standalone mode.

Examples:
  kraken-client job --submit @test-job.json

"""

from ... import __version__
from .server import MasterServer

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

    fmt = '%(asctime)s\t%(name)-16s\t%(levelname)-5s\t%(message)s'
    stream_handler.setFormatter(lg.Formatter(fmt))
    logger.addHandler(stream_handler)
    
def main(argv=None):
    
    args = docopt(__doc__, argv=argv, version=__version__)
    
    configure_logging()
    
    server = MasterServer( standalone = True if args['--standalone'] else False)
    server.start()

if __name__ == '__main__':
    main()