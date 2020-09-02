#!/usr/bin/env python
# encoding: utf-8

from ... import __version__
from .server import WorkerServer

import requests as rq
import logging as lg

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
    
    configure_logging()
    
    server = WorkerServer()
    server.start()

if __name__ == '__main__':
    main()