#!/usr/bin/env python
# encoding: utf-8

import logging as lg

from .server import WorkerServer


def configure_logging():
    # capture warnings issued by the warnings module
    lg.captureWarnings(True)

    logger = lg.getLogger()
    logger.setLevel(lg.DEBUG)
    lg.getLogger("requests_kerberos.kerberos_").setLevel(lg.CRITICAL)
    lg.getLogger("google.resumable_media").setLevel(lg.ERROR)
    lg.getLogger("requests").setLevel(lg.ERROR)

    # Configure stream logging if applicable
    stream_handler = lg.StreamHandler()
    stream_handler.setLevel(lg.INFO)

    fmt = "%(asctime)s\t%(name)-16s\t%(levelname)-5s\t%(message)s"
    stream_handler.setFormatter(lg.Formatter(fmt))
    logger.addHandler(stream_handler)


def main(argv=None):
    configure_logging()

    server = WorkerServer()
    server.start()


if __name__ == "__main__":
    main()
