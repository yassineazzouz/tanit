#!/usr/bin/env python
# encoding: utf-8

"""kraken-client: A simple thrift client for kraken service.

Usage:
  kraken-client job [-v...] [--submit job-spec] [--status jid] [--list]
  kraken-client worker [-v...] [--list]
  kraken-client -h | --help
  kraken-client --version

Options:
  --version                     Show version and exit.
  -h --help                     Show help and exit.
  -v --verbose                  Enable log output. Can be specified up to three
                                times (increasing verbosity each time).
  --status JOB_ID               Print the status of a job.
  --submit job-spec             Submit a job to the kraken master.
  --list                        List all jobs.

Examples:
  kraken-client --submit "{ \"src\":\"prod\", \"dest\": \"dr\", \"src_path\": \"/data/important_dataset\", \"dest_path\": \"/data/important_dataset\", \"preserve\": False }"

"""

from ... import __version__
from .client import MasterClient, MasterWorkerClient
from ..config.config import MasterConfig
import json
import requests as rq
import logging as lg
from docopt import docopt

_logger = lg.getLogger(__name__)

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
    
    if (args['job']):
        client = MasterClient(config.client_service_host, config.client_service_port)
        client.start()
        if (args['--list']):
            for job in client.list_jobs():
                print(str(job))
        elif (args['--submit']):
            try:
                if (args['--submit'].startswith('@')):
                    with open(args['--submit'][1:], "r") as json_spec_file:
                        job_spec = json.load(json_spec_file)
                else:
                    job_spec = json.loads(args['--submit'])
            except Exception as e:
                _logger.error("Error parsing job json specification.")
                raise e
            client.submit_job(job_spec)
        elif (args['--status']):
            job = client.job_status(args['--status'])
            if (job == None):
                _logger.info("No such job %s", args['--status'])
            else:
                print(str(job))
        else:
            _logger.error("Nothing to do !")
        
        client.stop()
    elif (args['worker']):
        client = MasterWorkerClient(config.worker_service_host, config.worker_service_port)
        client.start()
        if (args['--list']):
            for worker in client.list_workers():
                print(str(worker))
        else:
            _logger.error("Nothing to do !")
            
        client.stop()
if __name__ == '__main__':
    main()