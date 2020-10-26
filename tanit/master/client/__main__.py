"""
tanit-client: A simple thrift client for tanit service.

Usage:
  tanit-client job [-v...] [--submit job-spec] [--status jid] [--list]
  tanit-client worker [-v...] [--list]
  tanit-client -h | --help
  tanit-client --version

Options:
  --version                     Show version and exit.
  -h --help                     Show help and exit.
  -v --verbose                  Enable log output. Can be specified up to three
                                times (increasing verbosity each time).
  --status JOB_ID               Print the status of a job.
  --submit job-spec             Submit a job to the tanit master.
  --list                        List all jobs.

Examples:
  tanit-client --submit "{
        "src": "prod",
        "dest": "dr",
        "src_path":
        "/data/important_dataset",
        "dest_path": "/data/important_dataset",
        "preserve": "False" }"

"""
import json
import logging as lg

from docopt import docopt

from ... import __version__
from ...common.model.execution_type import ExecutionType
from ...common.model.job import Job
from ..config.config import MasterConfig
from .client import ClientType
from .client import ThriftClientFactory

_logger = lg.getLogger(__name__)


def configure_logging():
    # capture warnings issued by the warnings module
    lg.captureWarnings(True)

    logger = lg.getLogger()
    logger.setLevel(lg.DEBUG)
    lg.getLogger("requests_kerberos.kerberos_").setLevel(lg.CRITICAL)
    lg.getLogger("requests").setLevel(lg.ERROR)

    # Configure stream logging if applicable
    stream_handler = lg.StreamHandler()
    stream_handler.setLevel(lg.INFO)

    fmt = "%(levelname)s\t%(message)s"
    stream_handler.setFormatter(lg.Formatter(fmt))
    logger.addHandler(stream_handler)


def main(argv=None):
    args = docopt(__doc__, argv=argv, version=__version__)

    configure_logging()
    config = MasterConfig()

    if args["job"]:
        client = ThriftClientFactory(
            config.client_service_host, config.client_service_port
        ).create_client(ClientType.USER_SERVICE)
        client.start()
        if args["--list"]:
            for job in client.list_jobs():
                print(str(job))
        elif args["--submit"]:
            try:
                if args["--submit"].startswith("@"):
                    with open(args["--submit"][1:], "r") as json_spec_file:  # NOQA
                        job_spec = json.load(json_spec_file)
                else:
                    job_spec = json.loads(args["--submit"])
            except Exception as e:
                _logger.error("Error parsing job json specification.")
                raise e

            jtype = ExecutionType._NAMES_TO_VALUES[job_spec["type"]]
            params = job_spec["params"]
            client.submit_job(Job(jtype, params))
        elif args["--status"]:
            job = client.job_status(args["--status"])
            if job is None:
                _logger.info("No such job %s", args["--status"])
            else:
                print(str(job))
        else:
            _logger.error("Nothing to do !")

        client.stop()
    elif args["worker"]:
        client = ThriftClientFactory(
            config.worker_service_host, config.worker_service_port
        ).create_client(ClientType.WORKER_SERVICE)
        client.start()
        if args["--list"]:
            for worker in client.list_workers():
                print(str(worker))
        else:
            _logger.error("Nothing to do !")

        client.stop()


if __name__ == "__main__":
    main()
