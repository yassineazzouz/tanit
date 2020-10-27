import click

import json
import logging as lg

from .. import __version__

from ..master.server.server import MasterServer
from ..worker.server.server import WorkerServer
from ..common.model.execution_type import ExecutionType
from ..common.model.job import Job
from ..master.config.config import MasterConfig
from ..master.client.client import ClientType
from ..master.client.client import ThriftClientFactory

_logger = lg.getLogger(__name__)


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

    fmt = "%(levelname)s\t%(message)s"
    stream_handler.setFormatter(lg.Formatter(fmt))
    logger.addHandler(stream_handler)


def get_master_client():
    config = MasterConfig()
    client = ThriftClientFactory(
        config.client_service_host, config.client_service_port
    ).create_client(ClientType.USER_SERVICE)
    return client


def get_worker_client():
    config = MasterConfig()
    client = ThriftClientFactory(
        config.worker_service_host, config.worker_service_port
    ).create_client(ClientType.WORKER_SERVICE)
    client.start()
    return client


@click.group()
@click.version_option(version=__version__, message='Tanit, version %(version)s')
def tanit():
    """Tanit service client."""
    configure_logging()


@tanit.command()
@click.option("--standalone", "-s", is_flag=True, help="Enables standalone mode.")
def master(standalone):
    """Run the Tanit master."""
    server = MasterServer(standalone)
    server.start()


@tanit.command()
def worker():
    """Run the Tanit worker."""
    server = WorkerServer()
    server.start()


@tanit.group("jobs")
def jobs():
    """Manage jobs."""


@jobs.command("submit")
@click.argument("job")
def job_submit(job):
    """Submits a job."""
    client = get_master_client()
    client.start()

    try:
        if job.startswith("@"):
            with open(job[1:], "r") as json_spec_file:  # NOQA
                job_spec = json.load(json_spec_file)
        else:
            job_spec = json.loads(job)
    except Exception as e:
        _logger.error("Error parsing job json specification.")
        raise e

    params = job_spec["params"]
    client.submit_job(
        Job(ExecutionType._NAMES_TO_VALUES[job_spec["type"]], params)
    )

    client.stop()


@jobs.command("list")
def job_list():
    """Lists jobs."""

    client = get_master_client()
    client.start()

    for job in client.list_jobs():
        print(str(job))

    client.stop()


@jobs.command("status")
@click.argument("jid")
def job_status(jid):
    """Prints job information."""

    client = get_master_client()
    client.start()

    job = client.job_status(jid)
    if job is None:
        _logger.info("No such job %s", job)
    else:
        print(str(job))

    client.stop()


@tanit.group("workers")
def workers():
    """Manage workers."""


@workers.command("list")
def workers_list():
    """Lists workers."""

    client = get_worker_client()
    client.start()

    for worker in client.list_workers():
        print(str(worker))

    client.stop()


if __name__ == "__main__":
    tanit()
