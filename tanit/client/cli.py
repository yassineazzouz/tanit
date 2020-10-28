import json
import logging as lg

import click

from .. import __version__
from ..common.model.execution_type import ExecutionType
from ..common.model.job import Job
from ..master.client.client import ClientType
from ..master.client.client import ThriftClientFactory
from ..master.config.config import MasterConfig
from ..master.server.server import MasterServer
from ..worker.server.server import WorkerServer

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


def get_client():
    config = MasterConfig()
    client = ThriftClientFactory(
        config.client_service_host, config.client_service_port
    ).create_client(ClientType.USER_SERVICE)
    return client


@click.group()
@click.version_option(version=__version__, message="Tanit, version %(version)s")
def tanit():
    """Tanit service client."""
    configure_logging()


@tanit.command()
@click.option("--standalone", "-s", is_flag=True, help="Enables standalone mode.")
def master(standalone):
    """Run the Tanit master."""
    server = MasterServer(standalone=standalone)
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
    """Submit a job."""
    client = get_client()
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
    client.submit_job(Job(ExecutionType._NAMES_TO_VALUES[job_spec["type"]], params))

    client.stop()


@jobs.command("list")
def job_list():
    """List jobs."""
    client = get_client()
    client.start()

    for job in client.list_jobs():
        print(str(job))

    client.stop()


@jobs.command("status")
@click.argument("jid")
def job_status(jid):
    """Print job information."""
    client = get_client()
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
    """List workers."""
    client = get_client()
    client.start()

    for worker in client.list_workers():
        print(str(worker))

    client.stop()


@workers.command("deactivate")
@click.argument("wid")
def workers_deactivate(wid):
    """Decommission a workers."""
    client = get_client()
    client.start()

    client.deactivate_worker(wid)

    client.stop()


@workers.command("activate")
@click.argument("wid")
def workers_activate(wid):
    """Decommission a workers."""
    client = get_client()
    client.start()

    client.activate_worker(wid)

    client.stop()


@workers.command("status")
@click.argument("wid")
def worker_stats(wid):
    """Print worker stats."""
    client = get_client()
    client.start()

    worker = client.worker_stats(wid)
    if worker is None:
        _logger.info("No such worker %s", wid)
    else:
        print(str(worker))

    client.stop()


if __name__ == "__main__":
    tanit()
