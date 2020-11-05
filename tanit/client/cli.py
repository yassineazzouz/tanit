import json
import logging as lg

import click

from tanit.filesystem.model import FileSystem, FileSystemType
from ..master.dfs.client import DistributedFileSystemClient
from ..master.client.client import ClientType
from ..master.client.client import ThriftClientFactory
from ..master.config.config import MasterConfig
from ..master.server.server import MasterServer
from ..worker.server.server import WorkerServer

from .. import __version__

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


def get_dfs_client():
    client = DistributedFileSystemClient()
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


@tanit.group("filesystems")
def filesystems():
    """Manage filesystems."""


@filesystems.command("register")
@click.option("--name", "-n", required=True, nargs=1, type=str, help="Filesystem name.")
@click.option("--type", "-t", '_type',
              required=True, type=click.Choice(['LOCAL', 'HDFS', 'S3', 'GCS'], case_sensitive=False),
              nargs=1,
              help="Filesystem type.")
@click.argument("parameters")
def filesystem_register(name, _type, parameters):
    """Register a filesystem."""
    client = get_client()
    client.start()
    try:
        params = json.loads(parameters)
    except Exception as e:
        _logger.error("Error parsing filesystem json specification.")
        raise e
    client.register_filesystem(
        FileSystem(name, FileSystemType._NAMES_TO_VALUES[_type.upper()], params)
    )
    client.stop()


@filesystems.command("list")
def filesystem_list():
    """List Registered filesystems and mounts."""
    client = get_client()
    client.start()
    for filesystem in client.list_filesystems():
        print(filesystem)
    client.stop()


@filesystems.command("mount")
@click.argument("name")
@click.argument("mount_point", type=click.Path())
@click.option("--mount-path", "mount_path",
              required=False, default="", nargs=1, type=click.Path(),
              help="Path on the filesystem to mount.")
def filesystem_mount(name, mount_point, mount_path):
    """Mount a filesystem."""
    client = get_client()
    client.start()
    client.mount_filesystem(name, mount_point, mount_path)
    client.stop()


@filesystems.command("umount")
@click.argument("mount_point", type=click.Path())
def filesystem_umount(mount_point):
    """Unmount a filesystem."""
    client = get_client()
    client.start()
    client.umount_filesystem(mount_point)
    client.stop()


@tanit.group("dfs")
def dfs():
    """Tanit Distributed Filesystem command line tool."""


@dfs.command("ls")
@click.argument("path")
def dfs_ls(path):
    """List the contents that match the specified file pattern."""
    client = get_dfs_client()
    client.start()
    print(client.list(path))
    client.stop()


@dfs.command("mkdir")
@click.argument("path")
def dfs_mkdir(path):
    """Create a directory in specified location."""
    client = get_dfs_client()
    client.start()
    client.mkdir(path)
    client.stop()


@dfs.command("rm")
@click.argument("path")
@click.option("--recursive", "-R", is_flag=True, help="Recursive delete.")
def dfs_rm(path, recursive):
    """Delete all files that match the specified file pattern."""
    client = get_dfs_client()
    client.start()
    client.rm(path, recursive)
    client.stop()


@dfs.command("du")
@click.argument("path")
def dfs_du(path):
    """Show the amount of space, in bytes, used by the files,
       or recursively the directory that match the specified
       file pattern."""
    client = get_dfs_client()
    client.start()
    content = client.content(path, strict=True)
    print("%s    %s" %
          (content['length'], path)
          )
    client.stop()


@dfs.command("count")
@click.argument("path")
def dfs_count(path):
    """Count the number of directories, files and bytes under path."""
    client = get_dfs_client()
    client.start()
    content = client.content(path, strict=True)
    print("%s    %s    %s  %s" %
          (content['directoryCount'], content['fileCount'], content['length'], path)
          )
    client.stop()


@dfs.command("stats")
@click.argument("path")
def dfs_stats(path):
    """Print statistics about the file/directory at path"""
    client = get_dfs_client()
    client.start()
    print(client.status(path, strict=True))
    client.stop()


@dfs.command("cp")
@click.argument("src_path")
@click.argument("dst_path")
@click.option("--force", "-f", is_flag=True, help="Force copy even if size match.")
@click.option("--checksum", "-c", is_flag=True, help="Force checksum check.")
def dfs_copy(src_path, dst_path, force, checksum):
    """Copy files that match the file pattern <src> to a destination."""
    if force and checksum:
        _logger.error("'force' and 'checksum' are mutually exclusive.")
        exit(1)
    client = get_dfs_client()
    client.start()
    client.cp(src_path, dst_path, True, force, checksum)
    client.stop()


@dfs.command("mv")
@click.argument("src_path")
@click.argument("dst_path")
def dfs_move(src_path, dst_path):
    """Move files that match the specified file pattern <src> to a destination <dst>."""
    client = get_dfs_client()
    client.start()
    client.move(src_path, dst_path)
    client.stop()


@dfs.command("checksum")
@click.argument("path")
@click.option("--algorithm", "-a", default="md5", help="The checksum algorithm.")
def dfs_checksum(path, algorithm):
    """Print checksum information for the file at path."""
    client = get_dfs_client()
    client.start()
    print("%s    %s  %s" %
          (client.checksum(path, algorithm), algorithm, path)
          )
    client.stop()


if __name__ == "__main__":
    tanit()
