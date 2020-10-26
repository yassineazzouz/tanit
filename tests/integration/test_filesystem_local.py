import time

import pytest

from tanit.filesystem.filesystem_factory import FileSystemFactory
from tanit.worker.filesystem.service import LocalFileSystemService

from .test_filesystem import BaseFilesystemTest


@pytest.fixture(scope="session")
def filesystem_local_server():
    filesystem_server = LocalFileSystemService(address="0.0.0.0", port=9989)
    filesystem_server.start()


@pytest.fixture(
    scope="class",
    params=[
        {
            "name": "local",
            "type": "local",
            "address": "127.0.0.1",
            "port": "9989",
        },
        {"name": "local", "type": "local"},
    ],
)
def filesystem(filesystem_local_server, request):
    filesystems_factory = FileSystemFactory.getInstance()
    filesystems_factory.register_filesystem(request.param)
    filesystem = filesystems_factory.get_filesystem(request.param["name"])
    yield filesystem


@pytest.fixture(scope="class")
def test_data(filesystem):
    test_dir = "/tmp/integration-test/dataset-%s" % int(time.time() * 1000)
    filesystem.mkdir(test_dir)
    yield test_dir
    filesystem.delete(test_dir, recursive=True)


class TestLocalFilesystem(BaseFilesystemTest):
    pass
