import time

import pytest

from tanit.filesystem.filesystem_factory import FileSystemFactory

from .base_test import BaseFilesystemTest


@pytest.fixture(scope="class")
def filesystem():
    filesystems_factory = FileSystemFactory.getInstance()
    filesystems_factory.register_filesystem(
        {
            "name": "test-hdfs-cluster",
            "type": "hdfs",
            "auth_mechanism": "NONE",
            "user": "hdfs",
            "nameservices": [{"urls": ["http://localhost:50070"], "mounts": ["/"]}],
        }
    )
    filesystem = filesystems_factory.get_filesystem("test-hdfs-cluster")
    yield filesystem


@pytest.fixture(scope="class")
def test_data(filesystem):
    test_dir = "/data/integration-test/dataset-%s" % int(time.time() * 1000)
    filesystem.mkdir(test_dir)
    yield test_dir
    filesystem.delete(test_dir, recursive=True)


class TestS3Filesystem(BaseFilesystemTest):
    pass
