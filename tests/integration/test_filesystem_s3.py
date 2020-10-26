import time

import pytest

from tanit.filesystem.filesystem_factory import FileSystemFactory

from .test_filesystem import BaseFilesystemTest


@pytest.fixture(scope="class")
def filesystem():
    filesystems_factory = FileSystemFactory.getInstance()
    filesystems_factory.register_filesystem(
        {"name": "s3-karken-test-bucket", "type": "s3", "bucket": "kraken-test"}
    )
    filesystem = filesystems_factory.get_filesystem("s3-karken-test-bucket")
    yield filesystem


@pytest.fixture(scope="class")
def test_data(filesystem):
    test_dir = "/data/integration-test/dataset-%s" % int(time.time() * 1000)
    filesystem.mkdir(test_dir)
    yield test_dir
    filesystem.delete(test_dir, recursive=True)


class TestS3Filesystem(BaseFilesystemTest):
    pass
