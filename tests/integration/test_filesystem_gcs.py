import time

import pytest

from tanit.filesystem.filesystem_factory import FileSystemFactory

from .test_filesystem import BaseFilesystemTest


@pytest.fixture(scope="class")
def filesystem():
    filesystems_factory = FileSystemFactory.getInstance()
    filesystems_factory.register_filesystem(
        {"name": "gcs-kraken-test-bucket", "type": "gcs", "bucket": "kraken-test"}
    )
    filesystem = filesystems_factory.get_filesystem("gcs-kraken-test-bucket")
    yield filesystem


@pytest.fixture(scope="class")
def test_data(filesystem):
    test_dir = "/data/integration-test/dataset-%s" % int(time.time() * 1000)
    filesystem.mkdir(test_dir)
    yield test_dir
    filesystem.delete(test_dir, recursive=True)


class TestGCSFilesystem(BaseFilesystemTest):
    pass
