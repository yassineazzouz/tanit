import os
import random
import time

import pytest

from tanit.filesystem.filesystem_factory import FileSystemFactory

from .base_test import BaseFilesystemTest


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
    def test_file_write(self, filesystem, test_data):
        with filesystem.open(os.path.join(test_data, "ttf.txt"), mode="wb") as tfile:
            tfile.write(bytearray(random.getrandbits(8) for _ in range(1024)))

    def test_file_seek_size_read(self, filesystem, test_data):
        file = filesystem.open(os.path.join(test_data, "ttf.txt"), mode="rb")
        file.seek(1024)
        assert file.read(1) == b""

    def test_file_seek_lastbyte_read(self, filesystem, test_data):
        file = filesystem.open(os.path.join(test_data, "ttf.txt"), mode="rb")
        file.seek(1023)
        assert len(file.read()) == 1

    def test_file_seek_beyondsize_read(self, filesystem, test_data):
        file = filesystem.open(os.path.join(test_data, "ttf.txt"), mode="rb")
        file.seek(2048)
        assert file.read(1) == b""

    def test_file_seek_tell(self, filesystem, test_data):
        file = filesystem.open(os.path.join(test_data, "ttf.txt"), mode="rb")
        file.seek(2048)
        assert file.tell() == 2048

    def test_file_read_line(self, filesystem, test_data):
        file = filesystem.open(os.path.join(test_data, "ttf.txt"), mode="rb")
        file.readline()
        assert file.readline() != b""

    def test_file_read_line_limit(self, filesystem, test_data):
        file = filesystem.open(os.path.join(test_data, "ttf.txt"), mode="rb")
        file.seek(1023)
        assert len(file.readline()) == 1
