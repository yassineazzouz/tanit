import os
import shutil
import time

import pytest

from kraken.filesystem.filesystem_factory import FileSystemFactory
from kraken.worker.filesystem.service import LocalFileSystemService

test_dir = "/tmp/kest"
test_timestamp = int(time.time())


@pytest.fixture(scope="class")
def filesystem_server():
    filesystem = LocalFileSystemService(address="0.0.0.0", port=9989)
    filesystem.start()

    yield filesystem


@pytest.fixture(scope="class")
def test_data():
    os.mkdir(test_dir)
    # create 5 directories
    for i in range(5):
        d = os.path.join(test_dir, "ktd-%s" % i)
        os.mkdir(d)
        with open(os.path.join(d, "ktf.txt"), mode="wb") as tfile:
            tfile.seek(1023)
            tfile.write(bytes([0]))
    yield
    shutil.rmtree(test_dir)


@pytest.fixture
def filesystem_client():
    filesystems_factory = FileSystemFactory.getInstance()
    filesystems_factory.register_filesystem(
        {
            "name": "local",
            "type": "local",
            "address": "127.0.0.1",
            "port": "9989",
        }
    )
    filesystem = filesystems_factory.get_filesystem("local")
    yield filesystem


@pytest.mark.usefixtures("filesystem_server", "test_data")
class TestRemoteFilesystem:
    def test_list(self, filesystem_client):
        assert len(filesystem_client.list(test_dir)) == 5

    def test_walk(self, filesystem_client):
        for dpath, dnames, fnames in filesystem_client.walk(test_dir):
            assert dpath in [test_dir] + [
                os.path.join(test_dir, "ktd-%s" % i) for i in range(5)
            ]
            if dpath == test_dir:
                assert len(dnames) == 5
                assert len(fnames) == 0
            else:
                assert len(dnames) == 0
                assert len(fnames) == 1

    def test_status(self, filesystem_client):
        assert (
            filesystem_client.status(os.path.join(test_dir, "ktd-1"))["type"]
            == "DIRECTORY"
        )
        assert (
            filesystem_client.status(os.path.join(test_dir, "ktd-1", "ktf.txt"))["type"]
            == "FILE"
        )
        assert filesystem_client.status(os.path.join(test_dir, "ktd-1", "ktf.txt"))[
            "length"
        ] in ["1024", "1026"]

    def test_content(self, filesystem_client):
        assert filesystem_client.content(os.path.join(test_dir))["fileCount"] == "5"
        assert (
            filesystem_client.content(os.path.join(test_dir))["directoryCount"] == "5"
        )

    def test_delete(self, filesystem_client):
        filesystem_client.delete(os.path.join(test_dir, "ktd-4"), recursive=True)
        assert (
            filesystem_client.status(os.path.join(test_dir, "ktd-4"), strict=False)
            is None
        )

    def test_mkdir(self, filesystem_client):
        test_mkdir_dir = os.path.join(test_dir, "ktd-mkdir-%s" % test_timestamp)
        filesystem_client.mkdir(test_mkdir_dir)
        assert filesystem_client.status(test_mkdir_dir)["type"] == "DIRECTORY"

    def test_rename(self, filesystem_client):
        test_rename_dir = os.path.join(test_dir, "ktd-rename-%s" % test_timestamp)
        filesystem_client.rename(os.path.join(test_dir, "ktd-3"), test_rename_dir)
        assert filesystem_client.status(test_rename_dir)["type"] == "DIRECTORY"
        assert (
            filesystem_client.status(os.path.join(test_dir, "ktd-3"), strict=False)
            is None
        )

    def test_write_read(self, filesystem_client):
        test_writer_file = os.path.join(test_dir, "ktf-%s" % test_timestamp)
        wbts = "This is a test generated file".encode("utf-8")
        with filesystem_client.write(test_writer_file) as writer:
            writer.write(wbts)
        assert filesystem_client.status(test_writer_file)["type"] == "FILE"
        assert filesystem_client.status(test_writer_file)["length"] == str(len(wbts))

        with filesystem_client.read(test_writer_file) as reader:
            rbts = reader.read()
        assert rbts == wbts
