import os
import time

import pytest

from tanit.common.utils.glob import iglob
from tanit.filesystem.filesystem_factory import FileSystemFactory


@pytest.fixture(scope="class")
def filesystem():
    filesystems_factory = FileSystemFactory.getInstance()
    filesystems_factory.register_filesystem({"name": "local", "type": "local"})
    filesystem = filesystems_factory.get_filesystem("local")
    yield filesystem


@pytest.fixture(scope="class")
def test_data(filesystem):
    test_dir = "/tmp/unit-test/glob/dataset-%s" % int(time.time() * 1000)
    filesystem.mkdir(test_dir)
    for i in range(5):
        filesystem.mkdir(os.path.join(test_dir, "ktd-%s" % i))
    yield test_dir
    filesystem.delete(test_dir, recursive=True)


class TestGlob:
    def test_list_glob(self, filesystem, test_data):
        files = [
            i_file for i_file in iglob(filesystem, os.path.join(test_data, "ktd-*"))
        ]
        assert len(files) == 5

    def test_list_glob_no_magic(self, filesystem, test_data):
        files = [
            i_file for i_file in iglob(filesystem, os.path.join(test_data, "ktd-1"))
        ]
        assert len(files) == 1

    def test_list_file_glob_no_magic(self, filesystem, test_data):
        assert len([i_file for i_file in iglob(filesystem, test_data)]) == 1

    def test_list_file_glob_no_magic_slash(self, filesystem, test_data):
        assert len([i_file for i_file in iglob(filesystem, test_data + "/")]) == 1
