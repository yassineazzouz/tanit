import os
import random


class BaseFilesystemTest:
    """Test the filesystem interface.

    The same set of tests need to run successfully on
    all the filesystem implementations regardless of their type,
    This verify that all filesystem implementations respect
    the same interface and can be used transparently regardless
    of the underlying implementation.
    """

    def test_mkdir(self, filesystem, test_data):
        for i in range(5):
            d = os.path.join(test_data, "ktd-%s" % i)
            filesystem.mkdir(d)
        assert len(filesystem.list(test_data)) == 5

    def test_create_file(self, filesystem, test_data):
        for f in filesystem.list(test_data):
            for i in range(5):
                with filesystem.open(
                    os.path.join(test_data, f, "ktf-%s.txt" % i), mode="wb"
                ) as tfile:
                    tfile.write(bytearray(random.getrandbits(8) for _ in range(1024)))
            assert len(filesystem.list(os.path.join(test_data, f))) == 5

    def test_list(self, filesystem, test_data):
        assert len(filesystem.list(test_data)) == 5

    def test_list_glob(self, filesystem, test_data):
        assert len(filesystem.list(os.path.join(test_data, "ktd-*"), glob=True)) == 5

    def test_walk(self, filesystem, test_data):
        for dpath, dnames, fnames in filesystem.walk(test_data):
            assert dpath in [test_data] + [
                os.path.join(test_data, "ktd-%s" % i) for i in range(5)
            ]
            if dpath == test_data:
                assert len(dnames) == 5
                assert len(fnames) == 0
            else:
                assert len(dnames) == 0
                assert len(fnames) == 5

    def test_status(self, filesystem, test_data):
        assert (
            filesystem.status(os.path.join(test_data, "ktd-1"))["type"] == "DIRECTORY"
        )
        assert (
            filesystem.status(os.path.join(test_data, "ktd-1", "ktf-1.txt"))["type"]
            == "FILE"
        )
        assert filesystem.status(os.path.join(test_data, "ktd-1", "ktf-1.txt"))[
            "length"
        ] in ["1024", "1026"]

    def test_content(self, filesystem, test_data):
        assert filesystem.content(os.path.join(test_data))["fileCount"] == "25"
        assert filesystem.content(os.path.join(test_data))["directoryCount"] == "5"

    def test_copy(self, filesystem, test_data):
        test_src_dir = os.path.join(test_data, "ktd-1")
        test_rename_dir = os.path.join(test_data, "ktd-1-copy")

        filesystem.copy(test_src_dir, test_rename_dir)
        assert filesystem.status(test_rename_dir)["type"] == "DIRECTORY"

        src_content = filesystem.content(test_src_dir)
        dst_content = filesystem.content(test_rename_dir)
        assert src_content["length"] == dst_content["length"]
        assert src_content["fileCount"] == dst_content["fileCount"]
        assert src_content["directoryCount"] == dst_content["directoryCount"]

    def test_rename(self, filesystem, test_data):
        test_src_dir = os.path.join(test_data, "ktd-1-copy")
        test_dst_dir = os.path.join(test_data, "ktd-1-renamed")

        src_content = filesystem.content(test_src_dir)
        filesystem.rename(test_src_dir, test_dst_dir)
        assert filesystem.exists(test_dst_dir)
        assert not filesystem.exists(test_src_dir)

        dst_content = filesystem.content(test_dst_dir)
        assert src_content["length"] == dst_content["length"]
        assert src_content["fileCount"] == dst_content["fileCount"]
        assert src_content["directoryCount"] == dst_content["directoryCount"]

    def test_delete(self, filesystem, test_data):
        test_delete_dir = os.path.join(test_data, "ktd-1-renamed")
        filesystem.delete(test_delete_dir, recursive=True)
        assert not filesystem.exists(test_delete_dir)

    def test_write_read(self, filesystem, test_data):
        test_writer_file = os.path.join(test_data, "ktf-write")
        wbts = "This is a test generated file".encode("utf-8")
        with filesystem.write(test_writer_file) as writer:
            writer.write(wbts)
        assert filesystem.status(test_writer_file)["type"] == "FILE"
        assert filesystem.status(test_writer_file)["length"] == str(len(wbts))

        with filesystem.read(test_writer_file) as reader:
            rbts = reader.read()
        assert rbts == wbts
