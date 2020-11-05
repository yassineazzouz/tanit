class FileSystemType:
    LOCAL = 1
    HDFS = 2
    S3 = 3
    GCS = 4

    _VALUES_TO_NAMES = {
        1: "LOCAL",
        2: "HDFS",
        3: "S3",
        4: "GCS",
    }

    _NAMES_TO_VALUES = {
        "LOCAL": 1,
        "HDFS": 2,
        "S3": 3,
        "GCS": 4,
    }


class FileSystem(object):
    def __init__(self, name, type, parameters):
        self.name = name
        self.type = type
        self.parameters = parameters

    def __str__(self):
        return "{ name: %s, type: %s, parameters: %s}" % (
            self.name,
            FileSystemType._VALUES_TO_NAMES[self.type],
            self.parameters,
        )


class FileSystemMounts(object):
    def __init__(self, filesystem, mounts):
        self.filesystem = filesystem
        self.mounts = mounts

    def __str__(self):
        return "{ filesystem: %s, mounted: %s, mounts: %s}" % (
            self.filesystem,
            "True" if len(self.mounts) > 0 else "False",
            self.mounts,
        )
