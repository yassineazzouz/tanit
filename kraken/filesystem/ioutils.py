from ..common.core.exception import KrakenError


class ChunkFileReader:
    def __init__(self, file, chunk_size=1024):
        self.file = file
        self.chunk_size = chunk_size

    def read(self):
        data = self.file.read(self.chunk_size)
        if not data:
            return None
        return data


class DelimitedFileReader:
    def __init__(self, file, delimiter="\n"):
        self.file = file
        self.delimiter = delimiter
        self.end = False
        self.data = []
        self.buffer = ""

    def read(self):
        # TODO
        pass


class FileSystemError(KrakenError):
    pass
