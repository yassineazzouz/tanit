from ..common.core.exception import TanitError


class FileReader:
    pass


class ChunkFileReader(FileReader):
    def __init__(self, file, chunk_size=1024):
        self.file = file
        self.chunk_size = chunk_size

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        while True:
            data = self.file.read(self.chunk_size)
            if not data:
                raise StopIteration
            return data


class DelimitedFileReader(FileReader):
    def __init__(self, file, delimiter="\n"):
        self.file = file
        self.delimiter = delimiter
        self.end = False
        self.data = []
        self.buffer = ""

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        # TODO
        pass


class FileSystemError(TanitError):
    pass
