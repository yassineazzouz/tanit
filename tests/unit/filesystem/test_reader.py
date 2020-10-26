import random
import string

from tanit.filesystem.ioutils import ChunkFileReader


class MockDataGenerator:
    def __init__(self, size):
        self.size = size
        self.pos = 0

    def get_random_string(self, length):
        letters = string.ascii_lowercase
        return "".join(random.choice(letters) for i in range(length))

    def read(self, length):
        if self.size == self.pos:
            return None
        else:
            self.pos += length
            return self.get_random_string(min(length, self.size - self.pos))


class TestReader:
    def test_chunk_reader(self):
        reader = ChunkFileReader(MockDataGenerator(32), 16)
        for data in reader:
            assert len(data) == 16

        reader = ChunkFileReader(MockDataGenerator(34), 16)
        for data in reader:
            last_data = data
        assert len(last_data) == 2
