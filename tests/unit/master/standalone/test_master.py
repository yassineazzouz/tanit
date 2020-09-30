import pytest

from kraken.master.standalone.master import StandaloneMaster

@pytest.fixture
def master(): 
        master = StandaloneMaster()
        master.start()
        yield master
        master.stop()


class TestStandalonetMaster:

    def test_master_setup(self, master):
        assert(len(master.list_workers()) == 1)