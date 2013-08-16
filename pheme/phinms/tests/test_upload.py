import unittest
from pheme.phinms.upload import Batchfile_Feeder, archive_by_date


class TestUpload(unittest.TestCase):

    def test_instance(self):
        f = Batchfile_Feeder()
        assert(f)

    def test_upload(self):
        f = Batchfile_Feeder()
        old_file = '1231028419873'
        old_filedate = '2009-01-03T16:20:19'
        f.upload(old_file, old_filedate)
        # no easy way to check or mock - best done by hand
        # go to PHEME_http_receiver channel in Mirth, and see if it arrived


def test_archive_by_date():
    #[('1231028419873', '2009-01-03T16:20:19')]
    results = archive_by_date('/tmp', '2009-01-03T16:20:19')
    assert('/tmp/2009-01' == results)


if '__main__' == __name__:
    unittest.main()
