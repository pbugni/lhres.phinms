import unittest
from lhres.phinms.phinms_receiver import PHINMS_DB

class TestPhinmsDB(unittest.TestCase):
    """Tests for the PHINMS_DB class"""

    def setUp(self):
        super(TestPhinmsDB, self).setUp()
        self.phinms = PHINMS_DB()

    def tearDown(self):
        self.phinms.close()
        super(TestPhinmsDB, self).tearDown()

    def testFeederTable(self):
        "Confirm feeder table exists or can be created"
        self.phinms._create_feeder_table()

        cursor = self.phinms._connect().cursor()
        sql = "SELECT COUNT(*) FROM %s" % self.phinms.feedertable
        cursor.execute(sql)
        result = cursor.fetchone()
        self.assertTrue(int(result[0]) >= 0)

    def test_filelist(self):
        "Confirm query works"
        files = self.phinms.filelist(progression=None)
        self.assertEquals(len(files), self.phinms.LIMIT)

    def test_name_dates(self):
        files = 'missing',
        self.assertRaises(ValueError, self.phinms.name_dates, files)

        fls = ('1369690811698', )
        results = self.phinms.name_dates(fls)
        self.assertEquals(len(results), 1)
        self.assertEquals(results[0][0], fls[0])


if '__main__' == __name__:
    unittest.main()
