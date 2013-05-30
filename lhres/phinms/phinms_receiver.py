#!/usr/bin/env python
# (C) 2011. University of Washington. All rights reserved.
import logging
import MySQLdb as mysql
from MySQLdb import IntegrityError

from lhres.util.config import Config

class PHINMS_DB(object):
    """ Abstraction for interacting w/ the PHIN-MS Database

    A MYSQL database used by PHIN-MS, which contains information about
    files routed through PHIN-MS.  An account with only SELECT grants
    on the PHIN-MS tables should be used.  (The 'feeder' table is used
    for tracking which files have been processed, and therefore
    requires INSERT and UPDATE grants.)

    This database is where the batch filenames and related data can be
    looked up.  It is also where we persist the state for any files
    'fed' to mirth.

    """

    LIMIT = 50

    def __init__(self):
        config = Config()
        self.db = config.get('phinms', 'database')
        self.username = config.get('phinms', 'user')
        self.passwd = config.get('phinms', 'password')
        self.workerqueue = config.get('phinms', 'workerqueue')
        self.feedertable = self.workerqueue + '_feeder'

    def _create_feeder_table(self):
        """ Create the feeder table, if it doesn't already exist.

        This process is the sole user of the feeder table.  Create if
        it it hasn't been done already.

        NB: the 'feeder' table is per workerqueue and named 
        <workerqueue>_feeder

        """
        SQL = """CREATE TABLE IF NOT EXISTS %s (workerqueue_fk
        BIGINT(20) NOT NULL UNIQUE, INDEX (workerqueue_fk));""" %\
        self.feedertable
        cursor = self._connect().cursor()
        cursor.execute(SQL)

    def filelist(self, progression):
        """ Query the source database for a batch of filenames

        Returns a set (count of up to self.LIMIT) of touples defining
        the (filenames, filedates), needing to be processed.
        Progression determines if they are the oldest
        (progression='forwards') or the newest
        (progression='backwards') available.  Empty list imples no
        unprocessed files are available.

        """
        cursor = self._connect().cursor()
        sort_order = 'DESC' if progression == 'backwards' else ''
        query = """SELECT localFileName, lastUpdateTime FROM
        %(workerqueue)s LEFT JOIN %(table)s ON recordId=workerqueue_fk
        WHERE workerqueue_fk IS NULL ORDER BY lastUpdateTime %(sort)s LIMIT
        %(limit)s""" % {'workerqueue': self.workerqueue,
                        'table': self.feedertable, 'sort': sort_order,
                        'limit': self.LIMIT}
        cursor.execute(query)
        files = []
        while True:
            results = cursor.fetchmany()
            if not results:
                break
            for row in results:
                files.append((row[0], row[1]))

        return files

    def name_dates(self, filenames):
        """ Query the source database for dates matching files

        :param filenames: list of 'localFileName's from worker queue

        Returns a list of touples defining the (filenames, filedates)
        as a list, for the given list of filenames.

        If any of the provided filenames aren't found, an exception is
        raised.

        """
        cursor = self._connect().cursor()
        filename_strings = ','.join(['%s'] * len(filenames))
        query = """SELECT localFileName, lastUpdateTime FROM
        %(workerqueue)s WHERE localFileName IN (%(files)s)""" %\
            {'workerqueue': self.workerqueue, 'files': filename_strings}
        cursor.execute(query, tuple(filenames))
        results = []
        while True:
            row = cursor.fetchone()
            if not row:
                break
            results.append((row[0], row[1]))

        if len(results) != len(filenames):
            raise ValueError("Not all files found in PHIN-MS db: %s",
                             str(filenames))
        return results

    def markfed(self, localFileNames):
        """Mark the given list of filenames as read"""
        sql = """INSERT INTO %(table)s SELECT recordId FROM
        %(workerqueue)s WHERE localFileName IN (%(filenames)s)""" %\
        {'workerqueue': self.workerqueue,'table': self.feedertable,
         'filenames': ','.join(['%s' % f for f in localFileNames])}
        cursor = self._connect().cursor()
        try:
            cursor.execute(sql)
        except IntegrityError, e:
            # Happens too frequently, and disguises real issues
            #logging.error("Failed to insert localFiles %s",
            #              str(localFileNames))
            #logging.exception(e)

            # Try again avoiding those already there
            sql = """INSERT INTO %(table)s SELECT recordId FROM
            %(workerqueue)s LEFT JOIN %(table)s ON recordId =
            workerqueue_fk WHERE workerqueue_fk IS NULL AND
            localFileName IN (%(filenames)s)""" %\
            {'workerqueue': self.workerqueue,
             'table': self.feedertable,
             'filenames': ','.join(['%s' % f for f in localFileNames])}
            cursor = self._connect().cursor()
            cursor.execute(sql)
        except Exception, e:
            logging.error("Failed to insert localFiles %s",
                          str(localFileNames))
            logging.exception(e)
            raise e

    def _connect(self):
        if getattr(self, 'conn', None):
            return self.conn
        try:
            self.conn = mysql.connect(host="localhost",
                                      user=self.username,
                                      passwd=self.passwd,
                                      db=self.db)
            return self.conn
        except mysql.Error, e:  # pragma: no cover
            raise Exception("Source database connection error %d: %s"
                            % (e.args[0], e.args[1]))

    def close(self):
        """ Close the database connection (when done with it).
        """
        if hasattr(self, 'conn'):
            self.conn.close()
            del(self.conn)

    def __del__(self):  # pragma: no cover
        if hasattr(self, 'conn'):
            print "ERROR: source database connection wasn't closed!"
