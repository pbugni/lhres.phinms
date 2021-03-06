#!/usr/bin/env python

usage = """%prog [options] [-f file(s)]

Script to upload files from a local PHINMS instance to the PHEME_http_receiver
channel, likely runningon a different server.

Looks up all HL/7 batch files that haven't previously been uploaded,
and feeds each one via HTTP POST to the configured service.

If the files option (-f) is used, only the named files will be
uploaded, and then the process will shut down.

Otherwise, this acts as a long running process, occasionally polling
the receivers, such as the `phinms_receiver` for new files.

Try `%prog --help` for more information.
"""
from datetime import datetime
import logging
from optparse import OptionParser
import os
from time import sleep
from urllib3 import HTTPConnectionPool

from pheme.phinms.phinms_receiver import PHINMS_DB
from pheme.util.config import Config, configure_logging
from pheme.util.compression import expand_file
from pheme.util.util import systemUnderLoad


def archive_by_date(base_dir, the_date):
    """ Fetch the archive dir for the given date

    In an effort to manage the large number of files, incoming files
    over a month old are moved into a series of archive directories,
    organized by date.  Specifically:

    base_dir/
      YYYY-MM
      YYYY-MM

    returns the path to the directory likely to contain the archived
    file in question

    """
    if not (getattr(the_date, 'year', None) and
            getattr(the_date, 'month', None)):
        try:
            the_date = datetime.strptime(the_date,
                                         "%Y-%m-%dT%H:%M:%S")
        except:
            logging.error("archive_by_date can't lookup %s" %
                          str(the_date))
            raise ValueError("invalid arg to archive_by_date %s" %
                             str(the_date))

    return os.path.join(base_dir, "%d-%02d" % (the_date.year,
                                               the_date.month))


class Batchfile_Feeder(object):
    """Uploads avaiable batch files to PHEME_http_receiver channel """

    def __init__(self, verbosity=0, source_db=None):
        self.verbosity = verbosity
        self.source_db = source_db
        config = Config()
        self.phinms_receiving_dir = config.get('phinms', 'receiving_dir')
        self.phinms_archive_dir = config.get('phinms', 'archive_dir')

        self.source_dir = self.phinms_receiving_dir

        # Confirm the required directories are present
        if not os.path.isdir(self.phinms_receiving_dir):
            raise ValueError("Can't find required directory %s" %
                             self.phinms_receiving_dir)

        UPLOAD_PORT = config.get('pheme_http_receiver', 'port')
        UPLOAD_HOST = config.get('pheme_http_receiver', 'host')
        self.http_pool = HTTPConnectionPool(host=UPLOAD_HOST,
                                            port=UPLOAD_PORT,
                                            timeout=20)
        self._copy_tempdir = None

    @property
    def copy_tempdir(self):
        return self._copy_tempdir

    @copy_tempdir.setter
    def copy_tempdir(self, tempdir):
        self._copy_tempdir = tempdir
        if tempdir is None:
            return
        if not os.access(tempdir, os.W_OK):
            raise RuntimeError("'%s' not found or not writeable" % tempdir)
        # Have a valid temp directory.  This means user isn't uploading
        # files, but rather just wants them copied to the tempdir.
        # Monkeypatch self to copy rather than feed.
        self._feed = self._copy

    def mime_parts(self, filepath, filename):
        """Wrap the file in a dictionary for HTTP multipart post

        :param filepath: full path to file containing data to upload
        :param filename: original filename (i.e. not a temp or zip version)
          matching the localFileName value from the workerqueue

        The naming of the dictionary keys (i.e. filedata) must sync
        with the service expectations.  For initial implementation,
        this is in the PHEME_http_receiver channel running in Mirth as an HTTP
        listener on the other side of the stunnel at [pheme_http_receiver]
        {host,port}.

        """
        d = {}
        with open(filepath, 'rb') as fh:
            contents = fh.read()

        d['filename'] = filename
        d['filedata'] = (filename, contents)
        return d

    def _post(self, filepath, filename):
        """POST the file to the PHEME_http_receiver channel

        :param filepath: full filesystem path to batchfile to upload
        :param filename: original filename (i.e. not a temp or zip version)
          matching the localFileName value from the workerqueue

        raises an exception unless a 200 is returned from the server.

        """
        url = "%(scheme)s://%(host)s:%(port)s/" %\
            {'scheme': self.http_pool.scheme, 'host':
             self.http_pool.host,  'port': self.http_pool.port}
        response = self.http_pool.request('POST',
                                          url,
                                          self.mime_parts(filepath, filename),
                                          retries=0)
        args = {'status': response.status, 'reason':
                response.reason, 'file': filename,
                'url': url}
        if response.status != 200:
            logging.error("Failed POST of %(file)s to %(url)s, "
                          "%(reason)s", args)
            raise RuntimeError("Error %(status)d, '%(reason)s' in "
                               "posting %(file)s to %(url)s" % args)
        else:
            logging.info("%(file)s posted to %(url)s", args)

    def _feed(self, filepath, filename):
        """Feed the file to mirth, and handle bookkeeping

        Upload the given file to the PHEME_http_receiver channel for
        further processing.  Bookkeeping is done to prevent multiple
        uploads of the same file.

        :param filepath: full path to file containing data to upload
        :param filename: original filename (i.e. not a temp or zip version)
          matching the localFileName value from the workerqueue

        """
        # Annually, when one of the upstream certificates expire,
        # PHINMS can't decrypt the files.  If the file looks illformed,
        # alert via logging, and move on.
        with open(filepath, 'r') as fh:
            first = fh.readline()

        if first and first.startswith('FHS|'):
            try:
                self._post(filepath, filename)
                self.source_db.markfed([filename, ])
            except Exception, e:
                # NB we do NOT markfed in this case - server may be
                # unreachable or some other situation - continue trying
                logging.error("Error: failed to upload %s", filename)
                logging.exception(e)
        else:
            logging.error("Error: batchfile '%s' doesn't begin with"
                          " expected FHS, but rather: '%s'", filename,
                          first[:25])
            # Mark fed, or we'll cycle on these types of files.
            self.source_db.markfed([filename, ])

    def _copy(self, filepath, filename):
        """Simply copy the file to a filesystem dir

        For debugging and reporting needs, just copy the file rather
        than uploading.  Do NOT mark as fed, as this isn't an
        offical upload.  Also skip sanity checks for encrypted files, etc.

        :param filepath: full path to file containing data to upload
        :param filename: original filename (i.e. not a temp or zip version)
          matching the localFileName value from the workerqueue

        """
        with open(filepath, 'r') as fh:
            try:
                with open(os.path.join(self.copy_tempdir, filename), 'w')\
                        as out:
                    out.writelines(fh.readlines())
            except Exception, e:
                logging.error("Error: failed to copy %s", filename)
                logging.exception(e)

    def upload(self, filename, filedate=None):
        """Upload the file to the PHEME_http_receiver channel

        :param filename: batch filename to upload
        :param filedate: needs to be defined for files that have been
            archived, as the date is necessary to locate the archived
            file.

        """
        src = os.path.join(self.phinms_receiving_dir, filename)
        if os.path.exists(src):
            # Common case, the file is available in the receiving_dir
            # as it hasn't yet been archived
            self._feed(src, filename)
        else:
            # See if we can find the archived version.  If so, it
            # needs to be expanded before adding to the
            # channelPath
            try:
                archive_dir = archive_by_date(self.phinms_archive_dir,
                                              filedate)
                src = os.path.join(archive_dir, filename + '.gz')
                if os.path.exists(src):
                    expanded_file = expand_file(filename=src,
                                                zip_protocol='gzip',
                                                output='file')
                    self._feed(expanded_file, filename)
                    # remove the expanded_file, providing the source
                    # is still intact
                    if os.path.exists(src):
                        os.remove(expanded_file)
                    else:
                        raise RuntimeError("Archived batch file gone "
                                           "after expansion")
                else:
                    logging.error("Couldn't locate hl7 batch file "
                                  "'%s' using date %s", filename,
                                  str(filedate))
            except:
                logging.error("failed to locate hl7 batch file "
                              "'%s'", filename)


class Execute(object):
    """ Handles invocation parameters and drives the rest of the script
    """

    def __init__(self):
        self.__progression = 'forwards'
        self.verbosity = 0
        self.files = None
        self.daemon_mode = True
        self.copy_tempdir = None

    def _get_progression(self):
        return self.__progression

    def _set_progression(self, progression):
        "Property setter controls available values of progression"
        options = ('forwards', 'backwards')
        if progression not in options:
            raise ValueError("Requested progression '%s' not in "
                             "available options %s" % (progression,
                                                       options))
        self.__progression = progression

    progression = property(_get_progression, _set_progression)

    def execute(self):
        source_db = PHINMS_DB()
        feeder = Batchfile_Feeder(verbosity=self.verbosity,
                                  source_db=source_db)
        feeder.copy_tempdir = self.copy_tempdir

        while True:
            try:  # long running process, capture interrupt
                if systemUnderLoad():
                    logging.info("system under load - continue anyhow")

                if self.files:
                    # Look up the given files for their filedates
                    self.files = source_db.name_dates(self.files)
                else:
                    self.files = source_db.filelist(self.progression)

                    # If we didn't get any back, we've caught up, take
                    # this opportunity to sleep for a while
                    if self.daemon_mode and not self.files:
                        logging.debug("no files found, sleeping")
                        sleep(5 * 60)

                for batch_file, filedate in self.files:
                    feeder.upload(batch_file, filedate)

                self.files = None  # done with that batch
                if not self.daemon_mode:
                    raise(SystemExit('non daemon-mode exit'))

            except:
                logging.info("Shutting down")
                raise  # now exit
            finally:
                source_db.close()

    def processArgs(self):
        """ Process any optional arguments and possitional parameters
        """
        parser = OptionParser(usage=usage)
        parser.add_option("-v", "--verbose", dest="verbosity",
                          action="count", default=self.verbosity,
                          help="increase output verbosity")
        parser.add_option("-b", "--backwards", dest="progression",
                          default=self.progression,
                          help="Progress backwards in time through "
                          "available batch files (default is forwards)")
        parser.add_option("-f", "--file", dest="namedfiles",
                          default=self.files, action='store_true',
                          help="only process named file(s)")
        parser.add_option("--copy-to-tempdir", dest="tempdir",
                          default=None, action='store',
                          help="Don't upload or track, just copy files "
                          "to named directory")

        (options, args) = parser.parse_args()
        if not parser.values.namedfiles:
            if len(args) != 0:
                parser.error("incorrect number of arguments")
        else:
            # User says they're feeding us files.  Gobble up the list
            self.daemon_mode = False
            if not len(args):
                parser.error("must supply at least one filename with"
                             "the files(-f) option")
            self.files = []
            for filename in args:
                self.files.append(filename)

        self.copy_tempdir = parser.values.tempdir
        self.verbosity = parser.values.verbosity
        configure_logging(verbosity=self.verbosity, logfile='stderr')
        self.execute()


def main():
    Execute().processArgs()

if __name__ == "__main__":
    main()
