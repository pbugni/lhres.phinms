pheme.phinms
============

**Public Health EHR Message Engine (PHEME), PHINMS module**

The ``pheme.phinms`` module provides a mechanism to feed files
received via `PHIN Messaging System`_ to the ``pheme.warehouse``
module for further processing.

Requirements
------------

* `MySQL`_
* `PHIN Messaging System`_ configured to use MySQL as its backing store.
* A [general] block in the ``pheme.util.config`` file defining the
  location of a directory writable by the same user running
  `phinms_receiver_upload`::

    [general]
    log_dir=/var/log/pheme

* A [phinms] block in the ``pheme.util.config`` file defining PHINMS and
  MySQL connection details::

    [phinms]
    # The user below should only be granted SELECT access, except on the
    # 'feeder' table.  To check, ask mysql: show grants for <user>;
    database=phinmsdb
    user=username
    password=fakepassword
    receiving_dir=/opt/PHINms/shared/receiverincoming
    archive_dir=/opt/receiverincoming-archive
    # workerqueue takes the MySQL table name set in the PHINMS
    #   SERVICE/action mappings.  [NB: the 'feeder' table is 
    #   per workerqueue and named <workerqueue>_feeder]
    workerqueue=testfile_worker_queue

Install
-------

Beyond the requirements listed above, ``pheme.phinms`` is dependent on
the ``pheme.util`` module.  Although future builds may automatically
pick it up, for now, clone and build it in the same virtual
environment (or native environment) being used for ``pheme.phinms``::

    git clone https://github.com/pbugni/pheme.util.git
    cd pheme.util
    ./setup.py develop
    cd ..

Then clone and build this module::

    git clone https://github.com/pbugni/pheme.phinms.git
    cd pheme.phinms
    ./setup.py develop

Running
-------

The executable programs provided by ``pheme.phinms`` are listed under
[console_scripts] within the project's setup.py file.  All take the
standard help options [-h, --help].  Invoke with help for more
information::

    phinms_receiver_upload --help

Testing
-------

From the root directory of ``pheme.phinms`` invoke the tests as follows::

    ./setup.py test

License
-------

BSD 3 clause license - See LICENSE.txt


.. _MySQL: http://www.mysql.com/
.. _PHIN Messaging System: http://www.cdc.gov/phin/tools/PHINms/
