#!/usr/bin/env python

from setuptools import setup

docs_require = ['Sphinx']
tests_require = ['nose', 'coverage']

setup(name='lhres.phinms',
      version='13.05',
      description="PHINMS upload client for LHRES",
      namespace_packages=['lhres'],
      packages=['lhres.phinms', ],
      include_package_data=True,
      install_requires=['setuptools', 'MySQL-python', 'urllib3', 'lhres.util'],
      setup_requires=['nose'],
      tests_require=tests_require,
      test_suite="nose.collector",
      extras_require = {'test': tests_require,
                        'docs': docs_require,
                        },
      entry_points=("""
                    [console_scripts]
                    phinms_receiver_upload=lhres.phinms.upload:main
                    """),
)
