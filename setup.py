#!/usr/bin/env python

from setuptools import setup

docs_require = ['Sphinx']
tests_require = ['nose', 'coverage']

setup(name='pheme.phinms',
      version='13.05',
      description="PHINMS upload client for PHEME",
      namespace_packages=['pheme'],
      packages=['pheme.phinms', ],
      include_package_data=True,
      install_requires=['setuptools', 'MySQL-python', 'urllib3', 'pheme.util'],
      setup_requires=['nose'],
      tests_require=tests_require,
      test_suite="nose.collector",
      extras_require = {'test': tests_require,
                        'docs': docs_require,
                        },
      entry_points=("""
                    [console_scripts]
                    phinms_receiver_upload=pheme.phinms.upload:main
                    """),
)
