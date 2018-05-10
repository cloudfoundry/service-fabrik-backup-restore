#!/usr/bin/env python3

import sys
from setuptools.command.test import test as TestCommand

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

settings = dict()

with open('requirements.txt') as file_requirements:
    requirements = file_requirements.read().splitlines()

with open('requirements-dev.txt') as file_requirements:
    requirements_dev = file_requirements.read().splitlines()

requirements_complete = requirements_dev + requirements

class PyTest(TestCommand):
    user_options = [
         # long option, short option, description
         ('coverage', 'c', 'Show coverage statistics'),
         ('capture', 'p', 'Show stdout stderr logs')
    ]
    description = 'run tests on Python source files'

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.coverage = False
        self.capture = False

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = [ 'tests/', '-vv']
        if self.coverage:
            self.test_args += ['--cov=lib', '--cov-report', 'html']
        
        if self.capture:
            self.test_args += ['--capture=no']

        self.test_suite = True

    def run_tests(self):
        #import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.test_args)
        sys.exit(errno)

settings.update(
    name='service_fabrik_backup_restore',
    version='1',
    description='Backup & Restore Python Library consumable by Cloud Foundry Services managed by the Service Fabrik',
    long_description='See README.md on https://github.com/SAP/service-fabrik-backup-restore',
    author=[],
    license='Apache 2.0',
    url='http://',
    keywords="service fabrik service-fabrik backup restore backup-restore broker cloud foundry",
    install_requires=requirements,
    tests_require=requirements_complete,
    cmdclass = {
        'test': PyTest
    }
)

setup(**settings)
