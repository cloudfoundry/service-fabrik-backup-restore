#!/usr/bin/env python3

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

settings = dict()

with open('requirements.txt') as file_requirements:
    requirements = file_requirements.read().splitlines()

settings.update(
    name='service_fabrik_backup_restore',
    version='1',
    description='Backup & Restore Python Library consumable by Cloud Foundry Services managed by the Service Fabrik',
    long_description='See README.md on https://github.com/SAP/service-fabrik-backup-restore',
    author=[],
    license='Apache 2.0',
    url='http://',
    keywords="service fabrik service-fabrik backup restore backup-restore broker cloud foundry",
    install_requires=requirements
)

setup(**settings)
