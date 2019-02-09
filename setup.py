# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


with open('README.adoc') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='pyadlgen2',
    version='0.0.1',
    description='Python wrapper for the REST API for Azure Data Lake Gen2',
    long_description=readme,
    author='Michele Stawowy',
    author_email='',
    url='https://github.com/stawo/python-azure-datalake-gen2-api',
    license=license,
    packages=find_packages(exclude=('tests', 'docs'))
)