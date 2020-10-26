#!/usr/bin/env python

"""tanit: python distributed data transfer tool."""

import os
import sys
import re
from setuptools import find_packages, setup

sys.path.insert(0, os.path.abspath('src'))


def _get_version():
    """Extract version from package."""
    with open('tanit/__init__.py') as reader:
        match = re.search(
            r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
            reader.read(),
            re.MULTILINE
        )
        if match:
            return match.group(1)
        else:
            raise RuntimeError('Unable to extract version.')


def _get_long_description():
    """Get README contents."""
    with open('README.rst') as reader:
        return reader.read()


setup(
    name='tanit',
    version=_get_version(),
    description=__doc__,
    keywords="thrift python tanit hdfs",
    long_description=_get_long_description(),
    author='Yassine Azzouz',
    author_email='yassine.azzouz@agmail.com',
    url='https://github.com/yassineazzouz/tanit',
    license='MIT',
    packages=find_packages(),
    classifiers=[
        'Topic :: Software Development',
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    python_requires='>=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*',
    install_requires=[
        'six',
        'docopt',
        'thrift>=0.10',
        'pywhdfs>=1.1.0',
        's3fs==0.2.2',
        'google-cloud-storage>=1.20.0',
        'google-auth>=1.2',
        'pyrsistent==0.15.0'
    ],
    entry_points={'console_scripts':
                      ['tanit-master = tanit.master.server.__main__:main',
                       'tanit-worker = tanit.worker.server.__main__:main',
                       'tanit-client = tanit.master.client.__main__:main']
                  },
    long_description_content_type='text/markdown'
)
