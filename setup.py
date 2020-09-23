#!/usr/bin/env python

"""kraken: python WebHDFS inter/intra-cluster data copy tool."""

import os
import sys
import re
from setuptools import find_packages, setup

sys.path.insert(0, os.path.abspath('src'))

def _get_version():
  """Extract version from package."""
  with open('kraken/__init__.py') as reader:
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
  name='kraken-pyds',
  version=_get_version(),
  description=__doc__,
  long_description=_get_long_description(),
  author='Yassine Azzouz',
  author_email='yassine.azzouz@agmail.com',
  url='https://github.com/yassineazzouz/kraken',
  license='MIT',
  packages=find_packages(),
  classifiers=[
    'Development Status :: 5 - Production/Stable',
    'Intended Audience :: Developers',
    'Intended Audience :: System Administrators',
    'License :: OSI Approved :: MIT License',
    'Programming Language :: Python',
    'Programming Language :: Python :: 2.6',
    'Programming Language :: Python :: 2.7',
  ],
  install_requires=[
    'docopt',
    'pywhdfs>=1.0.0',
    'thrift>=0.9',
    'pyrsistent<=0.15.0'
  ],
  entry_points={'console_scripts': 
     [ 'kraken-master = kraken.master.server.__main__:main',
       'kraken-worker = kraken.worker.server.__main__:main',
       'kraken-client = kraken.master.client.__main__:main']
  },
  long_description_content_type='text/markdown'
)
