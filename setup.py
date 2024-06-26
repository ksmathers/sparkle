#!/usr/bin/env python

from distutils.core import setup

setup(name='sparkle',
      version='0.03',
      description='Foundry Compatible Spark API for Local Development',
      author='Kevin Smathers',
      author_email='kevin@ank.com',
      url='https://github.com/sparkle',
      packages=['sparkle'],
      install_requires=['pyspark', 'pyarrow', 'grpcio', 'grpcio-status']
     )
