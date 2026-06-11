#!/usr/bin/env python

from setuptools import find_packages, setup

setup(
      name='sparkle',
      version='0.05',
      description='Foundry Compatible Spark API for Local Development',
      author='Kevin Smathers',
      author_email='kevin@ank.com',
      url='https://github.com/sparkle',
      packages=find_packages(include=['sparkle', 'sparkle.*', 'transforms', 'transforms.*']),
      install_requires=['pyspark', 'pyarrow', 'grpcio', 'grpcio-status'],
      extras_require={
            # Compatibility surface for importing sparkle's API from transforms.api.
            'transforms': [],
      },
)
