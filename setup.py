from setuptools import setup, find_packages
import sys, os

version = '0'

setup(name='ZFSpy',
      version=version,
      description="Python bindings for ZFS",
      long_description="""\
""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='zfs python',
      author='Chen Zheng',
      author_email='nkchenz@gmail.com',
      url='http://github.com/nkchenz/zfspy/',
      license='GPL v2',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          # -*- Extra requirements: -*-
      ],
      entry_points="""
      # -*- Entry points: -*-
      """,
      )

