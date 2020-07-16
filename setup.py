#! /usr/bin/env python
#
# Copyright (C) 2013-2020 Russell Poldrack <poldrack@stanford.edu>
# some portions borrowed from https://github.com/mwaskom/lyman/blob/master/setup.py
import os
from setuptools import setup, find_packages

descr = """nhanes: A Pythonic interface to the NHANES dataset"""

DISTNAME = "nhanes"
DESCRIPTION = descr
LONGDESCRIPTION = "This package allows access to the NHANES data from the US Centers for Disease Control  These data, available at https://www.cdc.gov/nchs/nhanes/index.htm, are in the public domain."
MAINTAINER = 'Russ Poldrack'
MAINTAINER_EMAIL = 'poldrack@stanford.edu'
LICENSE = 'MIT'
URL = 'https://github.com/poldrack/nhanes'
DOWNLOAD_URL = 'https://github.com/poldrack/nhanes'
VERSION = '0.5.1'


def check_dependencies():

    # Just make sure dependencies exist, I haven't rigorously
    # tested what the minimal versions that will work are
    needed_deps = [
        "pandas",
        "numpy",
        "xport",
        "requests",
        "bs4"]
    missing_deps = []
    for dep in needed_deps:
        try:
            __import__(dep)
        except ImportError:
            missing_deps.append(dep)

    if missing_deps:
        missing = (", ".join(missing_deps))
        raise ImportError("Missing dependencies: %s" % missing)


if __name__ == "__main__":

    if os.path.exists('MANIFEST'):
        os.remove('MANIFEST')

    import sys
    if (
        len(sys.argv) < 2
        or '--help' not in sys.argv[1:]
        and sys.argv[1]
        not in ('--help-commands', '--version', 'egg_info', 'clean')
    ):
        check_dependencies()

    setup(name=DISTNAME,
          maintainer=MAINTAINER,
          maintainer_email=MAINTAINER_EMAIL,
          description=DESCRIPTION,
          long_description=LONGDESCRIPTION,
          license=LICENSE,
          version=VERSION,
          url=URL,
          download_url=DOWNLOAD_URL,
          packages=find_packages(),
          package_data={'nhanes': ['combined_data/2017-2018/*', 'config/*']},
          scripts=[
              'bin/make_combined_NHANES_data.py'],
          classifiers=[
              'Intended Audience :: Science/Research',
              'Programming Language :: Python :: 3.6',
              'License :: OSI Approved :: BSD License',
              'Operating System :: POSIX',
              'Operating System :: Unix',
              'Operating System :: MacOS'])
