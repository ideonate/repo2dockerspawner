#!/usr/bin/env python
# coding: utf-8

# Copyright (c) Juptyer Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import print_function

import os
import sys


# At least we're on the python version we need, move on.

from setuptools import setup

pjoin = os.path.join
here = os.path.abspath(os.path.dirname(__file__))

# Get the current package version.
version_ns = {}
with open(pjoin(here, 'repo2dockerspawner', '_version.py')) as f:
    exec(f.read(), {}, version_ns)

install_requires = []
with open('requirements.txt') as f:
    for line in f.readlines():
        req = line.strip()
        if not req or req.startswith('#'):
            continue
        install_requires.append(req)

from setuptools.command.bdist_egg import bdist_egg
class bdist_egg_disabled(bdist_egg):
    """Disabled version of bdist_egg

    Prevents setup.py install from performing setuptools' default easy_install,
    which it should never ever do.
    """
    def run(self):
        sys.exit("Aborting implicit building of eggs. Use `pip install .` to install from source.")

setup_args = dict(
    name                = 'repo2dockerspawner',
    packages            = ['repo2dockerspawner'],
    version             = version_ns['__version__'],
    description         = """Repo2Dockerspawner: A custom spawner for Jupyterhub.""",
    long_description    = "Spawn single-user servers with Docker including repo2docker option.",
    author              = "Ideonate",
    author_email        = "dan@ideonate.com",
    url                 = "https://ideonate.com/",
    license             = "BSD",
    platforms           = "Linux, Mac OS X",
    keywords            = ['Interactive', 'Interpreter', 'Shell', 'Web'],
    classifiers         = [
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
    ],
    install_requires = install_requires,
    entry_points={
        'jupyterhub.spawners': [
            'docker = repo2dockerspawner:Repo2DockerSpawner'
        ],
    },
    cmdclass = {
        'bdist_egg': bdist_egg if 'bdist_egg' in sys.argv else bdist_egg_disabled,
    }
)


def main():
    setup(**setup_args)

if __name__ == '__main__':
    main()
