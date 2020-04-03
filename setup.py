"""
Setup for pimdb.

Developer cheat sheet
---------------------

Tag a release (simply replace ``0.x.x`` with the current version number)::

  $ git tag -a -m "Tagged version 0.x.x." v0.x.x
  $ git push --tags

Upload release to PyPI::

  $ python3 setup.py bdist_wheel
  $ twine check dist/*.whl
  $ twine upload --config-file ~/.pypyrc dist/*.whl
"""
# Copyright (c) 2020, Thomas Aglassinger.
# All rights reserved. Distributed under the BSD License.
import setuptools

from pimdb import __version__


setuptools.setup(version=__version__)
