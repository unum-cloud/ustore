#!/usr/bin/env bash
# Script is incomplete
# TODO manylinux version.
python setup.py bdist_wheel &&
twine upload --repository-url=https://test.pypi.org/legacy/ dist/* --verbose 