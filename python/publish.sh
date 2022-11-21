#!/usr/bin/env bash

python3.9 setup.py bdist_wheel --universal --build-number=$(date -d now '+%s') &&
twine upload --repository-url=https://test.pypi.org/legacy/ dist/* -u $1 -p $2 --verbose 