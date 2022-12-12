#!/usr/bin/env bash

cibuildwheel --platform linux &&
    twine upload --repository-url=https://test.pypi.org/legacy/ wheelhouse/*.whl -u $1 -p $2 --verbose
