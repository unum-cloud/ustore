#!/usr/bin/env bash
rm -rf src/ukv.egg-info ukv.egg-info build CMakeCache.txt && cmake . -DCMAKE_BUILD_TYPE=Debug && make -j 4 py_umemkv &&
pip install --upgrade --no-clean --force-reinstall .
