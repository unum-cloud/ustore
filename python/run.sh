#!/usr/bin/env bash
rm -rf build && rm -rf CMakeCache.txt && cmake . && make -j 4 py_stl &&
    pip install --upgrade --force-reinstall . && pytest --capture=no python/tests/
