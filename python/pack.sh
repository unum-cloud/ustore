#!/usr/bin/env bash
rm -rf src/ukv.egg-info ukv.egg-info build CMakeCache.txt && cmake . -DCMAKE_BUILD_TYPE=Release -DUKV_BUILD_PYTHON=1 && make -j 4 py_umem &&
    pip install --upgrade --force-reinstall . && pytest --capture=no python/tests/
