#!/usr/bin/env bash
rm -rf build && rm CMakeCache.txt && cmake . && make && pip install --upgrade --force-reinstall . && pytest --capture=no python/
