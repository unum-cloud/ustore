#!/usr/bin/env bash
rm -rf build && cmake . && make && pip install --upgrade --force-reinstall . && pytest --capture=no
