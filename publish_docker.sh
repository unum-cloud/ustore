#!/bin/bash

docker build . --file Dockerfile --tag unum/ukv:0.3.0-focal &&
    docker login
    docker push unum/ukv:0.3.0-focal