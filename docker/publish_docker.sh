#!/bin/bash

version=$(cat VERSION)
docker build . --file docker/Dockerfile --tag unum/ukv:$version-focal &&
    docker login &&
    docker push unum/ukv:$version-focal
