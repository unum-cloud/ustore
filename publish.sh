#!/bin/sh
set -e
version=$(cat VERSION)
# Clean up
echo -e "------ \e[93mClean up\e[0m ------"
rm -rf build _deps build_debug CMakeCache.txt CMakeFiles wheelhouse Makefile bin tmp/*

# Build and test
echo -e "------ \e[93mBuild\e[0m ------"
cmake -DCMAKE_BUILD_TYPE=Release -DUKV_BUILD_TESTS=1 \
      -DUKV_BUILD_ENGINE_UMEM=1 -DUKV_BUILD_ENGINE_LEVELDB=0 -DUKV_BUILD_ENGINE_ROCKSDB=0 -DUKV_BUILD_API_FLIGHT=0 \
      .
make -j32
echo -e "------ \e[92mBuild Passing\e[0m ------"

for test in $(ls ./build/bin/*test_units*); do
    echo -e "------ \e[93mRunning $test\e[0m ------";
    UKV_TEST_PATH=./tmp $test
done
echo -e "------ \e[92mTests Passing!\e[0m ------"

# Build and test python
echo -e "------ \e[93mBuild and Test python\e[0m ------"
cibuildwheel --platform linux
echo -e "------ \e[92mPython passing!\e[0m ------"

# Publish Python
read -p "Publish to Pypi? " -n 1 -r
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "------ \e[93mPublishing python to pypi\e[0m ------"
    twine upload wheelhouse/*.whl --verbose
    echo -e "------ \e[92mPython published!\e[0m ------"
fi

# Build Docker
echo -e "\n------ \e[93mBuilding docker\e[0m ------"
docker build . --file docker/Dockerfile --tag unum/ukv:$version-focal
echo -e "------ \e[92mDocker Built!\e[0m ------"

read -p "Publish to DockerHub? " -n 1 -r
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "------ \e[93mPublishing to DockerHub\e[0m ------"
    docker login && docker push unum/ukv:$version-focal
    echo -e "------ \e[92mDocker Publish!\e[0m ------"
fi

echo "Bye!"