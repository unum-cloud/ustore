#!/bin/sh
set -e
version=$(cat VERSION)
# Clean up
echo -e "------ \e[93mClean up\e[0m ------"
rm -rf CMakeCache.txt CMakeFiles wheelhouse Makefile bin tmp/*

# Build and test
echo -e "------ \e[93mBuild\e[0m ------"
cmake -DCMAKE_BUILD_TYPE=Release -DUKV_BUILD_TESTS=1 -B ./build_release \
      -DUKV_BUILD_ENGINE_UMEM=1 -DUKV_BUILD_ENGINE_LEVELDB=1 -DUKV_BUILD_ENGINE_ROCKSDB=1 -DUKV_BUILD_API_FLIGHT=0 \
      .
make -j 32 -C ./build_release
echo -e "------ \e[92mBuild Passing\e[0m ------"

for test in $(ls ./build_release/build/bin/*test_units*); do
    echo -e "------ \e[93mRunning $test\e[0m ------";
    $test
done
echo -e "------ \e[92mTests Passing!\e[0m ------"

# Build and Test Python
echo -e "------ \e[93mBuild and Test Python\e[0m ------"
cibuildwheel --platform linux
echo -e "------ \e[92mPython Tests Passing!\e[0m ------"

# Publish Python
read -p "Publish to PyPi? " -n 1 -r
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "------ \e[93mPublishing Python to PyPi\e[0m ------"
    twine upload wheelhouse/*.whl --verbose
    echo -e "------ \e[92mPython Published!\e[0m ------"
fi

# Build Docker
echo -e "\n------ \e[93mBuilding Docker\e[0m ------"
docker build . --file docker/Dockerfile --tag unum/ukv:$version-focal
echo -e "------ \e[92mDocker Built!\e[0m ------"

read -p "Publish to DockerHub? " -n 1 -r
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "------ \e[93mPublishing to DockerHub\e[0m ------"
    docker login && docker push unum/ukv:$version-focal
    echo -e "------ \e[92mDocker Publish!\e[0m ------"
fi

echo "Bye!"