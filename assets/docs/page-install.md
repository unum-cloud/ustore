## Installation & Deployment

* For Python: `pip install ukv`
* For Conan: `conan install ukv`
* For Docker image: `docker run --rm --name test_ukv -p 38709:38709 unum/ukv`

To build from source:

```sh
cmake \
    -DUKV_BUILD_PYTHON=1 \
    -DUKV_BUILD_TESTS=1 \
    -DUKV_BUILD_BENCHMARKS=1 \
    -DUKV_BUILD_FLIGHT_RPC=1 . && \
    make -j16
```

To add to your CMake project:

```cmake
cmake_minimum_required(VERSION 3.11)
project(ukv_example)

include(FetchContent)

FetchContent_Declare(
    ukv
    GIT_REPOSITORY https://github.com/unum-cloud/UKV.git
    GIT_SHALLOW TRUE
    GIT_TAG v0.3.0
)
FetchContent_MakeAvailable(ukv)
set(ukv_INCLUDE_DIR ${ukv_SOURCE_DIR}/include)
include_directories(${ukv_INCLUDE_DIR})

add_executable(ukv_example_test main.cpp)
target_link_libraries(ukv_example_test ukv)
```

For Flight RPC, Apache Arrow must be pre-installed.

To build language bindings:

```sh
./python/run.sh
./java/run.sh
./golang/run.sh
```

Building Flight RPC Docker Image:

```sh
docker build -t ukv .
```

Building Conan package, without installing it:

```sh
conan create . ukv/testing --build=missing
```

* To see a usage examples, check the [C][c-example] API and the [C++ API](cpp-example) tests.
* To read the documentation, [check unum.cloud/ukv](https://unum.cloud/UKV).
* To contribute to the development, [check the `src/`](https://github.com/unum-cloud/UKV/blob/main/src).

[c-example]: https://github.com/unum-cloud/UKV/blob/main/tests/compilation.cpp
[cpp-example]: https://github.com/unum-cloud/UKV/blob/main/tests/compilation.cpp
