# Installation

The most straight forward ways to start playing with UKV are:

* For Python: `pip install ukv`
* For Conan: `conan install ukv`
* For Docker image: `docker run --rm --name test_ukv -p 38709:38709 unum/ukv`

## Building From Source

We use CMake as the default build tool.
Most dependencies will be fetched with the integrated CMake scripts.

```sh
mkdir build_release && \
    cd build_release && \
    cmake \
        -DUKV_BUILD_TESTS=0 \
        -DUKV_BUILD_BENCHMARKS=0 \
        -DUKV_BUILD_PYTHON=0 \
        -DUKV_BUILD_REST_API=0 \
        -DUKV_BUILD_FLIGHT_RPC=0 .. && \
    make -j16
```

This will only produce 1 library for each embedded backend.
Building Flight API and REST API may require [additional steps](#partial).

### Adding UKV to Your CMake Project

Assuming CMake is the least straigthforward build tool in history, there is a pretty short exampl of you can link UKV to your CMake project.

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
target_link_libraries(ukv_example_test ukv::ukv_umem)
```

### Partial & Customized Builds

By default, our builds prioritize compatibility over performance.
Still, you can link against your favourite allocator or build every one of the provided bindings.
The CMake options include:

| Option               | Default | Manual Installation Requirements                                                             |
| :------------------- | :-----: | :------------------------------------------------------------------------------------------- |
| UKV_BUILD_TESTS      |    1    |                                                                                              |
| UKV_BUILD_BENCHMARKS |    0    | May require Arrow.                                                                           |
| UKV_BUILD_PYTHON     |    0    | Python: Interpreter, Development libraries. Apache Arrow: Dataset, Flight, Python libraries. |
| UKV_BUILD_FLIGHT_RPC |    0    | Apache Arrow: Flight.                                                                        |
| UKV_BUILD_REST_API   |    0    | Boost: Beast and ASIO.                                                                       |
| UKV_USE_JEMALLOC     |    0    | JeMalloc or AutoConf to be visible.                                                          |

Following scripts are provided to help:

* `./cmake/arrow.sh`: To pre-install needed Arrow components.
* `./cmake/boost.sh`: To pre-install needed Boost components.
* `./python/pack.sh`: build Python package.
* `./golang/pack.sh`: Build GoLang package.
* `./java/pack.sh`: Build Java package.

To build the Flight RPC Docker image locally:

```sh
docker build -t ukv .
```

To build the Conan package localy, without installing it:

```sh
conan create . ukv/testing --build=missing
```

To fetch the most recent Python bindings:

```sh
pip install --extra-index-url https://test.pypi.org/simple/ --force-reinstall ukv
```

## Cloud Deployments

UKV is coming to the clouds across the globe!
You can still manually `docker run` us, but for more entrprise-y deployments you may want:

* horizontal auto-scaling,
* higher performace,
* rolling updates,
* support.

For that, try us on your favourite marketplace:

* Amazon Web Services.
* Microsoft Azure.
* Google Cloud Platform.
* Alibaba Cloud.

## Open Shift Integration

Coming Soon.

---

* To see a usage examples, check the [C][c-example] API and the [C++ API](cpp-example) tests.
* To read the documentation, [check unum.cloud/ukv](https://unum.cloud/UKV).
* To contribute to the development, [check the `src/`](https://github.com/unum-cloud/UKV/blob/main/src).

[c-example]: https://github.com/unum-cloud/UKV/blob/main/tests/compilation.cpp
[cpp-example]: https://github.com/unum-cloud/UKV/blob/main/tests/compilation.cpp
