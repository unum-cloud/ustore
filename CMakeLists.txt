cmake_minimum_required(VERSION 3.22)
project(UKV)

option(UKV_BUILD_TESTS "Building C/C++ native tests" ON)
option(UKV_BUILD_PYTHON "Building Python bidings for all SDKs")
option(UKV_BUILD_BENCHMARKS "Building C/C++ native benchmarks")
option(UKV_BUILD_FLIGHT_API "Building Apache Arrow Flight RPC server for all backends")
option(UKV_BUILD_REST_API "Building REST API server for all backends")
option(UKV_USE_JEMALLOC "A faster allocator, that requires autoconf to be installed")
option(UKV_USE_ONEAPI "Faster concurrency primitives from Intel")

set(UKV_UDISK_PATH "" CACHE STRING "Pass a path to UDisk binary to produce a full range of bindings")

# Some settings are mutually dependant
include(CMakeDependentOption)
cmake_dependent_option(UKV_REBUILD_JEMALLOC "Enable to attempt to source and build JeMalloc locally" ON "UKV_USE_JEMALLOC" OFF)
cmake_dependent_option(UKV_REBUILD_ONEAPI "Enable to attempt to source and build Intel OneAPI locally" ON "UKV_USE_ONEAPI" OFF)
cmake_dependent_option(UKV_REBUILD_BOOST "Enable to attempt to source and build Boost locally" ON "UKV_BUILD_REST_API" OFF)
cmake_dependent_option(UKV_REBUILD_ARROW "Enable to attempt to source and build Apache Arrow locally" ON "UKV_BUILD_PYTHON OR UKV_BUILD_FLIGHT_API" OFF)

file(READ "VERSION" _VERSION_)
set(CMAKE_EXPORT_COMPILE_COMMANDS OFF)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/")
set(CMAKE_CACHEFILE_DIR "${CMAKE_SOURCE_DIR}/build")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/build/lib")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/build/lib")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/build/bin")

message("CMAKE_CXX_COMPILER_ID: ${CMAKE_CXX_COMPILER_ID}")
message("CMAKE_CXX_COMPILER_VERSION: ${CMAKE_CXX_COMPILER_VERSION}")
message("CMAKE_CXX_COMPILER: ${CMAKE_CXX_COMPILER}")

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fstrict-vtable-pointers")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wunknown-attributes")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ferror-limit=1")

elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-memcmp -fPIC")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wfatal-errors")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")

elseif(CMAKE_CXX_COMPILER_ID STREQUAL "IntelLLVM")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ferror-limit=1")
endif()

# Optimizations for common x86 CPUs.
# https://gcc.gnu.org/onlinedocs/gcc/x86-Options.html
if(${CMAKE_SYSTEM_PROCESSOR} MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse -msse4.2 -mavx2") # -march=native
endif()

# Optimizations for ARM CPUs.
if(${CMAKE_SYSTEM_PROCESSOR} MATCHES "arm")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mfloat-abi=softfp -mfpu=neon -march=armv8-a+simd")
endif()

# Risky optimizations, that require tuning.
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -momit-leaf-frame-pointer")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -funsafe-math-optimizations")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -funroll-loops")

# Other release build specs.
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -DNDEBUG -O3")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -DNDEBUG -O3")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -fvisibility=hidden")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -fvisibility=hidden")

# Debugging flags.
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -DUKV_DEBUG -g")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DUKV_DEBUG -g")

find_package(Threads REQUIRED)
set(CMAKE_FIND_LIBRARY_SUFFIXES .a)

if(NOT ${UKV_BUILD_PYTHON})
  set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -fsanitize=address -fsanitize-address-use-after-scope")
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -fsanitize-address-use-after-scope")
  set(CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fsanitize=address -fsanitize-address-use-after-scope")
endif()

# Install 3rd Party Packages
include(FetchContent)
include_directories(include/)

if(CMAKE_BUILD_TYPE MATCHES "Debug")
  set(FETCHCONTENT_QUIET OFF)
else()
  set(FETCHCONTENT_QUIET ON)
endif()

# Engines:
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/rocksdb.cmake")
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/leveldb.cmake")
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/consistent_set.cmake")

# Modalities:
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/fmt.cmake")
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/json.cmake")
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/bson.cmake")
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/yyjson.cmake")
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/simdjson.cmake")
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/pcre2.cmake")

if(${UKV_USE_JEMALLOC})
  include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/jemalloc.cmake")
endif()

if(${UKV_USE_ONEAPI})
  include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/oneapi.cmake")
endif()

# Distributions:
if(${UKV_BUILD_PYTHON} OR ${UKV_BUILD_FLIGHT_API})
  include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/arrow.cmake")
endif()

if(${UKV_BUILD_REST_API})
  include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/boost.cmake")
endif()

if(${UKV_BUILD_TESTS})
  include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/gtest.cmake")
endif()

if(${UKV_BUILD_BENCHMARKS})
  include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/gtest.cmake")
  include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/gbench.cmake")
endif()

# Define the libraries we will need to build
add_library(ukv_umem src/engine_umem.cpp src/modality_docs.cpp src/modality_paths.cpp src/modality_graph.cpp src/modality_vectors.cpp)
target_link_libraries(ukv_umem pthread yyjson simdjson bson pcre2 ${JEMALLOC_LIBRARIES} ${TBB_LIBRARIES})
target_compile_definitions(ukv_umem INTERFACE UKV_VERSION="${_VERSION_}")
target_compile_definitions(ukv_umem INTERFACE UKV_ENGINE_NAME=umem)

add_library(ukv_rocksdb src/engine_rocksdb.cpp src/modality_docs.cpp src/modality_paths.cpp src/modality_graph.cpp src/modality_vectors.cpp)
target_link_libraries(ukv_rocksdb rocksdb pthread yyjson simdjson bson pcre2 ${JEMALLOC_LIBRARIES})
target_compile_definitions(ukv_rocksdb INTERFACE UKV_VERSION="${_VERSION_}")
target_compile_definitions(ukv_rocksdb INTERFACE UKV_ENGINE_NAME=rocksdb)

add_library(ukv_leveldb src/engine_leveldb.cpp src/modality_docs.cpp src/modality_paths.cpp src/modality_graph.cpp src/modality_vectors.cpp)
target_link_libraries(ukv_leveldb leveldb pthread yyjson simdjson bson pcre2 ${JEMALLOC_LIBRARIES})
set_source_files_properties(src/engine_leveldb.cpp PROPERTIES COMPILE_FLAGS -fno-rtti)
target_compile_definitions(ukv_leveldb INTERFACE UKV_VERSION="${_VERSION_}")
target_compile_definitions(ukv_leveldb INTERFACE UKV_ENGINE_NAME=leveldb)

if((NOT APPLE) AND((DEFINED ${JEMALLOC_DEPENDENCY}) OR(DEFINED {ARROW_DEPENDENCY})))
  add_dependencies(ukv_umem ${JEMALLOC_DEPENDENCY} ${ARROW_DEPENDENCY})
  add_dependencies(ukv_rocksdb ${JEMALLOC_DEPENDENCY} ${ARROW_DEPENDENCY})
  add_dependencies(ukv_leveldb ${JEMALLOC_DEPENDENCY} ${ARROW_DEPENDENCY})
endif()

# The rest of CMake will apply to all client libraries
set(UKV_ENGINE_NAMES "umem" "leveldb" "rocksdb")
set(UKV_CLIENT_NAMES "umem" "leveldb" "rocksdb")

if(EXISTS ${UKV_UDISK_PATH})
  add_library(udisk STATIC IMPORTED)
  target_link_libraries(udisk INTERFACE dl pthread explain uring numa)
  set_property(TARGET udisk PROPERTY IMPORTED_LOCATION ${UKV_UDISK_PATH})
  set_property(TARGET udisk PROPERTY LINK_LIBRARIES "")

  add_library(ukv_udisk src/modality_docs.cpp src/modality_paths.cpp src/modality_graph.cpp src/modality_vectors.cpp)

  if((NOT APPLE) AND DEFINED ${JEMALLOC_DEPENDENCY})
    add_dependencies(ukv_udisk jemalloc)
  endif()

  target_link_libraries(ukv_udisk udisk pthread yyjson simdjson bson pcre2 nlohmann_json::nlohmann_json ${JEMALLOC_LIBRARIES})
  target_compile_definitions(ukv_udisk INTERFACE UKV_VERSION="${_VERSION_}")
  target_compile_definitions(ukv_udisk INTERFACE UKV_ENGINE_NAME=udisk)

  list(APPEND UKV_ENGINE_NAMES "udisk")
  list(APPEND UKV_CLIENT_NAMES "udisk")
endif()

# Generate Apache Arrow Flight RPC backends
if(${UKV_BUILD_FLIGHT_API})
  add_library(ukv_flight_client src/arrow_client.cpp src/modality_docs.cpp src/modality_graph.cpp src/modality_vectors.cpp)
  target_link_libraries(ukv_flight_client pthread yyjson simdjson bson pcre2 fmt::fmt arrow arrow_flight ${JEMALLOC_LIBRARIES})
  list(APPEND UKV_CLIENT_NAMES "flight_client")
  target_compile_definitions(ukv_flight_client INTERFACE UKV_FLIGHT_CLIENT=TRUE)

  if((NOT APPLE) AND DEFINED ${JEMALLOC_DEPENDENCY})
    add_dependencies(ukv_flight_client ${JEMALLOC_DEPENDENCY})
  endif()

  foreach(engine_name IN ITEMS ${UKV_ENGINE_NAMES})
    string(CONCAT target_name "ukv_" ${engine_name})
    get_target_property(target_LIBRARIES ${target_name} LINK_LIBRARIES)

    string(CONCAT server_name "ukv_" ${engine_name} "_flight_server")
    add_executable(${server_name} src/arrow_server.cpp ${lib_SOURCES})
    target_link_libraries(${server_name} pthread yyjson simdjson bson arrow arrow_flight ${target_name} ${target_LIBRARIES})
  endforeach()
endif()

# Enable warning and sanitization for our primary targets
foreach(client_name IN ITEMS ${UKV_CLIENT_NAMES})
  string(CONCAT target_name "ukv_" ${client_name})

  if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set_target_properties(${target_name} PROPERTIES CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-memcmp -fPIC")
    set_target_properties(${target_name} PROPERTIES CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wfatal-errors")
    set_target_properties(${target_name} PROPERTIES CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")
    set_target_properties(${target_name} PROPERTIES CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unknown-pragmas") # Allow "pragma region" source sections
    set_target_properties(${target_name} PROPERTIES CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=deprecated-copy -Wno-deprecated-copy -Wno-error=pessimizing-move -Wno-pessimizing-move")
  endif()
endforeach()

# Generate tests
# We build them with the original sources (instead of linking with already compiled libraries)
# to maximize the number of exposed symbols and make debugging easier.
if(${UKV_BUILD_TESTS})
  foreach(client_name IN ITEMS ${UKV_CLIENT_NAMES})
    string(CONCAT target_name "ukv_" ${client_name})
    get_target_property(target_LIBRARIES ${target_name} LINK_LIBRARIES)

    string(CONCAT test_name "ukv_" ${client_name} "_compilation_test")
    add_executable(${test_name} tests/compilation.cpp)
    target_compile_definitions(${test_name} PUBLIC UKV_TEST_PATH="tmp/${client_name}")
    target_link_libraries(${test_name} ${target_name} ${target_LIBRARIES})

    string(CONCAT test_name "ukv_" ${client_name} "_unit_test")
    add_executable(${test_name} tests/unit.cpp)
    target_link_libraries(${test_name} gtest ${target_name} ${target_LIBRARIES})
    target_compile_definitions(${test_name} PUBLIC UKV_TEST_PATH="tmp/${client_name}")
    add_test(NAME "${client_name}" COMMAND "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${test_name}")

    string(CONCAT test_name "ukv_" ${client_name} "_stress_test")
    add_executable(${test_name} tests/transaction_stress_test.cpp)
    target_link_libraries(${test_name} gtest ${target_name} ${target_LIBRARIES})
    target_compile_definitions(${test_name} PUBLIC UKV_TEST_PATH="tmp/${client_name}/")

    string(CONCAT test_name "ukv_" ${client_name} "_api_test")
    add_executable(${test_name} tests/api.cpp)
    target_compile_definitions(${test_name} PUBLIC UKV_TEST_PATH="tmp/${client_name}")
    target_link_libraries(${test_name} gtest ${target_name} ${target_LIBRARIES})
  endforeach()
endif()

# Generate benchmarks: Bitcoin Core & Twitter
if(${UKV_BUILD_BENCHMARKS})
  foreach(client_name IN ITEMS ${UKV_CLIENT_NAMES})
    string(CONCAT target_name "ukv_" ${client_name})
    get_target_property(target_LIBRARIES ${target_name} LINK_LIBRARIES)

    string(CONCAT bench_name "ukv_" ${client_name} "_twitter_benchmark")
    add_executable(${bench_name} benchmarks/twitter.cpp)
    target_link_libraries(${bench_name} benchmark ${target_name} ${target_LIBRARIES})

    string(CONCAT bench_name "ukv_" ${client_name} "_tabular_graph_benchmark")
    add_executable(${bench_name} benchmarks/tabular_graph.cpp)
    target_link_libraries(${bench_name} leveldb benchmark ${target_name} ${target_LIBRARIES})
  endforeach()
endif()

# Build Python bindings linking to precompiled client SDKs
if(${UKV_BUILD_PYTHON})
  find_package(Python 3.9 COMPONENTS Interpreter Development REQUIRED)
  include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/pybind11.cmake")
  file(MAKE_DIRECTORY ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/ukv)

  foreach(client_name IN ITEMS ${UKV_CLIENT_NAMES})
    string(CONCAT target_name "ukv_" ${client_name})
    string(CONCAT wrap_lib_name "py_" ${client_name})

    get_target_property(target_LIBRARIES ${target_name} LINK_LIBRARIES)

    pybind11_add_module(${wrap_lib_name} MODULE python/pybind.cpp python/database.cpp python/networkx.cpp python/documents.cpp python/pandas.cpp)
    target_include_directories(${wrap_lib_name} PUBLIC python/)
    target_link_libraries(${wrap_lib_name} PRIVATE pthread arrow::arrow arrow::dataset arrow::flight arrow::bundled arrow::python fmt::fmt ${target_name} ${target_LIBRARIES})
    target_compile_definitions(${wrap_lib_name} PRIVATE UKV_VERSION="${_VERSION_}" UKV_PYTHON_MODULE_NAME=${client_name})
    set_target_properties(${wrap_lib_name} PROPERTIES OUTPUT_NAME ukv/${client_name})
  endforeach()
endif()
