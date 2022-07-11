cmake_minimum_required(VERSION 3.15)
project(UKV)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

set(CMAKE_EXPORT_COMPILE_COMMANDS OFF)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/")
set(CMAKE_CACHEFILE_DIR "${CMAKE_SOURCE_DIR}/build")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/build/lib")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/build/lib")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/build/bin")

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ferror-limit=1")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fstrict-vtable-pointers")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wunknown-attributes")

elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wnon-virtual-dtor -Woverloaded-virtual")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-missing-field-initializers")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-memcmp -fPIC")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wreorder -Wunused-parameter")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wfatal-errors")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=deprecated-copy -Wno-deprecated-copy -Wno-error=pessimizing-move -Wno-pessimizing-move")
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
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -DDEBUG_SAFE_MODE -g")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUG_SAFE_MODE -g")

find_package(Threads REQUIRED)

# Install 3rd Party Packages
include(FetchContent)
set(FETCHCONTENT_QUIET OFF)

# Boost:
set(Boost_USE_STATIC_LIBS ON)
set(Boost_USE_STATIC_RUNTIME ON)
FetchContent_Declare(boost
  URL https://boostorg.jfrog.io/artifactory/main/release/1.79.0/source/boost_1_79_0.tar.bz2
  URL_HASH SHA256=475d589d51a7f8b3ba2ba4eda022b170e562ca3b760ee922c146b6c65856ef39
)
FetchContent_MakeAvailable(boost)
FetchContent_GetProperties(boost SOURCE_DIR BOOST_SRC_DIR)
include_directories(${BOOST_SRC_DIR})
include_directories(${Boost_INCLUDE_DIRS})

# FMT:
FetchContent_Declare(
  fmt
  GIT_REPOSITORY https://github.com/fmtlib/fmt.git
  GIT_TAG 8.1.1
  GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(fmt)

# JSON:
# https://json.nlohmann.me/integration/cmake/#fetchcontent
FetchContent_Declare(json URL https://github.com/nlohmann/json/releases/download/v3.10.5/json.tar.xz)
FetchContent_MakeAvailable(json)

# LZ4 Compression
# https://stackoverflow.com/questions/67537111/how-do-i-decide-between-lz4-and-snappy-compression
include("${CMAKE_SOURCE_DIR}/cmake/lz4.cmake")

# TurboPFor for 64-bit integers
# https://github.com/powturbo/TurboPFor-Integer-Compression
# https://github.com/topics/integer-compression
include("${CMAKE_SOURCE_DIR}/cmake/turbopfor.cmake")

# RocksDB
# include("${CMAKE_SOURCE_DIR}/cmake/rocksdb.cmake")

# LevelDB
include("${CMAKE_SOURCE_DIR}/cmake/leveldb.cmake")

# FastPFor Integer Compression for 32-bit integers
FetchContent_Declare(
  fpfor
  GIT_REPOSITORY https://github.com/lemire/FastPFor.git
  GIT_TAG v0.1.8
  GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(fpfor)

# GTest
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG release-1.11.0
)
FetchContent_MakeAvailable(googletest)

# Define the libraries we will need to build
include_directories(include/ ${rocksdb_SOURCE_DIR}/include)
include_directories(include/ ${leveldb_SOURCE_DIR}/include)

add_library(ukv_stl
  src/ukv_stl.cpp
  src/ukv_docs_nlohmann.cpp
  src/ukv_graph_pfor.cpp
)
target_link_libraries(ukv_stl
  lz4
  turbopfor
  nlohmann_json::nlohmann_json
)

# add_library(ukv_rocksdb
# src/ukv_rocksdb.cpp
# src/ukv_graph_pfor.cpp
# )
add_library(ukv_leveldb
  src/ukv_leveldb.cpp
  src/ukv_graph_pfor.cpp
)

# target_link_libraries(ukv_rocksdb
# rocksdb
# )
target_link_libraries(ukv_leveldb
  leveldb
)

add_library(ukv_beast_client src/ukv_beast_client.cpp)

# add_executable(ukv_beast_server src/ukv_beast_server.cpp)
# target_link_libraries(ukv_beast_server
# ukv_stl
# pthread
# fmt::fmt
# nlohmann_json::nlohmann_json
# )
add_executable(ukv_test
  src/test.cpp
  src/ukv_stl.cpp

  # src/ukv_docs_nlohmann.cpp
  src/ukv_graph_pfor.cpp
)
target_link_libraries(ukv_test
  gtest
)
