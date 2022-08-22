cmake_minimum_required(VERSION 3.15)
project(UKV)

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

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fstrict-vtable-pointers")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wunknown-attributes")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ferror-limit=1")

elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-memcmp -fPIC")

  # Warnings & errors:
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wfatal-errors")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unknown-pragmas") # Allow "pragma region" source sections
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
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -fsanitize=address -fsanitize-address-use-after-scope")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -fsanitize-address-use-after-scope")
set(CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fsanitize=address -fsanitize-address-use-after-scope")

find_package(Threads REQUIRED)

# Install 3rd Party Packages
include(FetchContent)
set(FETCHCONTENT_QUIET OFF)
set(UKV_PREINSTALLED_BOOST ON)
set(UKV_PREINSTALLED_ARROW ON)
set(UKV_PREINSTALLED_TURBOPFOR ON)

include("${CMAKE_SOURCE_DIR}/cmake/gtest.cmake")
include("${CMAKE_SOURCE_DIR}/cmake/json.cmake")
include("${CMAKE_SOURCE_DIR}/cmake/boost.cmake")
include("${CMAKE_SOURCE_DIR}/cmake/rocksdb.cmake")
include("${CMAKE_SOURCE_DIR}/cmake/leveldb.cmake")
include("${CMAKE_SOURCE_DIR}/cmake/fmt.cmake")
include("${CMAKE_SOURCE_DIR}/cmake/arrow.cmake")
include("${CMAKE_SOURCE_DIR}/cmake/jemalloc.cmake")

if(NOT ${UKV_PREINSTALLED_BOOST} AND NOT ${UKV_PREINSTALLED_ARROW})
  add_dependencies(Arrow-external Boost-external)
endif()

if(!APPLE)
  add_dependencies(ukv_stl jemalloc)
  add_dependencies(ukv_rocksdb jemalloc)
  add_dependencies(ukv_leveldb jemalloc)
  add_dependencies(ukv_rpc_client jemalloc)
  add_dependencies(ukv_test jemalloc)
  add_dependencies(ukv_leveldb_test jemalloc)
  add_dependencies(ukv_rocksdb_test jemalloc)
endif()

include_directories(include/)
include_directories($ENV{CONDA_PREFIX}/include)
link_directories($ENV{CONDA_PREFIX}/lib)
message("CONDA_PREFIX:" $ENV{CONDA_PREFIX})

# Define the libraries we will need to build
add_library(ukv_stl
  src/backend_stl.cpp
  src/logic_docs.cpp
  src/logic_graph.cpp
)
target_link_libraries(ukv_stl
  nlohmann_json::nlohmann_json
  ${jemalloc_LIBRARIES}
)
set_target_properties(ukv_stl PROPERTIES LINK_FLAGS_RELEASE -s)

add_library(ukv_rocksdb
  src/backend_rocksdb.cpp
  src/logic_graph.cpp
)
target_link_libraries(ukv_rocksdb
  rocksdb
  nlohmann_json::nlohmann_json
  ${jemalloc_LIBRARIES}
)

add_library(ukv_leveldb
  src/backend_leveldb.cpp
  src/logic_graph.cpp
)
target_link_libraries(ukv_leveldb
  leveldb
  nlohmann_json::nlohmann_json
  ${jemalloc_LIBRARIES}
)

add_library(ukv_arrow_client
  src/arrow_client.cpp
  src/logic_docs.cpp
  src/logic_graph.cpp
)
target_link_libraries(ukv_arrow_client
  nlohmann_json::nlohmann_json
  fmt::fmt
  arrow
  arrow_flight
)

set_target_properties(ukv_rocksdb PROPERTIES LINK_FLAGS_RELEASE -s)
set_target_properties(ukv_rocksdb PROPERTIES LINK_FLAGS_RELEASE -s)

add_executable(ukv_rest_server src/rest_server.cpp)
target_link_libraries(ukv_rest_server
  ukv_stl
  pthread
  fmt::fmt
  Boost::headers
  nlohmann_json::nlohmann_json
)

add_executable(ukv_arrow_server src/arrow_server.cpp)
target_link_libraries(ukv_arrow_server
  ukv_stl
  pthread
  nlohmann_json::nlohmann_json
  arrow
  arrow_flight
  arrow_dataset
  parquet
)
add_executable(ukv_test
  src/test.cpp
  src/backend_stl.cpp
  src/logic_docs.cpp
  src/logic_graph.cpp
)

target_link_libraries(ukv_test
  gtest
  nlohmann_json::nlohmann_json
  ${jemalloc_LIBRARIES}
)

add_executable(ukv_leveldb_test
  src/test.cpp
  src/backend_leveldb.cpp
  src/logic_docs.cpp
  src/logic_graph.cpp
)

target_link_libraries(ukv_leveldb_test
  gtest
  leveldb
  nlohmann_json::nlohmann_json
  ${jemalloc_LIBRARIES}
)

add_executable(ukv_rocksdb_test
  src/test.cpp
  src/backend_rocksdb.cpp
  src/logic_docs.cpp
  src/logic_graph.cpp
)

target_link_libraries(ukv_rocksdb_test
  gtest
  rocksdb
  nlohmann_json::nlohmann_json
  ${jemalloc_LIBRARIES}
)