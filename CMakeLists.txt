cmake_minimum_required(VERSION 3.15)
project(UKV)

file(READ "VERSION" _VERSION_)
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
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wfatal-errors")
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
set(UKV_PREINSTALLED_BOOST ON CACHE BOOL "True, if a pre-installed external version of Boost should be used")
set(UKV_PREINSTALLED_ARROW ON CACHE BOOL "True, if a pre-installed external version of Apache Arrow should be used")
set(UKV_PREINSTALLED_TURBOPFOR ON CACHE BOOL "True, if a pre-installed external version of TurboPFOR should be used")
set(UKV_BUILD_PYTHON ON CACHE BOOL "Building Python bidings for all SDKs")
set(UKV_BUILD_TESTS ON CACHE BOOL "Building C/C++ native tests")
set(UKV_BUILD_BENCHMARKS ON CACHE BOOL "Building C/C++ native benchmarks")
set(UKV_BUILD_FLIGHT_RPC ON CACHE BOOL "Building Apache Arrow Flight RPC server for all backends")

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
  add_dependencies(ukv_stl_unit_tests jemalloc)
  add_dependencies(ukv_leveldb_unit_tests jemalloc)
  add_dependencies(ukv_rocksdb_unit_tests jemalloc)
endif()

include_directories(include/)

# Apache Arrow might be coming from Anaconda
include_directories($ENV{CONDA_PREFIX}/include)
link_directories($ENV{CONDA_PREFIX}/lib)
message("CONDA_PREFIX:" $ENV{CONDA_PREFIX})

# Define the libraries we will need to build
add_library(ukv_stl src/backend_stl.cpp src/logic_docs.cpp src/logic_graph.cpp)
target_link_libraries(ukv_stl nlohmann_json::nlohmann_json ${jemalloc_LIBRARIES})

add_library(ukv_rocksdb src/backend_rocksdb.cpp src/logic_docs.cpp src/logic_graph.cpp)
target_link_libraries(ukv_rocksdb rocksdb nlohmann_json::nlohmann_json ${jemalloc_LIBRARIES})

add_library(ukv_leveldb src/backend_leveldb.cpp src/logic_docs.cpp src/logic_graph.cpp)
target_link_libraries(ukv_leveldb leveldb nlohmann_json::nlohmann_json ${jemalloc_LIBRARIES})
set_source_files_properties(src/backend_leveldb.cpp PROPERTIES COMPILE_FLAGS -fno-rtti)

add_library(ukv_flight_client src/arrow_client.cpp src/logic_docs.cpp src/logic_graph.cpp)
target_link_libraries(ukv_flight_client nlohmann_json::nlohmann_json fmt::fmt arrow arrow_flight)

# Generate Apache Arrow Flight RPC backends
if(${UKV_BUILD_FLIGHT_RPC})
  foreach(lib_name IN ITEMS ukv_stl ukv_rocksdb ukv_leveldb)
    get_target_property(lib_SOURCES ${lib_name} SOURCES)
    get_target_property(lib_LIBRARIES ${lib_name} LINK_LIBRARIES)
    string(CONCAT server_name ${lib_name} "_flight_server")
    add_executable(${server_name} src/arrow_server.cpp ${lib_SOURCES})
    target_link_libraries(${server_name} pthread nlohmann_json::nlohmann_json arrow arrow_flight ${lib_LIBRARIES})
  endforeach()
endif()

# Generate tests
# We build them with the original sources (instead of linking with already compiled libraries)
# to maximize the number of exposed symbols and make debugging easier.
if(${UKV_BUILD_TESTS})
  include("${CMAKE_SOURCE_DIR}/cmake/gtest.cmake")

  foreach(lib_name IN ITEMS ukv_stl ukv_rocksdb ukv_leveldb ukv_flight_client)
    get_target_property(lib_SOURCES ${lib_name} SOURCES)
    get_target_property(lib_LIBRARIES ${lib_name} LINK_LIBRARIES)
    string(CONCAT test_name ${lib_name} "_unit_tests")
    add_executable(${test_name} tests/units.cpp ${lib_SOURCES})
    target_link_libraries(${test_name} gtest ${lib_LIBRARIES})
    add_test(NAME "${lib_name}" COMMAND "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${test_name}")

    string(CONCAT test_name ${lib_name} "_api_test")
    add_executable(${test_name} tests/api.cpp ${lib_SOURCES})
    target_link_libraries(${test_name} ${lib_LIBRARIES})
  endforeach()
endif()

# Generate benchmarks: Bitcoin Core & Twitter
if(${UKV_BUILD_BENCHMARKS})
  foreach(lib_name IN ITEMS ukv_stl ukv_rocksdb ukv_leveldb ukv_flight_client)
    get_target_property(lib_SOURCES ${lib_name} SOURCES)
    get_target_property(lib_LIBRARIES ${lib_name} LINK_LIBRARIES)

    string(CONCAT bench_name ${lib_name} "_twitter_benchmark")
    add_executable(${bench_name} benchmarks/twitter.cpp ${lib_SOURCES})
    target_link_libraries(${bench_name} gtest ${lib_LIBRARIES})

    string(CONCAT bench_name ${lib_name} "_bitcoin_benchmark")
    add_executable(${bench_name} benchmarks/bitcoin.cpp ${lib_SOURCES})
    target_link_libraries(${bench_name} leveldb ${lib_LIBRARIES})
  endforeach()
endif()

# Enable warning and sanitization for our primary targets
foreach(lib_name IN ITEMS ukv_stl ukv_rocksdb ukv_leveldb ukv_flight_client)
  if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set_target_properties(${lib_name} PROPERTIES CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-memcmp -fPIC")
    set_target_properties(${lib_name} PROPERTIES CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wfatal-errors")
    set_target_properties(${lib_name} PROPERTIES CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")
    set_target_properties(${lib_name} PROPERTIES CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unknown-pragmas") # Allow "pragma region" source sections
    set_target_properties(${lib_name} PROPERTIES CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=deprecated-copy -Wno-deprecated-copy -Wno-error=pessimizing-move -Wno-pessimizing-move")
  endif()
endforeach()

# Build Python bindings linking to precompiled client SDKs
if(${UKV_BUILD_PYTHON})
  find_package(Python 3.9 COMPONENTS Interpreter Development REQUIRED)
  find_package(pybind11 CONFIG REQUIRED)
  file(MAKE_DIRECTORY ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/ukv)

  foreach(lib_name IN ITEMS stl rocksdb leveldb flight_client)
    string(CONCAT native_lib_name "ukv_" ${lib_name})
    string(CONCAT wrap_lib_name "py_" ${lib_name})

    get_target_property(lib_SOURCES ${native_lib_name} SOURCES)
    get_target_property(lib_LIBRARIES ${native_lib_name} LINK_LIBRARIES)

    pybind11_add_module(${wrap_lib_name} MODULE python/pybind.cpp python/pybind/database.cpp python/pybind/networkx.cpp python/pybind/documents.cpp python/pybind/pandas.cpp)
    target_include_directories(${wrap_lib_name} PUBLIC python/)
    target_include_directories(${wrap_lib_name} PUBLIC python/pybind/)
    target_link_libraries(${wrap_lib_name} PRIVATE arrow arrow_dataset arrow_flight arrow_python arrow_python_flight ${native_lib_name} ${lib_LIBRARIES})
    target_compile_definitions(${wrap_lib_name} PRIVATE UKV_VERSION="${_VERSION_}" UKV_PYTHON_MODULE_NAME=${lib_name})
    set_target_properties(${wrap_lib_name} PROPERTIES OUTPUT_NAME ukv/${lib_name})
  endforeach()
endif()
