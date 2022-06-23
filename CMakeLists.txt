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
set(CMAKE_BUILD_TYPE Release)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wnon-virtual-dtor -Woverloaded-virtual")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-missing-field-initializers")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-memcmp -fPIC")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wreorder -Wunused-parameter")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")

if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ferror-limit=1")
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -fstrict-vtable-pointers")
endif()

# Supported CPU architecture flags:
# https://gcc.gnu.org/onlinedocs/gcc/x86-Options.html
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=armv8-a+simd")

# Optimizations for common x86 CPUs.
if(${CMAKE_SYSTEM_PROCESSOR} MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse -msse4.2 -mavx2") # -march=native

  if(UNIX AND NOT APPLE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx512f")
  endif()
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

# SIMD-JSON: https://github.com/simdjson/simdjson/blob/master/doc/basics.md#using-simdjson-as-a-cmake-dependency
FetchContent_Declare(
  simdjson
  GIT_REPOSITORY https://github.com/simdjson/simdjson.git
  GIT_TAG tags/v2.0.4
  GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(simdjson)

# gRPC: https://grpc.io/blog/cmake-improvements/
# https://github.com/grpc/grpc/blob/master/test/distrib/cpp/run_distrib_test_cmake_fetchcontent.sh
# FetchContent_Declare(
# gRPC
# GIT_REPOSITORY https://github.com/grpc/grpc
# GIT_TAG v1.47.0
# GIT_SHALLOW TRUE
# )
# FetchContent_MakeAvailable(gRPC)
#
# Download Networking TS for us to implement RPC
# Example can be found here: https://github.com/bsergeev/CppNetworkingTS
# Discussions: https://www.reddit.com/r/cpp/comments/b12iob/networking_ts_beast_new_tutorials_read_this_to/
# find_package(Git)
# if(NOT GIT_FOUND)
# message(FATAL_ERROR "Git not found, aborting...")
# endif()
# set(NETTS_LOCATION ${CMAKE_CURRENT_SOURCE_DIR}/netts)
# find_path(NETTS_INCLUDE_DIR "net" NO_DEFAULT_PATH PATHS "${NETTS_LOCATION}/include/experimental")
# if(NOT NETTS_INCLUDE_DIR) # The repo wasn't found => clone it
# execute_process(COMMAND git clone https://github.com/chriskohlhoff/networking-ts-impl.git ${NETTS_LOCATION})
# else() # Pull the repo, in case it was updated
# execute_process(COMMAND git pull WORKING_DIRECTORY ${NETTS_LOCATION})
# endif()
# find_path(NETTS_INCLUDE_DIR "net" NO_DEFAULT_PATH PATHS "${NETTS_LOCATION}/include/experimental")
# if(NOT NETTS_INCLUDE_DIR)
# message(FATAL_ERROR "Something wrong with networking-ts-impl.git, aborting...")
# endif()
# include_directories("${NETTS_LOCATION}/include")

# Define the libraries we will need to build
include_directories(include/)
add_library(ukv_stl_embedded src/ukv_stl_embedded.cpp)
add_library(ukv_stl_client src/ukv_stl_client.cpp)
add_executable(ukv_beast_server src/ukv_beast_server.cpp)
target_link_libraries(ukv_beast_server simdjson)
