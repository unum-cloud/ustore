include(ExternalProject)

include("${CMAKE_MODULE_PATH}/hiredis.cmake")
set(PREFIX_DIR ${CMAKE_BINARY_DIR}/_deps)

ExternalProject_Add(
    redis_external

    GIT_REPOSITORY "https://github.com/sewenew/redis-plus-plus.git"
    GIT_TAG 1.3.7
    GIT_SHALLOW 1
    GIT_PROGRESS 0
    
    PREFIX "${PREFIX_DIR}"
    DOWNLOAD_DIR "${PREFIX_DIR}/redis-src"
    LOG_DIR "${PREFIX_DIR}/redis-log"
    STAMP_DIR "${PREFIX_DIR}/redis-stamp"
    TMP_DIR "${PREFIX_DIR}/redis-tmp"
    SOURCE_DIR "${PREFIX_DIR}/redis-src"
    INSTALL_DIR "${PREFIX_DIR}/redis-install"
    BINARY_DIR "${PREFIX_DIR}/redis-build"

    BUILD_ALWAYS 0
    UPDATE_COMMAND ""

    CMAKE_ARGS
    -DCMAKE_PREFIX_PATH:PATH=${PREFIX_DIR}/hiredis-install
    -DCMAKE_INSTALL_PREFIX:PATH=${PREFIX_DIR}/redis-install
    -DCMAKE_INSTALL_LIBDIR=lib
    -DCMAKE_INSTALL_RPATH:PATH=<INSTALL_DIR>/lib
    -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
    -DENABLE_STATIC:STRING=ON
    -DENABLE_CPPSUITE:BOOL=OFF
    -DCMAKE_C_FLAGS=-Wno-maybe-uninitialized
    -DCMAKE_CXX_FLAGS=-Wno-unused-variable
    -DREDIS_PLUS_PLUS_BUILD_TEST:BOOL=OFF
)

set(redis_INCLUDE_DIR ${PREFIX_DIR}/redis-install/include)
set(redis_LIBRARY_PATH ${PREFIX_DIR}/redis-install/lib/libredis++.a)

file(MAKE_DIRECTORY ${redis_INCLUDE_DIR})
add_library(redis STATIC IMPORTED)
set_target_properties(redis PROPERTIES IMPORTED_LINK_INTERFACE_LIBRARIES hiredis)

set_property(TARGET redis PROPERTY IMPORTED_LOCATION ${redis_LIBRARY_PATH})
set_property(TARGET redis APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${redis_INCLUDE_DIR})

include_directories(${redis_INCLUDE_DIR})
add_dependencies(redis_external hiredis)
add_dependencies(redis redis_external)