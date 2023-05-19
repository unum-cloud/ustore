include(ExternalProject)

set(PREFIX_DIR ${CMAKE_BINARY_DIR}/_deps)
ExternalProject_Add(
    hiredis_external

    GIT_REPOSITORY "https://github.com/redis/hiredis.git"
    GIT_TAG v1.1.0
    GIT_SHALLOW 1
    GIT_PROGRESS 0
    
    PREFIX "${PREFIX_DIR}"
    DOWNLOAD_DIR "${PREFIX_DIR}/hiredis-src"
    LOG_DIR "${PREFIX_DIR}/hiredis-log"
    STAMP_DIR "${PREFIX_DIR}/hiredis-stamp"
    TMP_DIR "${PREFIX_DIR}/hiredis-tmp"
    SOURCE_DIR "${PREFIX_DIR}/hiredis-src"
    INSTALL_DIR "${PREFIX_DIR}/hiredis-install"
    BINARY_DIR "${PREFIX_DIR}/hiredis-build"

    BUILD_ALWAYS 0
    UPDATE_COMMAND ""

    CMAKE_ARGS
    -DCMAKE_INSTALL_PREFIX:PATH=${PREFIX_DIR}/hiredis-install
    -DCMAKE_INSTALL_LIBDIR=lib
    -DCMAKE_INSTALL_RPATH:PATH=<INSTALL_DIR>/lib
    -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
    -DENABLE_STATIC:STRING=ON
    -DENABLE_CPPSUITE:BOOL=OFF
    -DCMAKE_C_FLAGS=-Wno-maybe-uninitialized
    -DCMAKE_CXX_FLAGS=-Wno-unused-variable
    -DENABLE_SSL:BOOL=OFF
    -DDISABLE_TESTS:BOOL=ON
    -DENABLE_SSL_TESTS:BOOL=OFF
    -DENABLE_ASYNC_TESTS:BOOL=OFF
)

set(hiredis_INCLUDE_DIR ${PREFIX_DIR}/hiredis-install/include)
if(CMAKE_BUILD_TYPE MATCHES "Debug")
    set(hiredis_LIBRARY_PATH ${PREFIX_DIR}/hiredis-install/lib/libhiredisd.a)
else()
    set(hiredis_LIBRARY_PATH ${PREFIX_DIR}/hiredis-install/lib/libhiredis.a)
endif()

file(MAKE_DIRECTORY ${hiredis_INCLUDE_DIR})
add_library(hiredis STATIC IMPORTED)
set_property(TARGET hiredis PROPERTY IMPORTED_LOCATION ${hiredis_LIBRARY_PATH})
set_property(TARGET hiredis APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${hiredis_INCLUDE_DIR})
include_directories(${hiredis_INCLUDE_DIR})
add_dependencies(hiredis hiredis_external)  