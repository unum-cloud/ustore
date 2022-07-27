include(ExternalProject)

ExternalProject_Add(
    arrow-external
    GIT_REPOSITORY https://github.com/apache/arrow.git
    GIT_TAG "apache-arrow-8.0.1"
    GIT_SHALLOW TRUE

    DOWNLOAD_DIR "_deps/arrow-src"
    LOG_DIR "_deps/arrow-log"
    STAMP_DIR "_deps/arrow-stamp"
    TMP_DIR "_deps/arrow-tmp"
    SOURCE_DIR "_deps/arrow-src"
    BINARY_DIR "_deps/arrow-build"
    INSTALL_DIR "_deps/arrow-install"
    SOURCE_SUBDIR "cpp"

    CMAKE_ARGS ${ARROW_CMAKE_ARGS}
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -DCMAKE_INSTALL_PREFIX=_deps/arrow-install
    -DARROW_BUILD_INTEGRATION=OFF
    -DARROW_BUILD_STATIC=ON
    -DARROW_BUILD_TESTS=OFF
    -DARROW_EXTRA_ERROR_CONTEXT=OFF
    -DARROW_WITH_RE2=OFF
    -DARROW_WITH_UTF8PROC=OFF
    -DARROW_CUDA=ON
    -DARROW_BUILD_UTILITIES=OFF
    -DARROW_FLIGHT=OFF
    -DARROW_DATASET=OFF
)

ExternalProject_Get_Property(arrow-external SOURCE_DIR)
ExternalProject_Get_Property(arrow-external BINARY_DIR)

set(ARROW_INCLUDE_DIR "${SOURCE_DIR}/cpp/src")
set(ARROW_INCLUDE_GEN_DIR "${BINARY_DIR}/src")
include_directories(${ARROW_INCLUDE_DIR} ${ARROW_INCLUDE_GEN_DIR})
link_directories("${BINARY_DIR}/release/")

# find_library(arrow_bundled_dependencies REQUIRED PATHS  NO_DEFAULT_PATH)
