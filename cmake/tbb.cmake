FetchContent_Declare(
    tbb
    GIT_REPOSITORY https://github.com/oneapi-src/oneTBB.git
    GIT_TAG v2021.7.0
)

FetchContent_GetProperties(tbb)

if(NOT tbb_POPULATED)
    set(BUILD_SHARED_LIBS OFF CACHE INTERNAL "")
    FetchContent_Populate(tbb)
    add_subdirectory(${tbb_SOURCE_DIR} ${tbb_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

include_directories(${tbb_SOURCE_DIR}/include)