include(FetchContent)
FetchContent_Declare(
    simdjson
    GIT_REPOSITORY https://github.com/simdjson/simdjson
    GIT_TAG v3.0.0
)

FetchContent_GetProperties(simdjson)
if(NOT simdjson_POPULATED)
    # Fetch the content using previously declared details
    set(SIMDJSON_ENABLE_THREADS ON CACHE INTERNAL "")
    FetchContent_Populate(simdjson)
    add_subdirectory(${simdjson_SOURCE_DIR} ${simdjson_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

FetchContent_MakeAvailable(simdjson)
include_directories(${simdjson_SOURCE_DIR}/include)
