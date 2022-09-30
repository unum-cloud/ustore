FetchContent_Declare(
    yyjson
    GIT_REPOSITORY https://github.com/ibireme/yyjson
)
FetchContent_MakeAvailable(yyjson)
include_directories(${yyjson_SOURCE_DIR}/include)
