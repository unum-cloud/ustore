include(FetchContent)
FetchContent_Declare(
    yyjson
    GIT_REPOSITORY https://github.com/ibireme/yyjson
    GIT_TAG 0.6.0
)
FetchContent_MakeAvailable(yyjson)
set_property(TARGET yyjson PROPERTY POSITION_INDEPENDENT_CODE ON)
include_directories(${yyjson_SOURCE_DIR}/include)
