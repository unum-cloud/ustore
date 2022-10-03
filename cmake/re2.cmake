
FetchContent_Declare(
    re2
    GIT_REPOSITORY https://github.com/google/re2.git
    GIT_TAG 2022-06-01
    GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(re2)
set(re2_INCLUDE_DIR ${re2_SOURCE_DIR}/include)
include_directories(${re2_INCLUDE_DIR})
