include(FetchContent)
FetchContent_Declare(
    mpack
    GIT_REPOSITORY https://github.com/unum-cloud/mpack.git
    GIT_TAG v1.1
)
FetchContent_MakeAvailable(mpack)
include_directories(${mpack_SOURCE_DIR}/src/mpack)