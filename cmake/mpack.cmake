include(FetchContent)
FetchContent_Declare(
    mpack
    URL https://github.com/ludocode/mpack/releases/download/v1.1/mpack-amalgamation-1.1.tar.gz
)
FetchContent_MakeAvailable(mpack)
include_directories(${mpack_SOURCE_DIR}/src/mpack)
add_library(mpack ${mpack_SOURCE_DIR}/src/mpack/mpack.c)