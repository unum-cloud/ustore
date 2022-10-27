# Consistent-Sets are also in active development we may want to reuse the local version
if(EXISTS ${CMAKE_SOURCE_DIR}/../consistent_set)
    include_directories(${CMAKE_SOURCE_DIR}/../consistent_set/include/)
else()
    include(FetchContent)
    FetchContent_Declare(
        consistent_set
        GIT_REPOSITORY https://github.com/ashvardanian/consistent_set
        GIT_TAG main
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
    )
    FetchContent_MakeAvailable(consistent_set)
    include_directories(${consistent_set_SOURCE_DIR}/include/)
endif()
