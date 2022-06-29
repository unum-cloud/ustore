# ---------------------------------------------------------------------------
# cengine
# ---------------------------------------------------------------------------

include(ExternalProject)
find_package(Git REQUIRED)
find_program(MAKE_EXE NAMES gmake nmake make)

# Get turbopfor
ExternalProject_Add(
    turbopfor_src
    PREFIX "_deps/turbopfor"
    GIT_REPOSITORY "https://github.com/powturbo/TurboPFor-Integer-Compression.git"
    TIMEOUT 10
    CONFIGURE_COMMAND ""
    BUILD_IN_SOURCE TRUE
    BUILD_COMMAND make libic.a
    UPDATE_COMMAND ""
    INSTALL_COMMAND ""
)

# Prepare turbopfor
ExternalProject_Get_Property(turbopfor_src source_dir)
set(turbopfor_INCLUDE_DIR ${source_dir})
set(turbopfor_LIBRARY_PATH ${source_dir}/libic.a)
file(MAKE_DIRECTORY ${turbopfor_INCLUDE_DIR})
add_library(turbopfor STATIC IMPORTED)

set_property(TARGET turbopfor PROPERTY IMPORTED_LOCATION ${turbopfor_LIBRARY_PATH})
set_property(TARGET turbopfor APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${turbopfor_INCLUDE_DIR})

# Dependencies
add_dependencies(turbopfor turbopfor_src)