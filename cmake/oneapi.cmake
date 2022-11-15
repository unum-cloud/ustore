find_package(TBB REQUIRED)
set(TBB_LIBRARIES tbb)
include_directories("/opt/intel/oneapi/tbb/latest/")
link_directories("/opt/intel/oneapi/tbb/latest/lib/intel64/gcc4.8/")
