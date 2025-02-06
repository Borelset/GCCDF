cmake_minimum_required(VERSION 3.10)
project(OdessStorage)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3")

aux_source_directory(Utility Utility)
aux_source_directory(RollHash RollHash)
aux_source_directory(EncodingMethod EncodingMethod)
aux_source_directory(MetadataManager MetadataManager)
aux_source_directory(Pipeline Pipeline)
aux_source_directory(FeatureMethod FeatureMethod)
aux_source_directory(edelta_lcg_lz4-v2 EdeltaRoot)
aux_source_directory(edelta_lcg_lz4-v2/xdelta XdeltaRoot)
aux_source_directory(edelta_lcg_lz4-v2/zdelta ZdeltaRoot)

include_directories(edelta_lcg_lz4-v2)
include_directories(edelta_lcg_lz4-v2/zdelta)
include_directories(edelta_lcg_lz4-v2/xdelta)
add_subdirectory(gflags-2.2.2)
# add_subdirectory(gtest)

#set(XDELTA_PATH ${CMAKE_SOURCE_DIR}/lib/)

link_libraries(gflags::gflags isal_crypto pthread crypto)

add_executable(Odess main.cpp ${Utility} ${RollHash} ${EncodingMethod} ${MetadataManager} ${Pipeline} ${FeatureMethod} ${EdeltaRoot})
# add_executable(lctree_test_gray gtest_tests/lctree_test_gray.cpp)

#target_link_libraries(Odess ${XDELTA_PATH}/libxdelta.so)