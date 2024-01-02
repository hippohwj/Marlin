cmake_minimum_required(VERSION 3.10)
project(arboretum)
message(STATUS "Current project dir: ${PROJECT_SOURCE_DIR}")


# add_compile_definitions(_GLIBCXX_USE_CXX11_ABI=0)

###############################################################################
# specify the C++ standard ####################################################
###############################################################################
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wno-comment -Wno-format-zero-length \
-pthread -g -lrt -L. -L./src/libs -L/usr/local/lib")
# -stdc++17

###############################################################################
# Set a default build type if none was specified ##############################
###############################################################################
set(default_build_type "Release")
if(EXISTS ${PROJECT_SOURCE_DIR}/.git)
    set(default_build_type "Debug")
endif()
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "Setting build type to '${default_build_type}' as none was specified.")
    set(CMAKE_BUILD_TYPE "${default_build_type}" CACHE STRING "Choose the type of build." FORCE)
    # Set the possible values of build type for find_cmake-gui
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "RelWithDebInfo")
endif()
set(CMAKE_CXX_FLAGS_DEBUG          "-O0 -g")
set(CMAKE_CXX_FLAGS_RELEASE        "-Ofast -DNDEBUG")
# release with debug info
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-Ofast -DNDEBUG -g")
message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

###############################################################################
## get dependencies from git ##################################################
###############################################################################
include(FetchContent)
FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
        #https://github.com/google/googletest/archive/refs/tags/release-1.12.1.zip
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# Cmake find modules
#list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")
find_package(gRPC CONFIG REQUIRED)
find_package(Protobuf REQUIRED)
find_package(PkgConfig REQUIRED)
find_package(azure-storage-blobs-cpp CONFIG REQUIRED)
find_package(cpprestsdk CONFIG REQUIRED)
find_library(CPPREST_LIBRARY cpprest)
find_library(BOOST_LOG_LIBRARY boost_log)
find_library(BOOST_THREAD_LIBRARY boost_thread)
find_library(UUID_LIBRARY uuid)
find_path(WASTORAGE_INCLUDE_DIR was/blob.h)
include_directories(${WASTORAGE_INCLUDE_DIR}) #/mydata/vcpkg/installed/x64-linux/include
message(STATUS "wa storage path: ${WASTORAGE_INCLUDE_DIR}")
find_library(WASTORAGE_LIBRARY azurestorage)

pkg_check_modules (JEMALLOC jemalloc)
pkg_search_module(JEMALLOC REQUIRED jemalloc)
include_directories(${JEMALLOC_INCLUDE_DIRS})

message(STATUS "CMAKE_CURRENT_SOURCE_DIR: ${CMAKE_CURRENT_SOURCE_DIR}")
add_subdirectory(include/cpp_redis)
target_include_directories(cpp_redis PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/cpp_redis/tacopie/includes>
        $<INSTALL_INTERFACE:cpp_redis/tacopie>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/cpp_redis/includes>
        $<INSTALL_INTERFACE:cpp_redis>)

##############################################################################
#Set proto file     ##########################################################
##############################################################################
set(PROTO_SRC_DIR ${PROJECT_SOURCE_DIR}/src/proto)
set(PROTO_SRC ${PROTO_SRC_DIR}/sundial.proto)
set(sundial_proto_cc "${CMAKE_CURRENT_BINARY_DIR}/sundial.pb.cc")
set(sundial_proto_hdr "${CMAKE_CURRENT_BINARY_DIR}/sundial.pb.h")
set(sundial_grpc_cc "${CMAKE_CURRENT_BINARY_DIR}/sundial.grpc.pb.cc")
set(sundial_grpc_hdr "${CMAKE_CURRENT_BINARY_DIR}/sundial.grpc.pb.h")
execute_process(COMMAND which grpc_cpp_plugin OUTPUT_VARIABLE GRPC_CPP_PLUGIN OUTPUT_STRIP_TRAILING_WHITESPACE)
add_custom_command(
       OUTPUT "${sundial_grpc_cc}" "${sundial_grpc_hdr}" "${sundial_proto_cc}" "${sundial_proto_hdr}"
       COMMAND protobuf::protoc
       ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
       --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
       -I "${PROTO_SRC_DIR}"
       --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
       "${PROTO_SRC}"
       DEPENDS "${PROTO_SRC}")
include_directories(${CMAKE_CURRENT_BINARY_DIR})
add_library(sundial_grpc_proto ${sundial_proto_cc} ${sundial_proto_hdr} ${sundial_grpc_cc} ${sundial_grpc_hdr})
target_link_libraries(sundial_grpc_proto
       gRPC::grpc++
       gRPC::grpc++_reflection
       protobuf::libprotobuf)

###############################################################################
# Set configure file ##########################################################
###############################################################################
file(GLOB_RECURSE ALL_CONFIG_FILES ${PROJECT_SOURCE_DIR}/configs/*)
foreach (FILENAME ${ALL_CONFIG_FILES})
    get_filename_component(CONFIGF ${FILENAME} NAME)
    configure_file(${FILENAME} configs/${CONFIGF} COPYONLY)
    message(STATUS "Recoginized config file: ${CONFIGF}")
endforeach(FILENAME ${ALL_CONFIG_FILES})

###############################################################################
# Add header files ############################################################
###############################################################################
set(COMMON_HEADER_DIRS
        benchmark
        src
        src/access
        src/common
        src/db
        src/local
        src/remote
        src/transport
        src/txn
        unit_tests)
include_directories(${COMMON_HEADER_DIRS})

###############################################################################
# Add source files ############################################################
###############################################################################
# add common src files
file(GLOB_RECURSE COMMON_CXX_SOURCE_FILES
        ${PROJECT_SOURCE_DIR}/src/common/*.cpp
        ${PROJECT_SOURCE_DIR}/src/db/*.cpp
        ${PROJECT_SOURCE_DIR}/src/transport/*.cpp
        ${PROJECT_SOURCE_DIR}/src/local/*.cpp
        ${PROJECT_SOURCE_DIR}/src/remote/*.cpp)

file(GLOB_RECURSE COMMON_CXX_HDR_FILES
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_SOURCE_DIR}/src/common
        ${PROJECT_SOURCE_DIR}/src/db
        ${PROJECT_SOURCE_DIR}/src/transport
        ${PROJECT_SOURCE_DIR}/src/local
        ${PROJECT_SOURCE_DIR}/src/remote)

# add individual src files for each benchmark test
file(GLOB_RECURSE YCSB_CXX_SOURCE_FILES
        ${PROJECT_SOURCE_DIR}/benchmark/common/*.cpp
        ${PROJECT_SOURCE_DIR}/benchmark/ycsb/*.cpp)
file(GLOB_RECURSE YCSB_CXX_HDR_FILES
        ${PROJECT_SOURCE_DIR}/benchmark
        ${PROJECT_SOURCE_DIR}/benchmark/common
        ${PROJECT_SOURCE_DIR}/benchmark/ycsb)
file(GLOB_RECURSE YCSB_MAIN_FILE benchmark/ycsb.cpp)

# add unit tests
file(GLOB_RECURSE TEST_CXX_SOURCE_FILES
        ${PROJECT_SOURCE_DIR}/unit_tests/*.cpp)

# add compute service 
file(GLOB_RECURSE COMPUTE_CXX_SOURCE_FILES
${PROJECT_SOURCE_DIR}/src/service/*.cpp)

# add experiments
file(GLOB_RECURSE EXP_CXX_SOURCE_FILES
        ${PROJECT_SOURCE_DIR}/experiments/*.cpp)

###############################################################################
# Add executable ##############################################################
###############################################################################
message(STATUS "Generating benchmark tests: ")
# ycsb
set(EXECUTABLE bench_ycsb)
message(STATUS "[benchmark test] executable: ${EXECUTABLE}")
add_executable(${EXECUTABLE} ${YCSB_MAIN_FILE} ${YCSB_CXX_SOURCE_FILES}
        ${COMMON_CXX_SOURCE_FILES} ${COMMON_CXX_HDR_FILES})
target_include_directories(${EXECUTABLE} PRIVATE ${COMMON_HEADER_DIRS}
        ${YCSB_CXX_HDR_FILES})
target_link_libraries(${EXECUTABLE} sundial_grpc_proto gRPC::grpc++ gRPC::grpc++_reflection protobuf::libprotobuf)
target_link_libraries(${EXECUTABLE} ${JEMALLOC_LIBRARIES})
target_link_libraries(${EXECUTABLE} cpp_redis)
target_link_libraries(${EXECUTABLE} ${WASTORAGE_LIBRARY}
${CPPREST_LIBRARY}
${BOOST_LOG_LIBRARY}
${BOOST_THREAD_LIBRARY}
${UUID_LIBRARY}
 Azure::azure-storage-blobs)

message(STATUS "Generating unit tests: ")
include(GoogleTest)
foreach (FILENAME ${TEST_CXX_SOURCE_FILES})
    get_filename_component(EXECUTABLE ${FILENAME} NAME_WE)
    message(STATUS "[unit test] executable: ${EXECUTABLE}")
    add_executable(${EXECUTABLE} ${FILENAME} ${COMMON_CXX_SOURCE_FILES} ${COMMON_CXX_HDR_FILES})
    target_include_directories(${EXECUTABLE} PRIVATE ${COMMON_HEADER_DIRS})
    target_link_libraries(${EXECUTABLE} gtest_main)
    target_link_libraries(${EXECUTABLE} sundial_grpc_proto gRPC::grpc++ gRPC::grpc++_reflection protobuf::libprotobuf)
    target_link_libraries(${EXECUTABLE} ${JEMALLOC_LIBRARIES})
    target_link_libraries(${EXECUTABLE} cpp_redis)
    target_link_libraries(${EXECUTABLE} ${WASTORAGE_LIBRARY}           
    ${CPPREST_LIBRARY}
    ${BOOST_LOG_LIBRARY}
    ${BOOST_THREAD_LIBRARY}
    ${UUID_LIBRARY}
    Azure::azure-storage-blobs)
    target_link_libraries(${EXECUTABLE} cpprestsdk::cpprest cpprestsdk::cpprestsdk_zlib_internal cpprestsdk::cpprestsdk_boost_internal cpprestsdk::cpprestsdk_openssl_internal)
endforeach(FILENAME ${TEST_CXX_SOURCE_FILES})

message(STATUS "Generating Compute Service: ")
foreach (FILENAME ${COMPUTE_CXX_SOURCE_FILES})
    get_filename_component(EXECUTABLE ${FILENAME} NAME_WE)
    message(STATUS "[service] executable: ${EXECUTABLE}")
    add_executable(${EXECUTABLE} ${FILENAME} ${YCSB_CXX_SOURCE_FILES}
            ${COMMON_CXX_SOURCE_FILES} ${COMMON_CXX_HDR_FILES})
    target_include_directories(${EXECUTABLE} PRIVATE
            ${COMMON_HEADER_DIRS} ${YCSB_CXX_HDR_FILES})
    target_link_libraries(${EXECUTABLE} sundial_grpc_proto gRPC::grpc++ gRPC::grpc++_reflection protobuf::libprotobuf)
    target_link_libraries(${EXECUTABLE} ${JEMALLOC_LIBRARIES})
    target_link_libraries(${EXECUTABLE} cpp_redis)
    target_link_libraries(${EXECUTABLE} ${WASTORAGE_LIBRARY} 
    ${CPPREST_LIBRARY}
    ${BOOST_LOG_LIBRARY}
    ${BOOST_THREAD_LIBRARY}
    ${UUID_LIBRARY}
    Azure::azure-storage-blobs)
endforeach(FILENAME ${COMPUTE_CXX_SOURCE_FILES})


message(STATUS "Generating experiments: ")
foreach (FILENAME ${EXP_CXX_SOURCE_FILES})
    get_filename_component(EXECUTABLE ${FILENAME} NAME_WE)
    message(STATUS "[experiment] executable: ${EXECUTABLE}")
    add_executable(${EXECUTABLE} ${FILENAME} ${YCSB_CXX_SOURCE_FILES}
            ${COMMON_CXX_SOURCE_FILES} ${COMMON_CXX_HDR_FILES})
    target_include_directories(${EXECUTABLE} PRIVATE
            ${COMMON_HEADER_DIRS} ${YCSB_CXX_HDR_FILES})
    target_link_libraries(${EXECUTABLE} sundial_grpc_proto gRPC::grpc++ gRPC::grpc++_reflection protobuf::libprotobuf)
    target_link_libraries(${EXECUTABLE} ${JEMALLOC_LIBRARIES})
    target_link_libraries(${EXECUTABLE} cpp_redis)
    target_link_libraries(${EXECUTABLE} ${WASTORAGE_LIBRARY} 
    ${CPPREST_LIBRARY}
    ${BOOST_LOG_LIBRARY}
    ${BOOST_THREAD_LIBRARY}
    ${UUID_LIBRARY}
    Azure::azure-storage-blobs)
endforeach(FILENAME ${EXP_CXX_SOURCE_FILES})
