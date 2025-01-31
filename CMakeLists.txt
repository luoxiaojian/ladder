cmake_minimum_required(VERSION 3.0)
project(Ladder)

set(CMAKE_CXX_STANDARD 17)

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel." FORCE)
endif ()

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party)

# find glog---------------------------------------------------------------------
include("cmake/FindGlog.cmake")
if (NOT GLOG_FOUND)
  message(FATAL_ERROR "glog not found, please install the glog library")
else ()
  include_directories(SYSTEM ${GLOG_INCLUDE_DIRS})
endif ()

# find Threads------------------------------------------------------------------
set(CMAKE_THREAD_PREFER_PTHREAD ON)
find_package(Threads REQUIRED)

find_package(MPI REQUIRED)
include_directories(SYSTEM ${MPI_CXX_INCLUDE_PATH})

set(LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR}/libs)

file(GLOB_RECURSE SOURCES "src/*.cc")
add_library(ladder SHARED ${SOURCES})
target_link_libraries(ladder ${GLOG_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT} ${MPI_CXX_LIBRARIES})
if (UNIX AND NOT APPLE)
    target_link_libraries(ladder dl)
endif()

add_executable(query_executor bin/query_executor.cc)
target_link_libraries(query_executor ladder ${GLOG_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT} ${MPI_CXX_LIBRARIES})

file(GLOB LIB_SOURCES "libs/*.cc")
foreach (SOURCE IN LISTS LIB_SOURCES)
    get_filename_component(LIB_NAME ${SOURCE} NAME_WE)
    add_library(${LIB_NAME} SHARED ${SOURCE})
    target_link_libraries(${LIB_NAME} ladder ${GLOG_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT} ${MPI_CXX_LIBRARIES})
    set_target_properties(${LIB_NAME} PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${LIBRARY_OUTPUT_PATH})
endforeach ()

file(GLOB TEST_SOURCES "tests/*.cc")
foreach (SOURCE IN LISTS TEST_SOURCES)
    get_filename_component(TEST_NAME ${SOURCE} NAME_WE)
    add_executable(${TEST_NAME} ${SOURCE})
    target_link_libraries(${TEST_NAME} ladder ${GLOG_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT} ${MPI_CXX_LIBRARIES})
endforeach ()
