cmake_minimum_required(VERSION 3.0)
project(Ladder)

set(CMAKE_CXX_STANDARD 17)

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel." FORCE)
endif ()

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party)

add_executable(query_executor bin/query_executor.cc)

set(LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR}/libs)

file(GLOB_RECURSE SOURCES "src/*.cc")
add_library(ladder SHARED ${SOURCES})

file(GLOB LIB_SOURCES "libs/*.cc")
foreach (SOURCE IN LISTS LIB_SOURCES)
    get_filename_component(LIB_NAME ${SOURCE} NAME_WE)
    add_library(${LIB_NAME} SHARED ${SOURCE})
    target_link_libraries(${LIB_NAME} ladder)
    set_target_properties(${LIB_NAME} PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${LIBRARY_OUTPUT_PATH})
endforeach ()

file(GLOB TEST_SOURCES "tests/*.cc")
foreach (SOURCE IN LISTS TEST_SOURCES)
    get_filename_component(TEST_NAME ${SOURCE} NAME_WE)
    add_executable(${TEST_NAME} ${SOURCE})
    target_link_libraries(${TEST_NAME} ladder)
endforeach ()
