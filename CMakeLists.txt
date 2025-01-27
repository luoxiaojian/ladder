cmake_minimum_required(VERSION 3.0)
project(Ladder)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

add_executable(query_executor bin/query_executor.cc)

set(LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR}/libs)

file(GLOB LIB_SOURCES "libs/*.cc")
foreach(SOURCE IN LISTS LIB_SOURCES)
    get_filename_component(LIB_NAME ${SOURCE} NAME_WE)
    add_library(${LIB_NAME} SHARED ${SOURCE})
    set_target_properties(${LIB_NAME} PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${LIBRARY_OUTPUT_PATH})
endforeach()
