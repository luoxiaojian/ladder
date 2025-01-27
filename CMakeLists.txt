cmake_minimum_required(VERSION 3.0)
project(Ladder)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/third_party/nlohmann_json/cmake")

find_package(nlohmann_json REQUIRED)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
include_directories(${NLOHMANN_JSON_INCLUDE_DIRS})

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
