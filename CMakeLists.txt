# Specify the minimum version for CMake
cmake_minimum_required(VERSION 2.8)

set(CMAKE_LEGACY_CYGWIN_WIN32 0) # Remove when CMake >= 2.8.4 is required

OPTION(WITH_SDL "Build with SDL support" OFF)
OPTION(WITH_GLFW "Build with GLFW support" ON)
OPTION(WITH_BOX2D "Build with Box2D support" ON)

# Project's name
project(boglfw)

# Set the output folder where your program will be created
set(CMAKE_BINARY_DIR ${CMAKE_SOURCE_DIR}/build)
set(LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR}/dist/lib)

# The following folder will be included
include_directories("${PROJECT_SOURCE_DIR}/include")
if (WIN32)
	link_directories("C:/mingw64/lib")
	include_directories("C:/mingw64/include")
else()
	link_directories("/usr/lib")
	include_directories("/usr/include")
endif()

file(GLOB_RECURSE sources	include/*.h)
file(GLOB_RECURSE sources 	src/*.cpp)
file(GLOB_RECURSE data		data/*.*)

list(REMOVE_ITEM sources ${CMAKE_SOURCE_DIR}/src/main.cpp)

add_library(boglfw ${sources} ${data})

set(API_DEFS "")
if (WITH_SDL)
	set(API_DEFS ${API_DEFS} -DWITH_SDL)
endif()
if (WITH_GLFW)
	set(API_DEFS ${API_DEFS} -DWITH_GLFW)
endif()
if (WITH_BOX2D)
	set(API_LIBS ${API_DEFS} -DWITH_BOX2D)
endif()

set_property(TARGET boglfw PROPERTY CXX_STANDARD 14)
target_compile_options(boglfw PUBLIC -Wall -Werror=return-type -DGLM_ENABLE_EXPERIMENTAL -DGLM_FORCE_RADIANS -std=c++14 ${API_DEFS})

# add this flag to enable profiling with perfMarkers
# target_compile_options(${PROJECT_NAME} PUBLIC -DENABLE_PERF_MARKERS)

if (WIN32 OR ${CMAKE_SYSTEM_NAME} STREQUAL "CYGWIN")
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D__WIN32__ -mthreads")
endif()

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUG")

INSTALL (
	DIRECTORY ${CMAKE_SOURCE_DIR}/include/
	DESTINATION ${CMAKE_BINARY_DIR}/dist/include/
	FILES_MATCHING PATTERN "*.h"
)
INSTALL (
	DIRECTORY ${CMAKE_SOURCE_DIR}/data/
	DESTINATION ${CMAKE_BINARY_DIR}/dist/data/
	FILES_MATCHING PATTERN "*.*"
)
INSTALL (
	FILES ${CMAKE_SOURCE_DIR}/src/main.cpp
		  ${CMAKE_SOURCE_DIR}/fw-version
		  ${CMAKE_SOURCE_DIR}/client.cmake.txt
	DESTINATION ${CMAKE_BINARY_DIR}/dist/
)
